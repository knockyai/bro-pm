from __future__ import annotations

import json
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, update
from sqlalchemy.orm import Session

from .. import models
from ..integrations import INTEGRATIONS, IntegrationError


class ExecutionOutboxConflictError(RuntimeError):
    pass


class ExecutionOutboxNotFoundError(RuntimeError):
    pass


class ExecutionOutboxService:
    CLAIM_STALE_AFTER = timedelta(minutes=5)

    def __init__(self, *, db_session: Session) -> None:
        self.db_session = db_session

    def _new_isolated_session(self) -> Session:
        return Session(bind=self.db_session.get_bind(), future=True)

    def _bind_item_to_caller(self, item_id: str) -> models.ExecutionOutbox:
        with self.db_session.no_autoflush:
            item = self.db_session.get(models.ExecutionOutbox, item_id)
        if item is None:
            raise ExecutionOutboxNotFoundError(f"execution outbox {item_id} not found")
        if not self.db_session.is_modified(item):
            self.db_session.refresh(item)
        return item

    def _bind_items_to_caller(self, item_ids: list[str]) -> list[models.ExecutionOutbox]:
        return [self._bind_item_to_caller(item_id) for item_id in item_ids]

    def _claim_item_if_match(self, *, item_id: str, worker_id: str, claim_time: datetime, conditions: list) -> models.ExecutionOutbox | None:
        claim_token = str(uuid4())
        claim_update = self.db_session.execute(
            update(models.ExecutionOutbox)
            .where(models.ExecutionOutbox.id == item_id, *conditions)
            .values(
                status="claimed",
                claimed_by=worker_id,
                claimed_at=claim_time,
                claim_token=claim_token,
                last_error=None,
                failed_at=None,
                attempt_count=func.coalesce(models.ExecutionOutbox.attempt_count, 0) + 1,
            )
        )
        if claim_update.rowcount != 1:
            self.db_session.rollback()
            return None
        self.db_session.commit()
        item = self.db_session.get(models.ExecutionOutbox, item_id)
        if item is None:
            raise ExecutionOutboxNotFoundError(f"execution outbox {item_id} not found")
        self.db_session.refresh(item)
        return item

    def claim_available(
        self,
        *,
        worker_id: str,
        limit: int,
        now: datetime | None = None,
    ) -> list[models.ExecutionOutbox]:
        isolated_session = self._new_isolated_session()
        try:
            isolated_worker = type(self)(db_session=isolated_session)
            claimed_ids = isolated_worker._claim_available_impl(worker_id=worker_id, limit=limit, now=now)
        finally:
            isolated_session.close()
        return self._bind_items_to_caller(claimed_ids)

    def _claim_available_impl(
        self,
        *,
        worker_id: str,
        limit: int,
        now: datetime | None = None,
    ) -> list[str]:
        claimed_ids: list[str] = []
        attempted_ids: set[str] = set()
        claim_time = now or datetime.utcnow()
        stale_before = claim_time - self.CLAIM_STALE_AFTER

        while len(claimed_ids) < limit:
            query = self.db_session.query(models.ExecutionOutbox).filter(
                (
                    (models.ExecutionOutbox.status == "queued")
                    & (models.ExecutionOutbox.available_at <= claim_time)
                )
                | (
                    (models.ExecutionOutbox.status == "claimed")
                    & (models.ExecutionOutbox.claimed_at <= stale_before)
                )
            )
            if attempted_ids:
                query = query.filter(~models.ExecutionOutbox.id.in_(attempted_ids))
            items = (
                query.order_by(
                    models.ExecutionOutbox.available_at.asc(),
                    models.ExecutionOutbox.created_at.asc(),
                    models.ExecutionOutbox.id.asc(),
                )
                .limit(limit - len(claimed_ids))
                .all()
            )
            if not items:
                break

            for item in items:
                attempted_ids.add(item.id)
                if item.status == "queued":
                    conditions = [
                        models.ExecutionOutbox.status == "queued",
                        models.ExecutionOutbox.available_at <= claim_time,
                    ]
                elif item.status == "claimed" and item.claimed_at is not None and item.claimed_at <= stale_before:
                    conditions = [
                        models.ExecutionOutbox.status == "claimed",
                        models.ExecutionOutbox.claimed_at <= stale_before,
                    ]
                else:
                    continue

                claimed_item = self._claim_item_if_match(
                    item_id=item.id,
                    worker_id=worker_id,
                    claim_time=claim_time,
                    conditions=conditions,
                )
                if claimed_item is None:
                    continue
                claimed_ids.append(claimed_item.id)
                if len(claimed_ids) >= limit:
                    break

        return claimed_ids

    def process_for_audit_event(
        self,
        *,
        audit_event_id: str,
        worker_id: str,
        now: datetime | None = None,
    ) -> models.ExecutionOutbox:
        isolated_session = self._new_isolated_session()
        try:
            isolated_worker = type(self)(db_session=isolated_session)
            item_id = isolated_worker._process_for_audit_event_impl(
                audit_event_id=audit_event_id,
                worker_id=worker_id,
                now=now,
            )
        finally:
            isolated_session.close()
        return self._bind_item_to_caller(item_id)

    def _process_for_audit_event_impl(
        self,
        *,
        audit_event_id: str,
        worker_id: str,
        now: datetime | None = None,
    ) -> str:
        item = (
            self.db_session.query(models.ExecutionOutbox)
            .filter_by(audit_event_id=audit_event_id)
            .one_or_none()
        )
        if item is None:
            raise ExecutionOutboxNotFoundError(f"execution outbox missing for audit event {audit_event_id}")

        claim_time = now or datetime.utcnow()
        claimed_here = False
        if item.status != "completed":
            stale_claim = (
                item.status == "claimed"
                and item.claimed_at is not None
                and item.claimed_at <= claim_time - self.CLAIM_STALE_AFTER
            )
            claimed_item: models.ExecutionOutbox | None = None
            if item.status == "queued":
                claimed_item = self._claim_item_if_match(
                    item_id=item.id,
                    worker_id=worker_id,
                    claim_time=claim_time,
                    conditions=[
                        models.ExecutionOutbox.status == "queued",
                        models.ExecutionOutbox.available_at <= claim_time,
                    ],
                )
            elif stale_claim:
                claimed_item = self._claim_item_if_match(
                    item_id=item.id,
                    worker_id=worker_id,
                    claim_time=claim_time,
                    conditions=[
                        models.ExecutionOutbox.status == "claimed",
                        models.ExecutionOutbox.claimed_at <= claim_time - self.CLAIM_STALE_AFTER,
                    ],
                )
            if claimed_item is not None:
                item = claimed_item
                claimed_here = True
            else:
                item = self.db_session.get(models.ExecutionOutbox, item.id)
                if item is None:
                    raise ExecutionOutboxNotFoundError(f"execution outbox missing for audit event {audit_event_id}")

        if item.status == "completed":
            return item.id
        if not claimed_here:
            raise ExecutionOutboxConflictError("execution outbox item is already claimed by another worker")
        if item.claim_token is None:
            raise ExecutionOutboxConflictError("execution outbox item is not claimable")
        return self._process_claimed_impl(outbox_id=item.id, claim_token=item.claim_token, now=claim_time)

    def process_claimed(
        self,
        *,
        outbox_id: str,
        claim_token: str,
        now: datetime | None = None,
    ) -> models.ExecutionOutbox:
        isolated_session = self._new_isolated_session()
        try:
            isolated_worker = type(self)(db_session=isolated_session)
            item_id = isolated_worker._process_claimed_impl(
                outbox_id=outbox_id,
                claim_token=claim_token,
                now=now,
            )
        finally:
            isolated_session.close()
        return self._bind_item_to_caller(item_id)

    def _process_claimed_impl(
        self,
        *,
        outbox_id: str,
        claim_token: str,
        now: datetime | None = None,
    ) -> str:
        processed_at = now or datetime.utcnow()
        exclusive_claim_token = str(uuid4())
        claim_update = self.db_session.execute(
            update(models.ExecutionOutbox)
            .where(
                models.ExecutionOutbox.id == outbox_id,
                models.ExecutionOutbox.status == "claimed",
                models.ExecutionOutbox.claim_token == claim_token,
            )
            .values(
                claim_token=exclusive_claim_token,
                claimed_at=processed_at,
                last_error=None,
                failed_at=None,
            )
        )
        self.db_session.commit()

        item = self.db_session.get(models.ExecutionOutbox, outbox_id)
        if item is None:
            raise ExecutionOutboxNotFoundError(f"execution outbox {outbox_id} not found")
        if claim_update.rowcount != 1:
            if item.status == "completed":
                return item.id
            raise ExecutionOutboxConflictError("execution outbox claim token does not match")

        audit_event = self.db_session.get(models.AuditEvent, item.audit_event_id)
        if audit_event is None:
            raise ExecutionOutboxNotFoundError(f"audit event {item.audit_event_id} not found")
        execution = (
            self.db_session.query(models.ActionExecution)
            .filter_by(audit_event_id=audit_event.id)
            .one_or_none()
        )

        if self._reconcile_from_audit_truth(
            item=item,
            audit_event=audit_event,
            execution=execution,
            processed_at=processed_at,
        ):
            self.db_session.commit()
            self.db_session.refresh(item)
            return item.id

        if (
            audit_event.result == "pending_integration"
            and (item.attempt_count or 0) > 1
            and item.claimed_by is not None
            and item.claimed_at is not None
        ):
            self._mark_failed(
                item=item,
                audit_event=audit_event,
                execution=execution,
                detail="stale claimed execution requires manual reconciliation before retry",
                processed_at=processed_at,
            )
            self.db_session.commit()
            self.db_session.refresh(item)
            return item.id

        payload = item.payload_json if isinstance(item.payload_json, dict) else {}
        integration_payload = payload.get("integration_payload")
        if not isinstance(integration_payload, dict):
            integration_payload = self._integration_payload_from_reservation(payload)
        integration_payload = self._integration_payload_for_item(item=item, payload=integration_payload)

        integration = INTEGRATIONS[item.integration_name]
        try:
            result = integration.execute(
                action=item.integration_action,
                payload=integration_payload,
            )
            if result.ok:
                detail = result.detail or f"{item.integration_name} executed: {item.integration_action}"
                self._mark_completed(
                    item=item,
                    audit_event=audit_event,
                    execution=execution,
                    detail=detail,
                    processed_at=processed_at,
                )
            else:
                detail = result.detail or "integration execution reported failure"
                self._mark_failed(
                    item=item,
                    audit_event=audit_event,
                    execution=execution,
                    detail=detail,
                    processed_at=processed_at,
                )
            self.db_session.commit()
            self.db_session.refresh(item)
            return item.id
        except IntegrationError as exc:
            self._mark_failed(
                item=item,
                audit_event=audit_event,
                execution=execution,
                detail=str(exc),
                processed_at=processed_at,
            )
            self.db_session.commit()
            self.db_session.refresh(item)
            return item.id
        except Exception as exc:
            self._mark_failed(
                item=item,
                audit_event=audit_event,
                execution=execution,
                detail=str(exc) or type(exc).__name__,
                processed_at=processed_at,
            )
            self.db_session.commit()
            raise

    def _integration_payload_from_reservation(self, payload: dict) -> dict:
        proposal = payload.get("proposal")
        if not isinstance(proposal, dict):
            return {}
        proposal_payload = proposal.get("payload")
        if not isinstance(proposal_payload, dict):
            proposal_payload = {}
        return {
            **proposal_payload,
            "project_id": proposal.get("project_id"),
        }

    def _integration_payload_for_item(self, *, item: models.ExecutionOutbox, payload: dict) -> dict:
        execution_metadata = payload.get("bro_pm_execution")
        if not isinstance(execution_metadata, dict):
            execution_metadata = {}
        audit_event = self.db_session.get(models.AuditEvent, item.audit_event_id)
        idempotency_key = audit_event.idempotency_key if audit_event and audit_event.idempotency_key else item.audit_event_id
        return {
            **payload,
            "bro_pm_execution": {
                **execution_metadata,
                "audit_event_id": item.audit_event_id,
                "execution_outbox_id": item.id,
                "idempotency_key": idempotency_key,
                "attempt_count": item.attempt_count or 0,
            },
        }

    def _reconcile_from_audit_truth(
        self,
        *,
        item: models.ExecutionOutbox,
        audit_event: models.AuditEvent,
        execution: models.ActionExecution | None,
        processed_at: datetime,
    ) -> bool:
        if audit_event.result == "executed":
            item.status = "completed"
            item.completed_at = item.completed_at or processed_at
            item.last_error = None
            item.claim_token = None
            if execution is not None:
                execution.status = "verified"
                execution.executed_at = execution.executed_at or processed_at
                execution.verified_at = execution.verified_at or processed_at
            return True

        if audit_event.result in {"denied", "rejected"}:
            item.status = "failed"
            item.failed_at = item.failed_at or processed_at
            item.claim_token = None
            if execution is not None:
                execution.status = "denied"
            return True

        return False

    def _reservation_payload(self, audit_event: models.AuditEvent) -> dict:
        try:
            payload = json.loads(audit_event.payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return payload

    def _mark_completed(
        self,
        *,
        item: models.ExecutionOutbox,
        audit_event: models.AuditEvent,
        execution: models.ActionExecution | None,
        detail: str,
        processed_at: datetime,
    ) -> None:
        payload = self._reservation_payload(audit_event)
        integration_payload = payload.get("integration")
        if not isinstance(integration_payload, dict):
            integration_payload = {}
        payload["integration"] = {
            **integration_payload,
            "name": integration_payload.get("name") or item.integration_name,
            "action": integration_payload.get("action") or item.integration_action,
            "status": "executed",
            "detail": detail,
        }
        audit_event.payload = json.dumps(payload, ensure_ascii=False)
        audit_event.result = "executed"

        item.status = "completed"
        item.completed_at = processed_at
        item.last_error = None
        item.failed_at = None
        item.claim_token = None

        if execution is not None:
            execution.status = "verified"
            execution.executed_at = execution.executed_at or processed_at
            execution.verified_at = processed_at

    def _mark_failed(
        self,
        *,
        item: models.ExecutionOutbox,
        audit_event: models.AuditEvent,
        execution: models.ActionExecution | None,
        detail: str,
        processed_at: datetime,
    ) -> None:
        payload = self._reservation_payload(audit_event)
        integration_payload = payload.get("integration")
        if not isinstance(integration_payload, dict):
            integration_payload = {}
        payload["integration"] = {
            **integration_payload,
            "name": integration_payload.get("name") or item.integration_name,
            "action": integration_payload.get("action") or item.integration_action,
            "status": "rejected",
            "detail": detail,
        }
        audit_event.payload = json.dumps(payload, ensure_ascii=False)
        audit_event.result = "denied"

        item.status = "failed"
        item.failed_at = processed_at
        item.last_error = detail
        item.claim_token = None

        if execution is not None:
            execution.status = "denied"
