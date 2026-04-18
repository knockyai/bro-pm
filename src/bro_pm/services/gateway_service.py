from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import models


PENDING_STATUSES = frozenset({"pending"})
CLAIMED_STATUS = "claimed"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(value: datetime | None) -> datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class DueActionConflictError(RuntimeError):
    pass


class DueActionNotFoundError(RuntimeError):
    pass


class GatewayService:
    def __init__(self, *, db_session: Session) -> None:
        self.db_session = db_session

    def enqueue_due_action(
        self,
        *,
        project_id: str | None,
        channel: str,
        recipient: str,
        kind: str,
        payload: dict,
        due_at: datetime | None = None,
        actor: str | None = None,
        idempotency_key: str | None = None,
    ) -> models.DueAction:
        existing = None
        if idempotency_key:
            existing = (
                self.db_session.query(models.DueAction)
                .filter(models.DueAction.idempotency_key == idempotency_key)
                .one_or_none()
            )
        if existing is not None:
            return existing

        due_action = models.DueAction(
            project_id=project_id,
            channel=channel.strip().lower(),
            recipient=recipient.strip(),
            kind=kind.strip().lower(),
            payload_json=payload,
            due_at=_normalize_timestamp(due_at),
            status="pending",
            actor=actor.strip() if isinstance(actor, str) and actor.strip() else None,
            idempotency_key=idempotency_key,
        )
        self.db_session.add(due_action)
        try:
            self.db_session.commit()
        except IntegrityError as exc:
            self.db_session.rollback()
            if idempotency_key:
                existing = (
                    self.db_session.query(models.DueAction)
                    .filter(models.DueAction.idempotency_key == idempotency_key)
                    .one_or_none()
                )
                if existing is not None:
                    return existing
            raise DueActionConflictError("failed to enqueue due action") from exc

        self.db_session.refresh(due_action)
        return due_action

    def claim_due_actions(self, *, gateway: str, limit: int, now: datetime | None = None) -> list[models.DueAction]:
        claimed_actions: list[models.DueAction] = []
        ready_at = _normalize_timestamp(now)
        due_actions = (
            self.db_session.query(models.DueAction)
            .filter(
                models.DueAction.status.in_(tuple(PENDING_STATUSES)),
                models.DueAction.due_at <= ready_at,
            )
            .order_by(models.DueAction.due_at.asc(), models.DueAction.created_at.asc(), models.DueAction.id.asc())
            .limit(limit)
            .all()
        )

        normalized_gateway = gateway.strip()
        for due_action in due_actions:
            due_action.status = CLAIMED_STATUS
            due_action.claimed_by = normalized_gateway
            due_action.claimed_at = ready_at
            due_action.claim_token = str(uuid4())
            claimed_actions.append(due_action)

        self.db_session.commit()
        for due_action in claimed_actions:
            self.db_session.refresh(due_action)
        return claimed_actions

    def record_delivery(
        self,
        *,
        due_action_id: str,
        claim_token: str,
        status: str,
        external_delivery_id: str | None = None,
        error_detail: str | None = None,
        now: datetime | None = None,
    ) -> models.DueAction:
        due_action = self.db_session.get(models.DueAction, due_action_id)
        if due_action is None:
            raise DueActionNotFoundError(f"due action {due_action_id} not found")
        if due_action.claim_token != claim_token:
            raise DueActionConflictError("claim token does not match due action")

        recorded_at = _normalize_timestamp(now)
        due_action.delivery_attempted_at = recorded_at
        due_action.external_delivery_id = external_delivery_id

        if status == "delivered":
            due_action.status = "delivered"
            due_action.delivered_at = recorded_at
            due_action.last_error = None
        elif status == "failed":
            due_action.status = "failed"
            due_action.failed_at = recorded_at
            due_action.last_error = error_detail
        elif status == "acked":
            due_action.status = "acked"
            due_action.acked_at = recorded_at
            due_action.last_error = None
        else:
            raise DueActionConflictError(f"unsupported delivery status: {status}")

        self.db_session.commit()
        self.db_session.refresh(due_action)
        return due_action
