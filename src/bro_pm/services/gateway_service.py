from __future__ import annotations

from datetime import datetime, timezone
import json
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


class InboundReferenceNotFoundError(RuntimeError):
    pass


class InboundReferenceConflictError(RuntimeError):
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

    def ingest_inbound_event(
        self,
        *,
        platform: str,
        chat_id: str | None,
        thread_id: str | None,
        actor: str,
        actor_role: str | None,
        project_id: str | None,
        text: str,
        normalized_intent: str | None,
        due_action_id: str | None,
        pending_audit_id: str | None,
        metadata: dict | None = None,
    ) -> models.ConversationEvent:
        disposition = "ignore"
        reason = "no allowed reaction for inbound event"

        project = self.db_session.get(models.Project, project_id) if project_id else None

        due_action = None
        if due_action_id:
            due_action = self.db_session.get(models.DueAction, due_action_id)
            if due_action is None:
                raise InboundReferenceNotFoundError(f"due action {due_action_id} not found")
            self._assert_due_action_matches_context(
                due_action=due_action,
                project_id=project_id,
                platform=platform,
                actor=actor,
            )
            if normalized_intent in {"ack", "acknowledge", "confirm", "confirmed"}:
                due_action.status = "acked"
                due_action.acked_at = _utc_now()
                disposition = "ack_due_action"
                reason = "due action acknowledgement recorded"

        pending_audit = None
        if pending_audit_id and disposition == "ignore":
            pending_audit = self.db_session.get(models.AuditEvent, pending_audit_id)
            if pending_audit is None:
                raise InboundReferenceNotFoundError(f"pending audit {pending_audit_id} not found")
            self._assert_pending_audit_matches_context(
                pending_audit=pending_audit,
                project_id=project_id,
            )
            if pending_audit.result == "awaiting_approval":
                approval_status = self._approval_status_for_intent(normalized_intent)
                if approval_status is not None and project is not None and self._actor_has_project_reply_privilege(
                    project=project,
                    actor=actor,
                ):
                    pending_audit.result = approval_status
                    payload = self._audit_payload(pending_audit.payload)
                    payload["approval"] = {
                        "status": approval_status,
                        "actor": actor.strip(),
                        "actor_role": actor_role.strip() if isinstance(actor_role, str) and actor_role.strip() else None,
                        "text": text.strip(),
                    }
                    pending_audit.payload = json.dumps(payload, ensure_ascii=False)
                    if pending_audit.action_execution is not None:
                        pending_audit.action_execution.status = approval_status
                    disposition = "approval_reply_recorded"
                    reason = "approval reply recorded for pending audit event"

        if disposition == "ignore":
            if project and self._actor_has_project_reply_privilege(project=project, actor=actor):
                disposition = "allow_reply"
                reason = "trusted project actor may receive a reply"

        event = models.ConversationEvent(
            project_id=project_id,
            due_action_id=due_action.id if due_action is not None else due_action_id,
            pending_audit_id=pending_audit.id if pending_audit is not None else pending_audit_id,
            platform=platform.strip().lower(),
            chat_id=chat_id.strip() if isinstance(chat_id, str) and chat_id.strip() else None,
            thread_id=thread_id.strip() if isinstance(thread_id, str) and thread_id.strip() else None,
            actor=actor.strip(),
            actor_role=actor_role.strip().lower() if isinstance(actor_role, str) and actor_role.strip() else None,
            text=text.strip(),
            normalized_intent=normalized_intent.strip().lower() if isinstance(normalized_intent, str) and normalized_intent.strip() else None,
            metadata_json=metadata or {},
            disposition=disposition,
            decision_reason=reason,
        )
        self.db_session.add(event)
        self.db_session.commit()
        self.db_session.refresh(event)
        return event

    def _actor_has_project_reply_privilege(
        self,
        *,
        project: models.Project,
        actor: str,
    ) -> bool:
        normalized_actor = actor.strip().lower()
        metadata = project.metadata_json if isinstance(project.metadata_json, dict) else {}
        onboarding = metadata.get("onboarding") if isinstance(metadata.get("onboarding"), dict) else {}
        boss = onboarding.get("boss")
        admin = onboarding.get("admin")
        if isinstance(boss, str) and boss.strip().lower() == normalized_actor:
            return True
        if isinstance(admin, str) and admin.strip().lower() == normalized_actor:
            return True
        team = onboarding.get("team")
        if isinstance(team, list):
            for team_entry in team:
                if not isinstance(team_entry, dict):
                    continue
                owner = team_entry.get("owner")
                if isinstance(owner, str) and owner.strip().lower() == normalized_actor:
                    return True

        membership = (
            self.db_session.query(models.ProjectMembership)
            .filter(
                models.ProjectMembership.project_id == project.id,
                models.ProjectMembership.actor == actor.strip(),
            )
            .one_or_none()
        )
        if membership is None:
            return False
        return membership.role.strip().lower() in {"owner", "admin"}

    def _assert_due_action_matches_context(
        self,
        *,
        due_action: models.DueAction,
        project_id: str | None,
        platform: str,
        actor: str,
    ) -> None:
        if due_action.project_id != project_id:
            raise InboundReferenceConflictError(
                f"due action {due_action.id} does not belong to project {project_id}"
            )
        if due_action.channel.strip().lower() != platform.strip().lower():
            raise InboundReferenceConflictError(
                f"due action {due_action.id} does not match inbound event context"
            )
        if due_action.recipient.strip().lower() != actor.strip().lower():
            raise InboundReferenceConflictError(
                f"due action {due_action.id} does not match inbound event context"
            )

    def _assert_pending_audit_matches_context(
        self,
        *,
        pending_audit: models.AuditEvent,
        project_id: str | None,
    ) -> None:
        if pending_audit.project_id != project_id:
            raise InboundReferenceConflictError(
                f"pending audit {pending_audit.id} does not belong to project {project_id}"
            )

    def _approval_status_for_intent(self, normalized_intent: str | None) -> str | None:
        if not normalized_intent:
            return None
        normalized = normalized_intent.strip().lower()
        if normalized in {"approve", "approved", "confirm"}:
            return "approved"
        if normalized in {"reject", "rejected", "deny", "denied"}:
            return "rejected"
        return None

    def _audit_payload(self, raw_payload: str | None) -> dict:
        try:
            payload = json.loads(raw_payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        if isinstance(payload, dict):
            return payload
        return {}
