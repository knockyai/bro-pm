from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from .. import models
from ..adapters.hermes_runtime import HermesAdapter
from ..integrations import INTEGRATIONS, IntegrationError
from ..policy import PolicyDecision, PolicyEngine
from ..schemas import CommandProposal


@dataclass
class ProposalExecution:
    success: bool
    proposal: CommandProposal
    audit_id: str
    result: str
    detail: str


@dataclass
class RollbackExecution:
    success: bool
    proposal: CommandProposal
    audit_id: str
    rollback_record_id: str
    result: str
    detail: str


class UnsafeIsolatedSqlitePersistenceError(RuntimeError):
    """Raised when a side-session sqlite write would share the caller transaction."""


class CommandService:
    """Convert parsed commands into durable audit-ready operations."""

    PENDING_INTEGRATION_STALE_AFTER = timedelta(minutes=5)
    PENDING_INTEGRATION_WAIT_ATTEMPTS = 100
    PENDING_INTEGRATION_WAIT_DELAY_SECONDS = 0.05
    SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL = (
        "assisted create_task requires a clean caller transaction before durable reservation on sqlite"
    )
    BOARD_INTEGRATIONS = {"notion", "jira", "trello", "yandex_tracker"}

    def __init__(self, db_session: Session, hermes: HermesAdapter | None = None, policy: PolicyEngine | None = None):
        self.db = db_session
        self.hermes = hermes or HermesAdapter()
        self.policy = policy or PolicyEngine()

    def parse(self, *, actor: str, command: str, project_id: str | None) -> CommandProposal:
        proposal = self.hermes.propose(actor=actor, command_text=command)
        if not proposal.project_id and project_id:
            proposal.project_id = project_id
        return proposal

    def _resolve_project_board_integration_name(self, project_id: str | None) -> str:
        if not project_id:
            return "notion"
        with self.db.no_autoflush:
            project = self.db.get(models.Project, project_id)
        if not project:
            return "notion"
        metadata = project.metadata_json or {}
        onboarding = metadata.get("onboarding") or {}
        board_integration = onboarding.get("board_integration")
        if not isinstance(board_integration, str):
            return "notion"
        normalized = board_integration.strip().lower()
        if not normalized:
            return "notion"
        if normalized not in self.BOARD_INTEGRATIONS:
            return "notion"
        if normalized not in INTEGRATIONS:
            return "notion"
        return normalized

    def execute(
        self,
        *,
        actor: str,
        role: str,
        proposal: CommandProposal,
        actor_trusted: bool = True,
        idempotency_key: str | None = None,
        dry_run: bool = False,
        validate_integration: bool = False,
        execute_integration: bool = False,
    ) -> ProposalExecution:
        project_id = proposal.project_id
        replay_context = {
            "actor": actor,
            "role": role,
            "actor_trusted": actor_trusted,
            "proposal": proposal.model_dump(),
            "dry_run": dry_run,
            "validate_integration": validate_integration,
            "execute_integration": execute_integration,
        }
        integration_name = None
        if validate_integration or execute_integration:
            integration_name = self._resolve_project_board_integration_name(project_id)
            integration = INTEGRATIONS[integration_name]
        else:
            integration = None
        if idempotency_key:
            with self.db.no_autoflush:
                existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            if (
                existing
                and existing.result == "pending_integration"
                and not self._is_stale_pending_integration(existing)
            ):
                existing = self._wait_for_existing_idempotent_record(
                    idempotency_key,
                    wait_for_stable_result=True,
                )
            if existing:
                return self._replay_existing_execution(
                    existing=existing,
                    replay_context=replay_context,
                    proposal=proposal,
                )

        if proposal.action == "noop":
            decision = PolicyDecision(False, proposal.reason or "unrecognized command")
        elif proposal.action == "draft_boss_escalation":
            if not project_id:
                decision = PolicyDecision(False, "project context required for draft_boss_escalation")
            else:
                with self.db.no_autoflush:
                    project = self.db.get(models.Project, project_id)
                if not project:
                    decision = PolicyDecision(False, "project not found for draft_boss_escalation")
                elif not proposal.payload.get("escalation_message"):
                    decision = PolicyDecision(False, "escalation message required for draft_boss_escalation")
                else:
                    decision = self.policy.evaluate(
                        actor_role=role,
                        actor_trusted=actor_trusted,
                        action=proposal.action,
                        safe_paused=bool(project.safe_paused),
                    )
        else:
            safe_paused = False
            if project_id:
                with self.db.no_autoflush:
                    project = self.db.get(models.Project, project_id)
                if project:
                    safe_paused = bool(project.safe_paused)

            decision: PolicyDecision = self.policy.evaluate(
                actor_role=role,
                actor_trusted=actor_trusted,
                action=proposal.action,
                safe_paused=safe_paused,
            )

        success = decision.allowed
        response_result = "rejected"
        detail = decision.reason
        stored_result = "denied"
        reserved_integration_record: models.AuditEvent | None = None
        unexpected_execute_exc: Exception | None = None

        if decision.allowed:
            if validate_integration:
                if proposal.action != "create_task":
                    response_result = "rejected"
                    stored_result = "denied"
                    detail = "integration validation mode currently supports create_task only"
                    success = False
                else:
                    try:
                        integration.validate(
                            action="create_task",
                            payload={
                                **proposal.payload,
                                "project_id": project_id,
                            },
                        )
                        response_result = "validated"
                        stored_result = "validated"
                        detail = f"policy accepted; {integration_name} validated create_task without execution"
                        success = True
                    except IntegrationError as exc:
                        response_result = "rejected"
                        stored_result = "denied"
                        detail = str(exc)
                        success = False
            elif execute_integration:
                if proposal.action != "create_task":
                    response_result = "rejected"
                    stored_result = "denied"
                    detail = "execute_integration mode currently supports create_task only"
                    success = False
                elif not project_id:
                    response_result = "rejected"
                    stored_result = "denied"
                    detail = "project context required for assisted create_task"
                    success = False
                elif dry_run:
                    response_result = "simulated"
                    stored_result = "simulated"
                else:
                    if idempotency_key:
                        if self._caller_session_holds_sqlite_write_transaction():
                            response_result = "rejected"
                            stored_result = "denied"
                            detail = self.SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL
                            success = False
                        else:
                            reservation_payload = {
                                "actor": actor,
                                "auth": {
                                    "role": role,
                                    "actor_trusted": actor_trusted,
                                    "dry_run": dry_run,
                                    "validate_integration": validate_integration,
                                    "execute_integration": execute_integration,
                                },
                                "proposal": proposal.model_dump(),
                                "policy": decision.__dict__,
                                "integration": {
                                    "name": integration_name,
                                    "action": proposal.action,
                                    "status": "pending",
                                    "detail": f"{integration_name} integration execution pending",
                                },
                            }
                            reservation_id: str | None = None
                            reservation_session = Session(bind=self.db.get_bind(), future=True)
                            try:
                                reserved_integration_record = models.AuditEvent(
                                    project_id=project_id,
                                    actor=actor,
                                    action=proposal.action,
                                    target_type="proposal",
                                    target_id=proposal.project_id,
                                    payload=json.dumps(reservation_payload, ensure_ascii=False),
                                    result="pending_integration",
                                    idempotency_key=idempotency_key,
                                    created_at=datetime.utcnow(),
                                )
                                reservation_session.add(reserved_integration_record)
                                reservation_session.commit()
                                reservation_id = reserved_integration_record.id
                            except IntegrityError:
                                reservation_session.rollback()
                                existing = self._wait_for_existing_idempotent_record(
                                    idempotency_key,
                                    wait_for_stable_result=True,
                                )
                                if existing:
                                    return self._replay_existing_execution(
                                        existing=existing,
                                        replay_context=replay_context,
                                        proposal=proposal,
                                    )
                                raise
                            finally:
                                reservation_session.close()

                            with self.db.no_autoflush:
                                reserved_integration_record = self.db.query(models.AuditEvent).filter_by(id=reservation_id).one()

                    if success:
                        try:
                            integration_result = integration.execute(
                                action="create_task",
                                payload={
                                    **proposal.payload,
                                    "project_id": project_id,
                                },
                            )
                            if integration_result.ok:
                                response_result = "executed"
                                stored_result = "accepted"
                                detail = integration_result.detail or f"{integration_name} executed: create_task"
                                success = True
                            else:
                                response_result = "rejected"
                                stored_result = "denied"
                                detail = integration_result.detail or "integration execution reported failure"
                                success = False
                        except IntegrationError as exc:
                            response_result = "rejected"
                            stored_result = "denied"
                            detail = str(exc)
                            success = False
                        except Exception as exc:
                            response_result = "failed"
                            stored_result = "denied"
                            detail = str(exc) or type(exc).__name__
                            success = False
                            unexpected_execute_exc = exc
            elif proposal.requires_approval or decision.requires_approval:
                if dry_run:
                    response_result = "simulated"
                    stored_result = "simulated"
                else:
                    response_result = "requires_approval"
                    stored_result = "awaiting_approval"
            else:
                if dry_run:
                    response_result = "simulated"
                    stored_result = "simulated"
                else:
                    response_result = "executed"
                    stored_result = "accepted"
                    detail = "policy accepted"

        payload = {
            "actor": actor,
            "auth": {
                "role": role,
                "actor_trusted": actor_trusted,
                "dry_run": dry_run,
                "validate_integration": validate_integration,
                "execute_integration": execute_integration,
            },
            "proposal": proposal.model_dump(),
            "policy": decision.__dict__,
        }
        if validate_integration or execute_integration:
            payload["integration"] = {
                "name": integration_name,
                "action": proposal.action,
                "status": response_result,
                "detail": detail,
            }

        if reserved_integration_record is not None:
            final_reserved_result = stored_result
            if success and response_result == "executed" and not dry_run:
                final_reserved_result = "executed"
            self._persist_audit_event_result_isolated(
                audit_event_id=reserved_integration_record.id,
                payload=payload,
                result=final_reserved_result,
            )
            reserved_integration_record.payload = json.dumps(payload, ensure_ascii=False)
            reserved_integration_record.result = final_reserved_result
            record = reserved_integration_record
        else:
            record = models.AuditEvent(
                project_id=project_id,
                actor=actor,
                action=proposal.action,
                target_type="proposal",
                target_id=proposal.project_id,
                payload=json.dumps(payload, ensure_ascii=False),
                result=stored_result,
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
            self.db.add(record)
            try:
                self.db.flush()
            except IntegrityError:
                self.db.rollback()
                existing = self._wait_for_existing_idempotent_record(idempotency_key)
                if existing:
                    return self._replay_existing_execution(
                        existing=existing,
                        replay_context=replay_context,
                        proposal=proposal,
                    )
                raise

        if success and response_result == "executed" and not dry_run:
            self._apply_action(proposal)
            if reserved_integration_record is None:
                self.db.query(models.AuditEvent).filter_by(id=record.id).update(
                    {models.AuditEvent.result: "executed"},
                    synchronize_session=False,
                )

        if unexpected_execute_exc is not None:
            raise unexpected_execute_exc.with_traceback(unexpected_execute_exc.__traceback__)

        return ProposalExecution(
            success=success,
            proposal=proposal,
            audit_id=record.id,
            result=response_result,
            detail=detail,
        )

    def rollback(
        self,
        *,
        actor: str,
        role: str,
        audit_event_id: str,
        reason: str,
        actor_trusted: bool = True,
        expected_project_id: str | None = None,
    ) -> RollbackExecution:
        original = self.db.get(models.AuditEvent, audit_event_id)
        if not original:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=None,
                    reason="no target action found",
                    payload={"rollback_of_audit_event_id": audit_event_id},
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="audit event not found",
            )

        project_id = original.project_id
        if expected_project_id is not None and project_id != expected_project_id:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="rollback target mismatch",
                    payload={"rollback_of_audit_event_id": audit_event_id},
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="audit event does not target this project",
            )

        if not project_id:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=None,
                    reason="target action lacks project context",
                    payload={"rollback_of_audit_event_id": audit_event_id},
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="target action lacks project context",
            )

        project = self.db.get(models.Project, project_id)
        if not project:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="target action project not found",
                    payload={"rollback_of_audit_event_id": audit_event_id},
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="target action project not found",
            )

        if original.result != "executed":
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="only executed audit events can be rolled back",
                    payload={
                        "rollback_of_audit_event_id": audit_event_id,
                        "rollback_of_action": original.action,
                    },
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="only executed audit events can be rolled back",
            )

        existing_rollback = (
            self.db.query(models.RollbackRecord)
            .filter_by(audit_event_id=original.id, executed=True)
            .one_or_none()
        )
        if existing_rollback:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="audit event already rolled back",
                    payload={
                        "rollback_of_audit_event_id": audit_event_id,
                        "rollback_of_action": original.action,
                        "rollback_record_id": existing_rollback.id,
                    },
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="audit event already rolled back",
            )

        rollback_action = self._rollback_action_for(original.action)
        if not rollback_action:
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="non-reversible action",
                    payload={
                        "rollback_of_audit_event_id": audit_event_id,
                        "rollback_of_action": original.action,
                    },
                ),
                audit_id=audit_event_id,
                rollback_record_id="",
                result="rejected",
                detail="non-reversible action",
            )

        rollback_payload = {
            "rollback_of_audit_event_id": original.id,
            "rollback_of_action": original.action,
            "rollback_target_action": rollback_action,
        }
        rollback_proposal = CommandProposal(
            action="rollback_action",
            project_id=project_id,
            reason="rollback of action",
            payload=rollback_payload,
        )

        decision: PolicyDecision = self.policy.evaluate(
            actor_role=role,
            actor_trusted=actor_trusted,
            action="rollback_action",
            safe_paused=bool(project.safe_paused),
        )
        if not decision.allowed:
            payload = {
                "actor": actor,
                "auth": {
                    "role": role,
                    "actor_trusted": actor_trusted,
                },
                "proposal": {
                    "action": "rollback_action",
                    "project_id": project_id,
                    "reason": "rollback of audit event",
                    "payload": rollback_payload,
                },
                "policy": decision.__dict__,
            }
            record = models.AuditEvent(
                project_id=project_id,
                actor=actor,
                action="rollback_action",
                target_type="rollback",
                target_id=original.id,
                payload=json.dumps(payload, ensure_ascii=False),
                result="denied",
                created_at=datetime.utcnow(),
            )
            self.db.add(record)
            self.db.flush()
            return RollbackExecution(
                success=False,
                proposal=rollback_proposal,
                audit_id=record.id,
                rollback_record_id="",
                result="rejected",
                detail=decision.reason,
            )

        rollback_record = models.RollbackRecord(
            audit_event_id=original.id,
            actor=actor,
            reason=reason,
            executed=True,
            created_at=datetime.utcnow(),
        )
        self.db.add(rollback_record)
        try:
            self.db.flush()
        except IntegrityError:
            self.db.rollback()
            return RollbackExecution(
                success=False,
                proposal=CommandProposal(
                    action="rollback_action",
                    project_id=project_id,
                    reason="audit event already rolled back",
                    payload={
                        "rollback_of_audit_event_id": audit_event_id,
                        "rollback_of_action": original.action,
                    },
                ),
                audit_id=original.id,
                rollback_record_id="",
                result="rejected",
                detail="audit event already rolled back",
            )

        payload = {
            "actor": actor,
            "auth": {
                "role": role,
                "actor_trusted": actor_trusted,
            },
            "proposal": {
                "action": "rollback_action",
                "project_id": project_id,
                "reason": "rollback of action",
                "payload": rollback_payload,
            },
            "policy": decision.__dict__,
        }
        record = models.AuditEvent(
            project_id=project_id,
            actor=actor,
            action="rollback_action",
            target_type="rollback",
            target_id=original.id,
            payload=json.dumps(payload, ensure_ascii=False),
            result="accepted",
            created_at=datetime.utcnow(),
        )
        self.db.add(record)
        self.db.flush()

        rollback_proposal = CommandProposal(
            action="rollback_action",
            project_id=project_id,
            reason="rollback of action",
            payload={
                "rollback_of_audit_event_id": original.id,
                "rollback_of_action": original.action,
                "rollback_target_action": rollback_action,
            },
        )
        self._apply_action(action=rollback_action, project_id=project_id)
        self.db.query(models.AuditEvent).filter_by(id=record.id).update(
            {models.AuditEvent.result: "executed"},
            synchronize_session=False,
        )

        return RollbackExecution(
            success=True,
            proposal=rollback_proposal,
            audit_id=record.id,
            rollback_record_id=rollback_record.id,
            result="executed",
            detail="rollback action applied",
        )

    def _caller_session_holds_sqlite_write_transaction(self) -> bool:
        bind = self.db.get_bind()
        if getattr(bind.dialect, "name", None) != "sqlite":
            return False
        connection = self.db.connection()
        dbapi_connection = getattr(connection, "connection", None)
        return bool(getattr(dbapi_connection, "in_transaction", False))

    def _persist_audit_event_result_isolated(
        self,
        *,
        audit_event_id: str,
        payload: dict,
        result: str,
        attempts: int = PENDING_INTEGRATION_WAIT_ATTEMPTS,
        delay_seconds: float = PENDING_INTEGRATION_WAIT_DELAY_SECONDS,
    ) -> None:
        if self._sqlite_side_session_isolation_is_unsafe() and self._caller_session_holds_sqlite_write_transaction():
            raise UnsafeIsolatedSqlitePersistenceError(self.SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL)

        for attempt in range(attempts):
            persistence_session = Session(bind=self.db.get_bind(), future=True)
            try:
                audit_event = persistence_session.get(models.AuditEvent, audit_event_id)
                if audit_event is None:
                    raise RuntimeError(f"audit event missing: {audit_event_id}")
                audit_event.payload = json.dumps(payload, ensure_ascii=False)
                audit_event.result = result
                persistence_session.commit()
                return
            except OperationalError as exc:
                persistence_session.rollback()
                if not self._is_retryable_sqlite_lock_error(exc) or attempt >= attempts - 1:
                    raise
                time.sleep(delay_seconds)
            finally:
                persistence_session.close()

    def _is_retryable_sqlite_lock_error(self, exc: OperationalError) -> bool:
        bind = self.db.get_bind()
        if getattr(bind.dialect, "name", None) != "sqlite":
            return False
        return "database is locked" in str(exc).lower()

    def _sqlite_side_session_isolation_is_unsafe(self) -> bool:
        bind = self.db.get_bind()
        if getattr(bind.dialect, "name", None) != "sqlite":
            return False
        return getattr(bind.url, "database", None) in {None, "", ":memory:"}

    def _load_existing_idempotent_record_snapshot(
        self,
        idempotency_key: str,
    ) -> models.AuditEvent | None:
        read_session = Session(bind=self.db.get_bind(), future=True)
        try:
            existing = read_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            if existing is not None:
                read_session.expunge(existing)
            return existing
        finally:
            read_session.close()

    def _load_existing_idempotent_record_in_caller_session(
        self,
        idempotency_key: str,
    ) -> models.AuditEvent | None:
        with self.db.no_autoflush:
            existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
        if existing is not None and not self.db.is_modified(existing):
            self.db.refresh(existing)
        return existing

    def _wait_for_existing_idempotent_record(
        self,
        idempotency_key: str | None,
        *,
        attempts: int = PENDING_INTEGRATION_WAIT_ATTEMPTS,
        delay_seconds: float = PENDING_INTEGRATION_WAIT_DELAY_SECONDS,
        wait_for_stable_result: bool = False,
    ) -> models.AuditEvent | None:
        if not idempotency_key:
            return None

        existing: models.AuditEvent | None = None
        for attempt in range(attempts):
            if self._sqlite_side_session_isolation_is_unsafe():
                existing = self._load_existing_idempotent_record_in_caller_session(idempotency_key)
            else:
                existing = self._load_existing_idempotent_record_snapshot(idempotency_key)
            if existing and (
                not wait_for_stable_result
                or existing.result != "pending_integration"
                or self._is_stale_pending_integration(existing)
            ):
                return existing
            if attempt < attempts - 1:
                time.sleep(delay_seconds)
        return existing

    def _is_stale_pending_integration(self, existing: models.AuditEvent) -> bool:
        return (
            existing.result == "pending_integration"
            and existing.created_at is not None
            and datetime.utcnow() - existing.created_at >= self.PENDING_INTEGRATION_STALE_AFTER
        )

    def _partial_mapping_matches(self, stored: object, current: object) -> bool:
        if isinstance(stored, dict):
            if not isinstance(current, dict):
                return False
            return all(
                key in current and self._partial_mapping_matches(value, current.get(key))
                for key, value in stored.items()
            )
        return stored == current

    def _mapping_payload_member(self, payload: object, key: str) -> dict:
        if not isinstance(payload, dict):
            return {}
        value = payload.get(key)
        return value if isinstance(value, dict) else {}

    def _stored_payload_has_complete_replay_context(self, payload: dict) -> bool:
        if not isinstance(payload, dict):
            return False

        auth = self._mapping_payload_member(payload, "auth")
        proposal_payload = self._mapping_payload_member(payload, "proposal")
        required_auth_keys = {
            "role",
            "actor_trusted",
            "dry_run",
            "validate_integration",
            "execute_integration",
        }
        required_proposal_keys = {
            "action",
            "project_id",
            "reason",
            "payload",
            "requires_approval",
        }
        return (
            bool(auth)
            and required_auth_keys.issubset(auth)
            and bool(proposal_payload)
            and required_proposal_keys.issubset(proposal_payload)
        )

    def _can_repair_incomplete_stale_pending_replay(self, replay_context: dict) -> bool:
        return (
            replay_context.get("actor_trusted") is True
            and replay_context.get("role") in {"admin", "owner"}
        )

    def _can_repair_stale_pending_integration_replay(
        self,
        *,
        existing: models.AuditEvent,
        replay_context: dict,
        proposal: CommandProposal,
    ) -> bool:
        if not (
            existing.result == "pending_integration"
            and self._is_stale_pending_integration(existing)
            and proposal.action == "create_task"
            and existing.action == "create_task"
            and replay_context.get("execute_integration", False)
            and not replay_context.get("dry_run", False)
            and not replay_context.get("validate_integration", False)
        ):
            return False

        if existing.actor != replay_context.get("actor"):
            return False

        if (
            existing.project_id != proposal.project_id
            or existing.target_type != "proposal"
            or existing.target_id != proposal.project_id
        ):
            return False

        try:
            payload = json.loads(existing.payload) if existing.payload else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return self._can_repair_incomplete_stale_pending_replay(replay_context)

        if isinstance(payload, dict):
            payload_actor = payload.get("actor")
            if payload_actor is not None and payload_actor != replay_context.get("actor"):
                return False

            auth = payload.get("auth")
            if auth is not None:
                if not isinstance(auth, dict):
                    return False
                for key in (
                    "role",
                    "actor_trusted",
                    "dry_run",
                    "validate_integration",
                    "execute_integration",
                ):
                    if key in auth and auth.get(key) != replay_context.get(key):
                        return False

            proposal_payload = payload.get("proposal")
            if proposal_payload is not None and not self._partial_mapping_matches(
                proposal_payload,
                replay_context.get("proposal"),
            ):
                return False
        if not self._stored_payload_has_complete_replay_context(payload):
            return self._can_repair_incomplete_stale_pending_replay(replay_context)

        auth = payload.get("auth")
        proposal_payload = payload.get("proposal")
        existing_context = {
            "actor": existing.actor,
            "role": auth.get("role"),
            "actor_trusted": auth.get("actor_trusted"),
            "proposal": proposal_payload,
            "dry_run": auth.get("dry_run", False),
            "validate_integration": auth.get("validate_integration", False),
            "execute_integration": auth.get("execute_integration", False),
        }
        return existing_context == replay_context

    def _mark_stale_pending_integration_denied(
        self,
        *,
        existing: models.AuditEvent,
        replay_context: dict,
    ) -> tuple[models.AuditEvent, bool]:
        detail = "stale pending integration request requires manual reconciliation before retry"
        try:
            payload = json.loads(existing.payload) if existing.payload else {}
            repair_payload = not isinstance(payload, dict) or not payload
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
            repair_payload = True

        if repair_payload:
            integration_payload = self._mapping_payload_member(payload, "integration")
            payload = {
                **(payload if isinstance(payload, dict) else {}),
                "actor": (payload.get("actor") if isinstance(payload, dict) else None) or existing.actor,
                "replay_repair": {
                    "source": "unreadable_pending_integration_payload",
                },
                "integration": {
                    **integration_payload,
                    "action": integration_payload.get("action") or existing.action,
                },
            }

        integration_payload = self._mapping_payload_member(payload, "integration")
        payload["integration"] = {
            **integration_payload,
            "action": integration_payload.get("action") or existing.action,
            "status": "failed",
            "detail": detail,
        }
        self._persist_audit_event_result_isolated(
            audit_event_id=existing.id,
            payload=payload,
            result="denied",
        )
        existing.payload = json.dumps(payload, ensure_ascii=False)
        existing.result = "denied"
        return existing, repair_payload

    def _replay_existing_execution(
        self,
        *,
        existing: models.AuditEvent,
        replay_context: dict,
        proposal: CommandProposal,
    ) -> ProposalExecution:
        repaired_stale_payload = False
        if self._can_repair_stale_pending_integration_replay(
            existing=existing,
            replay_context=replay_context,
            proposal=proposal,
        ):
            if self._caller_session_holds_sqlite_write_transaction():
                return ProposalExecution(
                    success=False,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="rejected",
                    detail=self.SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL,
                )
            try:
                existing, repaired_stale_payload = self._mark_stale_pending_integration_denied(
                    existing=existing,
                    replay_context=replay_context,
                )
            except UnsafeIsolatedSqlitePersistenceError as exc:
                return ProposalExecution(
                    success=False,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="rejected",
                    detail=str(exc),
                )

        try:
            existing_payload = json.loads(existing.payload)
        except (TypeError, ValueError, json.JSONDecodeError):
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail="idempotency key already used for unreadable stored request context",
            )
        if not isinstance(existing_payload, dict):
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail="idempotency key already used for unreadable stored request context",
            )

        existing_policy = self._mapping_payload_member(existing_payload, "policy")
        existing_auth = self._mapping_payload_member(existing_payload, "auth")
        existing_integration = self._mapping_payload_member(existing_payload, "integration")
        existing_replay_repair = self._mapping_payload_member(existing_payload, "replay_repair")
        detail = existing_policy.get("reason", "replayed idempotent result")
        integration_detail = existing_integration.get("detail")
        if (
            existing_auth.get("validate_integration", False)
            or existing_auth.get("execute_integration", False)
        ) and integration_detail:
            detail = integration_detail
        if repaired_stale_payload or (
            existing.result == "denied"
            and integration_detail == "stale pending integration request requires manual reconciliation before retry"
            and not self._stored_payload_has_complete_replay_context(existing_payload)
        ) or (
            existing_replay_repair.get("source") == "unreadable_pending_integration_payload"
        ):
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail=integration_detail or detail,
            )

        existing_context = {
            "actor": existing.actor,
            "role": existing_auth.get("role"),
            "actor_trusted": existing_auth.get("actor_trusted"),
            "proposal": existing_payload.get("proposal", {}),
            "dry_run": existing_auth.get("dry_run", False),
            "validate_integration": existing_auth.get("validate_integration", False),
            "execute_integration": existing_auth.get("execute_integration", False),
        }
        if existing_context != replay_context:
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail="idempotency key already used for different request context",
            )

        if existing.result == "denied":
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail=detail,
            )
        if existing.result == "simulated":
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=existing.id,
                result="simulated",
                detail=detail,
            )
        if existing.result == "awaiting_approval":
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=existing.id,
                result="requires_approval",
                detail=detail,
            )
        if existing.result == "validated":
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=existing.id,
                result="validated",
                detail=detail,
            )
        if existing.result == "pending_integration":
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail="idempotent request still pending integration execution",
            )
        return ProposalExecution(
            success=True,
            proposal=proposal,
            audit_id=existing.id,
            result="executed",
            detail=detail,
        )

    def _apply_action(self, proposal: CommandProposal | None = None, *, action: str | None = None, project_id: str | None = None) -> None:
        # Minimal command semantics for MVP: pause/unpause safe switch.
        resolved_action = action or (proposal.action if proposal else None)
        resolved_project_id = project_id or (proposal.project_id if proposal else None)

        if not resolved_action or not resolved_project_id:
            return

        if resolved_action == "pause_project":
            self.db.query(models.Project).filter_by(id=resolved_project_id).update(
                {models.Project.safe_paused: True},
                synchronize_session=False,
            )
        elif resolved_action == "unpause_project":
            self.db.query(models.Project).filter_by(id=resolved_project_id).update(
                {models.Project.safe_paused: False},
                synchronize_session=False,
            )

    @staticmethod
    def _rollback_action_for(action: str) -> str | None:
        rollback_map = {
            "pause_project": "unpause_project",
            "unpause_project": "pause_project",
        }
        return rollback_map.get(action)
