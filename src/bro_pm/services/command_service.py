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
from .execution_outbox_service import ExecutionOutboxConflictError, ExecutionOutboxService


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
    APPROVAL_TTL = timedelta(days=7)
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

    def _get_project(self, project_id: str | None) -> models.Project | None:
        if not project_id:
            return None
        with self.db.no_autoflush:
            return self.db.get(models.Project, project_id)

    def _project_metadata(self, project_id: str | None) -> dict:
        project = self._get_project(project_id)
        metadata = project.metadata_json if project else {}
        return metadata if isinstance(metadata, dict) else {}

    def _integration_payload(self, *, project_id: str | None, proposal_payload: dict) -> dict:
        payload = {
            **proposal_payload,
            "project_id": project_id,
        }
        project_metadata = self._project_metadata(project_id)
        if project_metadata:
            payload["project_metadata"] = project_metadata
        return payload

    def _resolve_project_board_integration_name(self, project_id: str | None) -> str:
        if not project_id:
            return "notion"
        project = self._get_project(project_id)
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

    def _build_integration_reservation_payload(
        self,
        *,
        actor: str,
        role: str,
        actor_trusted: bool,
        proposal: CommandProposal,
        decision: PolicyDecision,
        dry_run: bool,
        validate_integration: bool,
        execute_integration: bool,
        integration_name: str,
        integration_detail: str,
        integration_payload: dict,
    ) -> dict:
        return {
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
                "detail": integration_detail,
            },
            "integration_payload": integration_payload,
        }

    def _reserve_integration_execution_isolated(
        self,
        *,
        project_id: str | None,
        actor: str,
        proposal: CommandProposal,
        idempotency_key: str,
        reservation_payload: dict,
        integration_name: str,
    ) -> str:
        reservation_session = Session(bind=self.db.get_bind(), future=True)
        try:
            reserved_record = models.AuditEvent(
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
            reservation_session.add(reserved_record)
            reservation_session.flush()
            reservation_session.add(
                models.ActionExecution(
                    audit_event_id=reserved_record.id,
                    project_id=project_id,
                    actor=actor,
                    action=proposal.action,
                    status="requested",
                    requested_at=datetime.utcnow(),
                )
            )
            reservation_session.add(
                models.ExecutionOutbox(
                    audit_event_id=reserved_record.id,
                    project_id=project_id,
                    execution_kind="integration_execute",
                    integration_name=integration_name,
                    integration_action=proposal.action,
                    payload_json=reservation_payload,
                    status="queued",
                    available_at=datetime.utcnow(),
                )
            )
            reservation_session.commit()
            return reserved_record.id
        except Exception:
            reservation_session.rollback()
            raise
        finally:
            reservation_session.close()

    def _process_execution_outbox_isolated(
        self,
        *,
        audit_event_id: str,
        worker_id: str,
    ) -> models.AuditEvent:
        if self._caller_session_holds_sqlite_write_transaction():
            raise UnsafeIsolatedSqlitePersistenceError(self.SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL)

        processing_session = Session(bind=self.db.get_bind(), future=True)
        try:
            worker = ExecutionOutboxService(db_session=processing_session)
            worker.process_for_audit_event(audit_event_id=audit_event_id, worker_id=worker_id)
            audit_event = processing_session.get(models.AuditEvent, audit_event_id)
            if audit_event is None:
                raise RuntimeError(f"audit event missing: {audit_event_id}")
            processing_session.expunge(audit_event)
            return audit_event
        finally:
            processing_session.close()

    def _outbox_exists_for_audit(self, audit_event_id: str) -> bool:
        with self.db.no_autoflush:
            return (
                self.db.query(models.ExecutionOutbox)
                .filter_by(audit_event_id=audit_event_id)
                .one_or_none()
                is not None
            )

    def _record_action_execution(
        self,
        *,
        audit_event_id: str,
        actor: str,
        project_id: str | None,
        action: str,
        status: str,
        requested_at: datetime | None = None,
        awaiting_approval_at: datetime | None = None,
        executed_at: datetime | None = None,
        verified_at: datetime | None = None,
    ) -> None:
        execution = self.db.query(models.ActionExecution).filter_by(audit_event_id=audit_event_id).one_or_none()
        if execution is None:
            execution = models.ActionExecution(
                audit_event_id=audit_event_id,
                project_id=project_id,
                actor=actor,
                action=action,
                status="requested",
                requested_at=requested_at or datetime.utcnow(),
            )
            self.db.add(execution)
            self.db.flush()

        execution.status = status
        if awaiting_approval_at is not None:
            execution.awaiting_approval_at = awaiting_approval_at
        if executed_at is not None:
            execution.executed_at = executed_at
        if verified_at is not None:
            execution.verified_at = verified_at

    def _approval_expires_at(self, requested_at: datetime) -> datetime:
        return requested_at + self.APPROVAL_TTL

    def _approval_request_for_audit(self, audit_event_id: str) -> models.ApprovalRequest | None:
        return self.db.query(models.ApprovalRequest).filter_by(audit_event_id=audit_event_id).one_or_none()

    def _proposal_from_audit(self, audit_event: models.AuditEvent) -> CommandProposal | None:
        try:
            payload = json.loads(audit_event.payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        proposal_payload = payload.get("proposal")
        if not isinstance(proposal_payload, dict):
            return None
        try:
            return CommandProposal.model_validate(proposal_payload)
        except Exception:
            return None

    @staticmethod
    def _audit_payload_dict(audit_event: models.AuditEvent) -> dict:
        try:
            payload = json.loads(audit_event.payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _proposal_payload_from_audit(self, audit_event: models.AuditEvent) -> dict:
        payload = self._audit_payload_dict(audit_event)
        proposal_payload = payload.get("proposal")
        if not isinstance(proposal_payload, dict):
            return {}
        nested_payload = proposal_payload.get("payload")
        return nested_payload if isinstance(nested_payload, dict) else {}

    def _rollback_dependents_for(
        self,
        original: models.AuditEvent,
    ) -> tuple[list[models.AuditEvent], models.AuditEvent | None]:
        reversible_actions = {"pause_project", "unpause_project"}
        root_lineage_ids = {original.id}
        dependents: list[models.AuditEvent] = []
        conflicting_reversible_event: models.AuditEvent | None = None
        candidates = (
            self.db.query(models.AuditEvent)
            .filter(
                models.AuditEvent.project_id == original.project_id,
                models.AuditEvent.result == "executed",
                models.AuditEvent.created_at >= original.created_at,
                models.AuditEvent.id != original.id,
            )
            .order_by(models.AuditEvent.created_at.asc(), models.AuditEvent.id.asc())
            .all()
        )
        candidate_ids = [candidate.id for candidate in candidates]
        rolled_back_audit_event_ids = {
            audit_event_id
            for (audit_event_id,) in self.db.query(models.RollbackRecord.audit_event_id)
            .filter(
                models.RollbackRecord.executed.is_(True),
                models.RollbackRecord.audit_event_id.in_(candidate_ids),
            )
            .all()
        }
        for candidate in candidates:
            if candidate.id in rolled_back_audit_event_ids:
                continue
            candidate_payload = self._proposal_payload_from_audit(candidate)
            depends_on_audit_event_id = candidate_payload.get("depends_on_audit_event_id")
            explicitly_dependent = isinstance(depends_on_audit_event_id, str) and depends_on_audit_event_id in root_lineage_ids
            if explicitly_dependent:
                dependents.append(candidate)
                root_lineage_ids.add(candidate.id)
                continue
            if candidate.action in reversible_actions and conflicting_reversible_event is None:
                conflicting_reversible_event = candidate
        return dependents, conflicting_reversible_event

    def _rollback_plan_for(self, original: models.AuditEvent) -> tuple[list[models.AuditEvent], dict]:
        dependents, conflicting_reversible_event = self._rollback_dependents_for(original)
        plan_events = [*reversed(dependents), original]
        step_audit_event_ids = [event.id for event in plan_events]
        step_actions: list[str] = []
        blocked_event: models.AuditEvent | None = None
        blocked_reason: str | None = None
        for event in plan_events:
            rollback_action = self._rollback_action_for(event.action)
            if rollback_action is None:
                blocked_event = event
                blocked_reason = "irreversible_dependent_action"
                break
            step_actions.append(rollback_action)

        verification_target = step_actions[-1] if step_actions else self._rollback_action_for(original.action)
        expected_safe_paused = verification_target == "pause_project"
        verification_detail = f"verify project.safe_paused is {str(expected_safe_paused)} after rollback plan execution"
        remediation_detail = ""
        status = "ready"
        if blocked_event is not None:
            status = "blocked"
            verification_detail = "verify project.safe_paused before retrying"
            remediation_detail = "remediate the dependent side effect and verify project.safe_paused before retrying"
        elif conflicting_reversible_event is not None:
            blocked_event = conflicting_reversible_event
            blocked_reason = "later_independent_reversible_action"
            status = "blocked"
            verification_detail = "verify project.safe_paused before retrying"
            remediation_detail = "verify the newer operator decisions and attach an explicit dependency chain before retrying"

        plan_payload = {
            "status": status,
            "step_audit_event_ids": step_audit_event_ids,
            "step_actions": step_actions,
            "verification_detail": verification_detail,
            "remediation_detail": remediation_detail,
        }
        if blocked_event is not None:
            plan_payload["blocked_by_audit_event_id"] = blocked_event.id
            plan_payload["blocked_by_action"] = blocked_event.action
        if blocked_reason is not None:
            plan_payload["blocked_reason"] = blocked_reason
        return plan_events, plan_payload

    def _verify_rollback_plan(self, *, project_id: str, plan_payload: dict) -> bool:
        verification_detail = plan_payload.get("verification_detail")
        if not isinstance(verification_detail, str):
            return False
        if "project.safe_paused is True" in verification_detail:
            expected_safe_paused = True
        elif "project.safe_paused is False" in verification_detail:
            expected_safe_paused = False
        else:
            return False
        project = self._get_project(project_id)
        if project is None:
            return False
        self.db.refresh(project)
        return bool(project.safe_paused) is expected_safe_paused

    def _approval_payload_update(
        self,
        *,
        audit_event: models.AuditEvent,
        status: str,
        actor: str | None = None,
        role: str | None = None,
        decision_text: str | None = None,
    ) -> None:
        try:
            payload = json.loads(audit_event.payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        approval_payload = {
            "status": status,
        }
        if actor is not None:
            approval_payload["actor"] = actor
        if role is not None:
            approval_payload["actor_role"] = role
        if decision_text is not None:
            approval_payload["text"] = decision_text
        payload["approval"] = approval_payload
        audit_event.payload = json.dumps(payload, ensure_ascii=False)

    def _ensure_approval_request(
        self,
        *,
        audit_event: models.AuditEvent,
        actor: str,
        proposal: CommandProposal | None = None,
        requested_at: datetime | None = None,
    ) -> models.ApprovalRequest:
        approval = self._approval_request_for_audit(audit_event.id)
        if approval is not None:
            return approval

        proposal = proposal or self._proposal_from_audit(audit_event)
        requested_at = requested_at or datetime.utcnow()
        approval = models.ApprovalRequest(
            audit_event_id=audit_event.id,
            project_id=audit_event.project_id,
            action=audit_event.action,
            status="pending",
            requested_by=actor,
            requested_at=requested_at,
            expires_at=self._approval_expires_at(requested_at),
        )
        if proposal is not None and proposal.project_id is not None:
            approval.project_id = proposal.project_id
        self.db.add(approval)
        self.db.flush()
        return approval

    def _expire_approval_request(
        self,
        *,
        approval: models.ApprovalRequest,
        audit_event: models.AuditEvent,
    ) -> None:
        now = datetime.utcnow()
        approval.status = "expired"
        if approval.decided_at is None:
            approval.decided_at = now
        audit_event.result = "expired"
        self._approval_payload_update(audit_event=audit_event, status="expired")
        if audit_event.action_execution is not None:
            audit_event.action_execution.status = "expired"

    def decide_approval(
        self,
        *,
        audit_event_id: str,
        actor: str,
        role: str,
        reviewer_role: str | None = None,
        approved: bool,
        decision_text: str | None = None,
        decision_payload: dict | None = None,
        actor_trusted: bool = True,
    ) -> models.ApprovalRequest:
        audit_event = self.db.get(models.AuditEvent, audit_event_id)
        if audit_event is None:
            raise ValueError("approval audit event not found")

        project = self._get_project(audit_event.project_id)
        safe_paused = bool(project.safe_paused) if project is not None else False
        decision = self.policy.evaluate(
            actor_role=role,
            actor_trusted=actor_trusted,
            action="approve_action",
            safe_paused=safe_paused,
        )
        if not decision.allowed:
            raise ValueError(decision.reason)

        proposal = self._proposal_from_audit(audit_event)
        stored_reviewer_role = reviewer_role or role
        approval = self._ensure_approval_request(
            audit_event=audit_event,
            actor=audit_event.actor,
            proposal=proposal,
            requested_at=audit_event.created_at or datetime.utcnow(),
        )
        if approval.status == "executed":
            return approval
        if approval.expires_at is not None and approval.expires_at <= datetime.utcnow():
            self._expire_approval_request(approval=approval, audit_event=audit_event)
            self.db.flush()
            return approval

        approval.status = "approved" if approved else "rejected"
        approval.reviewer_actor = actor
        approval.reviewer_role = stored_reviewer_role
        approval.decision_text = decision_text
        approval.decision_payload_json = decision_payload
        approval.decided_at = datetime.utcnow()
        audit_event.result = approval.status
        self._approval_payload_update(
            audit_event=audit_event,
            status=approval.status,
            actor=actor,
            role=stored_reviewer_role,
            decision_text=decision_text,
        )
        if audit_event.action_execution is not None:
            audit_event.action_execution.status = approval.status
        self.db.flush()
        return approval

    def _reconcile_approval_from_audit(
        self,
        *,
        approval: models.ApprovalRequest,
        audit_event: models.AuditEvent,
    ) -> models.ApprovalRequest:
        try:
            payload = json.loads(audit_event.payload or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        approval_payload = payload.get("approval") if isinstance(payload, dict) else None

        desired_status = approval.status
        if audit_event.result in {"approved", "rejected", "expired", "executed"}:
            desired_status = "executed" if audit_event.result == "executed" else audit_event.result
        if isinstance(approval_payload, dict) and approval_payload.get("status") in {
            "approved",
            "rejected",
            "expired",
            "executed",
        }:
            desired_status = str(approval_payload["status"])

        if approval.status == "pending" and desired_status != "pending":
            approval.status = desired_status
            approval.reviewer_actor = approval.reviewer_actor or approval_payload.get("actor") if isinstance(approval_payload, dict) else approval.reviewer_actor
            approval.reviewer_role = approval.reviewer_role or approval_payload.get("actor_role") if isinstance(approval_payload, dict) else approval.reviewer_role
            approval.decision_text = approval.decision_text or approval_payload.get("text") if isinstance(approval_payload, dict) else approval.decision_text
            if desired_status in {"approved", "rejected", "expired"} and approval.decided_at is None:
                approval.decided_at = datetime.utcnow()
            if desired_status == "executed" and approval.resumed_at is None:
                approval.resumed_at = datetime.utcnow()
            self.db.flush()
        return approval

    def resume_approval(
        self,
        *,
        audit_event_id: str,
        actor: str,
        role: str,
        actor_trusted: bool = True,
    ) -> ProposalExecution:
        audit_event = self.db.get(models.AuditEvent, audit_event_id)
        if audit_event is None:
            return ProposalExecution(
                success=False,
                proposal=CommandProposal(
                    action="noop",
                    project_id=None,
                    reason="approval audit event not found",
                    payload={},
                ),
                audit_id=audit_event_id,
                result="rejected",
                detail="approval audit event not found",
            )

        proposal = self._proposal_from_audit(audit_event)
        if proposal is None:
            return ProposalExecution(
                success=False,
                proposal=CommandProposal(
                    action=audit_event.action,
                    project_id=audit_event.project_id,
                    reason="stored proposal is unreadable",
                    payload={},
                ),
                audit_id=audit_event.id,
                result="rejected",
                detail="stored proposal is unreadable",
            )

        approval = self._ensure_approval_request(
            audit_event=audit_event,
            actor=audit_event.actor,
            proposal=proposal,
            requested_at=audit_event.created_at or datetime.utcnow(),
        )
        approval = self._reconcile_approval_from_audit(approval=approval, audit_event=audit_event)
        if approval.status == "executed":
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=audit_event.id,
                result="executed",
                detail="approved action already resumed",
            )

        project = self._get_project(audit_event.project_id)
        safe_paused = bool(project.safe_paused) if project is not None else False
        decision = self.policy.evaluate(
            actor_role=role,
            actor_trusted=actor_trusted,
            action="approve_action",
            safe_paused=safe_paused,
        )
        if not decision.allowed:
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=audit_event.id,
                result="rejected",
                detail=decision.reason,
            )

        if approval.expires_at is not None and approval.expires_at <= datetime.utcnow():
            self._expire_approval_request(approval=approval, audit_event=audit_event)
            self.db.flush()
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=audit_event.id,
                result="rejected",
                detail="approval expired",
            )
        if approval.status == "rejected":
            audit_event.result = "rejected"
            if audit_event.action_execution is not None:
                audit_event.action_execution.status = "rejected"
            self.db.flush()
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=audit_event.id,
                result="rejected",
                detail="approval rejected",
            )
        if approval.status != "approved":
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=audit_event.id,
                result="rejected",
                detail="approval still pending",
            )

        execution_now = datetime.utcnow()
        self._apply_action(proposal)
        approval.status = "executed"
        approval.resumed_at = execution_now
        audit_event.result = "executed"
        if audit_event.action_execution is not None:
            audit_event.action_execution.status = "executed"
            audit_event.action_execution.executed_at = execution_now
        self.db.flush()
        if self._verify_applied_action(proposal):
            self._record_action_execution(
                audit_event_id=audit_event.id,
                actor=audit_event.actor,
                project_id=proposal.project_id,
                action=proposal.action,
                status="verified",
                verified_at=datetime.utcnow(),
            )
        return ProposalExecution(
            success=True,
            proposal=proposal,
            audit_id=audit_event.id,
            result="executed",
            detail="approved action resumed",
        )

    def _proposal_is_synchronously_verifiable(self, proposal: CommandProposal) -> bool:
        return proposal.action in {"pause_project", "unpause_project"} and bool(proposal.project_id)

    def _verify_applied_action(self, proposal: CommandProposal) -> bool:
        if not self._proposal_is_synchronously_verifiable(proposal):
            return False
        project = self._get_project(proposal.project_id)
        if project is None:
            return False
        self.db.refresh(project)
        if proposal.action == "pause_project":
            return bool(project.safe_paused) is True
        if proposal.action == "unpause_project":
            return bool(project.safe_paused) is False
        return False

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
        reserved_integration_processed = False
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
                            payload=self._integration_payload(project_id=project_id, proposal_payload=proposal.payload),
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
                            reservation_payload = self._build_integration_reservation_payload(
                                actor=actor,
                                role=role,
                                actor_trusted=actor_trusted,
                                proposal=proposal,
                                decision=decision,
                                dry_run=dry_run,
                                validate_integration=validate_integration,
                                execute_integration=execute_integration,
                                integration_name=integration_name,
                                integration_detail=f"{integration_name} integration execution pending",
                                integration_payload=self._integration_payload(
                                    project_id=project_id,
                                    proposal_payload=proposal.payload,
                                ),
                            )
                            reservation_id: str | None = None
                            try:
                                reservation_id = self._reserve_integration_execution_isolated(
                                    project_id=project_id,
                                    actor=actor,
                                    proposal=proposal,
                                    idempotency_key=idempotency_key,
                                    reservation_payload=reservation_payload,
                                    integration_name=integration_name,
                                )
                            except IntegrityError:
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

                            with self.db.no_autoflush:
                                reserved_integration_record = self.db.query(models.AuditEvent).filter_by(id=reservation_id).one()

                    if success:
                        try:
                            if reserved_integration_record is not None:
                                processed_record = self._process_execution_outbox_isolated(
                                    audit_event_id=reserved_integration_record.id,
                                    worker_id="command-service-inline",
                                )
                                reserved_integration_record.payload = processed_record.payload
                                reserved_integration_record.result = processed_record.result
                                processed_payload = json.loads(processed_record.payload)
                                processed_integration = self._mapping_payload_member(processed_payload, "integration")
                                detail = processed_integration.get("detail", detail)
                                reserved_integration_processed = True
                                if processed_record.result == "executed":
                                    response_result = "executed"
                                    stored_result = "executed"
                                    success = True
                                else:
                                    response_result = "rejected"
                                    stored_result = "denied"
                                    success = False
                            else:
                                integration_result = integration.execute(
                                    action="create_task",
                                    payload=self._integration_payload(project_id=project_id, proposal_payload=proposal.payload),
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
                        except ExecutionOutboxConflictError:
                            existing = self._wait_for_existing_idempotent_record(
                                idempotency_key,
                                wait_for_stable_result=True,
                            )
                            if existing is not None:
                                return self._replay_existing_execution(
                                    existing=existing,
                                    replay_context=replay_context,
                                    proposal=proposal,
                                )
                            return ProposalExecution(
                                success=True,
                                proposal=proposal,
                                audit_id=reserved_integration_record.id if reserved_integration_record is not None else "",
                                result="accepted",
                                detail="idempotent request still pending integration execution",
                            )
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
            if not reserved_integration_processed:
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

        execution_status = "requested"
        execution_requested_at = datetime.utcnow()
        execution_awaiting_approval_at = None
        execution_executed_at = None
        execution_verified_at = None
        if stored_result == "awaiting_approval":
            execution_status = "awaiting_approval"
            execution_awaiting_approval_at = datetime.utcnow()
        elif stored_result in {"accepted", "executed"}:
            execution_status = "executed"
            execution_executed_at = datetime.utcnow()
        elif stored_result in {"validated", "simulated", "denied"}:
            execution_status = stored_result

        should_manage_execution_record = not (
            reserved_integration_record is not None and execute_integration and not dry_run
        )
        if should_manage_execution_record:
            self._record_action_execution(
                audit_event_id=record.id,
                actor=actor,
                project_id=project_id,
                action=proposal.action,
                status=execution_status,
                requested_at=execution_requested_at,
                awaiting_approval_at=execution_awaiting_approval_at,
                executed_at=execution_executed_at,
                verified_at=execution_verified_at,
            )
            if stored_result == "awaiting_approval":
                self._ensure_approval_request(
                    audit_event=record,
                    actor=actor,
                    proposal=proposal,
                    requested_at=execution_requested_at,
                )

        if success and response_result == "executed" and not dry_run:
            self._apply_action(proposal)
            if reserved_integration_record is None:
                self.db.query(models.AuditEvent).filter_by(id=record.id).update(
                    {models.AuditEvent.result: "executed"},
                    synchronize_session=False,
                )
            if self._verify_applied_action(proposal):
                self._record_action_execution(
                    audit_event_id=record.id,
                    actor=actor,
                    project_id=project_id,
                    action=proposal.action,
                    status="verified",
                    verified_at=datetime.utcnow(),
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
        plan_events, rollback_plan = self._rollback_plan_for(original)
        if rollback_plan["status"] == "blocked":
            blocked_reason = rollback_plan.get("blocked_reason")
            if blocked_reason == "later_independent_reversible_action":
                detail = (
                    f"unsafe rollback: later audit event {rollback_plan['blocked_by_audit_event_id']} "
                    "changed reversible project state without explicit dependency linkage; "
                    f"{rollback_plan['remediation_detail']}"
                )
            else:
                detail = (
                    f"unsafe rollback: dependent audit event {rollback_plan['blocked_by_audit_event_id']} "
                    f"with action {rollback_plan['blocked_by_action']} cannot be safely reversed; "
                    f"{rollback_plan['remediation_detail']}"
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
                    "reason": "rollback blocked by dependent side effect",
                    "payload": rollback_payload,
                },
                "rollback_plan": rollback_plan,
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
                detail=detail,
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
                "rollback_plan": rollback_plan,
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

        rollback_records: list[models.RollbackRecord] = []
        for plan_event in plan_events:
            rollback_record = models.RollbackRecord(
                audit_event_id=plan_event.id,
                rollback_root_audit_event_id=original.id,
                actor=actor,
                reason=reason,
                plan_json=rollback_plan,
                verification_detail=str(rollback_plan["verification_detail"]),
                remediation_detail=str(rollback_plan["remediation_detail"]),
                executed=True,
                created_at=datetime.utcnow(),
            )
            rollback_records.append(rollback_record)
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
                "rollback_plan": rollback_plan,
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
        for plan_action in rollback_plan["step_actions"]:
            self._apply_action(action=plan_action, project_id=project_id)
        self.db.query(models.AuditEvent).filter_by(id=record.id).update(
            {models.AuditEvent.result: "executed"},
            synchronize_session=False,
        )
        if not self._verify_rollback_plan(project_id=project_id, plan_payload=rollback_plan):
            raise RuntimeError("rollback plan verification failed")

        return RollbackExecution(
            success=True,
            proposal=rollback_proposal,
            audit_id=record.id,
            rollback_record_id=rollback_records[0].id,
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
        if self._caller_session_holds_sqlite_write_transaction():
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

    def _persist_action_execution_status_isolated(
        self,
        *,
        audit_event_id: str,
        status: str,
        attempts: int = PENDING_INTEGRATION_WAIT_ATTEMPTS,
        delay_seconds: float = PENDING_INTEGRATION_WAIT_DELAY_SECONDS,
    ) -> None:
        if self._caller_session_holds_sqlite_write_transaction():
            raise UnsafeIsolatedSqlitePersistenceError(self.SQLITE_CLEAN_CALLER_TRANSACTION_DETAIL)

        for attempt in range(attempts):
            persistence_session = Session(bind=self.db.get_bind(), future=True)
            try:
                execution = persistence_session.query(models.ActionExecution).filter_by(audit_event_id=audit_event_id).one_or_none()
                if execution is None:
                    return
                execution.status = status
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
        self._persist_action_execution_status_isolated(
            audit_event_id=existing.id,
            status="denied",
        )
        existing.payload = json.dumps(payload, ensure_ascii=False)
        existing.result = "denied"
        execution_record = self.db.query(models.ActionExecution).filter_by(audit_event_id=existing.id).one_or_none()
        if execution_record is not None:
            execution_record.status = "denied"
        return existing, repair_payload

    def _replay_existing_execution(
        self,
        *,
        existing: models.AuditEvent,
        replay_context: dict,
        proposal: CommandProposal,
    ) -> ProposalExecution:
        try:
            replay_gate_payload = json.loads(existing.payload) if existing.payload else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            replay_gate_payload = {}
        if not isinstance(replay_gate_payload, dict):
            replay_gate_payload = {}
        replay_gate_auth = self._mapping_payload_member(replay_gate_payload, "auth")
        replay_gate_context = {
            "actor": existing.actor,
            "role": replay_gate_auth.get("role"),
            "actor_trusted": replay_gate_auth.get("actor_trusted"),
            "proposal": replay_gate_payload.get("proposal", {}),
            "dry_run": replay_gate_auth.get("dry_run", False),
            "validate_integration": replay_gate_auth.get("validate_integration", False),
            "execute_integration": replay_gate_auth.get("execute_integration", False),
        }
        can_process_stale_outbox = (
            self._stored_payload_has_complete_replay_context(replay_gate_payload)
            and replay_gate_context == replay_context
        )
        repaired_stale_payload = False
        if self._can_repair_stale_pending_integration_replay(
            existing=existing,
            replay_context=replay_context,
            proposal=proposal,
        ):
            if self._outbox_exists_for_audit(existing.id) and can_process_stale_outbox:
                try:
                    processed_record = self._process_execution_outbox_isolated(
                        audit_event_id=existing.id,
                        worker_id="command-service-replay",
                    )
                except UnsafeIsolatedSqlitePersistenceError as exc:
                    return ProposalExecution(
                        success=False,
                        proposal=proposal,
                        audit_id=existing.id,
                        result="rejected",
                        detail=str(exc),
                    )
                except ExecutionOutboxConflictError:
                    latest_existing = existing
                    if existing.idempotency_key:
                        if self._sqlite_side_session_isolation_is_unsafe():
                            latest_existing = self._load_existing_idempotent_record_in_caller_session(existing.idempotency_key)
                        else:
                            latest_existing = self._load_existing_idempotent_record_snapshot(existing.idempotency_key)
                    if latest_existing is not None:
                        existing = latest_existing
                        if existing.result != "pending_integration":
                            return self._replay_existing_execution(
                                existing=existing,
                                replay_context=replay_context,
                                proposal=proposal,
                            )
                    try:
                        conflict_payload = json.loads(existing.payload) if existing.payload else {}
                    except (TypeError, ValueError, json.JSONDecodeError):
                        conflict_payload = {}
                    conflict_integration = self._mapping_payload_member(conflict_payload, "integration")
                    return ProposalExecution(
                        success=True,
                        proposal=proposal,
                        audit_id=existing.id,
                        result="accepted",
                        detail=conflict_integration.get("detail") or "idempotent request still pending integration execution",
                    )
                else:
                    return self._replay_existing_execution(
                        existing=processed_record,
                        replay_context=replay_context,
                        proposal=proposal,
                    )
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

        approval = self._approval_request_for_audit(existing.id)
        if approval is None and existing.result in {"awaiting_approval", "approved", "rejected", "expired"}:
            approval = self._ensure_approval_request(
                audit_event=existing,
                actor=existing.actor,
                proposal=proposal,
                requested_at=existing.created_at or datetime.utcnow(),
            )
        if approval is not None:
            approval = self._reconcile_approval_from_audit(approval=approval, audit_event=existing)
        if approval is not None:
            if approval.expires_at is not None and approval.expires_at <= datetime.utcnow() and approval.status == "pending":
                self._expire_approval_request(approval=approval, audit_event=existing)
                self.db.flush()
            if approval.status == "approved":
                return ProposalExecution(
                    success=True,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="approved",
                    detail=detail,
                )
            if approval.status == "rejected":
                return ProposalExecution(
                    success=False,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="rejected",
                    detail=detail,
                )
            if approval.status == "expired":
                return ProposalExecution(
                    success=False,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="rejected",
                    detail="approval expired",
                )
            if approval.status == "executed":
                return ProposalExecution(
                    success=True,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="executed",
                    detail=detail,
                )
            if approval.status == "pending":
                return ProposalExecution(
                    success=True,
                    proposal=proposal,
                    audit_id=existing.id,
                    result="requires_approval",
                    detail=detail,
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
        if existing.result == "approved":
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=existing.id,
                result="approved",
                detail=detail,
            )
        if existing.result == "rejected":
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
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
                success=True,
                proposal=proposal,
                audit_id=existing.id,
                result="accepted",
                detail=integration_detail or "idempotent request still pending integration execution",
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
