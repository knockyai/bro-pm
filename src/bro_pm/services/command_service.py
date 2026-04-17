from __future__ import annotations

import json
import time
from datetime import datetime
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import models
from ..adapters.hermes_runtime import HermesAdapter
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


class CommandService:
    """Convert parsed commands into durable audit-ready operations."""

    def __init__(self, db_session: Session, hermes: HermesAdapter | None = None, policy: PolicyEngine | None = None):
        self.db = db_session
        self.hermes = hermes or HermesAdapter()
        self.policy = policy or PolicyEngine()

    def parse(self, *, actor: str, command: str, project_id: str | None) -> CommandProposal:
        proposal = self.hermes.propose(actor=actor, command_text=command)
        if not proposal.project_id and project_id:
            proposal.project_id = project_id
        return proposal

    def execute(
        self,
        *,
        actor: str,
        role: str,
        proposal: CommandProposal,
        actor_trusted: bool = True,
        idempotency_key: str | None = None,
    ) -> ProposalExecution:
        project_id = proposal.project_id
        replay_context = {
            "actor": actor,
            "role": role,
            "actor_trusted": actor_trusted,
            "proposal": proposal.model_dump(),
        }
        if idempotency_key:
            existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            if existing:
                return self._replay_existing_execution(
                    existing=existing,
                    replay_context=replay_context,
                    proposal=proposal,
                )

        if proposal.action == "draft_boss_escalation":
            if not project_id:
                decision = PolicyDecision(False, "project context required for draft_boss_escalation")
            else:
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

        if decision.allowed:
            if proposal.requires_approval or decision.requires_approval:
                response_result = "requires_approval"
                stored_result = "awaiting_approval"
            else:
                response_result = "executed"
                stored_result = "accepted"
                detail = "policy accepted"

        payload = {
            "actor": actor,
            "auth": {
                "role": role,
                "actor_trusted": actor_trusted,
            },
            "proposal": proposal.model_dump(),
            "policy": decision.__dict__,
        }
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

        if success and response_result == "executed":
            self._apply_action(proposal)
            self.db.query(models.AuditEvent).filter_by(id=record.id).update(
                {models.AuditEvent.result: "executed"},
                synchronize_session=False,
            )

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

    def _wait_for_existing_idempotent_record(
        self,
        idempotency_key: str | None,
        *,
        attempts: int = 20,
        delay_seconds: float = 0.01,
    ) -> models.AuditEvent | None:
        if not idempotency_key:
            return None

        for attempt in range(attempts):
            existing = self.db.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            if existing:
                return existing
            if attempt < attempts - 1:
                time.sleep(delay_seconds)
                self.db.expire_all()
        return None

    def _replay_existing_execution(
        self,
        *,
        existing: models.AuditEvent,
        replay_context: dict,
        proposal: CommandProposal,
    ) -> ProposalExecution:
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
        existing_context = {
            "actor": existing.actor,
            "role": existing_payload.get("auth", {}).get("role"),
            "actor_trusted": existing_payload.get("auth", {}).get("actor_trusted"),
            "proposal": existing_payload.get("proposal", {}),
        }
        if existing_context != replay_context:
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=existing.id,
                result="rejected",
                detail="idempotency key already used for different request context",
            )

        detail = existing_payload.get("policy", {}).get("reason", "replayed idempotent result")
        if existing.result == "denied":
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
