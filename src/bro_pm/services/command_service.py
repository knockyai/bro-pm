from __future__ import annotations

import json
import time
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

    def _apply_action(self, proposal: CommandProposal) -> None:
        # Minimal command semantics for MVP: pause/unpause safe switch.
        if proposal.action == "pause_project" and proposal.project_id:
            self.db.query(models.Project).filter_by(id=proposal.project_id).update(
                {models.Project.safe_paused: True},
                synchronize_session=False,
            )
        elif proposal.action == "unpause_project" and proposal.project_id:
            self.db.query(models.Project).filter_by(id=proposal.project_id).update(
                {models.Project.safe_paused: False},
                synchronize_session=False,
            )
