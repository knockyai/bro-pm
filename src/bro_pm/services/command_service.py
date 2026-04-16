from __future__ import annotations

import json
from dataclasses import dataclass

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

        payload = {
            "actor": actor,
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
            result="denied" if not decision.allowed else "accepted",
            idempotency_key=idempotency_key,
        )
        self.db.add(record)
        self.db.flush()

        if not decision.allowed:
            return ProposalExecution(
                success=False,
                proposal=proposal,
                audit_id=record.id,
                result="rejected",
                detail=decision.reason,
            )

        if proposal.requires_approval or decision.requires_approval:
            record.result = "awaiting_approval"
            return ProposalExecution(
                success=True,
                proposal=proposal,
                audit_id=record.id,
                result="requires_approval",
                detail=decision.reason,
            )

        # execute in-process in MVP for known commands
        self._apply_action(proposal)
        record.result = "executed"
        return ProposalExecution(
            success=True,
            proposal=proposal,
            audit_id=record.id,
            result="executed",
            detail="policy accepted",
        )

    def _apply_action(self, proposal: CommandProposal) -> None:
        # Minimal command semantics for MVP: pause/unpause safe switch.
        if proposal.action == "pause_project" and proposal.project_id:
            project = self.db.get(models.Project, proposal.project_id)
            if project:
                project.safe_paused = True
        elif proposal.action == "unpause_project" and proposal.project_id:
            project = self.db.get(models.Project, proposal.project_id)
            if project:
                project.safe_paused = False
