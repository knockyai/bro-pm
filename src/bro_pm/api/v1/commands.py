from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import models
from ...schemas import (
    ApprovalDecisionRequest,
    ApprovalDecisionResponse,
    ApprovalResumeRequest,
    CommandRequest,
    CommandResponse,
)
from ...adapters.hermes_runtime import HermesAdapter
from ...policy import PolicyEngine
from ...services.command_service import CommandService

router = APIRouter(prefix="/commands", tags=["commands"])


@router.post("", response_model=CommandResponse)
def propose_and_execute(
    payload: CommandRequest,
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> CommandResponse:
    service = CommandService(db_session=db, hermes=HermesAdapter(), policy=PolicyEngine())
    proposal = service.parse(
        actor=payload.actor,
        command=payload.command_text,
        project_id=payload.project_id,
    )

    target_project_id = proposal.project_id
    if target_project_id:
        project = db.query(models.Project).filter_by(id=target_project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="project not found")

    execution = service.execute(
        actor=payload.actor,
        role=payload.role,
        proposal=proposal,
        actor_trusted=bool(actor_trusted),
        idempotency_key=payload.idempotency_key,
        dry_run=payload.dry_run,
        validate_integration=payload.validate_integration,
        execute_integration=payload.execute_integration,
    )

    return CommandResponse(
        accepted=execution.success,
        result=execution.result,
        action=proposal.action,
        target=proposal.project_id,
        detail=execution.detail,
        audit_id=execution.audit_id,
        proposal_id=str(execution.audit_id),
    )


@router.post("/{audit_event_id}/approval", response_model=ApprovalDecisionResponse)
def decide_command_approval(
    audit_event_id: str,
    payload: ApprovalDecisionRequest,
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> ApprovalDecisionResponse:
    service = CommandService(db_session=db, hermes=HermesAdapter(), policy=PolicyEngine())
    try:
        approval = service.decide_approval(
            audit_event_id=audit_event_id,
            actor=payload.actor,
            role=payload.role,
            approved=payload.approved,
            decision_text=payload.decision_text,
            actor_trusted=bool(actor_trusted),
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if message == "approval audit event not found" else 409
        raise HTTPException(status_code=status_code, detail=message) from exc
    return ApprovalDecisionResponse(
        audit_id=audit_event_id,
        approval_id=approval.id,
        status=approval.status,
        reviewer_actor=approval.reviewer_actor,
        reviewer_role=approval.reviewer_role,
        decision_text=approval.decision_text,
    )


@router.post("/{audit_event_id}/resume", response_model=CommandResponse)
def resume_command_approval(
    audit_event_id: str,
    payload: ApprovalResumeRequest,
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> CommandResponse:
    service = CommandService(db_session=db, hermes=HermesAdapter(), policy=PolicyEngine())
    execution = service.resume_approval(
        audit_event_id=audit_event_id,
        actor=payload.actor,
        role=payload.role,
        actor_trusted=bool(actor_trusted),
    )
    return CommandResponse(
        accepted=execution.success,
        result=execution.result,
        action=execution.proposal.action,
        target=execution.proposal.project_id,
        detail=execution.detail,
        audit_id=execution.audit_id,
        proposal_id=str(execution.audit_id),
    )
