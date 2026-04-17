from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import models
from ...schemas import CommandRequest, CommandResponse
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
