from __future__ import annotations

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import models
from ...schemas import CommandRequest, CommandResponse
from ...adapters.hermes_runtime import HermesAdapter
from ...policy import PolicyEngine
from ...services.command_service import CommandService
from ...config import Settings, settings
from ...database import Database

router = APIRouter(prefix="/commands", tags=["commands"])


def _command_service(db: Session) -> CommandService:
    # request-scoped lightweight composition for now
    db_adapter = Database.create_from_url(str(Settings().database_url)) if False else None
    # keep runtime simple; re-use fastapi dependency session for audit writes
    service = CommandService(Database(lambda: db), HermesAdapter(), PolicyEngine())
    # above monkey pattern ensures only API-level session is used
    return service


@router.post("", response_model=CommandResponse)
def propose_and_execute(
    payload: CommandRequest,
    actor_trusted: bool | None = Header(default=False, alias="x-actor-trusted"),
    actor: str = Header(default="operator", alias="x-actor"),
    role: str = Header(default="viewer", alias="x-role"),
    db: Session = Depends(get_db_session),
):
    if payload.project_id:
        project = db.query(models.Project).filter_by(id=payload.project_id).first()
        if not project:
            raise HTTPException(status_code=404, detail="project not found")

    service = CommandService(db_adapter=db.adapter if hasattr(db, "adapter") else None, hermes=HermesAdapter(), policy=PolicyEngine())
    # fallback manual initialization using db session directly
    service = CommandService(Database(lambda: db), HermesAdapter(), PolicyEngine())
    proposal = service.parse_and_audit(
        actor=actor,
        role=role,
        command=payload.command_text,
        project_id=payload.project_id,
        idempotency_key=payload.idempotency_key,
    )

    execution = service.execute(
        actor=actor,
        role=role,
        proposal=proposal,
        actor_trusted=bool(actor_trusted),
    )

    return CommandResponse(
        accepted=execution.success,
        result=execution.result,
        action=proposal.action,
        target=payload.project_id,
        detail=execution.detail,
        audit_id=execution.audit_id,
        proposal_id=str(proposal.id),
    )
