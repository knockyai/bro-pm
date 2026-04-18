from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import schemas
from ...services.gateway_service import (
    DueActionConflictError,
    DueActionNotFoundError,
    GatewayService,
)

router = APIRouter(prefix="/gateway", tags=["gateway"])


@router.post("/due-actions:claim", response_model=schemas.DueActionClaimResponse)
def claim_due_actions(
    payload: schemas.DueActionClaimRequest,
    db: Session = Depends(get_db_session),
) -> schemas.DueActionClaimResponse:
    service = GatewayService(db_session=db)
    items = service.claim_due_actions(gateway=payload.gateway, limit=payload.limit)
    return schemas.DueActionClaimResponse(items=items)


@router.post("/due-actions/{due_action_id}/delivery", response_model=schemas.DueActionResponse)
def record_due_action_delivery(
    due_action_id: str,
    payload: schemas.DueActionDeliveryUpdateRequest,
    db: Session = Depends(get_db_session),
) -> schemas.DueActionResponse:
    service = GatewayService(db_session=db)
    try:
        due_action = service.record_delivery(
            due_action_id=due_action_id,
            claim_token=payload.claim_token,
            status=payload.status,
            external_delivery_id=payload.external_delivery_id,
            error_detail=payload.error_detail,
        )
    except DueActionNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except DueActionConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return schemas.DueActionResponse.model_validate(due_action)


@router.post("/events:ingest", response_model=schemas.InboundEventDispositionResponse)
def ingest_inbound_event(
    payload: schemas.InboundEventIngestRequest,
    db: Session = Depends(get_db_session),
) -> schemas.InboundEventDispositionResponse:
    service = GatewayService(db_session=db)
    event = service.ingest_inbound_event(
        platform=payload.platform,
        chat_id=payload.chat_id,
        thread_id=payload.thread_id,
        actor=payload.actor,
        actor_role=payload.actor_role,
        project_id=payload.project_id,
        text=payload.text,
        normalized_intent=payload.normalized_intent,
        due_action_id=payload.due_action_id,
        pending_audit_id=payload.pending_audit_id,
        metadata=payload.metadata,
    )
    return schemas.InboundEventDispositionResponse(
        event_id=event.id,
        disposition=event.disposition,
        reason=event.decision_reason,
        due_action_id=event.due_action_id,
        pending_audit_id=event.pending_audit_id,
        project_id=event.project_id,
    )
