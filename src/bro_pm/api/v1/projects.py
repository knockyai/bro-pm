from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException, Header, Query, status
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...integrations import INTEGRATIONS, IntegrationError
from ... import models
from ...schemas import (
    ProjectCreate,
    ProjectOnboardingCreate,
    ProjectOnboardingResponse,
    ProjectMembershipResponse,
    OnboardingGateChecks,
    OnboardingSmokeCheck,
    ProjectResponse,
    TaskCreate,
    TaskResponse,
    GoalCreate,
    GoalResponse,
    AuditResponse,
    AuditEventDetailResponse,
    ProjectReportRequest,
    ProjectReportResponse,
    RollbackRequest,
    RollbackResponse,
)

from ...policy import PolicyEngine
from ...services.command_service import CommandService
from ...services.reporting_service import ReportIdempotencyConflictError, ReportingService

router = APIRouter(prefix="/projects", tags=["projects"])


def _is_active_goal_conflict(exc: IntegrityError, *, status: str) -> bool:
    """Detect the IntegrityError raised by active-goal uniqueness collisions."""

    if status.strip().lower() != "active":
        return False

    statement = (exc.statement or "").lower()
    error_text = str(exc).lower()
    if "insert into goals" not in statement:
        return False
    if "uq_goals_project_active" in error_text:
        return True

    return "goals.project_id" in error_text and "unique" in error_text


def _project_to_response(project: models.Project) -> ProjectResponse:
    return ProjectResponse(
        id=str(project.id),
        name=project.name,
        slug=project.slug,
        description=project.description,
        timezone=project.timezone,
        safe_paused=project.safe_paused,
        created_by=project.created_by,
        visibility=project.visibility,
        metadata=project.metadata_json or {},
        created_at=project.created_at,
        updated_at=project.updated_at,
    )


def _membership_to_response(membership: models.ProjectMembership) -> ProjectMembershipResponse:
    return ProjectMembershipResponse(actor=membership.actor, role=membership.role)


def _onboarding_allowed_integration(name: str, allowed: set[str], integration_type: str) -> str:
    normalized = name.strip().lower()
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise HTTPException(status_code=422, detail=f"unsupported {integration_type} integration: {normalized}; allowed: {allowed_values}")
    return normalized


def _build_onboarding_response(project: models.Project) -> ProjectOnboardingResponse:
    metadata = project.metadata_json or {}
    onboarding = metadata.get("onboarding") or {}
    gate_checks = onboarding.get("gate_checks") or {}
    smoke_check = onboarding.get("smoke_check") or {}
    memberships = sorted(project.memberships, key=lambda membership: (membership.role, membership.actor))
    return ProjectOnboardingResponse(
        project=_project_to_response(project),
        timezone=project.timezone or "",
        policy=onboarding.get("policy", "default_mvp"),
        reporting_cadence=onboarding.get("reporting_cadence", "weekly"),
        memberships=[_membership_to_response(membership) for membership in memberships],
        gate_checks=OnboardingGateChecks(
            policy_attached=bool(gate_checks.get("policy_attached")),
            communication_ready=bool(gate_checks.get("communication_ready")),
            board_sync_healthy=bool(gate_checks.get("board_sync_healthy")),
            safe_pause_default_off=bool(gate_checks.get("safe_pause_default_off")),
        ),
        smoke_check=OnboardingSmokeCheck(
            status=str(smoke_check.get("status", "pending")),
            detail=str(smoke_check.get("detail", "")),
        ),
        status=str(onboarding.get("status", "draft")),
    )


def _audit_event_to_response(event: models.AuditEvent) -> AuditResponse:
    return AuditResponse(
        id=str(event.id),
        project_id=str(event.project_id) if event.project_id else None,
        actor=event.actor,
        action=event.action,
        target_type=event.target_type,
        target_id=event.target_id,
        result=event.result,
        detail=_audit_event_detail(event.payload),
        created_at=event.created_at,
    )


def _audit_event_payload(raw_payload: str | None) -> dict:
    try:
        payload = json.loads(raw_payload or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if isinstance(payload, dict):
        return payload
    return {}


def _audit_event_detail(raw_payload: str | None) -> str:
    payload = _audit_event_payload(raw_payload)
    if not payload:
        return ""

    integration = payload.get("integration")
    if isinstance(integration, dict):
        detail = integration.get("detail")
        if isinstance(detail, str) and detail:
            return detail

    policy = payload.get("policy")
    if isinstance(policy, dict):
        reason = policy.get("reason")
        if isinstance(reason, str) and reason:
            return reason

    fallback = payload.get("detail")
    if isinstance(fallback, str) and fallback:
        return fallback
    return ""


def _safe_audit_event_detail(raw_payload: str | None) -> str:
    return _audit_event_detail(raw_payload)


def _safe_audit_text(value: Any, *, max_length: int | None = 4000) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if max_length is not None and len(text) > max_length:
        return None
    if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
        return None
    return text


def _safe_audit_string_map(source: Any, allowed_keys: set[str]) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for key in allowed_keys:
        value = _safe_audit_text(source.get(key))
        if value is not None:
            sanitized[key] = value
    return sanitized


def _safe_audit_bool_map(source: Any, allowed_keys: set[str]) -> dict[str, bool]:
    if not isinstance(source, dict):
        return {}
    sanitized: dict[str, bool] = {}
    for key in allowed_keys:
        value = source.get(key)
        if isinstance(value, bool):
            sanitized[key] = value
    return sanitized


def _safe_audit_proposal_payload(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}

    safe_payload = _safe_audit_string_map(
        source,
        {
            "mode",
            "title",
            "target_type",
            "target_id",
            "escalation_message",
            "risk_level",
            "trace_label",
            "rollback_of_audit_event_id",
            "rollback_of_action",
        },
    )
    safe_payload.update(_safe_audit_bool_map(source, {"operator_confirmation"}))
    return safe_payload


def _safe_audit_idempotency_request(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}

    safe_request = _safe_audit_string_map(source, {"project_id", "actor", "role"})
    safe_request.update(
        _safe_audit_bool_map(
            source,
            {"actor_trusted", "execute_publish", "dry_run", "validate_integration", "execute_integration"},
        )
    )
    return safe_request


def _safe_audit_idempotency_replay(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict):
        return {}

    kind = _safe_audit_text(source.get("kind"))
    if kind is None:
        return {}

    safe_replay: dict[str, Any] = {"kind": kind}
    if kind != "response":
        detail = _safe_audit_text(source.get("detail"))
        if detail is not None:
            safe_replay["detail"] = detail
    return safe_replay


def _safe_audit_event_payload(raw_payload: str | None) -> dict[str, Any]:
    payload = _audit_event_payload(raw_payload)
    if not payload:
        return {}

    safe_payload: dict[str, Any] = {}

    auth = payload.get("auth")
    if isinstance(auth, dict):
        safe_auth = _safe_audit_string_map(auth, {"role"})
        safe_auth.update(
            _safe_audit_bool_map(auth, {"actor_trusted", "dry_run", "validate_integration", "execute_integration"})
        )
        if safe_auth:
            safe_payload["auth"] = safe_auth

    proposal = payload.get("proposal")
    if isinstance(proposal, dict):
        safe_proposal = _safe_audit_string_map(proposal, {"action", "project_id", "reason", "target_type", "target_id"})
        safe_proposal.update(_safe_audit_bool_map(proposal, {"requires_approval"}))
        proposal_payload = _safe_audit_proposal_payload(proposal.get("payload"))
        if proposal_payload:
            safe_proposal["payload"] = proposal_payload
        if safe_proposal:
            safe_payload["proposal"] = safe_proposal

    policy = payload.get("policy")
    if isinstance(policy, dict):
        safe_policy = _safe_audit_string_map(policy, {"reason"})
        safe_policy.update(_safe_audit_bool_map(policy, {"allowed", "requires_approval", "safe_pause_blocked"}))
        if safe_policy:
            safe_payload["policy"] = safe_policy

    integration = payload.get("integration")
    if isinstance(integration, dict):
        safe_integration = _safe_audit_string_map(integration, {"name", "action", "status", "detail"})
        if safe_integration:
            safe_payload["integration"] = safe_integration

    actor = _safe_audit_text(payload.get("actor"))
    if actor is not None:
        safe_payload["actor"] = actor

    visibility = _safe_audit_text(payload.get("visibility"))
    if visibility is not None:
        safe_payload["visibility"] = visibility

    target = _safe_audit_text(payload.get("target"))
    if target is not None:
        safe_payload["target"] = target

    created_via = _safe_audit_text(payload.get("created_via"))
    if created_via is not None:
        safe_payload["created_via"] = created_via

    idempotency = payload.get("idempotency")
    if isinstance(idempotency, dict):
        safe_idempotency: dict[str, Any] = {}
        request = _safe_audit_idempotency_request(idempotency.get("request"))
        if request:
            safe_idempotency["request"] = request
        replay = _safe_audit_idempotency_replay(idempotency.get("replay"))
        if replay:
            safe_idempotency["replay"] = replay
        if safe_idempotency:
            safe_payload["idempotency"] = safe_idempotency

    return safe_payload

def _audit_event_to_detail_response(event: models.AuditEvent) -> AuditEventDetailResponse:
    return AuditEventDetailResponse(
        id=str(event.id),
        project_id=str(event.project_id) if event.project_id else None,
        actor=event.actor,
        action=event.action,
        target_type=event.target_type,
        target_id=event.target_id,
        result=event.result,
        detail=_safe_audit_event_detail(event.payload),
        created_at=event.created_at,
        payload=_safe_audit_event_payload(event.payload),
    )


def _task_to_response(task: models.Task) -> TaskResponse:
    return TaskResponse(
        id=str(task.id),
        project_id=str(task.project_id),
        goal_id=str(task.goal_id) if task.goal_id else None,
        title=task.title,
        description=task.description,
        status=task.status,
        assignee=task.assignee,
        priority=task.priority,
        policy_flags=task.policy_flags or [],
        created_at=task.created_at,
        updated_at=task.updated_at,
        due_at=task.due_at,
    )


def _goal_to_response(goal: models.Goal) -> GoalResponse:
    return GoalResponse(
        id=str(goal.id),
        project_id=str(goal.project_id),
        title=goal.title,
        description=goal.description,
        status=goal.status,
        created_at=goal.created_at,
        updated_at=goal.updated_at,
        tasks=[_task_to_response(task) for task in goal.tasks],
    )


@router.get("", response_model=List[ProjectResponse])
def list_projects(db: Session = Depends(get_db_session)) -> List[ProjectResponse]:
    projects = db.query(models.Project).order_by(models.Project.created_at.desc()).all()
    return [_project_to_response(p) for p in projects]


@router.post("", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
def create_project(payload: ProjectCreate, db: Session = Depends(get_db_session)) -> ProjectResponse:
    if not payload.slug:
        raise HTTPException(status_code=400, detail="slug required")

    existing = db.query(models.Project).filter_by(slug=payload.slug).first()
    if existing:
        raise HTTPException(status_code=409, detail="slug already exists")

    project = models.Project(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        timezone=payload.timezone,
        visibility=payload.visibility,
        safe_paused=payload.safe_paused,
        created_by=payload.created_by,
        metadata_json=payload.metadata or {},
    )
    db.add(project)
    db.flush()
    db.refresh(project)
    return _project_to_response(project)


@router.post("/onboard", response_model=ProjectOnboardingResponse, status_code=status.HTTP_201_CREATED)
def onboard_project(payload: ProjectOnboardingCreate, db: Session = Depends(get_db_session)) -> ProjectOnboardingResponse:
    if not payload.slug:
        raise HTTPException(status_code=400, detail="slug required")

    existing = db.query(models.Project).filter_by(slug=payload.slug).first()
    if existing:
        raise HTTPException(status_code=409, detail="slug already exists")

    communication_integrations = [
        _onboarding_allowed_integration(name, {"slack", "telegram"}, "communication")
        for name in payload.communication_integrations
    ]
    board_integration = _onboarding_allowed_integration(payload.board_integration, {"jira", "notion", "trello"}, "board")

    project = models.Project(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        timezone=payload.timezone,
        visibility=payload.visibility,
        safe_paused=False,
        created_by=payload.created_by or payload.admin,
        metadata_json=payload.metadata or {},
    )
    db.add(project)
    db.flush()

    memberships = [models.ProjectMembership(project_id=project.id, actor=payload.boss, role="owner")]
    if payload.admin != payload.boss:
        memberships.append(models.ProjectMembership(project_id=project.id, actor=payload.admin, role="admin"))
    db.add_all(memberships)

    smoke_detail = ""
    try:
        smoke_result = INTEGRATIONS[board_integration].execute(
            action="create_task",
            payload={
                "project_id": project.id,
                "title": "Synthetic onboarding smoke check",
                "actor": payload.admin,
            },
        )
        gate_checks = {
            "policy_attached": True,
            "communication_ready": bool(communication_integrations),
            "board_sync_healthy": bool(smoke_result.ok),
            "safe_pause_default_off": not project.safe_paused,
        }
        smoke_detail = smoke_result.detail or f"{board_integration} executed: create_task"
        onboarding_metadata = {
            "status": "active",
            "policy": "default_mvp",
            "reporting_cadence": payload.reporting_cadence,
            "communication_integrations": communication_integrations,
            "board_integration": board_integration,
            "team": [team.model_dump() for team in payload.team],
            "gate_checks": gate_checks,
            "smoke_check": {
                "status": "passed" if smoke_result.ok else "failed",
                "detail": smoke_detail,
            },
        }
        project.metadata_json = {
            **(project.metadata_json or {}),
            "onboarding": onboarding_metadata,
        }
        db.add(
            models.AuditEvent(
                project_id=project.id,
                actor=payload.admin,
                action="onboard_project",
                target_type="project",
                target_id=project.id,
                payload=json.dumps(
                    {
                        "detail": smoke_detail,
                        "gate_checks": gate_checks,
                        "onboarding": onboarding_metadata,
                    },
                    ensure_ascii=False,
                ),
                result="executed" if smoke_result.ok else "failed",
            )
        )
        db.flush()
        db.refresh(project)
        return _build_onboarding_response(project)
    except IntegrationError as exc:
        detail = str(exc)
        project.safe_paused = True
        failure_gate_checks = {
            "policy_attached": True,
            "communication_ready": bool(communication_integrations),
            "board_sync_healthy": False,
            "safe_pause_default_off": False,
        }
        failure_event_created_at = datetime.utcnow()
        escalation_created_at = failure_event_created_at + timedelta(microseconds=1)
        project.metadata_json = {
            **(project.metadata_json or {}),
            "onboarding": {
                "status": "failed",
                "policy": "default_mvp",
                "reporting_cadence": payload.reporting_cadence,
                "communication_integrations": communication_integrations,
                "board_integration": board_integration,
                "team": [team.model_dump() for team in payload.team],
                "gate_checks": failure_gate_checks,
                "smoke_check": {
                    "status": "failed",
                    "detail": detail,
                },
            },
        }
        db.add(
            models.AuditEvent(
                project_id=project.id,
                actor=payload.admin,
                action="onboard_project",
                target_type="project",
                target_id=project.id,
                payload=json.dumps(
                    {
                        "detail": detail,
                        "gate_checks": failure_gate_checks,
                        "onboarding": (project.metadata_json or {}).get("onboarding", {}),
                    },
                    ensure_ascii=False,
                ),
                result="failed",
                created_at=failure_event_created_at,
            )
        )
        db.add(
            models.AuditEvent(
                project_id=project.id,
                actor=payload.admin,
                action="draft_boss_escalation",
                target_type="project",
                target_id=project.id,
                payload=json.dumps(
                    {
                        "detail": detail,
                        "proposal": {
                            "payload": {
                                "escalation_message": f"Onboarding failed for {project.slug}: {detail}",
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                result="requires_approval",
                created_at=escalation_created_at,
            )
        )
        db.commit()
        raise HTTPException(status_code=422, detail=detail) from exc


@router.post("/{project_id}/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
def create_task(project_id: str, payload: TaskCreate, db: Session = Depends(get_db_session)) -> TaskResponse:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    task = models.Task(
        project_id=project_id,
        title=payload.title,
        description=payload.description,
        status=payload.status,
        assignee=payload.assignee,
        priority=payload.priority,
        policy_flags=payload.policy_flags,
        due_at=payload.due_at,
    )
    db.add(task)
    db.flush()
    db.refresh(task)
    return _task_to_response(task)


@router.post("/{project_id}/goals", response_model=GoalResponse, status_code=status.HTTP_201_CREATED)
def create_goal(project_id: str, payload: GoalCreate, db: Session = Depends(get_db_session)) -> GoalResponse:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    if payload.status == "active":
        active_goal = (
            db.query(models.Goal)
            .filter(
                models.Goal.project_id == project_id,
                func.lower(func.trim(models.Goal.status)) == "active",
            )
            .first()
        )
        if active_goal:
            raise HTTPException(status_code=409, detail="an active goal already exists for this project")

    goal = models.Goal(
        project_id=project_id,
        title=payload.title,
        description=payload.description,
        status=payload.status,
    )

    try:
        db.add(goal)
        db.flush()

        for child_task in payload.tasks:
            task = models.Task(
                project_id=project_id,
                goal_id=goal.id,
                title=child_task.title,
                description=child_task.description,
                status=child_task.status,
                assignee=child_task.assignee,
                priority=child_task.priority,
                policy_flags=child_task.policy_flags,
                due_at=child_task.due_at,
            )
            db.add(task)

        db.flush()
        db.refresh(goal)
        return _goal_to_response(goal)
    except IntegrityError as exc:
        if _is_active_goal_conflict(exc, status=payload.status):
            raise HTTPException(
                status_code=409,
                detail="an active goal already exists for this project",
            ) from exc
        raise


@router.get("/{project_id}/tasks", response_model=List[TaskResponse])
def list_tasks(project_id: str, db: Session = Depends(get_db_session)) -> List[TaskResponse]:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")
    tasks = db.query(models.Task).filter_by(project_id=project_id).order_by(models.Task.created_at.desc()).all()
    return [_task_to_response(task) for task in tasks]


@router.get("/{project_id}/audit-events", response_model=List[AuditResponse])
def list_audit_events(
    project_id: str,
    db: Session = Depends(get_db_session),
) -> List[AuditResponse]:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    events = (
        db.query(models.AuditEvent)
        .filter_by(project_id=project_id)
        .order_by(
            models.AuditEvent.created_at.desc(),
            models.AuditEvent.id.desc(),
        )
        .all()
    )
    return [_audit_event_to_response(event) for event in events]


@router.get("/{project_id}/audit-events/{audit_event_id}", response_model=AuditEventDetailResponse)
def get_audit_event_detail(
    project_id: str,
    audit_event_id: str,
    role: str = Query(..., pattern="^(owner|admin|operator|viewer)$"),
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> AuditEventDetailResponse:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    decision = PolicyEngine().evaluate(
        actor_role=role,
        actor_trusted=bool(actor_trusted),
        action="audit_view",
        safe_paused=bool(project.safe_paused),
    )
    if not decision.allowed:
        raise HTTPException(status_code=403, detail=decision.reason)

    event = db.query(models.AuditEvent).filter_by(id=audit_event_id, project_id=project_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="audit event not found")

    return _audit_event_to_detail_response(event)


@router.post("/{project_id}/reports/project", response_model=ProjectReportResponse)
def generate_project_report(
    project_id: str,
    payload: ProjectReportRequest,
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> ProjectReportResponse:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    service = ReportingService(db_session=db)
    try:
        replayed = service.replay_existing_publish_if_available(
            project_id=project.id,
            actor=payload.actor,
            role=payload.role,
            actor_trusted=bool(actor_trusted),
            idempotency_key=payload.idempotency_key,
            execute_publish=payload.execute_publish,
        )
        if replayed is not None:
            return replayed

        policy_action = "publish_report" if payload.execute_publish else "audit_view"
        decision = PolicyEngine().evaluate(
            actor_role=payload.role,
            actor_trusted=bool(actor_trusted),
            action=policy_action,
            safe_paused=bool(project.safe_paused),
        )
        if not decision.allowed:
            raise HTTPException(status_code=403, detail=decision.reason)

        return service.generate_project_report(
            project=project,
            actor=payload.actor,
            role=payload.role,
            actor_trusted=bool(actor_trusted),
            execute_publish=payload.execute_publish,
            idempotency_key=payload.idempotency_key,
        )
    except ReportIdempotencyConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except IntegrationError as exc:
        if payload.execute_publish and not payload.idempotency_key:
            db.commit()
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/{project_id}/rollback", response_model=RollbackResponse)
def rollback_project_action(
    project_id: str,
    payload: RollbackRequest,
    actor_trusted: bool = Header(default=False, alias="x-actor-trusted"),
    db: Session = Depends(get_db_session),
) -> RollbackResponse:
    project = db.query(models.Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="project not found")

    service = CommandService(db_session=db)
    execution = service.rollback(
        actor=payload.actor,
        role=payload.role,
        audit_event_id=payload.audit_event_id,
        reason=payload.reason,
        actor_trusted=bool(actor_trusted),
        expected_project_id=project_id,
    )

    if execution.detail in {"audit event not found", "audit event does not target this project"}:
        raise HTTPException(status_code=404, detail=execution.detail)

    return RollbackResponse(
        accepted=execution.success,
        result=execution.result,
        action=execution.proposal.action,
        target=execution.proposal.project_id,
        detail=execution.detail,
        audit_id=execution.audit_id,
        rollback_record_id=execution.rollback_record_id,
    )
