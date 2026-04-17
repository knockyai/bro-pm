from __future__ import annotations

import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...integrations import IntegrationError
from ... import models
from ...schemas import (
    ProjectCreate,
    ProjectResponse,
    TaskCreate,
    TaskResponse,
    GoalCreate,
    GoalResponse,
    AuditResponse,
    ProjectReportRequest,
    ProjectReportResponse,
    RollbackRequest,
    RollbackResponse,
)
from ...policy import PolicyEngine
from ...services.command_service import CommandService
from ...services.reporting_service import ReportingService

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
        safe_paused=project.safe_paused,
        created_by=project.created_by,
        visibility=project.visibility,
        metadata=project.metadata_json or {},
        created_at=project.created_at,
        updated_at=project.updated_at,
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


def _audit_event_detail(raw_payload: str | None) -> str:
    try:
        payload = json.loads(raw_payload or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    detail = payload.get("integration", {}).get("detail")
    if isinstance(detail, str) and detail:
        return detail
    policy = payload.get("policy", {})
    if isinstance(policy, dict):
        reason = policy.get("reason")
        if isinstance(reason, str) and reason:
            return reason
    fallback = payload.get("detail")
    if isinstance(fallback, str):
        return fallback
    return ""


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
        visibility=payload.visibility,
        safe_paused=payload.safe_paused,
        created_by=payload.created_by,
        metadata_json=payload.metadata or {},
    )
    db.add(project)
    db.flush()
    db.refresh(project)
    return _project_to_response(project)


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

    policy_action = "publish_report" if payload.execute_publish else "audit_view"
    decision = PolicyEngine().evaluate(
        actor_role=payload.role,
        actor_trusted=bool(actor_trusted),
        action=policy_action,
        safe_paused=bool(project.safe_paused),
    )
    if not decision.allowed:
        raise HTTPException(status_code=403, detail=decision.reason)

    service = ReportingService(db_session=db)
    try:
        return service.generate_project_report(
            project=project,
            actor=payload.actor,
            execute_publish=payload.execute_publish,
        )
    except (ValueError, IntegrationError) as exc:
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
