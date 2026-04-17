from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Header, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import models
from ...schemas import ProjectCreate, ProjectResponse, TaskCreate, TaskResponse, AuditResponse, RollbackRequest, RollbackResponse
from ...services.command_service import CommandService

router = APIRouter(prefix="/projects", tags=["projects"])


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
        created_at=event.created_at,
    )


def _task_to_response(task: models.Task) -> TaskResponse:
    return TaskResponse(
        id=str(task.id),
        project_id=str(task.project_id),
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
