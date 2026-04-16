from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ... import models
from ...schemas import ProjectCreate, ProjectResponse, TaskCreate, TaskResponse

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
