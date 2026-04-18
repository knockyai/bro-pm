from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from fastapi import HTTPException
from sqlalchemy.orm import Session

from .. import models
from ..integrations import INTEGRATIONS, IntegrationError
from ..schemas import GoalCreate
from .planner_service import PlannerService
from .planning_state import seed_capacity_profiles, sync_executor_load
from .tracker_credentials import upsert_tracker_credentials


DEFAULT_COMMUNICATION_INTEGRATIONS = ["telegram"]
DEFAULT_POLICY = "default_mvp"
DEFAULT_EMPLOYEE_CAPACITY_UNITS = 160


@dataclass(frozen=True)
class TrackerCredentialInput:
    provider: str
    config: dict[str, str] = field(default_factory=dict)
    secrets: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class InitialGoalInput:
    title: str
    description: str | None = None
    commitment_due_at: datetime | None = None
    auto_decompose: bool = False
    max_generated_tasks: int = 3


@dataclass(frozen=True)
class OnboardingExecutionInput:
    name: str
    slug: str
    description: str | None
    timezone: str | None
    commitment_due_at: datetime | None
    created_by: str | None
    visibility: str
    boss: str
    admin: str
    reporting_cadence: str
    board_integration: str
    team: list[dict]
    metadata: dict
    communication_integrations: list[str] = field(default_factory=lambda: list(DEFAULT_COMMUNICATION_INTEGRATIONS))
    tracker_credentials: TrackerCredentialInput | None = None
    employee_rows: list[dict[str, str | int]] = field(default_factory=list)
    initial_goal: InitialGoalInput | None = None


@dataclass(frozen=True)
class OnboardingExecutionResult:
    project: models.Project
    initial_goal: models.Goal | None = None


def execute_project_onboarding(
    db: Session,
    *,
    payload: OnboardingExecutionInput,
) -> OnboardingExecutionResult:
    if not payload.slug:
        raise HTTPException(status_code=400, detail="slug required")

    existing = db.query(models.Project).filter_by(slug=payload.slug).first()
    if existing:
        raise HTTPException(status_code=409, detail="slug already exists")

    existing_name = db.query(models.Project).filter_by(name=payload.name).first()
    if existing_name:
        raise HTTPException(status_code=409, detail="project name already exists")

    communication_integrations = list(payload.communication_integrations or DEFAULT_COMMUNICATION_INTEGRATIONS)
    board_integration = payload.board_integration.strip().lower()
    if board_integration not in {"jira", "notion", "trello", "yandex_tracker"}:
        raise HTTPException(status_code=422, detail=f"unsupported board integration: {board_integration}")

    project = models.Project(
        name=payload.name,
        slug=payload.slug,
        description=payload.description,
        timezone=payload.timezone,
        commitment_due_at=payload.commitment_due_at,
        visibility=payload.visibility,
        safe_paused=False,
        created_by=payload.created_by or payload.admin,
        metadata_json=dict(payload.metadata or {}),
    )
    db.add(project)
    db.flush()

    seed_capacity_profiles(
        db,
        project_id=project.id,
        team_entries=payload.team,
        source="onboarding",
    )

    memberships = [models.ProjectMembership(project_id=project.id, actor=payload.boss, role="owner")]
    if payload.admin != payload.boss:
        memberships.append(models.ProjectMembership(project_id=project.id, actor=payload.admin, role="admin"))
    db.add_all(memberships)

    tracker_config = {}
    tracker_secrets = {}
    if payload.tracker_credentials is not None:
        tracker_config = dict(payload.tracker_credentials.config)
        tracker_secrets = dict(payload.tracker_credentials.secrets)
        upsert_tracker_credentials(
            db,
            project_id=project.id,
            provider=payload.tracker_credentials.provider,
            config=tracker_config,
            secrets=tracker_secrets,
        )

    project.metadata_json = _build_project_metadata(
        existing_metadata=project.metadata_json,
        board_integration=board_integration,
        reporting_cadence=payload.reporting_cadence,
        communication_integrations=communication_integrations,
        team=payload.team,
        employee_rows=payload.employee_rows,
        tracker_config=tracker_config,
    )

    try:
        smoke_result = INTEGRATIONS[board_integration].execute(
            action="create_task",
            payload={
                "project_id": project.id,
                "title": "Synthetic onboarding smoke check",
                "actor": payload.admin,
                "project_metadata": project.metadata_json or {},
                "tracker_credentials": {
                    "config": tracker_config,
                    "secrets": tracker_secrets,
                },
            },
        )
        gate_checks = {
            "policy_attached": True,
            "communication_ready": bool(communication_integrations),
            "board_sync_healthy": bool(smoke_result.ok),
            "safe_pause_default_off": not project.safe_paused,
        }
        smoke_detail = smoke_result.detail or f"{board_integration} executed: create_task"
        onboarding_metadata = _build_onboarding_metadata(
            status="active",
            reporting_cadence=payload.reporting_cadence,
            communication_integrations=communication_integrations,
            board_integration=board_integration,
            team=payload.team,
            employee_rows=payload.employee_rows,
            gate_checks=gate_checks,
            smoke_status="passed" if smoke_result.ok else "failed",
            smoke_detail=smoke_detail,
        )
        (project.metadata_json or {})["onboarding"] = onboarding_metadata

        created_goal = None
        if payload.initial_goal is not None:
            created_goal = _create_initial_goal(db, project_id=project.id, goal=payload.initial_goal)
            onboarding_metadata["initial_goal"] = {
                "title": created_goal.title,
                "status": created_goal.status,
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
        if created_goal is not None:
            db.refresh(created_goal)
        return OnboardingExecutionResult(project=project, initial_goal=created_goal)
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
        (project.metadata_json or {})["onboarding"] = _build_onboarding_metadata(
            status="failed",
            reporting_cadence=payload.reporting_cadence,
            communication_integrations=communication_integrations,
            board_integration=board_integration,
            team=payload.team,
            employee_rows=payload.employee_rows,
            gate_checks=failure_gate_checks,
            smoke_status="failed",
            smoke_detail=detail,
        )
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


def employee_rows_to_team_entries(employee_rows: list[dict[str, str]]) -> list[dict[str, str | int]]:
    team_entries: list[dict[str, str | int]] = []
    seen: set[tuple[str, str]] = set()
    for row in employee_rows:
        owner = str(row.get("name", "")).strip()
        team_name = str(row.get("function", "")).strip()
        if not owner or not team_name:
            continue
        key = (team_name.lower(), owner.lower())
        if key in seen:
            raise HTTPException(status_code=422, detail="employee entries must be unique by function and owner")
        seen.add(key)
        team_entries.append(
            {
                "name": team_name,
                "owner": owner,
                "capacity": DEFAULT_EMPLOYEE_CAPACITY_UNITS,
            }
        )
    if not team_entries:
        raise HTTPException(status_code=422, detail="at least one employee is required")
    return team_entries


def _build_project_metadata(
    *,
    existing_metadata: dict | None,
    board_integration: str,
    reporting_cadence: str,
    communication_integrations: list[str],
    team: list[dict],
    employee_rows: list[dict[str, str | int]],
    tracker_config: dict[str, str],
) -> dict:
    metadata = dict(existing_metadata or {})
    integrations = dict(metadata.get("integrations") or {})
    board_metadata = dict(integrations.get(board_integration) or {})
    board_metadata.update(tracker_config)
    integrations[board_integration] = board_metadata
    metadata["integrations"] = integrations
    metadata["onboarding"] = _build_onboarding_metadata(
        status="draft",
        reporting_cadence=reporting_cadence,
        communication_integrations=communication_integrations,
        board_integration=board_integration,
        team=team,
        employee_rows=employee_rows,
        gate_checks={
            "policy_attached": False,
            "communication_ready": bool(communication_integrations),
            "board_sync_healthy": False,
            "safe_pause_default_off": True,
        },
        smoke_status="pending",
        smoke_detail="",
    )
    return metadata


def _build_onboarding_metadata(
    *,
    status: str,
    reporting_cadence: str,
    communication_integrations: list[str],
    board_integration: str,
    team: list[dict],
    employee_rows: list[dict[str, str | int]],
    gate_checks: dict[str, bool],
    smoke_status: str,
    smoke_detail: str,
) -> dict:
    return {
        "status": status,
        "policy": DEFAULT_POLICY,
        "reporting_cadence": reporting_cadence,
        "communication_integrations": list(communication_integrations),
        "board_integration": board_integration,
        "team": [dict(entry) for entry in team],
        "employees": [dict(entry) for entry in employee_rows],
        "gate_checks": dict(gate_checks),
        "smoke_check": {
            "status": smoke_status,
            "detail": smoke_detail,
        },
    }


def _create_initial_goal(db: Session, *, project_id: str, goal: InitialGoalInput) -> models.Goal:
    goal_record = models.Goal(
        project_id=project_id,
        title=goal.title,
        description=goal.description,
        status="active",
        commitment_due_at=goal.commitment_due_at,
    )
    db.add(goal_record)
    db.flush()

    if goal.auto_decompose:
        recommendations = PlannerService(db).recommend_goal_tasks(
            goal_id=goal_record.id,
            max_tasks=min(max(goal.max_generated_tasks, 1), GoalCreate.model_fields["max_generated_tasks"].default),
        )
        for recommendation in recommendations:
            db.add(
                models.Task(
                    project_id=project_id,
                    goal_id=goal_record.id,
                    title=recommendation.title,
                    description=recommendation.description,
                    status=recommendation.status,
                    assignee=recommendation.assignee,
                    priority=recommendation.priority,
                )
            )
        db.flush()
        sync_executor_load(db, project_id=project_id)
    return goal_record
