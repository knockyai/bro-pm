from __future__ import annotations

from datetime import datetime, timezone
import importlib
import sys
from uuid import uuid4

import pytest
from pydantic import ValidationError

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult
from bro_pm.schemas import (
    GoalCreate,
    OnboardingTeamInput,
    ProjectCreate,
    ProjectOnboardingCreate,
    TaskCreate,
    TaskDecompositionRequest,
)


def _naive_utc(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute)


@pytest.fixture
def db_context(tmp_path, monkeypatch):
    db_path = tmp_path / f"bro_pm_autonomy_state_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("BRO_PM_DATABASE_URL", db_url)

    for mod_name in ("bro_pm.database", "bro_pm.api.v1.projects"):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    projects_api = importlib.import_module("bro_pm.api.v1.projects")

    session = database.SessionLocal()
    try:
        yield session, projects_api
    finally:
        session.rollback()
        session.close()


def test_onboarding_seeds_executor_capacity_profiles(db_context, monkeypatch):
    session, projects_api = db_context

    def notion_execute_stub(*, action: str, payload: dict):
        assert action == "create_task"
        return IntegrationResult(ok=True, detail="notion executed: create_task")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", notion_execute_stub)

    response = projects_api.onboard_project(
        ProjectOnboardingCreate(
            name="Project Nova",
            slug=f"project-nova-{uuid4().hex[:8]}",
            description="autonomy state onboarding",
            timezone="UTC",
            commitment_due_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            boss="olga",
            admin="alice",
            reporting_cadence="weekly",
            communication_integrations=["slack"],
            board_integration="notion",
            team=[
                OnboardingTeamInput(name="operations", owner="alice", capacity=3),
                OnboardingTeamInput(name="qa", owner="bob", capacity=2),
            ],
        ),
        db=session,
    )

    profiles = projects_api.list_capacity_profiles(response.project.id, db=session)

    assert response.project.commitment_due_at == _naive_utc(2026, 5, 1, 12, 0)
    assert [(profile.team_name, profile.actor, profile.capacity_units, profile.load_units, profile.source) for profile in profiles] == [
        ("operations", "alice", 3, 0, "onboarding"),
        ("qa", "bob", 2, 0, "onboarding"),
    ]


def test_onboarding_schema_rejects_duplicate_capacity_profiles():
    with pytest.raises(ValidationError) as exc_info:
        ProjectOnboardingCreate(
            name="Project Nova",
            slug=f"project-nova-{uuid4().hex[:8]}",
            description="autonomy state onboarding",
            timezone="UTC",
            boss="olga",
            admin="alice",
            reporting_cadence="weekly",
            communication_integrations=["slack"],
            board_integration="notion",
            team=[
                OnboardingTeamInput(name="operations", owner="alice", capacity=3),
                OnboardingTeamInput(name="operations", owner="alice", capacity=2),
            ],
        )

    assert "team entries must be unique by name and owner" in str(exc_info.value)


def test_project_and_goal_commitment_dates_are_persisted(db_context):
    session, projects_api = db_context

    project = projects_api.create_project(
        ProjectCreate(
            name="Commitment Project",
            slug=f"commitment-project-{uuid4().hex[:8]}",
            description="project with commitment state",
            visibility="internal",
            commitment_due_at=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
            metadata={"team": "ops"},
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )
    goal = projects_api.create_goal(
        project.id,
        GoalCreate(
            title="Hit committed milestone",
            description="goal carries target date",
            status="active",
            commitment_due_at=datetime(2026, 4, 25, 9, 30, tzinfo=timezone.utc),
            tasks=[],
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    stored_project = session.get(models.Project, project.id)
    stored_goal = session.get(models.Goal, goal.id)

    assert project.commitment_due_at == _naive_utc(2026, 5, 1, 12, 0)
    assert goal.commitment_due_at == _naive_utc(2026, 4, 25, 9, 30)
    assert stored_project.commitment_due_at == _naive_utc(2026, 5, 1, 12, 0)
    assert stored_goal.commitment_due_at == _naive_utc(2026, 4, 25, 9, 30)


def test_task_progress_timestamp_is_persisted_and_updates_capacity_load(db_context):
    session, projects_api = db_context

    project = projects_api.create_project(
        ProjectCreate(
            name="Progress Project",
            slug=f"progress-project-{uuid4().hex[:8]}",
            description="task progress state",
            visibility="internal",
            metadata={"team": "ops"},
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )
    session.add(
        models.ExecutorCapacityProfile(
            project_id=project.id,
            team_name="operations",
            actor="alice",
            capacity_units=3,
            load_units=0,
            source="manual",
        )
    )
    session.flush()

    task = projects_api.create_task(
        project.id,
        TaskCreate(
            title="Ship task progress heartbeat",
            description="task carries last progress timestamp",
            status="in_progress",
            assignee="alice",
            last_progress_at=datetime(2026, 4, 18, 10, 15, tzinfo=timezone.utc),
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    profiles = projects_api.list_capacity_profiles(project.id, db=session)
    listed_tasks = projects_api.list_tasks(project.id, db=session)

    assert task.last_progress_at == _naive_utc(2026, 4, 18, 10, 15)
    assert listed_tasks[0].last_progress_at == _naive_utc(2026, 4, 18, 10, 15)
    assert [(profile.actor, profile.load_units) for profile in profiles] == [("alice", 1)]


def test_goal_auto_decomposition_persists_generated_tasks_and_balances_capacity(db_context):
    session, projects_api = db_context

    project = projects_api.create_project(
        ProjectCreate(
            name="Planner Project",
            slug=f"planner-project-{uuid4().hex[:8]}",
            description="goal auto decomposition path",
            visibility="internal",
            metadata={"team": "ops"},
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )
    session.add(
        models.ExecutorCapacityProfile(
            project_id=project.id,
            team_name="delivery",
            actor="alice",
            capacity_units=2,
            load_units=0,
            source="manual",
        )
    )
    session.add(
        models.ExecutorCapacityProfile(
            project_id=project.id,
            team_name="qa",
            actor="bob",
            capacity_units=2,
            load_units=0,
            source="manual",
        )
    )
    session.flush()
    projects_api.create_task(
        project.id,
        TaskCreate(
            title="Existing Alice work",
            description="creates current load for alice",
            status="in_progress",
            assignee="alice",
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    goal = projects_api.create_goal(
        project.id,
        GoalCreate(
            title="Ship planner-backed decomposition",
            description="Need deterministic planning and backend assignment.",
            status="active",
            auto_decompose=True,
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    assert [task.title for task in goal.tasks] == [
        "Clarify scope for Ship planner-backed decomposition",
        "Execute next slice for Ship planner-backed decomposition",
        "Verify and report for Ship planner-backed decomposition",
    ]
    assert [task.assignee for task in goal.tasks] == ["bob", "alice", "bob"]

    profiles = projects_api.list_capacity_profiles(project.id, db=session)
    assert [(profile.actor, profile.load_units) for profile in profiles] == [("alice", 2), ("bob", 2)]


def test_task_decomposition_persists_follow_ups_under_active_goal_and_respects_capacity(db_context):
    session, projects_api = db_context

    project = projects_api.create_project(
        ProjectCreate(
            name="Task Planner Project",
            slug=f"task-planner-project-{uuid4().hex[:8]}",
            description="task decomposition path",
            visibility="internal",
            metadata={"team": "ops"},
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )
    session.add(
        models.ExecutorCapacityProfile(
            project_id=project.id,
            team_name="delivery",
            actor="alice",
            capacity_units=1,
            load_units=0,
            source="manual",
        )
    )
    session.flush()

    goal = projects_api.create_goal(
        project.id,
        GoalCreate(
            title="Deliver task decomposition support",
            description="Parent goal for follow-up tasks.",
            status="active",
            tasks=[],
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )
    task = projects_api.create_task(
        project.id,
        TaskCreate(
            title="Prepare release checklist",
            description="Undecomposed task that needs explicit next steps.",
            status="todo",
            assignee="alice",
        ),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    generated_tasks = projects_api.decompose_task(
        project.id,
        task.id,
        TaskDecompositionRequest(),
        actor="alice",
        role="admin",
        actor_trusted=True,
        db=session,
    )

    assert [item.assignee for item in generated_tasks] == [None, None, None]
    assert {item.goal_id for item in generated_tasks} == {goal.id}
    assert all("Prepare release checklist" in (item.description or "") for item in generated_tasks)
