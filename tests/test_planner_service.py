from __future__ import annotations

import importlib
import sys
from uuid import uuid4

import pytest

from bro_pm import models


@pytest.fixture
def planner_db(tmp_path, monkeypatch):
    db_path = tmp_path / f"bro_pm_planner_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"
    monkeypatch.setenv("BRO_PM_DATABASE_URL", db_url)

    for mod_name in (
        "bro_pm.database",
        "bro_pm.services.planner_service",
    ):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    planner_service = importlib.import_module("bro_pm.services.planner_service")

    session = database.SessionLocal()
    try:
        yield session, planner_service
    finally:
        session.rollback()
        session.close()


def _create_project(session) -> models.Project:
    project = models.Project(
        name=f"Project {uuid4().hex[:8]}",
        slug=f"project-{uuid4().hex[:8]}",
        description="planner test project",
        metadata_json={"team": "ops"},
    )
    session.add(project)
    session.flush()
    return project


def _create_goal(session, project_id: str) -> models.Goal:
    goal = models.Goal(
        project_id=project_id,
        title="Deliver onboarding automation MVP",
        description="Need next-step tasks for delivery, verification, and owner alignment.",
        status="active",
    )
    session.add(goal)
    session.flush()
    return goal


def _create_task(
    session,
    project_id: str,
    *,
    goal_id: str | None = None,
    assignee: str | None = None,
    status: str = "todo",
    title: str = "Prepare release checklist",
    description: str = "Undecomposed task that should produce concrete next steps.",
) -> models.Task:
    task = models.Task(
        project_id=project_id,
        goal_id=goal_id,
        title=title,
        description=description,
        status=status,
        priority="high",
        assignee=assignee,
    )
    session.add(task)
    session.flush()
    return task


def _create_capacity_profile(session, project_id: str, *, actor: str, capacity_units: int) -> None:
    session.add(
        models.ExecutorCapacityProfile(
            project_id=project_id,
            actor=actor,
            team_name=f"{actor}-team",
            capacity_units=capacity_units,
            load_units=0,
            source="test",
        )
    )
    session.flush()


def test_recommend_goal_tasks_generates_fixed_phases_and_balances_assignments(planner_db):
    session, planner_service = planner_db
    project = _create_project(session)
    goal = _create_goal(session, project.id)
    _create_capacity_profile(session, project.id, actor="alice", capacity_units=2)
    _create_capacity_profile(session, project.id, actor="bob", capacity_units=2)
    _create_task(
        session,
        project.id,
        assignee="alice",
        status="in_progress",
        title="Existing Alice load",
        description="Current work item already assigned to alice.",
    )

    planner = planner_service.PlannerService(session)

    recommendations = planner.recommend_goal_tasks(goal_id=goal.id)

    assert [task.title for task in recommendations] == [
        "Clarify scope for Deliver onboarding automation MVP",
        "Execute next slice for Deliver onboarding automation MVP",
        "Verify and report for Deliver onboarding automation MVP",
    ]
    assert [task.assignee for task in recommendations] == ["bob", "alice", "bob"]
    assert all(task.goal_id == goal.id for task in recommendations)


def test_recommend_task_tasks_avoids_over_capacity_assignments(planner_db):
    session, planner_service = planner_db
    project = _create_project(session)
    goal = _create_goal(session, project.id)
    task = _create_task(session, project.id, goal_id=goal.id)
    _create_capacity_profile(session, project.id, actor="alice", capacity_units=1)
    _create_capacity_profile(session, project.id, actor="bob", capacity_units=2)
    _create_task(
        session,
        project.id,
        assignee="alice",
        status="in_progress",
        title="Alice current work",
        description="Fills alice capacity.",
    )
    _create_task(
        session,
        project.id,
        assignee="bob",
        status="in_progress",
        title="Bob current work 1",
        description="Consumes bob capacity.",
    )
    _create_task(
        session,
        project.id,
        assignee="bob",
        status="todo",
        title="Bob current work 2",
        description="Consumes bob capacity.",
    )

    planner = planner_service.PlannerService(session)

    recommendations = planner.recommend_task_tasks(task_id=task.id)

    assert [item.assignee for item in recommendations] == [None, None, None]
    assert all(item.goal_id == goal.id for item in recommendations)
    assert all("Prepare release checklist" in (item.description or "") for item in recommendations)
