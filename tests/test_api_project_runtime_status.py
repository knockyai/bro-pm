from __future__ import annotations

import importlib
from datetime import datetime

from bro_pm import models

from tests.test_api import _create_project, api_client


def test_api_project_runtime_status_returns_dashboard_safe_durable_summary(api_client):
    project = _create_project(api_client)

    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        stored_project = session.get(models.Project, project["id"])
        assert stored_project is not None
        stored_project.safe_paused = True
        stored_project.updated_at = datetime(2026, 4, 19, 11, 0, 0)

        goal = models.Goal(
            project_id=project["id"],
            title="Runtime status goal",
            status="active",
            updated_at=datetime(2026, 4, 19, 11, 1, 0),
        )
        session.add(goal)
        session.flush()

        session.add(
            models.Task(
                project_id=project["id"],
                goal_id=goal.id,
                title="Runtime task",
                status="in_progress",
                updated_at=datetime(2026, 4, 19, 11, 2, 0),
            )
        )

        audit = models.AuditEvent(
            project_id=project["id"],
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project["id"],
            payload="{}",
            result="pending_integration",
            created_at=datetime(2026, 4, 19, 11, 3, 0),
        )
        session.add(audit)
        session.flush()
        session.add(
            models.ExecutionOutbox(
                audit_event_id=audit.id,
                project_id=project["id"],
                integration_name="notion",
                integration_action="create_task",
                payload_json={},
                status="queued",
                available_at=datetime(2026, 4, 19, 11, 3, 0),
                updated_at=datetime(2026, 4, 19, 11, 4, 0),
            )
        )
        session.commit()
    finally:
        session.close()

    response = api_client.get(
        f"/api/v1/projects/{project['id']}/runtime-status",
        params={"role": "operator"},
        headers={"x-actor-trusted": "true"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "project_id": project["id"],
        "safe_paused": True,
        "active_goal_count": 1,
        "task_counts": {
            "total": 1,
            "open": 1,
        },
        "approvals": {
            "pending": 0,
        },
        "executions": {
            "pending": 1,
            "failed": 0,
            "last_failure_at": None,
        },
        "revision_at": "2026-04-19T11:04:00",
        "generated_at": response.json()["generated_at"],
    }


def test_api_project_runtime_status_requires_operator_role(api_client):
    project = _create_project(api_client)

    response = api_client.get(
        f"/api/v1/projects/{project['id']}/runtime-status",
        params={"role": "viewer"},
        headers={"x-actor-trusted": "true"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "requires operator role"


def test_api_project_runtime_status_requires_trusted_actor(api_client):
    project = _create_project(api_client)

    response = api_client.get(
        f"/api/v1/projects/{project['id']}/runtime-status",
        params={"role": "operator"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "untrusted actor blocked"


def test_api_project_runtime_status_returns_not_found_for_unknown_project(api_client):
    response = api_client.get(
        "/api/v1/projects/not-a-real-project/runtime-status",
        params={"role": "operator"},
        headers={"x-actor-trusted": "true"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "project not found"
