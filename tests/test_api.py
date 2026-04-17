from __future__ import annotations

import importlib
import sys
from datetime import datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from bro_pm import models


@pytest.fixture
def api_client(tmp_path):
    db_path = tmp_path / f"bro_pm_api_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    for mod_name in ("bro_pm.database", "bro_pm.api.app", "bro_pm.api", "bro_pm.api.v1", "bro_pm.api.v1.commands", "bro_pm.api.v1.projects"):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url)) as client:
        yield client


def _create_project(client: TestClient) -> dict:
    slug = f"project-nova-{uuid4().hex[:8]}"
    payload = {
        "name": "Project Nova",
        "slug": slug,
        "description": "project under test",
        "visibility": "internal",
        "safe_paused": False,
        "metadata": {"team": "ops"},
    }
    response = client.post("/api/v1/projects", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == payload["name"]
    assert data["slug"] == payload["slug"]
    return data


def _goal_payload() -> dict:
    return {
        "title": "Deliver first onboarding milestone",
        "description": "Decompose high-level goal into executable tasks",
        "status": "active",
        "tasks": [
            {
                "title": "Design project plan",
                "description": "Write the first execution plan",
                "status": "todo",
                "priority": "medium",
            },
            {
                "title": "Assign owners",
                "description": "Identify owners for execution",
                "status": "todo",
                "priority": "medium",
            },
        ],
    }


def test_api_goal_intake_creates_goal_and_decomposes_tasks(api_client: TestClient):
    project = _create_project(api_client)
    goal_payload = _goal_payload()

    response = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=goal_payload)
    assert response.status_code == 201

    created_goal = response.json()
    assert created_goal["title"] == goal_payload["title"]
    assert created_goal["status"] == goal_payload["status"]
    assert created_goal["project_id"] == project["id"]
    assert len(created_goal["tasks"]) == 2

    goal_task_titles = {task["title"] for task in created_goal["tasks"]}
    expected_titles = {child["title"] for child in goal_payload["tasks"]}
    assert goal_task_titles == expected_titles

    listed_tasks = api_client.get(f"/api/v1/projects/{project['id']}/tasks").json()
    assert len(listed_tasks) == 2
    assert {task["project_id"] for task in listed_tasks} == {project["id"]}
    assert {task["goal_id"] for task in listed_tasks} == {created_goal["id"]}


def test_api_project_rejects_second_active_goal(api_client: TestClient):
    project = _create_project(api_client)

    first_goal = {
        "title": "Onboard goal",
        "description": "first intake",
        "status": "active",
        "tasks": [],
    }
    first_resp = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=first_goal)
    assert first_resp.status_code == 201

    second_goal = {
        "title": "Second active goal",
        "description": "should fail",
        "status": "active",
        "tasks": [],
    }
    second_resp = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=second_goal)
    assert second_resp.status_code == 409
    assert second_resp.json()["detail"] == "an active goal already exists for this project"


def test_api_goal_status_is_normalized_for_active_goal_semantics(api_client: TestClient):
    project = _create_project(api_client)

    first_goal = {
        "title": "Onboard goal",
        "description": "first intake",
        "status": " Active ",
        "tasks": [],
    }
    first_resp = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=first_goal)
    assert first_resp.status_code == 201
    assert first_resp.json()["status"] == "active"

    second_goal = {
        "title": "Second active goal",
        "description": "should fail",
        "status": "active",
        "tasks": [],
    }
    second_resp = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=second_goal)
    assert second_resp.status_code == 409
    assert second_resp.json()["detail"] == "an active goal already exists for this project"


def test_api_goal_intake_handles_inconsistent_multiple_active_goals(api_client: TestClient):
    project = _create_project(api_client)

    db_module = importlib.import_module("bro_pm.database")
    database_session = db_module.SessionLocal()
    try:
        database_session.execute(text("DROP INDEX IF EXISTS uq_goals_project_active"))
        database_session.add_all(
            [
                models.Goal(
                    id=f"goal-dup-{uuid4().hex[:8]}",
                    project_id=project["id"],
                    title="Existing active goal A",
                    status="active",
                ),
                models.Goal(
                    id=f"goal-dup-{uuid4().hex[:8]}",
                    project_id=project["id"],
                    title="Existing active goal B",
                    status="active",
                ),
            ]
        )
        database_session.commit()
    finally:
        database_session.close()

    response = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=_goal_payload())

    assert response.status_code == 409
    assert response.json()["detail"] == "an active goal already exists for this project"


def test_api_goal_intake_does_not_mask_unrelated_integrity_errors(tmp_path, monkeypatch):
    db_path = tmp_path / f"bro_pm_api_integrity_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
    ):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url), raise_server_exceptions=False) as client:
        project = _create_project(client)

        def explode_flush(self, *args, **kwargs):
            raise IntegrityError("INSERT INTO goals ...", {}, Exception("different constraint failure"))

        monkeypatch.setattr(Session, "flush", explode_flush)

        response = client.post(f"/api/v1/projects/{project['id']}/goals", json=_goal_payload())

    assert response.status_code == 500



def test_api_create_and_list_project(api_client: TestClient):

    project_data = _create_project(api_client)

    response = api_client.get("/api/v1/projects")
    assert response.status_code == 200

    listed = response.json()
    assert isinstance(listed, list)
    assert len(listed) == 1
    assert listed[0]["id"] == project_data["id"]
    assert listed[0]["slug"] == project_data["slug"]


def test_api_create_and_list_task_for_project(api_client: TestClient):
    project = _create_project(api_client)

    task_payload = {
        "title": "Draft release notes",
        "description": "Prepare notes for next release",
        "status": "todo",
        "assignee": "alice",
        "priority": "high",
        "policy_flags": ["needs-review"],
    }
    response = api_client.post(f"/api/v1/projects/{project['id']}/tasks", json=task_payload)
    assert response.status_code == 201
    created_task = response.json()
    assert created_task["title"] == task_payload["title"]

    response = api_client.get(f"/api/v1/projects/{project['id']}/tasks")
    assert response.status_code == 200
    tasks = response.json()
    assert isinstance(tasks, list)
    assert len(tasks) == 1
    assert tasks[0]["id"] == created_task["id"]
    assert tasks[0]["project_id"] == project["id"]


def test_api_pause_command_marks_project_safe_paused(api_client: TestClient):
    project = _create_project(api_client)

    command_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
    }
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )
    assert response.status_code == 200

    command_result = response.json()
    assert command_result["accepted"] is True
    assert command_result["result"] == "executed"
    assert command_result["action"] == "pause_project"

    response = api_client.get("/api/v1/projects")
    assert response.status_code == 200
    listed = response.json()
    assert listed[0]["safe_paused"] is True


def test_api_command_denial_and_approval_paths(api_client: TestClient):
    project = _create_project(api_client)

    deny_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "eve",
        "role": "admin",
    }
    deny_response = api_client.post("/api/v1/commands", json=deny_payload)
    assert deny_response.status_code == 200
    deny_result = deny_response.json()

    assert deny_result["accepted"] is False
    assert deny_result["result"] == "rejected"
    assert deny_result["detail"] == "untrusted actor blocked"

    approval_payload = {
        "command_text": "close task T-1",
        "actor": "alice",
        "role": "admin",
    }
    approval_response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=approval_payload,
    )
    assert approval_response.status_code == 200
    approval_result = approval_response.json()
    assert approval_result["accepted"] is True
    assert approval_result["result"] == "requires_approval"
    assert approval_result["action"] == "close_task"
    assert approval_result["detail"] == "approved with human confirmation"


def test_api_command_reuses_idempotency_key_without_crashing(api_client: TestClient):
    project = _create_project(api_client)
    command_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "idempotency_key": "pause-project-once",
    }

    first = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )
    second = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["result"] == "executed"
    assert second.json()["result"] == "executed"
    assert first.json()["audit_id"] == second.json()["audit_id"]


def test_api_command_rejects_same_idempotency_key_for_different_context(api_client: TestClient):
    project = _create_project(api_client)
    command_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "idempotency_key": "pause-project-context-lock",
    }

    first = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )
    second = api_client.post(
        "/api/v1/commands",
        json=command_payload,
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["result"] == "executed"
    assert second.json()["accepted"] is False
    assert second.json()["result"] == "rejected"
    assert second.json()["detail"] == "idempotency key already used for different request context"


def test_api_project_audit_list_is_newest_first_when_available(api_client: TestClient):
    project = _create_project(api_client)

    pause_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
    }
    pause_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=pause_payload,
    )
    assert pause_resp.status_code == 200

    resume_payload = {
        "command_text": f"resume project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
    }
    resume_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=resume_payload,
    )
    assert resume_resp.status_code == 200

    response = api_client.get(
        f"/api/v1/projects/{project['id']}/audit-events",
    )
    assert response.status_code == 200

    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 2
    assert {
        pause_resp.json()["audit_id"],
        resume_resp.json()["audit_id"],
    } == {events[0]["id"], events[1]["id"]}
    assert events[0]["project_id"] == project["id"]
    assert events[1]["project_id"] == project["id"]
    assert events[0]["action"] in {"pause_project", "unpause_project"}
    assert events[1]["action"] in {"pause_project", "unpause_project"}
    assert events[0]["result"] in {"accepted", "executed", "denied", "awaiting_approval"}
    assert "created_at" in events[0]

    first_created = datetime.fromisoformat(events[0]["created_at"].replace("Z", "+00:00"))
    second_created = datetime.fromisoformat(events[1]["created_at"].replace("Z", "+00:00"))
    assert first_created >= second_created

    assert {event["action"] for event in events} == {"pause_project", "unpause_project"}
    assert all(event["actor"] == "alice" for event in events)


def test_api_project_audit_list_missing_project_returns_404(api_client: TestClient):
    response = api_client.get(
        "/api/v1/projects/does-not-exist/audit-events",
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "project not found"


def test_api_project_audit_list_for_existing_project_without_events_is_empty(api_client: TestClient):
    project = _create_project(api_client)

    response = api_client.get(f"/api/v1/projects/{project['id']}/audit-events")
    assert response.status_code == 200
    assert response.json() == []


def test_api_project_audit_list_is_deterministic_for_ties(api_client: TestClient):
    project = _create_project(api_client)

    db_module = importlib.import_module("bro_pm.database")
    database_session = db_module.SessionLocal()
    try:
        shared_timestamp = datetime(2026, 1, 1, 0, 0, 0)
        first_event = models.AuditEvent(
            id="event-alpha",
            project_id=project["id"],
            actor="alice",
            action="pause_project",
            target_type="proposal",
            target_id=project["id"],
            payload='{"actor": "alice"}',
            result="accepted",
            created_at=shared_timestamp,
        )
        second_event = models.AuditEvent(
            id="event-omega",
            project_id=project["id"],
            actor="alice",
            action="unpause_project",
            target_type="proposal",
            target_id=project["id"],
            payload='{"actor": "alice"}',
            result="accepted",
            created_at=shared_timestamp,
        )
        database_session.add_all([first_event, second_event])
        database_session.commit()
    finally:
        database_session.close()

    response = api_client.get(
        f"/api/v1/projects/{project['id']}/audit-events",
    )
    assert response.status_code == 200

    events = response.json()
    assert len(events) == 2
    assert events[0]["id"] == "event-omega"
    assert events[1]["id"] == "event-alpha"


def test_api_project_rollback_reverses_pause_and_persists_rollback_record(api_client: TestClient):
    project = _create_project(api_client)

    pause_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
    }
    pause_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=pause_payload,
    )
    assert pause_resp.status_code == 200
    pause_result = pause_resp.json()
    assert pause_result["result"] == "executed"

    rollback_payload = {
        "actor": "alice",
        "role": "admin",
        "audit_event_id": pause_result["audit_id"],
        "reason": "undo accidental pause",
    }
    rollback_resp = api_client.post(
        f"/api/v1/projects/{project['id']}/rollback",
        headers={"x-actor-trusted": "true"},
        json=rollback_payload,
    )
    assert rollback_resp.status_code == 200
    rollback_result = rollback_resp.json()
    assert rollback_result["accepted"] is True
    assert rollback_result["result"] == "executed"
    assert rollback_result["action"] == "rollback_action"
    assert rollback_result["target"] == project["id"]
    assert rollback_result["rollback_record_id"]

    projects = api_client.get("/api/v1/projects").json()
    assert projects[0]["safe_paused"] is False

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    assert len(audit_events) == 2
    assert {event["action"] for event in audit_events} == {"rollback_action", "pause_project"}
    assert {event["id"] for event in audit_events} == {
        rollback_result["audit_id"],
        pause_result["audit_id"],
    }

    db_module = importlib.import_module("bro_pm.database")
    database_session = db_module.SessionLocal()
    try:
        rollback_record = database_session.query(models.RollbackRecord).filter_by(id=rollback_result["rollback_record_id"]).one()
        assert rollback_record.audit_event_id == pause_result["audit_id"]
        assert rollback_record.actor == "alice"
        assert rollback_record.reason == "undo accidental pause"
        assert rollback_record.executed is True
    finally:
        database_session.close()


def test_api_project_rollback_missing_project_returns_404(api_client: TestClient):
    response = api_client.post(
        "/api/v1/projects/does-not-exist/rollback",
        headers={"x-actor-trusted": "true"},
        json={
            "actor": "alice",
            "role": "admin",
            "audit_event_id": "missing-audit",
            "reason": "undo accidental pause",
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "project not found"
