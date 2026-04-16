from __future__ import annotations

import importlib
import sys
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient


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
