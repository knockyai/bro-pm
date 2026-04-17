from __future__ import annotations

import json
import importlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationError, IntegrationResult
from bro_pm.services.reporting_service import ReportingService


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


def _create_project(client: TestClient, *, visibility: str = "internal") -> dict:
    slug = f"project-nova-{uuid4().hex[:8]}"
    payload = {
        "name": "Project Nova",
        "slug": slug,
        "description": "project under test",
        "visibility": visibility,
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



def _report_auth_payload(
    *,
    actor: str = "alice",
    role: str = "admin",
    execute_publish: bool | None = None,
    idempotency_key: str | None = None,
) -> dict:
    payload = {
        "actor": actor,
        "role": role,
    }
    if execute_publish is not None:
        payload["execute_publish"] = execute_publish
    if idempotency_key is not None:
        payload["idempotency_key"] = idempotency_key
    return payload


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


def test_api_create_project_rejects_path_like_slug(api_client: TestClient):
    response = api_client.post(
        "/api/v1/projects",
        json={
            "name": "Project Nova",
            "slug": "reports/project-nova",
            "description": "project under test",
            "visibility": "internal",
            "safe_paused": False,
        },
    )

    assert response.status_code == 422
    assert "must not contain '/'" in response.text


def test_api_create_project_rejects_path_like_visibility(api_client: TestClient):
    response = api_client.post(
        "/api/v1/projects",
        json={
            "name": "Project Nova",
            "slug": f"project-nova-{uuid4().hex[:8]}",
            "description": "project under test",
            "visibility": "internal/restricted",
            "safe_paused": False,
        },
    )

    assert response.status_code == 422
    assert "must not contain '/'" in response.text


def test_api_create_project_normalizes_whitespace_only_visibility_to_internal(api_client: TestClient):
    response = api_client.post(
        "/api/v1/projects",
        json={
            "name": "Project Nova",
            "slug": f"project-nova-{uuid4().hex[:8]}",
            "description": "project under test",
            "visibility": "   ",
            "safe_paused": False,
        },
    )

    assert response.status_code == 201
    project = response.json()
    assert project["visibility"] == "internal"

    report_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert report_response.status_code == 200
    assert report_response.json()["visibility"] == "internal"



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


def test_api_pause_command_dry_run_does_not_mutate_project(api_client: TestClient):
    project = _create_project(api_client)

    command_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "dry_run": True,
    }
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )
    assert response.status_code == 200

    command_result = response.json()
    assert command_result["accepted"] is True
    assert command_result["result"] == "simulated"
    assert command_result["action"] == "pause_project"
    assert command_result["target"] == project["id"]

    response = api_client.get("/api/v1/projects")
    assert response.status_code == 200
    listed = response.json()
    assert listed[0]["safe_paused"] is False

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    assert len(audit_events) == 1
    assert audit_events[0]["action"] == "pause_project"
    assert audit_events[0]["result"] == "simulated"


def test_api_command_dry_run_replays_separate_from_live_execution(api_client: TestClient):
    project = _create_project(api_client)
    command_payload = {
        "command_text": f"pause project {project['id']}",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "dry_run": True,
        "idempotency_key": "pause-dry-run-key",
    }

    dry_run = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )

    live_run = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": f"pause project {project['id']}",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
            "idempotency_key": "pause-dry-run-key",
        },
    )

    assert dry_run.status_code == 200
    assert live_run.status_code == 200

    dry_result = dry_run.json()
    live_result = live_run.json()
    assert dry_result["result"] == "simulated"
    assert live_result["result"] == "rejected"
    assert live_result["accepted"] is False
    assert live_result["audit_id"] == dry_result["audit_id"]
    assert live_result["detail"] == "idempotency key already used for different request context"
    project_list = api_client.get("/api/v1/projects").json()
    assert project_list[0]["safe_paused"] is False


def test_api_create_task_validation_mode_runs_validate_only_and_does_not_mutate_state(api_client: TestClient):
    project = _create_project(api_client)

    command_payload = {
        "command_text": "create task finalize deployment checklist",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "validate_integration": True,
        "idempotency_key": "create-task-validation-key",
    }
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=command_payload,
    )

    assert response.status_code == 200
    command_result = response.json()
    assert command_result["accepted"] is True
    assert command_result["result"] == "validated"
    assert command_result["action"] == "create_task"
    assert command_result["target"] == project["id"]
    assert "validated" in command_result["detail"].lower()

    tasks_response = api_client.get(f"/api/v1/projects/{project['id']}/tasks")
    assert tasks_response.status_code == 200
    assert tasks_response.json() == []

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    assert len(audit_events) == 1
    assert audit_events[0]["action"] == "create_task"
    assert audit_events[0]["result"] == "validated"


def test_api_create_task_validation_idempotency_isolation_between_validation_and_dry_run(api_client: TestClient):
    project = _create_project(api_client)

    validation_payload = {
        "command_text": "create task validate integration path",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "validate_integration": True,
        "idempotency_key": "create-task-mode-key",
    }
    validation_response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=validation_payload,
    )
    assert validation_response.status_code == 200

    dry_response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "create task validate integration path",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
            "dry_run": True,
            "idempotency_key": "create-task-mode-key",
        },
    )
    assert dry_response.status_code == 200

    validation_result = validation_response.json()
    dry_result = dry_response.json()

    assert validation_result["result"] == "validated"
    assert dry_result["accepted"] is False
    assert dry_result["result"] == "rejected"
    assert dry_result["detail"] == "idempotency key already used for different request context"
    assert dry_result["audit_id"] == validation_result["audit_id"]

    tasks_response = api_client.get(f"/api/v1/projects/{project['id']}/tasks")
    assert tasks_response.status_code == 200
    assert tasks_response.json() == []




def test_api_create_task_assisted_execution_mode_calls_integration_execute_and_returns_detail(api_client: TestClient, monkeypatch):
    project = _create_project(api_client)
    notion = INTEGRATIONS["notion"]

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        assert action == "create_task"
        assert payload["project_id"] == project["id"]
        assert payload["title"] == "create notion task from api"
        assert payload["raw_command"] == "create task create notion task from api"
        return IntegrationResult(ok=True, detail="notion API created task")

    def validate_forbidden(*, action: str, payload: dict) -> None:
        raise AssertionError("validate should not run in assisted execution mode")

    monkeypatch.setattr(notion, "execute", execute_stub)
    monkeypatch.setattr(notion, "validate", validate_forbidden)

    command_payload = {
        "command_text": "create task create notion task from api",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "execute_integration": True,
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
    assert command_result["action"] == "create_task"
    assert command_result["target"] == project["id"]
    assert command_result["detail"] == "notion API created task"

    database = importlib.import_module("bro_pm.database")
    session = database.SessionLocal()
    try:
        audit = session.query(models.AuditEvent).filter_by(id=command_result["audit_id"]).one()
        payload = json.loads(audit.payload)
        assert payload["auth"]["execute_integration"] is True
        assert payload["integration"]["name"] == "notion"
        assert payload["integration"]["action"] == "create_task"
        assert payload["integration"]["status"] == "executed"
        assert payload["integration"]["detail"] == "notion API created task"
    finally:
        session.close()

    tasks_response = api_client.get(f"/api/v1/projects/{project['id']}/tasks")
    assert tasks_response.status_code == 200
    assert tasks_response.json() == []


def test_api_create_task_assisted_execution_idempotency_isolated_from_live_dry_run_and_validation_modes(api_client: TestClient, monkeypatch):
    project = _create_project(api_client)
    notion = INTEGRATIONS["notion"]

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        return IntegrationResult(ok=True, detail="notion API created task")

    monkeypatch.setattr(notion, "execute", execute_stub)

    base_payload = {
        "command_text": "create task api assisted idempotency isolation",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "execute_integration": True,
        "idempotency_key": "api-assist-mode-key",
    }
    assisted = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json=base_payload,
    )
    assert assisted.status_code == 200

    live = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "create task api assisted idempotency isolation",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
            "idempotency_key": "api-assist-mode-key",
        },
    )
    dry = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "create task api assisted idempotency isolation",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
            "dry_run": True,
            "idempotency_key": "api-assist-mode-key",
        },
    )
    validated = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "create task api assisted idempotency isolation",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
            "validate_integration": True,
            "idempotency_key": "api-assist-mode-key",
        },
    )

    assisted_result = assisted.json()
    live_result = live.json()
    dry_result = dry.json()
    validated_result = validated.json()

    assert assisted_result["result"] == "executed"
    assert assisted_result["accepted"] is True
    assert assisted_result["detail"] == "notion API created task"

    assert live_result["accepted"] is False
    assert live_result["result"] == "rejected"
    assert live_result["detail"] == "idempotency key already used for different request context"

    assert dry_result["accepted"] is False
    assert dry_result["result"] == "rejected"
    assert dry_result["detail"] == "idempotency key already used for different request context"

    assert validated_result["accepted"] is False
    assert validated_result["result"] == "rejected"
    assert validated_result["detail"] == "idempotency key already used for different request context"

    assert live_result["audit_id"] == assisted_result["audit_id"]
    assert dry_result["audit_id"] == assisted_result["audit_id"]
    assert validated_result["audit_id"] == assisted_result["audit_id"]


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


def test_api_draft_boss_escalation_is_audit_only_and_requires_confirmation(api_client: TestClient):
    project = _create_project(api_client)
    command_payload = {
        "command_text": "draft_boss_escalation customers are blocked by API outage",
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
    assert command_result["result"] == "requires_approval"
    assert command_result["action"] == "draft_boss_escalation"
    assert "operator confirmation" in command_result["detail"]

    response = api_client.get("/api/v1/projects")
    assert response.status_code == 200
    listed = response.json()
    assert listed[0]["safe_paused"] is False

    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        audit = session.query(models.AuditEvent).filter_by(id=command_result["audit_id"]).one()
        payload = json.loads(audit.payload)
        assert audit.action == "draft_boss_escalation"
        assert payload["proposal"]["payload"]["raw_command"] == "draft_boss_escalation customers are blocked by API outage"
        assert payload["proposal"]["payload"]["trace_label"] == "draft_boss_escalation"
    finally:
        session.close()


def test_api_command_draft_boss_escalation_idempotent_replay(api_client: TestClient):
    project = _create_project(api_client)
    command_payload = {
        "command_text": "draft_boss_escalation database partition full",
        "project_id": project["id"],
        "actor": "alice",
        "role": "admin",
        "idempotency_key": "draft-escalation-once",
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
    assert first.json()["result"] == "requires_approval"
    assert second.json()["result"] == "requires_approval"
    assert first.json()["audit_id"] == second.json()["audit_id"]


def test_api_draft_boss_escalation_rejects_viewer(api_client: TestClient):
    project = _create_project(api_client)
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "draft_boss_escalation viewer cannot escalate",
            "project_id": project["id"],
            "actor": "alice",
            "role": "viewer",
        },
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is False
    assert response.json()["result"] == "rejected"
    assert response.json()["detail"] == "requires operator role"


def test_api_draft_boss_escalation_requires_project_context(api_client: TestClient):
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "draft_boss_escalation missing project context",
            "actor": "alice",
            "role": "admin",
        },
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is False
    assert response.json()["result"] == "rejected"
    assert response.json()["detail"] == "project context required for draft_boss_escalation"


def test_api_draft_boss_escalation_requires_message(api_client: TestClient):
    project = _create_project(api_client)
    response = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "draft_boss_escalation   ",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
        },
    )

    assert response.status_code == 200
    assert response.json()["accepted"] is False
    assert response.json()["result"] == "rejected"
    assert response.json()["detail"] == "escalation message required for draft_boss_escalation"


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


def test_api_project_report_returns_notion_ready_payload_with_safe_publish_contract(api_client: TestClient, monkeypatch):
    project = _create_project(api_client)
    goal = api_client.post(f"/api/v1/projects/{project['id']}/goals", json=_goal_payload())
    assert goal.status_code == 201

    pause_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": f"pause project {project['id']}",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
        },
    )
    assert pause_resp.status_code == 200
    pause_result = pause_resp.json()
    assert pause_result["result"] == "executed"

    escalation_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": "draft_boss_escalation customers are blocked by API outage",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
        },
    )
    assert escalation_resp.status_code == 200
    escalation_result = escalation_resp.json()
    assert escalation_result["result"] == "requires_approval"

    def fail_if_publish_executes(*, action: str, payload: dict) -> IntegrationResult:
        raise AssertionError(f"report endpoint must not execute {action}")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", fail_if_publish_executes)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 200
    report = response.json()
    assert report["project_id"] == project["id"]
    assert report["report_type"] == "project_report"
    assert report["visibility"] == "internal"
    assert "Project Nova" in report["summary"]
    assert "publish_report" not in report["summary"]
    assert report["kpis"] == {
        "total_tasks": 2,
        "completed_tasks": 0,
        "open_tasks": 2,
        "active_goals": 1,
        "audit_events": 2,
    }
    assert report["risks"] == [
        {
            "kind": "boss_escalation",
            "audit_id": escalation_result["audit_id"],
            "action": "draft_boss_escalation",
            "status": "awaiting_approval",
            "summary": "customers are blocked by API outage",
        }
    ]
    assert report["decisions"] == [
        {
            "audit_id": pause_result["audit_id"],
            "action": "pause_project",
            "result": "executed",
            "summary": "policy accepted",
        }
    ]
    assert report["action_ids"] == [escalation_result["audit_id"], pause_result["audit_id"]]
    assert report["links"] == {
        "project": f"Bro-PM/Projects/internal/{project['slug']}",
        "tasks": f"Bro-PM/Projects/internal/{project['slug']}/Tasks",
        "audit_events": f"Bro-PM/Projects/internal/{project['slug']}/Audit",
        "report": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
        "notion_parent": "Bro-PM/Reports/internal",
        "notion_project": f"Bro-PM/Projects/internal/{project['slug']}",
    }
    assert report["publish"] == {
        "integration": "notion",
        "action": "publish_report",
        "status": "contract_ready",
        "target": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
        "detail": "Notion-ready publish contract prepared; external publish not executed",
        "visibility": "internal",
    }

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    assert len(audit_events) == 2
    assert {event["action"] for event in audit_events} == {"draft_boss_escalation", "pause_project"}


def test_api_project_report_execute_publish_calls_notion_and_persists_audit(api_client: TestClient, monkeypatch):
    project = _create_project(api_client)
    captured: dict[str, dict] = {}

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        captured["call"] = {"action": action, "payload": payload}
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True),
    )

    assert response.status_code == 200
    report = response.json()
    assert report["summary"].endswith("Latest audit signal: no recent audit signal.")
    assert report["kpis"]["audit_events"] == 0
    assert report["publish"] == {
        "integration": "notion",
        "action": "publish_report",
        "status": "executed",
        "target": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
        "detail": "notion executed: publish_report",
        "visibility": "internal",
    }
    assert captured["call"] == {
        "action": "publish_report",
        "payload": {
            "workspace_root": "Bro-PM",
            "parent_page": "Bro-PM/Reports/internal",
            "project_page": f"Bro-PM/Projects/internal/{project['slug']}",
            "visibility": "internal",
            "report": {
                "project_id": project["id"],
                "report_type": "project_report",
                "visibility": "internal",
                "summary": "Project Nova is tracking no active goal with 0 open tasks. Latest audit signal: no recent audit signal.",
                "kpis": {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "open_tasks": 0,
                    "active_goals": 0,
                    "audit_events": 0,
                },
                "risks": [],
                "decisions": [],
                "action_ids": [],
                "links": {
                    "project": f"Bro-PM/Projects/internal/{project['slug']}",
                    "tasks": f"Bro-PM/Projects/internal/{project['slug']}/Tasks",
                    "audit_events": f"Bro-PM/Projects/internal/{project['slug']}/Audit",
                    "report": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
                    "notion_parent": "Bro-PM/Reports/internal",
                    "notion_project": f"Bro-PM/Projects/internal/{project['slug']}",
                },
            },
        },
    }

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    assert audit_events == [
        {
            "id": audit_events[0]["id"],
            "project_id": project["id"],
            "actor": "alice",
            "action": "publish_report",
            "target_type": "report",
            "target_id": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
            "result": "executed",
            "detail": "notion executed: publish_report",
            "created_at": audit_events[0]["created_at"],
        }
    ]


def test_api_project_report_execute_publish_commits_pending_reservation_before_notion_execute(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")
    observed: dict[str, dict | None] = {}
    idempotency_key = "report-publish-durable-pending"

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        observer_session = db_module.SessionLocal()
        try:
            record = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            observed["record"] = (
                None
                if record is None
                else {
                    "result": record.result,
                    "target_id": record.target_id,
                }
            )
        finally:
            observer_session.close()
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
    )

    assert response.status_code == 200
    assert observed["record"] == {
        "result": "pending_publish",
        "target_id": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
    }



def test_service_project_report_execute_publish_commits_final_audit_before_caller_commit(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")
    idempotency_key = "report-publish-durable-final"

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    service_session = db_module.SessionLocal()
    try:
        stored_project = service_session.query(models.Project).filter_by(id=project["id"]).one()
        service = ReportingService(db_session=service_session)

        response = service.generate_project_report(
            project=stored_project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=True,
            idempotency_key=idempotency_key,
        )

        observer_session = db_module.SessionLocal()
        try:
            record = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            observed = (
                None
                if record is None
                else {
                    "result": record.result,
                    "detail": json.loads(record.payload)["integration"]["detail"],
                }
            )
        finally:
            observer_session.close()
    finally:
        service_session.close()

    assert response.publish.status == "executed"
    assert observed == {
        "result": "executed",
        "detail": "notion executed: publish_report",
    }



def test_service_project_report_execute_publish_without_idempotency_key_defers_audit_until_caller_commit(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    service_session = db_module.SessionLocal()
    try:
        stored_project = service_session.query(models.Project).filter_by(id=project["id"]).one()
        service = ReportingService(db_session=service_session)

        response = service.generate_project_report(
            project=stored_project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=True,
        )

        observer_session = db_module.SessionLocal()
        try:
            observed_before_commit = observer_session.query(models.AuditEvent).filter_by(action="publish_report").all()
        finally:
            observer_session.close()

        service_session.commit()

        observer_session = db_module.SessionLocal()
        try:
            observed_after_commit = observer_session.query(models.AuditEvent).filter_by(action="publish_report").all()
        finally:
            observer_session.close()
    finally:
        service_session.close()

    assert response.publish.status == "executed"
    assert observed_before_commit == []
    assert len(observed_after_commit) == 1
    assert observed_after_commit[0].result == "executed"



def test_api_project_report_execute_publish_replays_idempotent_success_without_second_notion_call(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    call_count = {"count": 0}

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-success"),
    )
    second_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-success"),
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert second_response.json() == first_response.json()
    assert call_count["count"] == 1

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    publish_events = [event for event in audit_events if event["action"] == "publish_report"]
    assert len(publish_events) == 1
    assert publish_events[0]["result"] == "executed"
    assert publish_events[0]["detail"] == "notion executed: publish_report"


def test_api_project_report_execute_publish_replays_stored_failure_without_second_notion_call(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    call_count = {"count": 0}

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        raise IntegrationError("publish integration unavailable")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-failure"),
    )
    second_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-failure"),
    )

    assert first_response.status_code == 422
    assert second_response.status_code == 422
    assert first_response.json() == second_response.json() == {"detail": "publish integration unavailable"}
    assert call_count["count"] == 1

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    publish_events = [event for event in audit_events if event["action"] == "publish_report"]
    assert len(publish_events) == 1
    assert publish_events[0]["result"] == "failed"
    assert publish_events[0]["detail"] == "publish integration unavailable"



def test_api_project_report_execute_publish_finalizes_unexpected_idempotent_failure_for_replay(
    monkeypatch, tmp_path
):
    db_path = tmp_path / f"bro_pm_api_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in ("bro_pm.database", "bro_pm.api.app", "bro_pm.api", "bro_pm.api.v1", "bro_pm.api.v1.commands", "bro_pm.api.v1.projects"):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url), raise_server_exceptions=False) as client:
        project = _create_project(client)
        call_count = {"count": 0}
        idempotency_key = "report-publish-unexpected-failure"

        def boom(*, action: str, payload: dict) -> IntegrationResult:
            call_count["count"] += 1
            raise RuntimeError("publish exploded")

        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", boom)

        first_response = client.post(
            f"/api/v1/projects/{project['id']}/reports/project",
            headers={"x-actor-trusted": "true"},
            json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
        )
        second_response = client.post(
            f"/api/v1/projects/{project['id']}/reports/project",
            headers={"x-actor-trusted": "true"},
            json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
        )
        audit_events = client.get(f"/api/v1/projects/{project['id']}/audit-events").json()

    assert first_response.status_code == 500
    assert second_response.status_code == 422
    assert second_response.json() == {"detail": "publish exploded"}
    assert call_count["count"] == 1

    publish_events = [event for event in audit_events if event["action"] == "publish_report"]
    assert len(publish_events) == 1
    assert publish_events[0]["result"] == "failed"
    assert publish_events[0]["detail"] == "publish exploded"



def test_api_project_report_execute_publish_replays_soft_failure_without_second_notion_call(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    call_count = {"count": 0}

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        return IntegrationResult(ok=False, detail="notion rejected publish payload")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-soft-failure"),
    )
    second_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-soft-failure"),
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert second_response.json() == first_response.json()
    assert first_response.json()["publish"]["status"] == "failed"
    assert first_response.json()["publish"]["detail"] == "notion rejected publish payload"
    assert call_count["count"] == 1

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    publish_events = [event for event in audit_events if event["action"] == "publish_report"]
    assert len(publish_events) == 1
    assert publish_events[0]["result"] == "failed"
    assert publish_events[0]["detail"] == "notion rejected publish payload"



def test_api_project_report_execute_publish_replays_success_under_safe_pause_and_state_drift(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")
    call_count = {"count": 0}
    idempotency_key = "report-publish-state-drift"

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
    )
    assert first_response.status_code == 200

    session = db_module.SessionLocal()
    try:
        stored_project = session.query(models.Project).filter_by(id=project["id"]).one()
        stored_project.safe_paused = True
        stored_project.visibility = "internal/restricted"
        session.commit()
    finally:
        session.close()

    second_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
    )

    assert second_response.status_code == 200
    assert second_response.json() == first_response.json()
    assert call_count["count"] == 1



def test_api_project_report_execute_publish_recovers_stale_pending_with_manual_reconciliation_error(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")
    idempotency_key = "report-publish-stale-pending"
    call_count = {"count": 0}
    stale_detail = "stale pending publish request requires manual reconciliation before retry"

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        return IntegrationResult(ok=True, detail="unexpected second execution")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    session = db_module.SessionLocal()
    try:
        session.add(
            models.AuditEvent(
                project_id=project["id"],
                actor="alice",
                action="publish_report",
                target_type="report",
                target_id=f"Bro-PM/Reports/internal/Projects/{project['slug']}",
                payload=json.dumps(
                    {
                        "integration": {
                            "name": "notion",
                            "action": "publish_report",
                            "status": "pending",
                            "detail": "report publish execution pending",
                        },
                        "visibility": "internal",
                        "target": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
                        "created_via": "project_report",
                        "actor": "alice",
                        "report": {"project_id": project["id"]},
                        "idempotency": {
                            "request": {
                                "project_id": project["id"],
                                "actor": "alice",
                                "role": "admin",
                                "actor_trusted": True,
                                "execute_publish": True,
                            },
                            "replay": {"kind": "pending"},
                        },
                    },
                    ensure_ascii=False,
                ),
                result="pending_publish",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        session.commit()
    finally:
        session.close()

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
    )
    second_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key=idempotency_key),
    )

    assert first_response.status_code == 422
    assert second_response.status_code == 422
    assert first_response.json() == second_response.json() == {"detail": stale_detail}
    assert call_count["count"] == 0

    session = db_module.SessionLocal()
    try:
        stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        stored_payload = json.loads(stored.payload)
        stored_result = stored.result
    finally:
        session.close()

    assert stored_result == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail
    assert stored_payload["idempotency"]["replay"] == {"kind": "error", "detail": stale_detail}



def test_api_project_report_execute_publish_rejects_conflicting_idempotency_reuse(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    call_count = {"count": 0}

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    first_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="report-publish-conflict"),
    )
    conflicting_response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(actor="bob", execute_publish=True, idempotency_key="report-publish-conflict"),
    )

    assert first_response.status_code == 200
    assert conflicting_response.status_code == 409
    assert conflicting_response.json() == {"detail": "idempotency key already used for different request context"}
    assert call_count["count"] == 1

    audit_events = api_client.get(f"/api/v1/projects/{project['id']}/audit-events").json()
    publish_events = [event for event in audit_events if event["action"] == "publish_report"]
    assert len(publish_events) == 1
    assert publish_events[0]["actor"] == "alice"



def test_service_project_report_execute_publish_replays_concurrent_duplicate_without_second_notion_call(
    api_client: TestClient, monkeypatch
):
    project = _create_project(api_client)
    db_module = importlib.import_module("bro_pm.database")
    call_count = {"count": 0}
    start_barrier = Barrier(2)
    idempotency_key = "report-publish-concurrent"

    def execute_publish(*, action: str, payload: dict) -> IntegrationResult:
        call_count["count"] += 1
        assert action == "publish_report"
        assert payload["report"]["project_id"] == project["id"]
        time.sleep(0.05)
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_publish)

    def _run_once() -> dict:
        session = db_module.SessionLocal()
        try:
            stored_project = session.query(models.Project).filter_by(id=project["id"]).one()
            service = ReportingService(db_session=session)
            start_barrier.wait(timeout=5)
            response = service.generate_project_report(
                project=stored_project,
                actor="alice",
                role="admin",
                actor_trusted=True,
                execute_publish=True,
                idempotency_key=idempotency_key,
            )
            session.commit()
            return {
                "status": "ok",
                "publish_status": response.publish.status,
                "detail": response.publish.detail,
                "target": response.publish.target,
            }
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [future.result() for future in [executor.submit(_run_once), executor.submit(_run_once)]]

    assert all(outcome["status"] == "ok" for outcome in outcomes)
    assert {outcome["publish_status"] for outcome in outcomes} == {"executed"}
    assert len({outcome["detail"] for outcome in outcomes}) == 1
    assert len({outcome["target"] for outcome in outcomes}) == 1
    assert call_count["count"] == 1

    audit_session = db_module.SessionLocal()
    try:
        publish_events = audit_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).all()
        assert len(publish_events) == 1
        assert publish_events[0].result == "executed"
    finally:
        audit_session.close()



def test_api_project_report_rejects_idempotency_key_longer_than_audit_column(api_client: TestClient):
    project = _create_project(api_client)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(execute_publish=True, idempotency_key="x" * 121),
    )

    assert response.status_code == 422
    assert "at most 120" in response.text



def test_api_project_report_execute_publish_rejects_viewer_role(api_client: TestClient):
    project = _create_project(api_client)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(role="viewer", execute_publish=True),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "requires operator role"


def test_api_project_report_execute_publish_returns_422_on_integration_error(monkeypatch, tmp_path):
    db_path = tmp_path / f"bro_pm_api_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in ("bro_pm.database", "bro_pm.api.app", "bro_pm.api", "bro_pm.api.v1", "bro_pm.api.v1.commands", "bro_pm.api.v1.projects"):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url), raise_server_exceptions=False) as client:
        project = _create_project(client)

        def boom(*, action: str, payload: dict) -> IntegrationResult:
            raise IntegrationError("publish integration unavailable")

        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", boom)

        response = client.post(
            f"/api/v1/projects/{project['id']}/reports/project",
            headers={"x-actor-trusted": "true"},
            json=_report_auth_payload(execute_publish=True),
        )
        audit_events = client.get(f"/api/v1/projects/{project['id']}/audit-events").json()

    assert response.status_code == 422
    assert response.json()["detail"] == "publish integration unavailable"
    assert audit_events == [
        {
            "id": audit_events[0]["id"],
            "project_id": project["id"],
            "actor": "alice",
            "action": "publish_report",
            "target_type": "report",
            "target_id": f"Bro-PM/Reports/internal/Projects/{project['slug']}",
            "result": "failed",
            "detail": "publish integration unavailable",
            "created_at": audit_events[0]["created_at"],
        }
    ]


def test_api_project_report_requires_trusted_actor(api_client: TestClient):
    project = _create_project(api_client)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        json=_report_auth_payload(),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "untrusted actor blocked"


def test_api_project_report_rejects_viewer_role(api_client: TestClient):
    project = _create_project(api_client)

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(role="viewer"),
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "requires operator role"



def test_api_project_report_ignores_publish_audits_in_report_content(api_client: TestClient):
    project = _create_project(api_client)

    pause_resp = api_client.post(
        "/api/v1/commands",
        headers={"x-actor-trusted": "true"},
        json={
            "command_text": f"pause project {project['id']}",
            "project_id": project["id"],
            "actor": "alice",
            "role": "admin",
        },
    )
    assert pause_resp.status_code == 200
    pause_result = pause_resp.json()

    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        legacy_publish = models.AuditEvent(
            id=f"audit-publish-{uuid4().hex[:8]}",
            project_id=project["id"],
            actor="legacy-bot",
            action="publish_report",
            target_type="report",
            target_id=project["id"],
            payload=json.dumps({"report": {"project_id": project["id"]}}, ensure_ascii=False),
            result="executed",
            created_at=datetime(2026, 1, 2, 0, 0, 0),
        )
        session.add(legacy_publish)
        session.commit()
    finally:
        session.close()

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 200
    report = response.json()
    assert "Latest audit signal: pause_project." in report["summary"]
    assert report["kpis"]["audit_events"] == 1
    assert report["decisions"] == [
        {
            "audit_id": pause_result["audit_id"],
            "action": "pause_project",
            "result": "executed",
            "summary": "policy accepted",
        }
    ]
    assert report["action_ids"] == [pause_result["audit_id"]]



def test_api_project_report_uses_project_visibility_in_notion_paths(api_client: TestClient):
    project = _create_project(api_client, visibility="private")

    response = api_client.post(
        f"/api/v1/projects/{project['id']}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 200
    report = response.json()
    assert report["visibility"] == "private"
    assert report["links"] == {
        "project": f"Bro-PM/Projects/private/{project['slug']}",
        "tasks": f"Bro-PM/Projects/private/{project['slug']}/Tasks",
        "audit_events": f"Bro-PM/Projects/private/{project['slug']}/Audit",
        "report": f"Bro-PM/Reports/private/Projects/{project['slug']}",
        "notion_parent": "Bro-PM/Reports/private",
        "notion_project": f"Bro-PM/Projects/private/{project['slug']}",
    }
    assert report["publish"]["target"] == f"Bro-PM/Reports/private/Projects/{project['slug']}"
    assert report["publish"]["visibility"] == "private"


def test_api_project_report_rejects_path_like_stored_slug(api_client: TestClient):
    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        project = models.Project(
            name="Project Nova",
            slug=f"projects/{uuid4().hex[:8]}",
            description="project with unsafe stored slug",
            visibility="internal",
            safe_paused=False,
            metadata_json={"team": "ops"},
        )
        session.add(project)
        session.commit()
        project_id = project.id
    finally:
        session.close()

    response = api_client.post(
        f"/api/v1/projects/{project_id}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "slug must not contain '/'"


def test_api_project_report_rejects_whitespace_only_stored_visibility(api_client: TestClient):
    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        project = models.Project(
            name="Project Nova",
            slug=f"project-nova-{uuid4().hex[:8]}",
            description="project under test",
            visibility="   ",
            safe_paused=False,
            metadata_json={"team": "ops"},
        )
        session.add(project)
        session.commit()
        project_id = project.id
    finally:
        session.close()

    response = api_client.post(
        f"/api/v1/projects/{project_id}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "visibility must not be empty"


def test_api_project_report_rejects_path_like_stored_visibility(api_client: TestClient):
    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        project = models.Project(
            name="Project Nova",
            slug=f"project-nova-{uuid4().hex[:8]}",
            description="project under test",
            visibility="internal/restricted",
            safe_paused=False,
            metadata_json={"team": "ops"},
        )
        session.add(project)
        session.commit()
        project_id = project.id
    finally:
        session.close()

    response = api_client.post(
        f"/api/v1/projects/{project_id}/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 422
    assert response.json()["detail"] == "visibility must not contain '/'"


def test_api_project_report_missing_project_returns_404(api_client: TestClient):
    response = api_client.post(
        "/api/v1/projects/does-not-exist/reports/project",
        headers={"x-actor-trusted": "true"},
        json=_report_auth_payload(),
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "project not found"

