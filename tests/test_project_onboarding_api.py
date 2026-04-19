from __future__ import annotations

import importlib
import json
import sys
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from bro_pm import models, schemas
from bro_pm.integrations import INTEGRATIONS, IntegrationError, IntegrationResult


def _reset_onboarding_api_modules() -> None:
    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
        "bro_pm.api.ui",
    ):
        sys.modules.pop(mod_name, None)


@pytest.fixture
def api_context(tmp_path):
    db_path = tmp_path / f"bro_pm_onboarding_api_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    _reset_onboarding_api_modules()

    api_app = importlib.import_module("bro_pm.api.app")
    api_app.create_app(database_url=db_url, enable_scheduler=False)
    db_module = importlib.import_module("bro_pm.database")
    projects_api = importlib.import_module("bro_pm.api.v1.projects")
    session = db_module.SessionLocal()
    try:
        yield projects_api, session, db_module
    finally:
        session.close()


@pytest.fixture
def api_client(tmp_path):
    db_path = tmp_path / f"bro_pm_onboarding_http_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    _reset_onboarding_api_modules()

    api_app = importlib.import_module("bro_pm.api.app")
    app = api_app.create_app(database_url=db_url, enable_scheduler=False)
    db_module = importlib.import_module("bro_pm.database")
    projects_api = importlib.import_module("bro_pm.api.v1.projects")
    with TestClient(app) as client:
        yield client, projects_api, db_module


def _onboarding_payload() -> dict:
    return {
        "name": "Project Nova",
        "slug": f"project-nova-{uuid4().hex[:8]}",
        "description": "project under onboarding test",
        "timezone": "UTC",
        "boss": "olga",
        "admin": "alice",
        "reporting_cadence": "weekly",
        "communication_integrations": ["slack"],
        "board_integration": "notion",
        "team": [
            {
                "name": "operations",
                "owner": "alice",
                "capacity": 3,
            }
        ],
    }


def _onboard_project(projects_api, session, payload: dict):
    return projects_api.onboard_project(payload=schemas.ProjectOnboardingCreate(**payload), db=session)


def test_api_project_onboarding_creates_ready_project_with_memberships_and_gate_checks(api_context):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()

    result = _onboard_project(projects_api, session, payload)

    body = result.model_dump(mode="json")
    assert body["status"] == "active"
    assert body["timezone"] == payload["timezone"]
    assert body["policy"] == "default_mvp"
    assert body["reporting_cadence"] == payload["reporting_cadence"]
    assert body["gate_checks"] == {
        "policy_attached": True,
        "communication_ready": True,
        "board_sync_healthy": True,
        "safe_pause_default_off": True,
    }
    assert body["smoke_check"]["status"] == "passed"
    assert body["project"]["safe_paused"] is False
    assert body["project"]["timezone"] == payload["timezone"]
    assert {member["role"] for member in body["memberships"]} == {"owner", "admin"}
    assert {member["actor"] for member in body["memberships"]} == {payload["boss"], payload["admin"]}

    project = session.get(models.Project, body["project"]["id"])
    assert project is not None
    assert project.timezone == payload["timezone"]
    assert project.safe_paused is False

    metadata = project.metadata_json or {}
    onboarding = metadata.get("onboarding") or {}
    assert onboarding["status"] == "active"
    assert onboarding["policy"] == "default_mvp"
    assert onboarding["reporting_cadence"] == payload["reporting_cadence"]
    assert onboarding["communication_integrations"] == payload["communication_integrations"]
    assert onboarding["board_integration"] == payload["board_integration"]
    assert onboarding["boss"] == payload["boss"]
    assert onboarding["admin"] == payload["admin"]

    memberships = (
        session.query(models.ProjectMembership)
        .filter_by(project_id=project.id)
        .order_by(models.ProjectMembership.role.asc())
        .all()
    )
    assert [(membership.actor, membership.role) for membership in memberships] == [
        (payload["admin"], "admin"),
        (payload["boss"], "owner"),
    ]

    onboarding_audit = (
        session.query(models.AuditEvent)
        .filter_by(project_id=project.id, action="onboard_project")
        .one()
    )
    assert onboarding_audit.result == "executed"
    audit_payload = json.loads(onboarding_audit.payload)
    assert audit_payload["gate_checks"]["board_sync_healthy"] is True

    due_actions = (
        session.query(models.DueAction)
        .filter_by(project_id=project.id)
        .order_by(models.DueAction.created_at.asc(), models.DueAction.id.asc())
        .all()
    )
    assert len(due_actions) == 1
    due_action = due_actions[0]
    assert due_action.channel == "slack"
    assert due_action.recipient == payload["boss"]
    assert due_action.kind == "project_launch_bootstrap"
    assert due_action.status == "pending"
    assert due_action.idempotency_key == f"onboarding-launch:{project.id}"
    assert due_action.payload_json["trace_label"] == "onboarding_launch_bootstrap"
    assert due_action.payload_json["goal_summary"]["present"] is False
    assert due_action.payload_json["follow_up"] == {
        "required": True,
        "type": "capture_initial_goal",
    }
    assert "first-goal follow-up is required" in due_action.payload_json["text"].lower()


def test_api_project_onboarding_rolls_back_if_response_build_fails(api_client, monkeypatch):
    client, projects_api, db_module = api_client
    payload = _onboarding_payload()

    def fail_response_build(*args, **kwargs):
        raise RuntimeError("synthetic response-build failure")

    monkeypatch.setattr(projects_api, "_build_onboarding_response", fail_response_build)

    with pytest.raises(RuntimeError, match="synthetic response-build failure"):
        client.post("/api/v1/projects/onboard", json=payload)

    verification_session = db_module.SessionLocal()
    try:
        assert verification_session.query(models.Project).count() == 0
        assert verification_session.query(models.ProjectMembership).count() == 0
        assert verification_session.query(models.AuditEvent).count() == 0
        assert verification_session.query(models.DueAction).count() == 0
    finally:
        verification_session.close()


def test_api_project_onboarding_requires_at_least_one_communication_integration(api_context):
    projects_api, session, _ = api_context

    payload = _onboarding_payload()
    payload["communication_integrations"] = []

    with pytest.raises(ValueError, match="at least one communication integration is required"):
        _onboard_project(projects_api, session, payload)


def test_api_project_onboarding_rejects_duplicate_capacity_profiles(api_context):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()
    payload["team"] = [
        {
            "name": "operations",
            "owner": "alice",
            "capacity": 3,
        },
        {
            "name": "operations",
            "owner": "alice",
            "capacity": 2,
        },
    ]

    with pytest.raises(ValueError, match="team entries must be unique by name and owner"):
        _onboard_project(projects_api, session, payload)


def test_api_project_onboarding_rejects_duplicate_project_name(api_context):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()
    result = _onboard_project(projects_api, session, payload)
    assert result.status == "active"

    duplicate_name_payload = _onboarding_payload()
    duplicate_name_payload["name"] = payload["name"]
    duplicate_name_payload["slug"] = f"{payload['slug']}-second"

    with pytest.raises(HTTPException) as exc:
        _onboard_project(projects_api, session, duplicate_name_payload)

    assert exc.value.status_code == 409
    assert exc.value.detail == "project name already exists"


def test_api_project_onboarding_accepts_yandex_tracker_board_integration(api_context, monkeypatch):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()
    payload["board_integration"] = "yandex_tracker"
    payload["metadata"] = {
        "integrations": {
            "yandex_tracker": {
                "backend": "mcp",
                "queue": "OPS",
            }
        }
    }

    def yandex_execute_stub(*, action: str, payload: dict):
        assert action == "create_task"
        assert payload["title"] == "Synthetic onboarding smoke check"
        assert payload["actor"] == "alice"
        assert payload["project_metadata"]["integrations"]["yandex_tracker"]["backend"] == "mcp"
        assert payload["project_metadata"]["integrations"]["yandex_tracker"]["queue"] == "OPS"
        return IntegrationResult(ok=True, detail="yandex_tracker created task ONBOARD-1 (id: 101)")

    monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "execute", yandex_execute_stub)

    result = _onboard_project(projects_api, session, payload)

    body = result.model_dump(mode="json")
    assert body["status"] == "active"
    assert body["smoke_check"]["status"] == "passed"
    assert body["smoke_check"]["detail"] == "yandex_tracker created task ONBOARD-1 (id: 101)"

    project = session.get(models.Project, body["project"]["id"])
    assert project is not None
    metadata = project.metadata_json or {}
    onboarding = metadata.get("onboarding") or {}
    assert onboarding["board_integration"] == "yandex_tracker"
    assert metadata["integrations"]["yandex_tracker"]["backend"] == "mcp"
    assert metadata["integrations"]["yandex_tracker"]["queue"] == "OPS"


def test_api_project_onboarding_allows_same_actor_as_boss_and_admin(api_context):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()
    payload["boss"] = "alice"

    result = _onboard_project(projects_api, session, payload)

    body = result.model_dump(mode="json")
    assert body["status"] == "active"
    assert body["project"]["safe_paused"] is False
    assert body["memberships"] == [{"actor": "alice", "role": "owner"}]


def test_api_project_onboarding_failure_marks_project_paused_and_escalates(api_context, monkeypatch):
    projects_api, session, _ = api_context
    payload = _onboarding_payload()

    def fail_execute(*, action: str, payload: dict):
        raise IntegrationError("synthetic smoke check failed")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", fail_execute)

    with pytest.raises(HTTPException) as exc:
        _onboard_project(projects_api, session, payload)

    assert exc.value.status_code == 422
    assert exc.value.detail == "synthetic smoke check failed"

    projects = session.query(models.Project).order_by(models.Project.created_at.asc(), models.Project.id.asc()).all()
    assert len(projects) == 1
    project = projects[0]
    assert project.slug == payload["slug"]
    assert project.safe_paused is True
    assert project.timezone == payload["timezone"]

    audit_events = (
        session.query(models.AuditEvent)
        .filter_by(project_id=project.id)
        .order_by(models.AuditEvent.created_at.desc(), models.AuditEvent.id.desc())
        .all()
    )
    assert [event.action for event in audit_events] == ["draft_boss_escalation", "onboard_project"]
    assert audit_events[0].result == "requires_approval"
    assert audit_events[1].result == "failed"
    assert json.loads(audit_events[1].payload)["detail"] == "synthetic smoke check failed"
