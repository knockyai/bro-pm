from __future__ import annotations

import importlib
import json
import sys
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationError


@pytest.fixture
def api_client(tmp_path):
    db_path = tmp_path / f"bro_pm_onboarding_api_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
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
    with TestClient(api_app.create_app(database_url=db_url)) as client:
        yield client


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


def test_api_project_onboarding_creates_ready_project_with_memberships_and_gate_checks(api_client: TestClient):
    payload = _onboarding_payload()

    response = api_client.post("/api/v1/projects/onboard", json=payload)

    assert response.status_code == 201
    body = response.json()
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

    db_module = importlib.import_module("bro_pm.database")
    database_session = db_module.SessionLocal()
    try:
        project = database_session.get(models.Project, body["project"]["id"])
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

        memberships = (
            database_session.query(models.ProjectMembership)
            .filter_by(project_id=project.id)
            .order_by(models.ProjectMembership.role.asc())
            .all()
        )
        assert [(membership.actor, membership.role) for membership in memberships] == [
            (payload["admin"], "admin"),
            (payload["boss"], "owner"),
        ]

        onboarding_audit = (
            database_session.query(models.AuditEvent)
            .filter_by(project_id=project.id, action="onboard_project")
            .one()
        )
        assert onboarding_audit.result == "executed"
        audit_payload = json.loads(onboarding_audit.payload)
        assert audit_payload["gate_checks"]["board_sync_healthy"] is True
    finally:
        database_session.close()


def test_api_project_onboarding_requires_at_least_one_communication_integration(api_client: TestClient):
    payload = _onboarding_payload()
    payload["communication_integrations"] = []

    response = api_client.post("/api/v1/projects/onboard", json=payload)

    assert response.status_code == 422
    assert "at least one communication integration" in response.text


def test_api_project_onboarding_allows_same_actor_as_boss_and_admin(api_client: TestClient):
    payload = _onboarding_payload()
    payload["boss"] = "alice"

    response = api_client.post("/api/v1/projects/onboard", json=payload)

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "active"
    assert body["project"]["safe_paused"] is False
    assert body["memberships"] == [{"actor": "alice", "role": "owner"}]


def test_api_project_onboarding_failure_marks_project_paused_and_escalates(api_client: TestClient, monkeypatch):
    payload = _onboarding_payload()

    def fail_execute(*, action: str, payload: dict):
        raise IntegrationError("synthetic smoke check failed")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", fail_execute)

    response = api_client.post("/api/v1/projects/onboard", json=payload)

    assert response.status_code == 422
    assert response.json()["detail"] == "synthetic smoke check failed"

    projects_response = api_client.get("/api/v1/projects")
    assert projects_response.status_code == 200
    projects = projects_response.json()
    assert len(projects) == 1
    project = projects[0]
    assert project["slug"] == payload["slug"]
    assert project["safe_paused"] is True
    assert project["timezone"] == payload["timezone"]

    audit_response = api_client.get(f"/api/v1/projects/{project['id']}/audit-events")
    assert audit_response.status_code == 200
    audit_events = audit_response.json()
    assert [event["action"] for event in audit_events] == ["draft_boss_escalation", "onboard_project"]
    assert audit_events[0]["result"] == "requires_approval"
    assert audit_events[1]["result"] == "failed"
    assert audit_events[1]["detail"] == "synthetic smoke check failed"
