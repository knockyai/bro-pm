from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult


@pytest.fixture
def web_client(tmp_path):
    db_path = tmp_path / f"bro_pm_onboarding_ui_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.ui",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
    ):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url, enable_scheduler=False)) as client:
        yield client


@pytest.fixture
def onboarding_context(tmp_path):
    db_path = tmp_path / f"bro_pm_onboarding_ui_submit_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.ui",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
    ):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    api_app.create_app(database_url=db_url, enable_scheduler=False)
    ui_module = importlib.import_module("bro_pm.api.ui")
    db_module = importlib.import_module("bro_pm.database")
    session = db_module.SessionLocal()
    try:
        yield ui_module, session, db_module
    finally:
        session.close()


def _build_request(form_data) -> Request:
    body = urlencode(form_data, doseq=True).encode("utf-8")
    delivered = False

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/onboarding/",
            "raw_path": b"/onboarding/",
            "query_string": b"",
            "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        },
        receive,
    )


def _submit_onboarding_form(ui_module, session, form_data):
    response = asyncio.run(ui_module.submit_onboarding_page(_build_request(form_data), db=session))
    if response.status_code < 400:
        session.commit()
    else:
        session.rollback()
    return response, response.body.decode("utf-8")


def test_onboarding_page_renders_form(web_client: TestClient):
    response = web_client.get("/onboarding/")

    assert response.status_code == 200
    body = response.text
    assert "Project Basics" in body
    assert "Tracker Setup" in body
    assert 'name="communication_integrations"' not in body
    assert 'name="capacity"' not in body
    assert "160 hours per employee per month" in body
    assert 'value="yandex_tracker"' in body
    assert 'value="jira"' not in body
    assert 'value="trello"' not in body
    assert 'value="notion"' not in body


def test_onboarding_page_submit_creates_project_credentials_and_goal(onboarding_context, monkeypatch):
    ui_module, database_session, db_module = onboarding_context

    def yandex_execute_stub(*, action: str, payload: dict):
        assert action == "create_task"
        assert payload["title"] == "Synthetic onboarding smoke check"
        assert payload["tracker_credentials"]["config"]["org_id"] == "org-99"
        assert payload["tracker_credentials"]["config"]["queue"] == "OPS"
        assert payload["tracker_credentials"]["secrets"]["token"] == "secret-token"
        return IntegrationResult(ok=True, detail="yandex_tracker created task OPS-17 (id: 17)")

    monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "execute", yandex_execute_stub)

    response, body = _submit_onboarding_form(
        ui_module,
        database_session,
        [
            ("name", "Launch Radar"),
            ("slug", f"launch-radar-{uuid4().hex[:8]}"),
            ("description", "Server-rendered onboarding page"),
            ("timezone", "UTC"),
            ("commitment_due_at", "2026-05-01T10:00"),
            ("boss", "olga"),
            ("admin", "alice"),
            ("board_integration", "yandex_tracker"),
            ("reporting_cadence", "weekly"),
            ("yandex_tracker_org_id", "org-99"),
            ("yandex_tracker_queue", "OPS"),
            ("yandex_tracker_token", "secret-token"),
            ("employee_name", "alice"),
            ("employee_function", "operations"),
            ("employee_name", "bob"),
            ("employee_function", "qa"),
            ("goal_title", "Launch first milestone"),
            ("goal_description", "Create a visible first slice"),
            ("goal_commitment_due_at", "2026-05-03T09:30"),
            ("goal_auto_decompose", "on"),
            ("goal_max_generated_tasks", "2"),
        ],
    )

    assert response.status_code == 201
    assert "Project launched." in body
    assert "Launch first milestone" in body

    database_session = db_module.SessionLocal()
    try:
        project = database_session.query(models.Project).filter_by(name="Launch Radar").one()
        metadata = project.metadata_json or {}
        onboarding = metadata.get("onboarding") or {}
        assert onboarding["communication_integrations"] == ["telegram"]
        assert onboarding["board_integration"] == "yandex_tracker"
        assert onboarding["employees"] == [
            {"name": "alice", "function": "operations", "capacity_hours": 160},
            {"name": "bob", "function": "qa", "capacity_hours": 160},
        ]
        assert metadata["integrations"]["yandex_tracker"] == {"org_id": "org-99", "queue": "OPS"}

        credential = (
            database_session.query(models.TrackerCredential)
            .filter_by(project_id=project.id, provider="yandex_tracker")
            .one()
        )
        assert credential.config_json == {"org_id": "org-99", "queue": "OPS"}
        assert credential.secret_json == {"token": "[redacted]"}
        assert "secret-token" not in json.dumps(credential.secret_json)

        capacity_profiles = (
            database_session.query(models.ExecutorCapacityProfile)
            .filter_by(project_id=project.id)
            .order_by(models.ExecutorCapacityProfile.actor.asc())
            .all()
        )
        assert [(profile.actor, profile.team_name, profile.capacity_units) for profile in capacity_profiles] == [
            ("alice", "operations", 160),
            ("bob", "qa", 160),
        ]

        goal = database_session.query(models.Goal).filter_by(project_id=project.id).one()
        assert goal.title == "Launch first milestone"
        assert goal.status == "active"
        tasks = (
            database_session.query(models.Task)
            .filter_by(project_id=project.id, goal_id=goal.id)
            .order_by(models.Task.title.asc())
            .all()
        )
        assert len(tasks) == 2
    finally:
        database_session.close()


def test_onboarding_page_duplicate_project_name_returns_html_conflict_without_secret_echo(onboarding_context, monkeypatch):
    ui_module, database_session, _ = onboarding_context

    def yandex_execute_stub(*, action: str, payload: dict):
        return IntegrationResult(ok=True, detail="yandex_tracker created task OPS-17 (id: 17)")

    monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "execute", yandex_execute_stub)

    first_payload = [
        ("name", "Launch Radar"),
        ("slug", f"launch-radar-{uuid4().hex[:8]}"),
        ("description", "Server-rendered onboarding page"),
        ("timezone", "UTC"),
        ("boss", "olga"),
        ("admin", "alice"),
        ("board_integration", "yandex_tracker"),
        ("reporting_cadence", "weekly"),
        ("yandex_tracker_org_id", "org-99"),
        ("yandex_tracker_queue", "OPS"),
        ("yandex_tracker_token", "secret-token"),
        ("employee_name", "alice"),
        ("employee_function", "operations"),
    ]
    first_response, _ = _submit_onboarding_form(ui_module, database_session, first_payload)
    assert first_response.status_code == 201

    second_response, second_body = _submit_onboarding_form(
        ui_module,
        database_session,
        [
            ("name", "Launch Radar"),
            ("slug", f"launch-radar-{uuid4().hex[:8]}"),
            ("description", "Duplicate name should fail cleanly"),
            ("timezone", "UTC"),
            ("boss", "olga"),
            ("admin", "alice"),
            ("board_integration", "yandex_tracker"),
            ("reporting_cadence", "weekly"),
            ("yandex_tracker_org_id", "org-99"),
            ("yandex_tracker_queue", "OPS"),
            ("yandex_tracker_token", "super-secret-duplicate-token"),
            ("employee_name", "alice"),
            ("employee_function", "operations"),
        ],
    )

    assert second_response.status_code == 409
    assert "project name already exists" in second_body
    assert "super-secret-duplicate-token" not in second_body


def test_onboarding_page_smoke_check_failure_returns_html_error_without_secret_echo(onboarding_context, monkeypatch):
    ui_module, database_session, _ = onboarding_context
    from bro_pm.integrations import IntegrationError

    def yandex_execute_stub(*, action: str, payload: dict):
        raise IntegrationError("synthetic smoke check failed")

    monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "execute", yandex_execute_stub)

    response, body = _submit_onboarding_form(
        ui_module,
        database_session,
        {
            "name": "Launch Radar",
            "slug": f"launch-radar-{uuid4().hex[:8]}",
            "description": "Server-rendered onboarding page",
            "timezone": "UTC",
            "boss": "olga",
            "admin": "alice",
            "board_integration": "yandex_tracker",
            "reporting_cadence": "weekly",
            "yandex_tracker_org_id": "org-99",
            "yandex_tracker_queue": "OPS",
            "yandex_tracker_token": "secret-token",
            "employee_name": "alice",
            "employee_function": "operations",
        },
    )

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("text/html")
    assert "synthetic smoke check failed" in body
    assert "secret-token" not in body


def test_onboarding_page_validation_error_does_not_echo_secret_values(onboarding_context):
    ui_module, database_session, _ = onboarding_context

    response, body = _submit_onboarding_form(
        ui_module,
        database_session,
        {
            "name": "Launch Radar",
            "slug": f"launch-radar-{uuid4().hex[:8]}",
            "description": "Server-rendered onboarding page",
            "timezone": "UTC",
            "boss": "olga",
            "admin": "alice",
            "board_integration": "yandex_tracker",
            "reporting_cadence": "weekly",
            "yandex_tracker_org_id": "org-99",
            "yandex_tracker_queue": "OPS",
            "yandex_tracker_token": "secret-token",
            "employee_name": "alice",
            "employee_function": "",
        },
    )

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("text/html")
    assert "Submission failed." in body
    assert "secret-token" not in body


def test_core_api_handlers_and_db_dependency_remain_sync(web_client: TestClient):
    database = importlib.import_module("bro_pm.database")
    commands_api = importlib.import_module("bro_pm.api.v1.commands")
    gateway_api = importlib.import_module("bro_pm.api.v1.gateway")
    projects_api = importlib.import_module("bro_pm.api.v1.projects")

    assert not inspect.isasyncgenfunction(database.get_db_session)
    assert not inspect.iscoroutinefunction(commands_api.router.routes[0].endpoint)
    assert all(not inspect.iscoroutinefunction(route.endpoint) for route in gateway_api.router.routes)
    assert all(not inspect.iscoroutinefunction(route.endpoint) for route in projects_api.router.routes)


def test_packaging_metadata_includes_onboarding_runtime_dependencies_and_template():
    pyproject = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(encoding="utf-8"))

    dependencies = pyproject["project"]["dependencies"]
    assert any(dep.startswith("jinja2") for dep in dependencies)
    assert any(dep.startswith("python-multipart") for dep in dependencies)

    package_data = pyproject["tool"]["setuptools"]["package-data"]["bro_pm"]
    assert "templates/*.html" in package_data
