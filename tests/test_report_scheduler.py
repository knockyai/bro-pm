from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult


@pytest.fixture
def scheduler_db(tmp_path):
    db_path = tmp_path / f"bro_pm_scheduler_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.v1",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
        "bro_pm.services.report_scheduler",
    ):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    yield database


def _create_project(database, *, reporting_cadence: str, safe_paused: bool = False, timezone_name: str = "UTC") -> str:
    session = database.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone=timezone_name,
            safe_paused=safe_paused,
            created_by="alice",
            metadata_json={
                "onboarding": {
                    "status": "active",
                    "policy": "default_mvp",
                    "reporting_cadence": reporting_cadence,
                    "board_integration": "notion",
                    "communication_integrations": ["slack"],
                }
            },
        )
        session.add(project)
        session.commit()
        return project.id
    finally:
        session.close()


def _publish_events(database, project_id: str) -> list[models.AuditEvent]:
    session = database.SessionLocal()
    try:
        return (
            session.query(models.AuditEvent)
            .filter_by(project_id=project_id, action="publish_report")
            .order_by(models.AuditEvent.created_at.asc(), models.AuditEvent.id.asc())
            .all()
        )
    finally:
        session.close()


def test_run_due_once_publishes_due_weekly_project_once_per_window(scheduler_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    project_id = _create_project(scheduler_db, reporting_cadence="weekly")
    publish_calls: list[str] = []

    def execute_stub(*, action: str, payload: dict):
        publish_calls.append(payload["report"]["project_id"])
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)
    now = datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc)

    first_run = scheduler.run_due_once(session_factory=scheduler_db.SessionLocal, now=now)
    second_run = scheduler.run_due_once(session_factory=scheduler_db.SessionLocal, now=now)

    assert first_run == 1
    assert second_run == 0
    assert publish_calls == [project_id]
    publish_events = _publish_events(scheduler_db, project_id)
    assert len(publish_events) == 1
    assert publish_events[0].result == "executed"


def test_run_due_once_skips_safe_paused_manual_and_unsupported_projects(scheduler_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    paused_project_id = _create_project(scheduler_db, reporting_cadence="weekly", safe_paused=True)
    manual_project_id = _create_project(scheduler_db, reporting_cadence="manual")
    unsupported_project_id = _create_project(scheduler_db, reporting_cadence="monthly")
    publish_calls: list[str] = []

    def execute_stub(*, action: str, payload: dict):
        publish_calls.append(payload["report"]["project_id"])
        return IntegrationResult(ok=True, detail="notion executed: publish_report")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    result = scheduler.run_due_once(
        session_factory=scheduler_db.SessionLocal,
        now=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )

    assert result == 0
    assert publish_calls == []
    assert _publish_events(scheduler_db, paused_project_id) == []
    assert _publish_events(scheduler_db, manual_project_id) == []
    assert _publish_events(scheduler_db, unsupported_project_id) == []


def test_create_app_scheduler_startup_is_explicitly_controllable(tmp_path, monkeypatch):
    db_path = tmp_path / f"bro_pm_scheduler_app_{uuid4().hex}.db"
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
    start_calls: list[float] = []

    def start_stub(*, poll_interval_seconds: float, session_factory):
        start_calls.append(poll_interval_seconds)
        return asyncio.get_running_loop().create_task(asyncio.sleep(3600))

    monkeypatch.setattr(api_app, "_start_report_scheduler_task", start_stub)

    async def run_lifespan(
        *,
        database_url_value: str | None,
        enable_scheduler: bool | None,
        poll_interval_seconds: float | None = None,
    ) -> None:
        app = api_app.create_app(
            database_url=database_url_value,
            enable_scheduler=enable_scheduler,
            scheduler_poll_interval_seconds=poll_interval_seconds,
        )
        async with app.router.lifespan_context(app):
            pass

    assert start_calls == []

    asyncio.run(run_lifespan(database_url_value=db_url, enable_scheduler=None))
    assert start_calls == []

    asyncio.run(run_lifespan(database_url_value=db_url, enable_scheduler=False))
    assert start_calls == []

    asyncio.run(run_lifespan(database_url_value=db_url, enable_scheduler=True, poll_interval_seconds=5.0))
    assert start_calls == [5.0]
