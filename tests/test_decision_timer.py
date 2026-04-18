from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult


@pytest.fixture
def decision_db(tmp_path):
    db_path = tmp_path / f"bro_pm_decision_timer_{uuid4().hex}.db"
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


def _create_project(decision_db, *, safe_paused: bool = False, reporting_cadence: str = "weekly") -> str:
    session = decision_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
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


def _create_goal(decision_db, project_id: str, *, status: str = "active") -> str:
    session = decision_db.SessionLocal()
    try:
        goal = models.Goal(
            project_id=project_id,
            title="Keep momentum",
            description="active goal for decision timer tests",
            status=status,
        )
        session.add(goal)
        session.commit()
        return goal.id
    finally:
        session.close()


def _create_task(
    decision_db,
    project_id: str,
    *,
    status: str = "todo",
    due_at: datetime | None = None,
    title: str | None = None,
) -> str:
    session = decision_db.SessionLocal()
    try:
        task = models.Task(
            project_id=project_id,
            title=title or f"Task {uuid4().hex[:8]}",
            description="timer decision task",
            status=status,
            priority="medium",
            due_at=due_at,
        )
        session.add(task)
        session.commit()
        return task.id
    finally:
        session.close()


def _insert_audit_event(
    decision_db,
    *,
    project_id: str,
    actor: str,
    action: str,
    result: str,
    payload: dict,
    created_at: datetime,
    idempotency_key: str | None = None,
) -> str:
    session = decision_db.SessionLocal()
    try:
        event = models.AuditEvent(
            project_id=project_id,
            actor=actor,
            action=action,
            target_type="proposal",
            target_id=project_id,
            payload=json.dumps(payload, ensure_ascii=False),
            result=result,
            idempotency_key=idempotency_key,
            created_at=created_at,
        )
        session.add(event)
        session.commit()
        return event.id
    finally:
        session.close()


def _events_for(decision_db, project_id: str, *, actor: str | None = None, action: str | None = None) -> list[models.AuditEvent]:
    session = decision_db.SessionLocal()
    try:
        query = session.query(models.AuditEvent).filter_by(project_id=project_id)
        if actor is not None:
            query = query.filter_by(actor=actor)
        if action is not None:
            query = query.filter_by(action=action)
        return query.order_by(models.AuditEvent.created_at.asc(), models.AuditEvent.id.asc()).all()
    finally:
        session.close()


def _payload(event: models.AuditEvent) -> dict:
    return json.loads(event.payload)


def test_run_due_decisions_once_escalates_after_repeated_failures(decision_db):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(decision_db)

    _insert_audit_event(
        decision_db,
        project_id=project_id,
        actor="system",
        action="publish_report",
        result="failed",
        payload={"detail": "report publish failed"},
        created_at=now - timedelta(hours=2),
    )
    _insert_audit_event(
        decision_db,
        project_id=project_id,
        actor="system",
        action="create_task",
        result="denied",
        payload={"detail": "integration rejected create_task"},
        created_at=now - timedelta(hours=1),
    )

    result = scheduler.run_due_decisions_once(session_factory=decision_db.SessionLocal, now=now)

    assert result == 1
    autonomy_events = _events_for(
        decision_db,
        project_id,
        actor=scheduler.AUTONOMOUS_ACTOR,
        action="draft_boss_escalation",
    )
    assert len(autonomy_events) == 1
    assert autonomy_events[0].result == "awaiting_approval"
    payload = _payload(autonomy_events[0])
    assert payload["proposal"]["payload"]["trace_label"] == "timer_failure_escalation"
    assert "recent failures" in payload["proposal"]["payload"]["escalation_message"].lower()


def test_run_due_decisions_once_creates_followup_task_for_active_goal_without_open_tasks(decision_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(decision_db)
    _create_goal(decision_db, project_id, status="active")
    create_calls: list[dict] = []

    def execute_stub(*, action: str, payload: dict):
        create_calls.append(payload)
        return IntegrationResult(ok=True, detail="notion executed: create_task")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    result = scheduler.run_due_decisions_once(session_factory=decision_db.SessionLocal, now=now)

    assert result == 1
    assert len(create_calls) == 1
    autonomy_events = _events_for(
        decision_db,
        project_id,
        actor=scheduler.AUTONOMOUS_ACTOR,
        action="create_task",
    )
    assert len(autonomy_events) == 1
    payload = _payload(autonomy_events[0])
    assert payload["proposal"]["payload"]["trace_label"] == "timer_goal_without_open_tasks"
    assert payload["auth"]["execute_integration"] is True

    second_run = scheduler.run_due_decisions_once(
        session_factory=decision_db.SessionLocal,
        now=now + timedelta(minutes=10),
    )
    assert second_run == 0


def test_run_due_decisions_once_creates_overdue_replan_task(decision_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(decision_db)
    for idx in range(3):
        _create_task(
            decision_db,
            project_id,
            status="todo",
            due_at=now - timedelta(hours=idx + 1),
            title=f"Overdue {idx}",
        )
    create_calls: list[dict] = []

    def execute_stub(*, action: str, payload: dict):
        create_calls.append(payload)
        return IntegrationResult(ok=True, detail="notion executed: create_task")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    result = scheduler.run_due_decisions_once(session_factory=decision_db.SessionLocal, now=now)

    assert result == 1
    assert len(create_calls) == 1
    autonomy_events = _events_for(
        decision_db,
        project_id,
        actor=scheduler.AUTONOMOUS_ACTOR,
        action="create_task",
    )
    assert len(autonomy_events) == 1
    payload = _payload(autonomy_events[0])
    assert payload["proposal"]["payload"]["trace_label"] == "timer_overdue_replan"
    assert "overdue" in payload["proposal"]["payload"]["description"].lower()


def test_run_due_decisions_once_skips_safe_paused_projects(decision_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(decision_db, safe_paused=True)
    _create_goal(decision_db, project_id, status="active")
    _insert_audit_event(
        decision_db,
        project_id=project_id,
        actor="system",
        action="publish_report",
        result="failed",
        payload={"detail": "report publish failed"},
        created_at=now - timedelta(hours=2),
    )
    create_calls: list[dict] = []

    def execute_stub(*, action: str, payload: dict):
        create_calls.append(payload)
        return IntegrationResult(ok=True, detail="notion executed: create_task")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    result = scheduler.run_due_decisions_once(session_factory=decision_db.SessionLocal, now=now)

    assert result == 0
    assert create_calls == []
    assert _events_for(decision_db, project_id, actor=scheduler.AUTONOMOUS_ACTOR) == []


def test_run_due_decisions_once_respects_recent_autonomy_cooldown(decision_db, monkeypatch):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(decision_db)
    _create_goal(decision_db, project_id, status="active")
    _insert_audit_event(
        decision_db,
        project_id=project_id,
        actor=scheduler.AUTONOMOUS_ACTOR,
        action="create_task",
        result="executed",
        payload={
            "proposal": {
                "payload": {
                    "trace_label": "timer_goal_without_open_tasks"
                }
            }
        },
        created_at=now - timedelta(hours=1),
        idempotency_key="timer-decision:cooldown",
    )
    create_calls: list[dict] = []

    def execute_stub(*, action: str, payload: dict):
        create_calls.append(payload)
        return IntegrationResult(ok=True, detail="notion executed: create_task")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    result = scheduler.run_due_decisions_once(session_factory=decision_db.SessionLocal, now=now)

    assert result == 0
    assert create_calls == []
    autonomy_events = _events_for(decision_db, project_id, actor=scheduler.AUTONOMOUS_ACTOR, action="create_task")
    assert len(autonomy_events) == 1
