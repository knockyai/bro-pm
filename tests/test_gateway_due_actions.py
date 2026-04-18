from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from bro_pm import models


@pytest.fixture
def gateway_db(tmp_path):
    db_path = tmp_path / f"bro_pm_gateway_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.v1.gateway",
        "bro_pm.services.gateway_service",
        "bro_pm.services.report_scheduler",
    ):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    yield database


def _create_project(gateway_db, *, communication_integrations: list[str] | None = None) -> str:
    session = gateway_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={
                "onboarding": {
                    "status": "active",
                    "policy": "default_mvp",
                    "reporting_cadence": "weekly",
                    "board_integration": "notion",
                    "boss": "boss-user",
                    "admin": "admin-user",
                    "communication_integrations": communication_integrations or ["telegram", "slack"],
                }
            },
        )
        session.add(project)
        session.commit()
        return project.id
    finally:
        session.close()


def test_gateway_claim_returns_due_action_and_marks_it_claimed(gateway_db):
    gateway_api = importlib.import_module("bro_pm.api.v1.gateway")
    due_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    project_id = _create_project(gateway_db)

    session = gateway_db.SessionLocal()
    try:
        action = models.DueAction(
            project_id=project_id,
            channel="telegram",
            recipient="boss-user",
            kind="boss_escalation",
            payload_json={"text": "Escalate to boss"},
            due_at=due_at,
            status="pending",
            idempotency_key=f"due-action-{uuid4().hex}",
        )
        session.add(action)
        session.commit()
        due_action_id = action.id

        response = gateway_api.claim_due_actions(
            payload=gateway_api.schemas.DueActionClaimRequest(gateway="hermes-poller", limit=10),
            db=session,
        )

        assert len(response.items) == 1
        claimed = response.items[0]
        assert claimed.id == due_action_id
        assert claimed.status == "claimed"
        assert claimed.channel == "telegram"
        assert claimed.recipient == "boss-user"
        assert claimed.claim_token
        assert claimed.claimed_by == "hermes-poller"
    finally:
        session.close()

    session = gateway_db.SessionLocal()
    try:
        stored = session.get(models.DueAction, due_action_id)
        assert stored is not None
        assert stored.status == "claimed"
        assert stored.claimed_by == "hermes-poller"
        assert stored.claimed_at is not None
    finally:
        session.close()


def test_gateway_delivery_ack_marks_due_action_delivered(gateway_db):
    gateway_api = importlib.import_module("bro_pm.api.v1.gateway")
    project_id = _create_project(gateway_db)

    session = gateway_db.SessionLocal()
    try:
        action = models.DueAction(
            project_id=project_id,
            channel="telegram",
            recipient="admin-user",
            kind="approval_request",
            payload_json={"text": "Need approval"},
            due_at=datetime.now(timezone.utc) - timedelta(minutes=1),
            status="pending",
        )
        session.add(action)
        session.commit()
        due_action_id = action.id

        claim_response = gateway_api.claim_due_actions(
            payload=gateway_api.schemas.DueActionClaimRequest(gateway="hermes-ack", limit=10),
            db=session,
        )
        claimed = claim_response.items[0]

        delivery_response = gateway_api.record_due_action_delivery(
            due_action_id=due_action_id,
            payload=gateway_api.schemas.DueActionDeliveryUpdateRequest(
                status="delivered",
                claim_token=claimed.claim_token,
                external_delivery_id="telegram-msg-123",
            ),
            db=session,
        )

        assert delivery_response.id == due_action_id
        assert delivery_response.status == "delivered"
        assert delivery_response.external_delivery_id == "telegram-msg-123"
        assert delivery_response.delivered_at is not None
    finally:
        session.close()


def test_decision_timer_failure_escalation_enqueues_due_action_for_gateway(gateway_db):
    scheduler = importlib.import_module("bro_pm.services.report_scheduler")
    now = datetime(2026, 4, 18, 10, 4, tzinfo=timezone.utc)
    project_id = _create_project(gateway_db)

    session = gateway_db.SessionLocal()
    try:
        session.add(
            models.AuditEvent(
                project_id=project_id,
                actor="system",
                action="publish_report",
                target_type="project",
                target_id=project_id,
                payload=json.dumps({"detail": "report publish failed"}),
                result="failed",
                created_at=now - timedelta(hours=2),
            )
        )
        session.add(
            models.AuditEvent(
                project_id=project_id,
                actor="system",
                action="create_task",
                target_type="project",
                target_id=project_id,
                payload=json.dumps({"detail": "integration rejected create_task"}),
                result="denied",
                created_at=now - timedelta(hours=1),
            )
        )
        session.commit()
    finally:
        session.close()

    result = scheduler.run_due_decisions_once(session_factory=gateway_db.SessionLocal, now=now)

    assert result == 1

    session = gateway_db.SessionLocal()
    try:
        due_actions = (
            session.query(models.DueAction)
            .filter(models.DueAction.project_id == project_id)
            .order_by(models.DueAction.created_at.asc(), models.DueAction.id.asc())
            .all()
        )
        assert len(due_actions) == 1
        due_action = due_actions[0]
        assert due_action.channel == "telegram"
        assert due_action.recipient == "boss-user"
        assert due_action.kind == "boss_escalation"
        assert due_action.status == "pending"
        assert due_action.idempotency_key == (
            "timer-decision:"
            f"{project_id}:timer_failure_escalation:"
            f"{scheduler._decision_window_for(now=now).key}"
        )
        assert "recent failures" in due_action.payload_json["text"].lower()

        autonomy_events = (
            session.query(models.AuditEvent)
            .filter(
                models.AuditEvent.project_id == project_id,
                models.AuditEvent.actor == scheduler.AUTONOMOUS_ACTOR,
            )
            .all()
        )
        assert autonomy_events == []
    finally:
        session.close()
