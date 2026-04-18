from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from bro_pm import models


@pytest.fixture
def api_client(tmp_path):
    db_path = tmp_path / f"bro_pm_gateway_inbound_{uuid4().hex}.db"
    if db_path.exists():
        db_path.unlink()
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.app",
        "bro_pm.api",
        "bro_pm.api.v1",
        "bro_pm.api.v1.gateway",
        "bro_pm.api.v1.commands",
        "bro_pm.api.v1.projects",
        "bro_pm.services.gateway_service",
    ):
        sys.modules.pop(mod_name, None)

    api_app = importlib.import_module("bro_pm.api.app")
    with TestClient(api_app.create_app(database_url=db_url)) as client:
        yield client


def _create_project(api_client: TestClient) -> dict:
    payload = {
        "name": "Project Hermes",
        "slug": f"project-hermes-{uuid4().hex[:8]}",
        "description": "gateway inbound events test",
        "timezone": "UTC",
        "boss": "boss-user",
        "admin": "admin-user",
        "reporting_cadence": "weekly",
        "communication_integrations": ["telegram"],
        "board_integration": "notion",
        "team": [{"name": "ops", "owner": "owner-user", "capacity": 3}],
    }
    response = api_client.post("/api/v1/projects/onboard", json=payload)
    assert response.status_code == 201
    return response.json()["project"]


def test_gateway_inbound_event_acks_due_action_and_persists_event(api_client: TestClient):
    gateway_api = importlib.import_module("bro_pm.api.v1.gateway")
    database = importlib.import_module("bro_pm.database")
    project = _create_project(api_client)

    session = database.SessionLocal()
    try:
        due_action = models.DueAction(
            project_id=project["id"],
            channel="telegram",
            recipient="owner-user",
            kind="follow_up",
            payload_json={"text": "Please confirm"},
            due_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            status="delivered",
            delivered_at=datetime.now(timezone.utc) - timedelta(minutes=4),
        )
        session.add(due_action)
        session.commit()
        due_action_id = due_action.id
    finally:
        session.close()

    response = api_client.post(
        "/api/v1/gateway/events:ingest",
        json={
            "platform": "telegram",
            "chat_id": "chat-123",
            "thread_id": "thread-7",
            "project_id": project["id"],
            "actor": "owner-user",
            "actor_role": "owner",
            "text": "ack",
            "normalized_intent": "acknowledge",
            "due_action_id": due_action_id,
            "metadata": {"telegram_message_id": "msg-1"},
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "event_id": response.json()["event_id"],
        "disposition": "ack_due_action",
        "reason": "due action acknowledgement recorded",
        "due_action_id": due_action_id,
        "pending_audit_id": None,
        "project_id": project["id"],
    }

    session = database.SessionLocal()
    try:
        stored_action = session.get(models.DueAction, due_action_id)
        assert stored_action is not None
        assert stored_action.status == "acked"
        assert stored_action.acked_at is not None

        events = session.query(models.ConversationEvent).all()
        assert len(events) == 1
        event = events[0]
        assert event.platform == "telegram"
        assert event.chat_id == "chat-123"
        assert event.thread_id == "thread-7"
        assert event.project_id == project["id"]
        assert event.actor == "owner-user"
        assert event.actor_role == "owner"
        assert event.normalized_intent == "acknowledge"
        assert event.due_action_id == due_action_id
        assert event.disposition == "ack_due_action"
        assert event.decision_reason == "due action acknowledgement recorded"
        assert event.metadata_json == {"telegram_message_id": "msg-1"}
    finally:
        session.close()


def test_gateway_inbound_event_allows_reply_for_project_owner_context(api_client: TestClient):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(api_client)

    response = api_client.post(
        "/api/v1/gateway/events:ingest",
        json={
            "platform": "telegram",
            "chat_id": "chat-allow",
            "project_id": project["id"],
            "actor": "owner-user",
            "actor_role": "owner",
            "text": "What is the current status?",
            "normalized_intent": "status_request",
            "metadata": {"source": "group_chat"},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["disposition"] == "allow_reply"
    assert body["reason"] == "trusted project actor may receive a reply"
    assert body["project_id"] == project["id"]
    assert body["due_action_id"] is None
    assert body["pending_audit_id"] is None

    session = database.SessionLocal()
    try:
        event = session.query(models.ConversationEvent).one()
        assert event.disposition == "allow_reply"
        assert event.decision_reason == "trusted project actor may receive a reply"
    finally:
        session.close()


def test_gateway_inbound_event_records_minimal_approval_reply(api_client: TestClient):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(api_client)

    session = database.SessionLocal()
    try:
        pending_audit = models.AuditEvent(
            project_id=project["id"],
            actor="alice",
            action="draft_boss_escalation",
            target_type="proposal",
            target_id=project["id"],
            payload=json.dumps(
                {
                    "proposal": {
                        "action": "draft_boss_escalation",
                        "project_id": project["id"],
                        "payload": {"escalation_message": "Customers are blocked"},
                    }
                }
            ),
            result="awaiting_approval",
        )
        session.add(pending_audit)
        session.commit()
        pending_audit_id = pending_audit.id
    finally:
        session.close()

    response = api_client.post(
        "/api/v1/gateway/events:ingest",
        json={
            "platform": "telegram",
            "chat_id": "approval-chat",
            "project_id": project["id"],
            "actor": "boss-user",
            "actor_role": "boss",
            "text": "approved",
            "normalized_intent": "approve",
            "pending_audit_id": pending_audit_id,
            "metadata": {"source": "dm"},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["disposition"] == "approval_reply_recorded"
    assert body["reason"] == "approval reply recorded for pending audit event"
    assert body["pending_audit_id"] == pending_audit_id
    assert body["project_id"] == project["id"]

    session = database.SessionLocal()
    try:
        stored_audit = session.get(models.AuditEvent, pending_audit_id)
        assert stored_audit is not None
        assert stored_audit.result == "approved"
        payload = json.loads(stored_audit.payload)
        assert payload["approval"] == {
            "status": "approved",
            "actor": "boss-user",
            "actor_role": "boss",
            "text": "approved",
        }

        event = session.query(models.ConversationEvent).one()
        assert event.pending_audit_id == pending_audit_id
        assert event.disposition == "approval_reply_recorded"
    finally:
        session.close()
