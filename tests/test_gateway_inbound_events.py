from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException

from bro_pm import models


@pytest.fixture
def gateway_db(tmp_path):
    db_path = tmp_path / f"bro_pm_gateway_inbound_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in (
        "bro_pm.database",
        "bro_pm.api.v1.gateway",
        "bro_pm.services.gateway_service",
    ):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    yield database


def _create_project(gateway_db) -> dict:
    session = gateway_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-hermes-{uuid4().hex[:8]}",
            description="gateway inbound events test",
            timezone="UTC",
            created_by="admin-user",
            metadata_json={
                "onboarding": {
                    "status": "active",
                    "policy": "default_mvp",
                    "reporting_cadence": "weekly",
                    "board_integration": "notion",
                    "boss": "boss-user",
                    "admin": "admin-user",
                    "communication_integrations": ["telegram"],
                    "team": [{"name": "ops", "owner": "owner-user", "capacity": 3}],
                }
            },
        )
        session.add(project)
        session.flush()
        session.add(models.ProjectMembership(project_id=project.id, actor="boss-user", role="owner"))
        session.add(models.ProjectMembership(project_id=project.id, actor="admin-user", role="admin"))
        session.commit()
        return {"id": project.id}
    finally:
        session.close()


def _ingest_event(gateway_db, **payload):
    gateway_api = importlib.import_module("bro_pm.api.v1.gateway")
    session = gateway_db.SessionLocal()
    try:
        response = gateway_api.ingest_inbound_event(
            payload=gateway_api.schemas.InboundEventIngestRequest(**payload),
            db=session,
        )
        session.commit()
        return response
    finally:
        session.close()


def test_gateway_inbound_event_acks_due_action_and_persists_event(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

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

    response = _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="chat-123",
        thread_id="thread-7",
        project_id=project["id"],
        actor="owner-user",
        actor_role="owner",
        text="ack",
        normalized_intent="acknowledge",
        due_action_id=due_action_id,
        metadata={"telegram_message_id": "msg-1"},
    )

    assert response.model_dump() == {
        "event_id": response.event_id,
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


def test_gateway_inbound_event_allows_reply_for_project_owner_context(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

    response = _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="chat-allow",
        project_id=project["id"],
        actor="owner-user",
        actor_role="owner",
        text="What is the current status?",
        normalized_intent="status_request",
        metadata={"source": "group_chat"},
    )

    body = response.model_dump()
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


def test_gateway_inbound_event_records_minimal_approval_reply(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

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

    response = _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="approval-chat",
        project_id=project["id"],
        actor="boss-user",
        actor_role="boss",
        text="approved",
        normalized_intent="approve",
        pending_audit_id=pending_audit_id,
        metadata={"source": "dm"},
    )

    body = response.model_dump()
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


def test_gateway_inbound_event_updates_action_execution_status_for_approval_reply(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

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
        session.flush()
        session.add(
            models.ActionExecution(
                audit_event_id=pending_audit.id,
                project_id=project["id"],
                actor="alice",
                action="draft_boss_escalation",
                status="awaiting_approval",
                awaiting_approval_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        )
        session.commit()
        pending_audit_id = pending_audit.id
    finally:
        session.close()

    _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="approval-chat",
        project_id=project["id"],
        actor="boss-user",
        actor_role="boss",
        text="approved",
        normalized_intent="approve",
        pending_audit_id=pending_audit_id,
        metadata={"source": "dm"},
    )

    session = database.SessionLocal()
    try:
        execution = session.query(models.ActionExecution).filter_by(audit_event_id=pending_audit_id).one()
        assert execution.status == "approved"
        assert execution.awaiting_approval_at is not None
        assert execution.executed_at is None
        assert execution.verified_at is None
    finally:
        session.close()


def test_gateway_inbound_event_does_not_allow_reply_from_forged_actor_role(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

    response = _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="chat-forged-role",
        project_id=project["id"],
        actor="mallory",
        actor_role="owner",
        text="Let me answer that",
        normalized_intent="status_request",
    )

    body = response.model_dump()
    assert body["disposition"] == "ignore"
    assert body["reason"] == "no allowed reaction for inbound event"

    session = database.SessionLocal()
    try:
        event = session.query(models.ConversationEvent).one()
        assert event.actor == "mallory"
        assert event.actor_role == "owner"
        assert event.disposition == "ignore"
    finally:
        session.close()


def test_gateway_inbound_event_does_not_mutate_pending_audit_for_untrusted_actor(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

    session = database.SessionLocal()
    try:
        pending_audit = models.AuditEvent(
            project_id=project["id"],
            actor="alice",
            action="draft_boss_escalation",
            target_type="proposal",
            target_id=project["id"],
            payload=json.dumps({"proposal": {"action": "draft_boss_escalation", "project_id": project["id"]}}),
            result="awaiting_approval",
        )
        session.add(pending_audit)
        session.commit()
        pending_audit_id = pending_audit.id
    finally:
        session.close()

    response = _ingest_event(
        gateway_db,
        platform="telegram",
        chat_id="approval-chat-untrusted",
        project_id=project["id"],
        actor="mallory",
        actor_role="boss",
        text="approved",
        normalized_intent="approve",
        pending_audit_id=pending_audit_id,
    )

    body = response.model_dump()
    assert body["disposition"] == "ignore"
    assert body["pending_audit_id"] == pending_audit_id

    session = database.SessionLocal()
    try:
        stored_audit = session.get(models.AuditEvent, pending_audit_id)
        assert stored_audit is not None
        assert stored_audit.result == "awaiting_approval"
        payload = json.loads(stored_audit.payload)
        assert "approval" not in payload

        event = session.query(models.ConversationEvent).one()
        assert event.pending_audit_id == pending_audit_id
        assert event.disposition == "ignore"
    finally:
        session.close()


def test_gateway_inbound_event_rejects_due_action_ack_when_context_does_not_match(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

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

    with pytest.raises(HTTPException) as exc_info:
        _ingest_event(
            gateway_db,
            platform="slack",
            chat_id="chat-mismatch",
            project_id=project["id"],
            actor="admin-user",
            actor_role="admin",
            text="ack",
            normalized_intent="acknowledge",
            due_action_id=due_action_id,
        )

    assert exc_info.value.status_code == 409
    assert "does not match inbound event context" in exc_info.value.detail

    session = database.SessionLocal()
    try:
        stored_action = session.get(models.DueAction, due_action_id)
        assert stored_action is not None
        assert stored_action.status == "delivered"
        assert stored_action.acked_at is None
        assert session.query(models.ConversationEvent).count() == 0
    finally:
        session.close()


def test_gateway_inbound_event_rejects_unknown_references_without_persisting_event(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)

    with pytest.raises(HTTPException) as due_action_exc:
        _ingest_event(
            gateway_db,
            platform="telegram",
            chat_id="chat-unknown-due-action",
            project_id=project["id"],
            actor="owner-user",
            actor_role="owner",
            text="ack",
            normalized_intent="acknowledge",
            due_action_id="missing-due-action",
        )
    assert due_action_exc.value.status_code == 404
    assert "missing-due-action" in due_action_exc.value.detail

    with pytest.raises(HTTPException) as pending_audit_exc:
        _ingest_event(
            gateway_db,
            platform="telegram",
            chat_id="chat-unknown-pending-audit",
            project_id=project["id"],
            actor="boss-user",
            actor_role="boss",
            text="approved",
            normalized_intent="approve",
            pending_audit_id="missing-pending-audit",
        )
    assert pending_audit_exc.value.status_code == 404
    assert "missing-pending-audit" in pending_audit_exc.value.detail

    session = database.SessionLocal()
    try:
        assert session.query(models.ConversationEvent).count() == 0
    finally:
        session.close()


def test_gateway_inbound_event_rejects_cross_project_pending_audit_reference(gateway_db):
    database = importlib.import_module("bro_pm.database")
    project = _create_project(gateway_db)
    other_project = _create_project(gateway_db)

    session = database.SessionLocal()
    try:
        pending_audit = models.AuditEvent(
            project_id=other_project["id"],
            actor="alice",
            action="draft_boss_escalation",
            target_type="proposal",
            target_id=other_project["id"],
            payload=json.dumps({"proposal": {"action": "draft_boss_escalation", "project_id": other_project["id"]}}),
            result="awaiting_approval",
        )
        session.add(pending_audit)
        session.commit()
        pending_audit_id = pending_audit.id
    finally:
        session.close()

    with pytest.raises(HTTPException) as exc_info:
        _ingest_event(
            gateway_db,
            platform="telegram",
            chat_id="approval-chat-cross-project",
            project_id=project["id"],
            actor="boss-user",
            actor_role="boss",
            text="approved",
            normalized_intent="approve",
            pending_audit_id=pending_audit_id,
        )

    assert exc_info.value.status_code == 409
    assert "does not belong to project" in exc_info.value.detail

    session = database.SessionLocal()
    try:
        stored_audit = session.get(models.AuditEvent, pending_audit_id)
        assert stored_audit is not None
        assert stored_audit.result == "awaiting_approval"
        assert session.query(models.ConversationEvent).count() == 0
    finally:
        session.close()
