from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult
from bro_pm.services.execution_outbox_service import ExecutionOutboxService


def _seed_outbox_record(session, *, project, integration_name: str, idempotency_key: str):
    payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "proposal": {
            "action": "create_task",
            "project_id": project.id,
            "reason": "parsed command",
            "payload": {
                "title": "verification task",
                "raw_command": "create task verification task",
            },
            "requires_approval": False,
        },
        "policy": {
            "allowed": True,
            "reason": "policy accepted",
            "requires_approval": False,
            "safe_pause_blocked": False,
        },
        "integration": {
            "name": integration_name,
            "action": "create_task",
            "status": "pending",
            "detail": f"{integration_name} integration execution pending",
        },
    }

    audit = models.AuditEvent(
        project_id=project.id,
        actor="alice",
        action="create_task",
        target_type="proposal",
        target_id=project.id,
        payload=json.dumps(payload, ensure_ascii=False),
        result="pending_integration",
        idempotency_key=idempotency_key,
    )
    session.add(audit)
    session.flush()
    session.add(
        models.ActionExecution(
            audit_event_id=audit.id,
            project_id=project.id,
            actor="alice",
            action="create_task",
            status="requested",
            requested_at=datetime.utcnow() - timedelta(minutes=1),
        )
    )
    session.add(
        models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name=integration_name,
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
    )
    session.commit()
    return audit


def _make_isolated_db_session():
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")
    session = database.SessionLocal()
    return session


def test_execution_outbox_worker_fails_closed_when_adapter_verification_fails(monkeypatch):
    session = _make_isolated_db_session()
    try:
        project = models.Project(name="Outbox verification project", slug="outbox-verification")
        session.add(project)
        session.flush()
        audit = _seed_outbox_record(
            session,
            project=project,
            integration_name="yandex_tracker",
            idempotency_key="verification-failure-outbox",
        )

        execute_calls = {"count": 0}
        verify_calls = {"count": 0}

        def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
            execute_calls["count"] += 1
            assert payload["bro_pm_execution"]["audit_event_id"] == audit.id
            return IntegrationResult(ok=True, detail="yandex_tracker executed: create_task", metadata={"issue_key": "OPS-42", "issue_id": "42"})

        def supports_verification_stub(*, action: str, payload: dict) -> bool:
            return True

        def verify_stub(*, action: str, payload: dict, result: IntegrationResult) -> IntegrationResult:
            verify_calls["count"] += 1
            assert result.metadata["issue_key"] == "OPS-42"
            return IntegrationResult(ok=False, detail="adapter verification mismatch")

        monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "execute", execute_stub)
        monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "supports_verification", supports_verification_stub)
        monkeypatch.setattr(INTEGRATIONS["yandex_tracker"], "verify_action_result", verify_stub)

        worker = ExecutionOutboxService(db_session=session)
        claimed = worker.claim_available(worker_id="worker-a", limit=1, now=datetime.utcnow())
        reconciled = worker.process_claimed(outbox_id=claimed[0].id, claim_token=claimed[0].claim_token)

        session.refresh(reconciled)
        session.refresh(audit)
        execution = session.query(models.ActionExecution).filter_by(audit_event_id=audit.id).one()
    finally:
        session.close()

    assert execute_calls["count"] == 1
    assert verify_calls["count"] == 1
    assert reconciled.status == "failed"
    assert reconciled.last_error == "adapter verification mismatch"
    assert audit.result == "denied"
    assert execution.status == "denied"


def test_execution_outbox_worker_preserves_legacy_execute_semantics_when_adapter_has_no_verifier(monkeypatch):
    session = _make_isolated_db_session()
    try:
        project = models.Project(name="Outbox legacy project", slug="outbox-legacy")
        session.add(project)
        session.flush()
        audit = _seed_outbox_record(
            session,
            project=project,
            integration_name="jira",
            idempotency_key="verification-skipped-outbox",
        )

        execute_calls = {"count": 0}

        def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
            execute_calls["count"] += 1
            assert payload["bro_pm_execution"]["audit_event_id"] == audit.id
            return IntegrationResult(ok=True, detail="jira executed: create_task", metadata={"issue_key": "JIRA-42"})

        def supports_verification_stub(*, action: str, payload: dict) -> bool:
            return False

        def verify_stub(*, action: str, payload: dict, result: IntegrationResult) -> IntegrationResult:
            raise AssertionError("verify_action_result must not run when adapter opts out of verification")

        monkeypatch.setattr(INTEGRATIONS["jira"], "execute", execute_stub)
        monkeypatch.setattr(INTEGRATIONS["jira"], "supports_verification", supports_verification_stub)
        monkeypatch.setattr(INTEGRATIONS["jira"], "verify_action_result", verify_stub)

        worker = ExecutionOutboxService(db_session=session)
        claimed = worker.claim_available(worker_id="worker-b", limit=1, now=datetime.utcnow())
        reconciled = worker.process_claimed(outbox_id=claimed[0].id, claim_token=claimed[0].claim_token)

        session.refresh(reconciled)
        session.refresh(audit)
        execution = session.query(models.ActionExecution).filter_by(audit_event_id=audit.id).one()
        audit_payload = json.loads(audit.payload)
    finally:
        session.close()

    assert execute_calls["count"] == 1
    assert reconciled.status == "completed"
    assert audit.result == "executed"
    assert execution.status == "verified"
    assert audit_payload["integration"]["detail"] == "jira executed: create_task"
    assert audit_payload["integration"]["execution_metadata"] == {"issue_key": "JIRA-42"}
    assert "verification" not in audit_payload["integration"]
