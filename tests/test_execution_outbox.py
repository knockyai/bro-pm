from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import importlib
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from uuid import uuid4

from bro_pm import models
from bro_pm.integrations import INTEGRATIONS, IntegrationResult
from bro_pm.services.execution_outbox_service import ExecutionOutboxConflictError, ExecutionOutboxService
from sqlalchemy.orm import Query as SAQuery, Session as SASession


def _make_isolated_db_session():
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")
    session = database.SessionLocal()
    return database, session


def _make_file_backed_db_module(tmp_path):
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    db_path = Path(tmp_path) / f"bro_pm_outbox_{uuid4().hex}.db"
    database.init_db(f"sqlite:///{db_path}")
    return database


def test_execution_outbox_worker_reconciles_stale_claim_from_terminal_audit_without_second_execute(monkeypatch):
    _, session = _make_isolated_db_session()
    try:
        project = models.Project(name="Outbox reconcile project", slug="outbox-reconcile")
        session.add(project)
        session.flush()

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
                    "title": "reconcile task",
                    "raw_command": "create task reconcile task",
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
                "name": "notion",
                "action": "create_task",
                "status": "executed",
                "detail": "notion executed before crash",
            },
        }

        audit = models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(payload, ensure_ascii=False),
            result="executed",
            idempotency_key="stale-claimed-outbox",
        )
        session.add(audit)
        session.flush()
        session.add(
            models.ActionExecution(
                audit_event_id=audit.id,
                project_id=project.id,
                actor="alice",
                action="create_task",
                status="executed",
                requested_at=datetime.utcnow() - timedelta(minutes=10),
                executed_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="claimed",
            claim_token="stale-claim-token",
            claimed_by="worker-a",
            claimed_at=datetime.utcnow() - timedelta(minutes=10),
            available_at=datetime.utcnow() - timedelta(minutes=10),
        )
        session.add(outbox)
        session.commit()

        execute_calls = {"count": 0}

        def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
            execute_calls["count"] += 1
            raise AssertionError("integration execute must not rerun during terminal stale-claim reconciliation")

        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_forbidden)

        worker = ExecutionOutboxService(db_session=session)
        claimed = worker.claim_available(worker_id="worker-b", limit=1, now=datetime.utcnow())
        assert len(claimed) == 1

        reconciled = worker.process_claimed(outbox_id=claimed[0].id, claim_token=claimed[0].claim_token)

        session.refresh(reconciled)
        execution = session.query(models.ActionExecution).filter_by(audit_event_id=audit.id).one()
    finally:
        session.close()

    assert execute_calls["count"] == 0
    assert reconciled.status == "completed"
    assert reconciled.last_error is None
    assert reconciled.completed_at is not None
    assert execution.status == "verified"
    assert execution.verified_at is not None


def test_execution_outbox_worker_marks_stale_claimed_pending_item_failed_without_reexecuting(monkeypatch):
    _, session = _make_isolated_db_session()
    try:
        project = models.Project(name="Outbox stale pending project", slug="outbox-stale-pending")
        session.add(project)
        session.flush()

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
                    "title": "stale pending task",
                    "raw_command": "create task stale pending task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
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
            idempotency_key="stale-claimed-pending-outbox",
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
                requested_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="claimed",
            claim_token="stale-pending-claim-token",
            claimed_by="worker-a",
            claimed_at=datetime.utcnow() - timedelta(minutes=10),
            available_at=datetime.utcnow() - timedelta(minutes=10),
            attempt_count=1,
        )
        session.add(outbox)
        session.commit()

        execute_calls = {"count": 0}

        def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
            execute_calls["count"] += 1
            raise AssertionError("stale claimed pending outbox must fail closed before re-executing integration")

        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_forbidden)

        worker = ExecutionOutboxService(db_session=session)
        claimed = worker.claim_available(worker_id="worker-b", limit=1, now=datetime.utcnow())
        assert len(claimed) == 1

        reconciled = worker.process_claimed(outbox_id=claimed[0].id, claim_token=claimed[0].claim_token)

        session.refresh(reconciled)
        session.refresh(audit)
        execution = session.query(models.ActionExecution).filter_by(audit_event_id=audit.id).one()
    finally:
        session.close()

    assert execute_calls["count"] == 0
    assert reconciled.status == "failed"
    assert reconciled.last_error == "stale claimed execution requires manual reconciliation before retry"
    assert audit.result == "denied"
    assert execution.status == "denied"


def test_execution_outbox_process_for_audit_event_rejects_fresh_claim_owned_by_other_worker(monkeypatch):
    _, session = _make_isolated_db_session()
    try:
        project = models.Project(name="Outbox fresh claim conflict project", slug="outbox-fresh-claim-conflict")
        session.add(project)
        session.flush()

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
                    "title": "fresh claim task",
                    "raw_command": "create task fresh claim task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
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
            idempotency_key="fresh-claimed-outbox",
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
                integration_name="notion",
                integration_action="create_task",
                payload_json=payload,
                status="claimed",
                claim_token="fresh-claim-token",
                claimed_by="worker-a",
                claimed_at=datetime.utcnow(),
                available_at=datetime.utcnow() - timedelta(minutes=1),
                attempt_count=1,
            )
        )
        session.commit()

        execute_calls = {"count": 0}

        def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
            execute_calls["count"] += 1
            raise AssertionError("fresh claim owned by another worker must not execute")

        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_forbidden)

        worker = ExecutionOutboxService(db_session=session)
        try:
            worker.process_for_audit_event(audit_event_id=audit.id, worker_id="worker-b")
            raise AssertionError("expected fresh foreign claim to raise conflict")
        except ExecutionOutboxConflictError:
            pass

        session.refresh(audit)
        stored_outbox = session.query(models.ExecutionOutbox).filter_by(audit_event_id=audit.id).one()
    finally:
        session.close()

    assert execute_calls["count"] == 0
    assert audit.result == "pending_integration"
    assert stored_outbox.status == "claimed"
    assert stored_outbox.claimed_by == "worker-a"
    assert stored_outbox.claim_token == "fresh-claim-token"


def test_execution_outbox_process_for_audit_event_executes_integration_once_under_concurrent_queue_claim(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Outbox queued claim project", slug="outbox-queued-claim")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "queued claim task",
                    "raw_command": "create task queued claim task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "queued claim task",
                "raw_command": "create task queued claim task",
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
            idempotency_key="queued-claim-outbox",
        )
        setup_session.add(audit)
        setup_session.flush()
        setup_session.add(
            models.ActionExecution(
                audit_event_id=audit.id,
                project_id=project.id,
                actor="alice",
                action="create_task",
                status="requested",
                requested_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        setup_session.add(
            models.ExecutionOutbox(
                audit_event_id=audit.id,
                project_id=project.id,
                execution_kind="integration_execute",
                integration_name="notion",
                integration_action="create_task",
                payload_json=payload,
                status="queued",
                available_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        setup_session.commit()
        audit_id = audit.id
    finally:
        setup_session.close()

    original_execute = INTEGRATIONS["notion"].execute
    start_barrier = Barrier(2)
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        time.sleep(0.05)
        return IntegrationResult(ok=True, detail="notion executed queued claim once")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    def _run_once(worker_id: str):
        session = database.SessionLocal()
        try:
            worker = ExecutionOutboxService(db_session=session)
            start_barrier.wait(timeout=5)
            item = worker.process_for_audit_event(audit_event_id=audit_id, worker_id=worker_id)
            session.refresh(item)
            return {"status": "ok", "outbox_status": item.status}
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [
                future.result()
                for future in [
                    executor.submit(_run_once, "worker-a"),
                    executor.submit(_run_once, "worker-b"),
                ]
            ]
    finally:
        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", original_execute)

    observer = database.SessionLocal()
    try:
        stored_outbox = observer.query(models.ExecutionOutbox).filter_by(audit_event_id=audit_id).one()
    finally:
        observer.close()

    assert call_counter["execute"] == 1
    assert sum(1 for outcome in outcomes if outcome["status"] == "ok") == 1
    assert stored_outbox.status == "completed"


def test_execution_outbox_claim_available_returns_single_claim_under_concurrent_workers(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Concurrent claim-available project", slug="concurrent-claim-available")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "claim available task",
                    "raw_command": "create task claim available task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "claim available task",
                "raw_command": "create task claim available task",
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
            idempotency_key="concurrent-claim-available-outbox",
        )
        setup_session.add(audit)
        setup_session.flush()
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add(outbox)
        setup_session.commit()
        outbox_id = outbox.id
    finally:
        setup_session.close()

    select_barrier = Barrier(2)
    original_all = SAQuery.all

    def synchronized_all(self):
        results = original_all(self)
        if self.session.info.get("sync_outbox_claim_select"):
            entities = {desc.get("entity") for desc in self.column_descriptions}
            if models.ExecutionOutbox in entities:
                select_barrier.wait(timeout=5)
        return results

    monkeypatch.setattr(SAQuery, "all", synchronized_all)
    start_barrier = Barrier(2)

    def _claim_once(worker_id: str):
        session = database.SessionLocal()
        session.info["sync_outbox_claim_select"] = True
        try:
            worker = ExecutionOutboxService(db_session=session)
            start_barrier.wait(timeout=5)
            claimed = worker.claim_available(worker_id=worker_id, limit=1, now=datetime.utcnow())
            return {
                "status": "ok",
                "items": [
                    {
                        "id": item.id,
                        "claim_token": item.claim_token,
                        "claimed_by": item.claimed_by,
                    }
                    for item in claimed
                ],
            }
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [
            future.result()
            for future in [
                executor.submit(_claim_once, "worker-a"),
                executor.submit(_claim_once, "worker-b"),
            ]
        ]

    observer = database.SessionLocal()
    try:
        stored_outbox = observer.get(models.ExecutionOutbox, outbox_id)
    finally:
        observer.close()

    claimed_items = [item for outcome in outcomes if outcome["status"] == "ok" for item in outcome["items"]]

    assert len(claimed_items) == 1
    assert claimed_items[0]["id"] == outbox_id
    assert stored_outbox is not None
    assert stored_outbox.status == "claimed"
    assert stored_outbox.claimed_by in {"worker-a", "worker-b"}
    assert claimed_items[0]["claim_token"] == stored_outbox.claim_token


def test_execution_outbox_claim_available_refreshes_preloaded_caller_row(tmp_path):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Preloaded caller refresh project", slug="preloaded-caller-refresh")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "refresh caller row task",
                    "raw_command": "create task refresh caller row task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "refresh caller row task",
                "raw_command": "create task refresh caller row task",
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
            idempotency_key="preloaded-caller-refresh",
        )
        setup_session.add(audit)
        setup_session.flush()
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add(outbox)
        setup_session.commit()
        outbox_id = outbox.id
    finally:
        setup_session.close()

    caller_session = database.SessionLocal()
    try:
        preloaded = caller_session.get(models.ExecutionOutbox, outbox_id)
        assert preloaded is not None
        assert preloaded.status == "queued"
        assert preloaded.claim_token is None

        worker = ExecutionOutboxService(db_session=caller_session)
        claimed = worker.claim_available(worker_id="worker-a", limit=1, now=datetime.utcnow())
    finally:
        caller_session.close()

    assert len(claimed) == 1
    assert claimed[0].id == outbox_id
    assert claimed[0].status == "claimed"
    assert claimed[0].claimed_by == "worker-a"
    assert claimed[0].claim_token is not None


def test_execution_outbox_claim_available_refills_after_first_claim_race(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Claim refill project", slug="claim-refill-project")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "claim refill task",
                    "raw_command": "create task claim refill task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "claim refill task",
                "raw_command": "create task claim refill task",
            },
        }

        audit_one = models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key="claim-refill-1",
        )
        audit_two = models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key="claim-refill-2",
        )
        setup_session.add_all([audit_one, audit_two])
        setup_session.flush()
        outbox_one = models.ExecutionOutbox(
            audit_event_id=audit_one.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=2),
        )
        outbox_two = models.ExecutionOutbox(
            audit_event_id=audit_two.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add_all([outbox_one, outbox_two])
        setup_session.commit()
        outbox_ids = {outbox_one.id, outbox_two.id}
    finally:
        setup_session.close()

    select_barrier = Barrier(2)
    original_all = SAQuery.all

    def synchronized_all(self):
        results = original_all(self)
        if self.session.info.get("sync_claim_refill_query"):
            entities = {desc.get("entity") for desc in self.column_descriptions}
            if models.ExecutionOutbox in entities:
                select_barrier.wait(timeout=5)
        return results

    monkeypatch.setattr(SAQuery, "all", synchronized_all)
    start_barrier = Barrier(2)

    def _claim_once(worker_id: str):
        session = database.SessionLocal()
        session.info["sync_claim_refill_query"] = True
        try:
            worker = ExecutionOutboxService(db_session=session)
            start_barrier.wait(timeout=5)
            claimed = worker.claim_available(worker_id=worker_id, limit=1, now=datetime.utcnow())
            return {"status": "ok", "ids": [item.id for item in claimed]}
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [
            future.result()
            for future in [
                executor.submit(_claim_once, "worker-a"),
                executor.submit(_claim_once, "worker-b"),
            ]
        ]

    claimed_ids = [outbox_id for outcome in outcomes if outcome["status"] == "ok" for outbox_id in outcome["ids"]]

    assert sorted(claimed_ids) == sorted(outbox_ids)


def test_execution_outbox_process_for_audit_event_does_not_double_execute_when_stale_session_overwrites_claim(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Outbox stale overwrite project", slug="outbox-stale-overwrite")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "stale overwrite task",
                    "raw_command": "create task stale overwrite task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "stale overwrite task",
                "raw_command": "create task stale overwrite task",
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
            idempotency_key="stale-overwrite-outbox",
        )
        setup_session.add(audit)
        setup_session.flush()
        setup_session.add(
            models.ActionExecution(
                audit_event_id=audit.id,
                project_id=project.id,
                actor="alice",
                action="create_task",
                status="requested",
                requested_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="queued",
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add(outbox)
        setup_session.commit()
        audit_id = audit.id
        outbox_id = outbox.id
    finally:
        setup_session.close()

    release_worker_b_claim = Event()
    query_barrier = Barrier(2)
    start_barrier = Barrier(2)
    execute_calls = {"count": 0}
    original_execute = INTEGRATIONS["notion"].execute
    original_all = SAQuery.all
    original_commit = SASession.commit
    original_claim_item_if_match = ExecutionOutboxService._claim_item_if_match

    def synchronized_all(self):
        results = original_all(self)
        if self.session.info.get("sync_outbox_process_query"):
            entities = {desc.get("entity") for desc in self.column_descriptions}
            if models.ExecutionOutbox in entities:
                query_barrier.wait(timeout=5)
        return results

    def synchronized_commit(self):
        if self.info.get("delay_orm_claim_commit_until_execute") and not self.info.get("orm_claim_commit_released"):
            self.info["orm_claim_commit_released"] = True
            release_worker_b_claim.wait(timeout=5)
        return original_commit(self)

    def synchronized_claim_item_if_match(self, *, item_id: str, worker_id: str, claim_time: datetime, conditions: list):
        if self.db_session.info.get("delay_atomic_claim_until_execute") and not self.db_session.info.get("atomic_claim_released"):
            self.db_session.info["atomic_claim_released"] = True
            release_worker_b_claim.wait(timeout=5)
        return original_claim_item_if_match(
            self,
            item_id=item_id,
            worker_id=worker_id,
            claim_time=claim_time,
            conditions=conditions,
        )

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        execute_calls["count"] += 1
        release_worker_b_claim.set()
        time.sleep(0.05)
        return IntegrationResult(ok=True, detail=f"notion execute call {execute_calls['count']}")

    monkeypatch.setattr(SAQuery, "all", synchronized_all)
    monkeypatch.setattr(SASession, "commit", synchronized_commit)
    monkeypatch.setattr(ExecutionOutboxService, "_claim_item_if_match", synchronized_claim_item_if_match)
    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    def _run_worker(worker_id: str):
        session = database.SessionLocal()
        session.info["sync_outbox_process_query"] = True
        if worker_id == "worker-b":
            session.info["delay_orm_claim_commit_until_execute"] = True
            session.info["delay_atomic_claim_until_execute"] = True
        try:
            worker = ExecutionOutboxService(db_session=session)
            start_barrier.wait(timeout=5)
            item = worker.process_for_audit_event(audit_event_id=audit_id, worker_id=worker_id)
            session.refresh(item)
            return {"status": "ok", "outbox_status": item.status}
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [
                future.result()
                for future in [
                    executor.submit(_run_worker, "worker-a"),
                    executor.submit(_run_worker, "worker-b"),
                ]
            ]
    finally:
        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", original_execute)

    observer = database.SessionLocal()
    try:
        stored_outbox = observer.get(models.ExecutionOutbox, outbox_id)
        stored_audit = observer.get(models.AuditEvent, audit_id)
    finally:
        observer.close()

    assert execute_calls["count"] == 1
    assert sum(1 for outcome in outcomes if outcome["status"] == "ok") == 1
    assert stored_outbox is not None
    assert stored_outbox.status == "completed"
    assert stored_audit is not None
    assert stored_audit.result == "executed"


def test_execution_outbox_claimed_item_executes_integration_once_under_concurrent_processing(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Concurrent outbox project", slug="concurrent-outbox")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "concurrent outbox task",
                    "raw_command": "create task concurrent outbox task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "concurrent outbox task",
                "raw_command": "create task concurrent outbox task",
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
            idempotency_key="concurrent-outbox-claim",
        )
        setup_session.add(audit)
        setup_session.flush()
        setup_session.add(
            models.ActionExecution(
                audit_event_id=audit.id,
                project_id=project.id,
                actor="alice",
                action="create_task",
                status="requested",
                requested_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="claimed",
            claim_token="shared-claim-token",
            claimed_by="worker-a",
            claimed_at=datetime.utcnow(),
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add(outbox)
        setup_session.commit()
        outbox_id = outbox.id
    finally:
        setup_session.close()

    original_execute = INTEGRATIONS["notion"].execute
    start_barrier = Barrier(2)
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        time.sleep(0.05)
        return IntegrationResult(ok=True, detail="notion executed exactly once")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    def _run_once():
        session = database.SessionLocal()
        try:
            worker = ExecutionOutboxService(db_session=session)
            start_barrier.wait(timeout=5)
            item = worker.process_claimed(outbox_id=outbox_id, claim_token="shared-claim-token")
            session.commit()
            return {"status": "ok", "outbox_status": item.status}
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    outcomes = []
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [future.result() for future in [executor.submit(_run_once), executor.submit(_run_once)]]
    finally:
        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", original_execute)

    observer = database.SessionLocal()
    try:
        stored_outbox = observer.get(models.ExecutionOutbox, outbox_id)
        stored_execution = observer.query(models.ActionExecution).filter_by(audit_event_id=stored_outbox.audit_event_id).one()
    finally:
        observer.close()

    assert call_counter["execute"] == 1
    assert sum(1 for outcome in outcomes if outcome["status"] == "ok") == 1
    assert stored_outbox.status == "completed"
    assert stored_execution.status == "verified"


def test_execution_outbox_processing_does_not_commit_unrelated_caller_session_changes(tmp_path, monkeypatch):
    database = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = models.Project(name="Outbox isolation project", slug="outbox-isolation")
        setup_session.add(project)
        setup_session.flush()

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
                    "title": "isolation task",
                    "raw_command": "create task isolation task",
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
                "name": "notion",
                "action": "create_task",
                "status": "pending",
                "detail": "notion integration execution pending",
            },
            "integration_payload": {
                "project_id": project.id,
                "title": "isolation task",
                "raw_command": "create task isolation task",
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
            idempotency_key="outbox-isolation-claim",
        )
        setup_session.add(audit)
        setup_session.flush()
        setup_session.add(
            models.ActionExecution(
                audit_event_id=audit.id,
                project_id=project.id,
                actor="alice",
                action="create_task",
                status="requested",
                requested_at=datetime.utcnow() - timedelta(minutes=1),
            )
        )
        outbox = models.ExecutionOutbox(
            audit_event_id=audit.id,
            project_id=project.id,
            execution_kind="integration_execute",
            integration_name="notion",
            integration_action="create_task",
            payload_json=payload,
            status="claimed",
            claim_token="isolation-claim-token",
            claimed_by="worker-a",
            claimed_at=datetime.utcnow(),
            available_at=datetime.utcnow() - timedelta(minutes=1),
        )
        setup_session.add(outbox)
        setup_session.commit()
        project_id = project.id
        audit_id = audit.id
        outbox_id = outbox.id
    finally:
        setup_session.close()

    original_execute = INTEGRATIONS["notion"].execute
    captured_payload: dict[str, object] = {}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        captured_payload.clear()
        captured_payload.update(payload)
        return IntegrationResult(ok=True, detail="notion executed without leaking caller transaction")

    monkeypatch.setattr(INTEGRATIONS["notion"], "execute", execute_stub)

    caller_session = database.SessionLocal()
    try:
        caller_project = caller_session.get(models.Project, project_id)
        caller_project.description = "uncommitted caller mutation"
        worker = ExecutionOutboxService(db_session=caller_session)
        processed = worker.process_claimed(outbox_id=outbox_id, claim_token="isolation-claim-token")
        caller_session.refresh(processed)
    finally:
        caller_session.close()
        monkeypatch.setattr(INTEGRATIONS["notion"], "execute", original_execute)

    observer = database.SessionLocal()
    try:
        observed_project = observer.get(models.Project, project_id)
        observed_outbox = observer.get(models.ExecutionOutbox, outbox_id)
    finally:
        observer.close()

    assert observed_outbox.status == "completed"
    assert observed_project.description is None
    assert captured_payload["project_id"] == project_id
    assert captured_payload["bro_pm_execution"] == {
        "audit_event_id": audit_id,
        "execution_outbox_id": outbox_id,
        "idempotency_key": "outbox-isolation-claim",
        "attempt_count": 0,
    }
