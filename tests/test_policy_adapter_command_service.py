from __future__ import annotations

import json
import importlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest

from bro_pm.policy import PolicyEngine
from bro_pm.adapters.hermes_runtime import HermesAdapter
from bro_pm.schemas import CommandProposal
from bro_pm.integrations import INTEGRATIONS, IntegrationError, IntegrationResult
from bro_pm.services.command_service import CommandService
from bro_pm import models


def _make_isolated_db_session():
    """Return fresh in-memory DB module/session tuple for deterministic tests."""

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")
    session = database.SessionLocal()
    return database, session


def _make_shared_connection_db_session():
    """Return fresh sqlite:// DB module/session tuple for shared-connection tests."""

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite://")
    session = database.SessionLocal()
    return database, session


def _create_project(session, *, name: str = "Default", slug: str = "default") -> models.Project:
    project = models.Project(name=name, slug=slug)
    session.add(project)
    session.flush()
    return project


@pytest.fixture
def db_session():
    _, session = _make_isolated_db_session()
    try:
        yield session
    finally:
        session.close()


def _make_file_backed_db_module(tmp_path) -> tuple[object, str]:
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    db_path = Path(tmp_path) / f"bro_pm_service_{uuid4().hex}.db"
    database.init_db(f"sqlite:///{db_path}")
    return database, str(db_path)


def test_policy_engine_trusted_and_untrusted_actor_behavior():
    engine = PolicyEngine()
    trusted_decision = engine.evaluate(
        actor_role="operator",
        actor_trusted=True,
        action="create_task",
        safe_paused=False,
    )
    untrusted_decision = engine.evaluate(
        actor_role="owner",
        actor_trusted=False,
        action="create_task",
        safe_paused=False,
    )

    assert trusted_decision.allowed is True
    assert "accepted" in trusted_decision.reason.lower()
    assert untrusted_decision.allowed is False
    assert untrusted_decision.reason == "untrusted actor blocked"


def test_policy_engine_safe_pause_blocks_unsafe_actions():
    engine = PolicyEngine()
    blocked = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="create_task",
        safe_paused=True,
    )
    allowed = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="unpause_project",
        safe_paused=True,
    )
    draft_boss_allowed = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="draft_boss_escalation",
        safe_paused=True,
    )

    assert blocked.allowed is False
    assert blocked.safe_pause_blocked is True
    assert blocked.reason == "project is safe-paused"

    assert allowed.allowed is True
    assert allowed.safe_pause_blocked is False
    assert draft_boss_allowed.allowed is True
    assert draft_boss_allowed.safe_pause_blocked is False


def test_policy_engine_draft_boss_escalation_rejects_viewer():
    engine = PolicyEngine()
    decision = engine.evaluate(
        actor_role="viewer",
        actor_trusted=True,
        action="draft_boss_escalation",
        safe_paused=False,
    )

    assert decision.allowed is False
    assert decision.reason == "requires operator role"


def test_policy_engine_allows_rollback_action_in_safe_pause_for_privileged_role():
    engine = PolicyEngine()
    allowed = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="rollback_action",
        safe_paused=True,
    )

    assert allowed.allowed is True
    assert allowed.reason == "policy accepted"
    assert allowed.safe_pause_blocked is False


def test_policy_engine_high_risk_action_requires_approval():
    engine = PolicyEngine()
    decision = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="close_task",
        safe_paused=False,
    )

    assert decision.allowed is True
    assert decision.requires_approval is True
    assert "approved with human confirmation" in decision.reason


def test_policy_engine_draft_boss_escalation_requires_approval():
    engine = PolicyEngine()
    decision = engine.evaluate(
        actor_role="admin",
        actor_trusted=True,
        action="draft_boss_escalation",
        safe_paused=False,
    )

    assert decision.allowed is True
    assert decision.requires_approval is True
    assert "operator confirmation" in decision.reason


@pytest.mark.parametrize(
    (
        "command_text",
        "expected_action",
        "expected_project_id",
        "expected_payload_keys",
    ),
    [
        ("pause project p-1", "pause_project", "p-1", {"mode", "raw_command"}),
        ("resume project p-1", "unpause_project", "p-1", {"mode", "raw_command"}),
        ("create task do this", "create_task", None, {"title", "raw_command"}),
        ("close task t-42", "close_task", None, {"target_type", "target_id", "raw_command"}),
    ],
)
def test_hermes_adapter_deterministic_parser_parses_known_commands(command_text, expected_action, expected_project_id, expected_payload_keys):
    adapter = HermesAdapter()
    proposal = adapter.propose("alice", command_text)

    assert proposal.action == expected_action
    assert proposal.project_id == expected_project_id
    assert set(proposal.payload.keys()) == expected_payload_keys
    assert "parsed command" in proposal.reason


def test_hermes_adapter_deterministic_parser_parses_draft_boss_escalation():
    adapter = HermesAdapter()
    command = "draft_boss_escalation customers are blocked by API outage"
    proposal = adapter.propose("alice", command)

    assert proposal.action == "draft_boss_escalation"
    assert proposal.project_id is None
    assert proposal.requires_approval is True
    assert proposal.payload["raw_command"] == command
    assert proposal.payload["escalation_message"] == "customers are blocked by API outage"
    assert proposal.payload["risk_level"] == "high"
    assert proposal.payload["trace_label"] == "draft_boss_escalation"


def test_hermes_adapter_deterministic_parser_preserves_empty_draft_boss_escalation_message_for_validation():
    adapter = HermesAdapter()
    proposal = adapter.propose("alice", "draft_boss_escalation   ")

    assert proposal.action == "draft_boss_escalation"
    assert proposal.payload["escalation_message"] == ""


def test_hermes_adapter_deterministic_parser_noop_on_unknown_command():
    adapter = HermesAdapter()
    proposal = adapter.propose("alice", "sing a song")

    assert proposal.action == "noop"
    assert proposal.reason == "unrecognized command"
    assert proposal.payload == {}


def test_command_service_parse_fills_project_id_when_omitted(db_session):
    session = db_session
    project = _create_project(session, name="Parse project", slug="parse")

    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command="create task add docs",
        project_id=project.id,
    )

    assert proposal.project_id == project.id


def test_command_service_rejects_unrecognized_noop_command_and_persists_denied_audit(db_session):
    session = db_session
    project = _create_project(session, name="Unknown command project", slug="unknown-command")
    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command="sing a song",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "unrecognized command"

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.project_id == project.id
    assert audit.action == "noop"
    assert audit.result == "denied"
    payload = json.loads(audit.payload)
    assert payload["proposal"]["action"] == "noop"
    assert payload["proposal"]["reason"] == "unrecognized command"
    assert payload["policy"]["reason"] == "unrecognized command"


def test_command_service_execute_writes_audit_event(db_session):
    session = db_session
    project = _create_project(session, name="Audit project", slug="audit")

    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    start_count = session.query(models.AuditEvent).count()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )
    end_count = session.query(models.AuditEvent).count()

    assert execution.success is True
    assert execution.result == "executed"
    assert end_count == start_count + 1

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.project_id == project.id
    assert audit.action == "pause_project"
    assert audit.result == "executed"

    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=execution.audit_id).one()
    assert execution_record.project_id == project.id
    assert execution_record.action == "pause_project"
    assert execution_record.status == "verified"
    assert execution_record.requested_at is not None
    assert execution_record.executed_at is not None
    assert execution_record.verified_at is not None


# dry-run path intentionally reuses policy and audit evidence
# while never applying any mutating project state changes.
def test_command_service_dry_run_pause_does_not_apply_action(db_session):
    session = db_session
    project = _create_project(session, name="Dry run project", slug="dry-run")

    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    start_count = session.query(models.AuditEvent).count()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        dry_run=True,
    )
    end_count = session.query(models.AuditEvent).count()

    assert execution.success is True
    assert execution.result == "simulated"
    assert end_count == start_count + 1

    session.refresh(project)
    assert project.safe_paused is False

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.action == "pause_project"
    assert audit.result == "simulated"
    payload = json.loads(audit.payload)
    assert payload["proposal"]["action"] == "pause_project"
    assert payload["auth"]["actor_trusted"] is True


def test_command_service_create_task_read_only_integration_validation_calls_validate_not_execute(db_session, monkeypatch):
    session = db_session
    project = _create_project(session, name="Read-only integration project", slug="readonly-integration")

    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    integration_events = {
        "validate_calls": 0,
        "execute_calls": 0,
    }

    def validate_stub(*, action: str, payload: dict) -> None:
        integration_events["validate_calls"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "implement read-only validation"
        assert payload["raw_command"] == "create task implement read-only validation"

    def execute_forbidden(*, action: str, payload: dict) -> None:
        integration_events["execute_calls"] += 1
        raise AssertionError("integration execute must not run in read-only validation mode")

    monkeypatch.setattr(notion, "validate", validate_stub)
    monkeypatch.setattr(notion, "execute", execute_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task implement read-only validation",
        project_id=project.id,
    )
    start_task_count = session.query(models.Task).count()
    start_audit_count = session.query(models.AuditEvent).count()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-create-task-key",
        dry_run=False,
        validate_integration=True,
    )

    end_task_count = session.query(models.Task).count()
    end_audit_count = session.query(models.AuditEvent).count()

    assert execution.success is True
    assert execution.result == "validated"
    assert "validated" in execution.detail.lower()
    assert integration_events["validate_calls"] == 1
    assert integration_events["execute_calls"] == 0
    assert start_task_count == end_task_count
    assert end_audit_count == start_audit_count + 1

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.action == "create_task"
    assert audit.result == "validated"
    payload = json.loads(audit.payload)
    assert payload["auth"]["validate_integration"] is True
    assert payload["integration"]["name"] == "notion"
    assert payload["integration"]["action"] == "create_task"
    assert payload["integration"]["status"] == "validated"
    assert payload["integration"]["detail"]


def test_command_service_create_task_validation_routes_to_project_yandex_tracker_integration(db_session, monkeypatch):
    session = db_session
    project = _create_project(session, name="Yandex validation project", slug="yandex-validation")
    project.metadata_json = {
        "integrations": {
            "yandex_tracker": {
                "backend": "mcp",
                "queue": "OPS",
            }
        },
        "onboarding": {
            "board_integration": "yandex_tracker",
        }
    }
    session.commit()

    service = CommandService(db_session=session)
    yandex_tracker = INTEGRATIONS["yandex_tracker"]
    notion = INTEGRATIONS["notion"]
    integration_events = {
        "yandex_validate_calls": 0,
        "notion_validate_calls": 0,
    }

    def yandex_validate_stub(*, action: str, payload: dict) -> None:
        integration_events["yandex_validate_calls"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "implement yandex validation"
        assert payload["raw_command"] == "create task implement yandex validation"
        assert payload["project_metadata"]["integrations"]["yandex_tracker"]["backend"] == "mcp"
        assert payload["project_metadata"]["integrations"]["yandex_tracker"]["queue"] == "OPS"

    def notion_validate_forbidden(*, action: str, payload: dict) -> None:
        integration_events["notion_validate_calls"] += 1
        raise AssertionError("notion validate must not run when project selects yandex_tracker")

    monkeypatch.setattr(yandex_tracker, "validate", yandex_validate_stub)
    monkeypatch.setattr(notion, "validate", notion_validate_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task implement yandex validation",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="yandex-readonly-create-task-key",
        validate_integration=True,
    )

    assert execution.success is True
    assert execution.result == "validated"
    assert execution.detail == "policy accepted; yandex_tracker validated create_task without execution"
    assert integration_events["yandex_validate_calls"] == 1
    assert integration_events["notion_validate_calls"] == 0

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    payload = json.loads(audit.payload)
    assert payload["integration"]["name"] == "yandex_tracker"
    assert payload["integration"]["status"] == "validated"
    assert payload["integration"]["detail"] == execution.detail


def test_command_service_create_task_validation_falls_back_to_notion_for_legacy_project_without_onboarding_metadata(db_session, monkeypatch):
    session = db_session
    project = _create_project(session, name="Legacy integration project", slug="legacy-integration")
    session.commit()

    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    yandex_tracker = INTEGRATIONS["yandex_tracker"]
    integration_events = {
        "notion_validate_calls": 0,
        "yandex_validate_calls": 0,
    }

    def notion_validate_stub(*, action: str, payload: dict) -> None:
        integration_events["notion_validate_calls"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "legacy validation fallback"

    def yandex_validate_forbidden(*, action: str, payload: dict) -> None:
        integration_events["yandex_validate_calls"] += 1
        raise AssertionError("yandex validate must not run for legacy projects without onboarding metadata")

    monkeypatch.setattr(notion, "validate", notion_validate_stub)
    monkeypatch.setattr(yandex_tracker, "validate", yandex_validate_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task legacy validation fallback",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="legacy-readonly-create-task-key",
        validate_integration=True,
    )

    assert execution.success is True
    assert execution.result == "validated"
    assert execution.detail == "policy accepted; notion validated create_task without execution"
    assert integration_events["notion_validate_calls"] == 1
    assert integration_events["yandex_validate_calls"] == 0

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    payload = json.loads(audit.payload)
    assert payload["integration"]["name"] == "notion"
    assert payload["integration"]["status"] == "validated"
    assert payload["integration"]["detail"] == execution.detail


def test_command_service_create_task_validation_falls_back_to_notion_when_board_integration_is_not_a_board_adapter(db_session, monkeypatch):
    session = db_session
    project = _create_project(session, name="Malformed board integration project", slug="malformed-board-integration")
    project.metadata_json = {
        "onboarding": {
            "board_integration": "slack",
        }
    }
    session.flush()
    session.commit()

    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    slack = INTEGRATIONS["slack"]
    integration_events = {
        "notion_validate_calls": 0,
        "slack_validate_calls": 0,
    }

    def notion_validate_stub(*, action: str, payload: dict) -> None:
        integration_events["notion_validate_calls"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "board adapter guard"

    def slack_validate_forbidden(*, action: str, payload: dict) -> None:
        integration_events["slack_validate_calls"] += 1
        raise AssertionError("slack validate must not run when board_integration metadata is malformed")

    monkeypatch.setattr(notion, "validate", notion_validate_stub)
    monkeypatch.setattr(slack, "validate", slack_validate_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task board adapter guard",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="non-board-adapter-fallback-key",
        validate_integration=True,
    )

    assert execution.success is True
    assert execution.result == "validated"
    assert execution.detail == "policy accepted; notion validated create_task without execution"
    assert integration_events["notion_validate_calls"] == 1
    assert integration_events["slack_validate_calls"] == 0

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    payload = json.loads(audit.payload)
    assert payload["integration"]["name"] == "notion"
    assert payload["integration"]["status"] == "validated"
    assert payload["integration"]["detail"] == execution.detail


def test_command_service_create_task_assisted_execution_calls_integrations_execute_not_validate(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted execution project",
        slug="assisted-execution",
    )
    session.commit()
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    call_counter = {"execute": 0, "validate": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "assisted sync"
        assert payload["raw_command"] == "create task assisted sync"
        return IntegrationResult(ok=True, detail="notion executed assisted create_task")

    def validate_forbidden(*, action: str, payload: dict) -> None:
        call_counter["validate"] += 1
        raise AssertionError("integration validate should not be called in assisted execution mode")

    monkeypatch.setattr(notion, "execute", execute_stub)
    monkeypatch.setattr(notion, "validate", validate_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task assisted sync",
        project_id=project.id,
    )
    start_task_count = session.query(models.Task).count()
    start_audit_count = session.query(models.AuditEvent).count()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-integration-key",
        execute_integration=True,
    )

    end_task_count = session.query(models.Task).count()
    end_audit_count = session.query(models.AuditEvent).count()

    assert execution.success is True
    assert execution.result == "executed"
    assert execution.detail == "notion executed assisted create_task"
    assert call_counter["execute"] == 1
    assert call_counter["validate"] == 0
    assert start_task_count == end_task_count
    assert end_audit_count == start_audit_count + 1

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.result == "executed"
    payload = json.loads(audit.payload)
    assert payload["auth"]["execute_integration"] is True
    assert payload["integration"]["name"] == "notion"
    assert payload["integration"]["action"] == "create_task"
    assert payload["integration"]["status"] == "executed"
    assert payload["integration"]["detail"] == "notion executed assisted create_task"


def test_command_service_create_task_execution_routes_to_project_yandex_tracker_integration(db_session, monkeypatch):
    session = db_session
    project = _create_project(session, name="Yandex execution project", slug="yandex-execution")
    project.metadata_json = {
        "integrations": {
            "yandex_tracker": {
                "queue": "OPS",
            }
        },
        "onboarding": {
            "board_integration": "yandex_tracker",
        }
    }
    session.commit()

    service = CommandService(db_session=session)
    yandex_tracker = INTEGRATIONS["yandex_tracker"]
    notion = INTEGRATIONS["notion"]
    call_counter = {"yandex_execute": 0, "notion_execute": 0}

    def yandex_execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["yandex_execute"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "assisted yandex sync"
        assert payload["raw_command"] == "create task assisted yandex sync"
        assert payload["project_metadata"]["integrations"]["yandex_tracker"]["queue"] == "OPS"
        return IntegrationResult(ok=True, detail="yandex_tracker executed assisted create_task")

    def notion_execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["notion_execute"] += 1
        raise AssertionError("notion execute must not run when project selects yandex_tracker")

    monkeypatch.setattr(yandex_tracker, "execute", yandex_execute_stub)
    monkeypatch.setattr(notion, "execute", notion_execute_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task assisted yandex sync",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-yandex-integration-key",
        execute_integration=True,
    )

    assert execution.success is True
    assert execution.result == "executed"
    assert execution.detail == "yandex_tracker executed assisted create_task"
    assert call_counter["yandex_execute"] == 1
    assert call_counter["notion_execute"] == 0

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    payload = json.loads(audit.payload)
    assert payload["integration"]["name"] == "yandex_tracker"
    assert payload["integration"]["status"] == "executed"
    assert payload["integration"]["detail"] == execution.detail


def test_command_service_create_task_assisted_execution_commits_pending_reservation_before_integration_execute(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Assisted durable pending project",
            slug="assisted-durable-pending",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-durable-pending-key"
    observed: dict[str, dict | None] = {}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        observer_session = database.SessionLocal()
        try:
            record = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
            observed["record"] = (
                None
                if record is None
                else {
                    "result": record.result,
                    "target_id": record.target_id,
                    "payload": json.loads(record.payload),
                }
            )
        finally:
            observer_session.close()
        return IntegrationResult(ok=True, detail="notion executed assisted create_task")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(notion, "execute", execute_stub)
    try:
        session = database.SessionLocal()
        try:
            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task durable pending reservation",
                project_id=project_id,
            )
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key=idempotency_key,
                execute_integration=True,
            )
            session.commit()
        finally:
            session.close()
    finally:
        monkeypatch.undo()

    assert execution.result == "executed"
    assert observed["record"] == {
        "result": "pending_integration",
        "target_id": project_id,
        "payload": {
            "actor": "alice",
            "auth": {
                "role": "admin",
                "actor_trusted": True,
                "dry_run": False,
                "validate_integration": False,
                "execute_integration": True,
            },
            "proposal": proposal.model_dump(),
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
        },
    }


def test_command_service_create_task_assisted_execution_pending_reservation_does_not_commit_unrelated_session_changes(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Assisted pending isolated transaction project",
            slug="assisted-pending-isolated-transaction",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    observed: dict[str, str | None] = {}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        observer_session = database.SessionLocal()
        try:
            observed_project = observer_session.get(models.Project, project_id)
            observed["description"] = None if observed_project is None else observed_project.description
        finally:
            observer_session.close()
        return IntegrationResult(ok=True, detail="notion executed assisted create_task")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(notion, "execute", execute_stub)
    try:
        session = database.SessionLocal()
        try:
            session.autoflush = True
            project = session.get(models.Project, project_id)
            assert project is not None
            project.description = "caller-owned pending change"

            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task durable pending reservation",
                project_id=project_id,
            )
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key="assisted-durable-pending-isolated-key",
                execute_integration=True,
            )

            assert execution.result == "executed"
            assert observed["description"] is None
            assert project.description == "caller-owned pending change"
        finally:
            session.rollback()
            session.close()
    finally:
        monkeypatch.undo()


def test_command_service_create_task_assisted_execution_rejects_sqlite_write_locked_caller_session(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Assisted sqlite lock guard project",
            slug="assisted-sqlite-lock-guard",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]

    def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
        raise AssertionError("integration execute must not run when sqlite caller transaction already holds a write lock")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(notion, "execute", execute_forbidden)
    try:
        session = database.SessionLocal()
        try:
            project = session.get(models.Project, project_id)
            assert project is not None
            project.description = "already flushed caller change"
            session.flush()

            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task durable pending reservation",
                project_id=project_id,
            )
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key="assisted-sqlite-lock-guard-key",
                execute_integration=True,
            )
            session.commit()
        finally:
            session.close()
    finally:
        monkeypatch.undo()

    assert execution.result == "rejected"
    assert execution.detail == "assisted create_task requires a clean caller transaction before durable reservation on sqlite"

    observer_session = database.SessionLocal()
    try:
        audit = observer_session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    finally:
        observer_session.close()

    assert audit.result == "denied"
    payload = json.loads(audit.payload)
    assert payload["integration"]["status"] == "rejected"
    assert payload["integration"]["detail"] == execution.detail


def test_command_service_create_task_assisted_execution_rejects_in_memory_sqlite_write_locked_caller_session():
    database, setup_session = _make_isolated_db_session()
    try:
        project = _create_project(
            setup_session,
            name="Assisted sqlite memory lock guard project",
            slug=f"assisted-sqlite-memory-lock-guard-{uuid4().hex[:8]}",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    execute_counter = {"count": 0}
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        notion,
        "execute",
        lambda *, action, payload: execute_counter.__setitem__("count", execute_counter["count"] + 1)
        or IntegrationResult(ok=True, detail="unexpected in-memory sqlite execution"),
    )
    try:
        session = database.SessionLocal()
        try:
            project = session.get(models.Project, project_id)
            assert project is not None
            project.description = "already flushed caller change"
            session.flush()

            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task durable pending reservation",
                project_id=project_id,
            )
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key="assisted-sqlite-memory-lock-guard-key",
                execute_integration=True,
            )
        finally:
            session.rollback()
            session.close()
    finally:
        monkeypatch.undo()

    observer_session = database.SessionLocal()
    try:
        observed_project = observer_session.get(models.Project, project_id)
        audit = observer_session.query(models.AuditEvent).filter_by(
            idempotency_key="assisted-sqlite-memory-lock-guard-key"
        ).one_or_none()
    finally:
        observer_session.close()

    assert observed_project is not None
    assert observed_project.description is None
    assert audit is None
    assert execute_counter == {"count": 0}
    assert execution.result == "rejected"
    assert execution.detail == "assisted create_task requires a clean caller transaction before durable reservation on sqlite"


def test_command_service_create_task_assisted_execution_rejects_shared_connection_sqlite_write_locked_caller_session(monkeypatch):
    database, setup_session = _make_shared_connection_db_session()
    try:
        project = _create_project(
            setup_session,
            name="Assisted sqlite shared connection lock guard project",
            slug=f"assisted-sqlite-shared-lock-guard-{uuid4().hex[:8]}",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    execute_counter = {"count": 0}
    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: execute_counter.__setitem__("count", execute_counter["count"] + 1)
        or IntegrationResult(ok=True, detail="unexpected sqlite:// create-path execution"),
    )

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "already flushed caller change"
        session.flush()

        service = CommandService(db_session=session)
        bind = session.get_bind()
        connection = session.connection().connection
        assert getattr(bind.url, "database", None) is None
        assert connection.in_transaction is True
        assert session.is_modified(project) is False

        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=service.parse(
                actor="alice",
                command="create task shared connection durable pending reservation",
                project_id=project_id,
            ),
            actor_trusted=True,
            idempotency_key="assisted-sqlite-shared-lock-guard-key",
            execute_integration=True,
        )

        assert connection.in_transaction is True
        assert project.description == "already flushed caller change"
        assert session.is_modified(project) is False
        assert execution.result == "rejected"
        assert execution.detail == "assisted create_task requires a clean caller transaction before durable reservation on sqlite"
    finally:
        session.rollback()
        session.close()

    observer_session = database.SessionLocal()
    try:
        observed_project = observer_session.get(models.Project, project_id)
        audit = observer_session.query(models.AuditEvent).filter_by(
            idempotency_key="assisted-sqlite-shared-lock-guard-key"
        ).one_or_none()
    finally:
        observer_session.close()

    assert observed_project is not None
    assert observed_project.description is None
    assert audit is None
    assert execute_counter == {"count": 0}


def test_command_service_create_task_assisted_execution_persists_terminal_audit_state_even_if_caller_rolls_back(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Assisted durable terminal state project",
            slug="assisted-durable-terminal-state",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        notion,
        "execute",
        lambda *, action, payload: IntegrationResult(ok=True, detail="notion executed assisted create_task"),
    )
    try:
        session = database.SessionLocal()
        try:
            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task durable pending reservation",
                project_id=project_id,
            )
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key="assisted-durable-terminal-state-key",
                execute_integration=True,
            )
            session.rollback()
        finally:
            session.close()
    finally:
        monkeypatch.undo()

    observer_session = database.SessionLocal()
    try:
        audit = observer_session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    finally:
        observer_session.close()

    assert audit.result == "executed"
    payload = json.loads(audit.payload)
    assert payload["integration"]["status"] == "executed"
    assert payload["integration"]["detail"] == "notion executed assisted create_task"


def test_command_service_create_task_assisted_execution_persists_terminal_failure_before_reraising_unexpected_exception(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Assisted unexpected integration exception project",
            slug="assisted-unexpected-integration-exception",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    failure_detail = "unexpected notion explode"

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        raise RuntimeError(failure_detail)

    monkeypatch.setattr(notion, "execute", execute_stub)

    session = database.SessionLocal()
    try:
        service = CommandService(db_session=session)
        proposal = service.parse(
            actor="alice",
            command="create task durable unexpected integration failure",
            project_id=project_id,
        )

        with pytest.raises(RuntimeError, match=failure_detail):
            service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key="assisted-unexpected-integration-exception-key",
                execute_integration=True,
            )
    finally:
        session.rollback()
        session.close()

    observer_session = database.SessionLocal()
    try:
        audit = observer_session.query(models.AuditEvent).filter_by(
            idempotency_key="assisted-unexpected-integration-exception-key"
        ).one()
    finally:
        observer_session.close()

    assert audit.result == "denied"
    payload = json.loads(audit.payload)
    assert payload["integration"]["status"] == "failed"
    assert payload["integration"]["detail"] == failure_detail


def test_command_service_wait_for_existing_idempotent_record_preserves_unrelated_pending_caller_changes(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    idempotency_key = "wait-preserves-caller-session-key"

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Wait preserves caller session project",
            slug="wait-preserves-caller-session",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload=json.dumps({}, ensure_ascii=False),
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "caller-owned pending change"

        service = CommandService(db_session=session)
        update_counter = {"sleep": 0}

        def fake_sleep(delay_seconds: float) -> None:
            update_counter["sleep"] += 1
            updater_session = database.SessionLocal()
            try:
                audit = updater_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
                audit.result = "denied"
                audit.payload = json.dumps(
                    {
                        "integration": {
                            "status": "failed",
                            "detail": "updated from isolated waiter",
                        }
                    },
                    ensure_ascii=False,
                )
                updater_session.commit()
            finally:
                updater_session.close()

        monkeypatch.setattr("bro_pm.services.command_service.time.sleep", fake_sleep)

        existing = service._wait_for_existing_idempotent_record(
            idempotency_key,
            attempts=2,
            delay_seconds=0.01,
            wait_for_stable_result=True,
        )

        assert existing is not None
        assert existing.result == "denied"
        assert update_counter == {"sleep": 1}
        assert project.description == "caller-owned pending change"
        assert session.is_modified(project)
    finally:
        session.rollback()
        session.close()


def test_command_service_wait_for_existing_idempotent_record_does_not_clobber_dirty_loaded_audit_event_on_in_memory_sqlite():
    database, setup_session = _make_isolated_db_session()
    idempotency_key = "wait-preserves-dirty-audit-event"

    try:
        project = _create_project(
            setup_session,
            name="Wait preserves dirty audit event project",
            slug=f"wait-preserves-dirty-audit-{uuid4().hex[:8]}",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload=json.dumps({"x": 1}, ensure_ascii=False),
                result="accepted",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )
        setup_session.commit()
    finally:
        setup_session.close()

    session = database.SessionLocal()
    try:
        audit = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        audit.payload = json.dumps({"x": 999}, ensure_ascii=False)
        assert session.is_modified(audit) is True

        service = CommandService(db_session=session)

        existing = service._wait_for_existing_idempotent_record(
            idempotency_key,
            attempts=1,
            delay_seconds=0.01,
        )

        assert existing is audit
        assert existing.payload == json.dumps({"x": 999}, ensure_ascii=False)
        assert session.is_modified(audit) is True
    finally:
        session.rollback()
        session.close()



def test_command_service_wait_for_existing_idempotent_record_keeps_in_memory_sqlite_caller_transaction_open():
    database, setup_session = _make_isolated_db_session()
    idempotency_key = "wait-preserves-in-memory-transaction"

    try:
        project = _create_project(
            setup_session,
            name="Wait preserves in-memory sqlite transaction project",
            slug=f"wait-preserves-in-memory-{uuid4().hex[:8]}",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload=json.dumps({}, ensure_ascii=False),
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "caller-owned change"
        session.flush()

        service = CommandService(db_session=session)
        connection = session.connection().connection
        assert connection.in_transaction is True
        assert session.is_modified(project) is False

        existing = service._wait_for_existing_idempotent_record(idempotency_key)

        assert existing is not None
        assert existing.result == "pending_integration"
        assert connection.in_transaction is True
        assert project.description == "caller-owned change"
        assert session.is_modified(project) is False
    finally:
        session.rollback()
        session.close()


def test_command_service_waited_stale_pending_replay_rewrites_storage_durably(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    idempotency_key = "waited-stale-detached-repair"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Waited stale detached repair project",
            slug="waited-stale-detached-repair",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: (_ for _ in ()).throw(
            AssertionError("integration execute must not run when waited pending replay is repaired as stale")
        ),
    )

    waited_updates = {"sleep": 0}

    def fake_sleep(delay_seconds: float) -> None:
        waited_updates["sleep"] += 1
        updater_session = database.SessionLocal()
        try:
            audit = updater_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
            audit.created_at = datetime.utcnow() - timedelta(minutes=10)
            updater_session.commit()
        finally:
            updater_session.close()

    monkeypatch.setattr("bro_pm.services.command_service.time.sleep", fake_sleep)

    session = database.SessionLocal()
    try:
        service = CommandService(db_session=session)
        proposal = service.parse(
            actor="alice",
            command="create task waited stale repair",
            project_id=project_id,
        )
        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=proposal,
            actor_trusted=True,
            idempotency_key=idempotency_key,
            execute_integration=True,
        )
        session.commit()
    finally:
        session.close()

    observer_session = database.SessionLocal()
    try:
        stored = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    finally:
        observer_session.close()

    assert waited_updates == {"sleep": 1}
    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == stale_detail
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["replay_repair"] == {
        "source": "unreadable_pending_integration_payload",
    }
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail


def test_command_service_waited_stale_pending_replay_repair_survives_caller_rollback(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    idempotency_key = "waited-stale-repair-survives-rollback"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Waited stale repair rollback durability project",
            slug="waited-stale-repair-rollback-durability",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow(),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: (_ for _ in ()).throw(
            AssertionError("integration execute must not run when waited pending replay is repaired as stale")
        ),
    )

    waited_updates = {"sleep": 0}

    def fake_sleep(delay_seconds: float) -> None:
        waited_updates["sleep"] += 1
        updater_session = database.SessionLocal()
        try:
            audit = updater_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
            audit.created_at = datetime.utcnow() - timedelta(minutes=10)
            updater_session.commit()
        finally:
            updater_session.close()

    monkeypatch.setattr("bro_pm.services.command_service.time.sleep", fake_sleep)

    session = database.SessionLocal()
    try:
        service = CommandService(db_session=session)
        proposal = service.parse(
            actor="alice",
            command="create task waited stale rollback durability",
            project_id=project_id,
        )
        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=proposal,
            actor_trusted=True,
            idempotency_key=idempotency_key,
            execute_integration=True,
        )
        session.rollback()
    finally:
        session.close()

    observer_session = database.SessionLocal()
    try:
        stored = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    finally:
        observer_session.close()

    assert waited_updates == {"sleep": 1}
    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == stale_detail
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["replay_repair"] == {
        "source": "unreadable_pending_integration_payload",
    }
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_stale_pending_replay_repair_does_not_flush_unrelated_caller_changes(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    idempotency_key = "stale-repair-no-caller-flush"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Stale repair no caller flush project",
            slug="stale-repair-no-caller-flush",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: (_ for _ in ()).throw(
            AssertionError("integration execute must not run when stale replay is repaired as denied")
        ),
    )

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "caller-owned dirty description"

        service = CommandService(db_session=session)
        proposal = service.parse(
            actor="alice",
            command="create task stale repair no flush",
            project_id=project_id,
        )
        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=proposal,
            actor_trusted=True,
            idempotency_key=idempotency_key,
            execute_integration=True,
        )

        assert execution.success is False
        assert execution.result == "rejected"
        assert execution.detail == stale_detail
        assert project.description == "caller-owned dirty description"
        assert session.is_modified(project) is True
    finally:
        session.rollback()
        session.close()



def test_command_service_stale_pending_replay_rejects_file_backed_sqlite_write_locked_caller_session(tmp_path, monkeypatch):
    database, _ = _make_file_backed_db_module(tmp_path)
    idempotency_key = "stale-repair-file-backed-write-lock"
    rejection_detail = "assisted create_task requires a clean caller transaction before durable reservation on sqlite"

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Stale repair file-backed sqlite lock guard project",
            slug="stale-repair-file-backed-lock-guard",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    execute_counter = {"count": 0}
    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: execute_counter.__setitem__("count", execute_counter["count"] + 1)
        or IntegrationResult(ok=True, detail="unexpected file-backed stale sqlite execution"),
    )

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "caller-owned change"
        session.flush()

        service = CommandService(db_session=session)
        connection = session.connection().connection
        assert connection.in_transaction is True
        assert session.is_modified(project) is False

        monkeypatch.setattr(
            service,
            "_mark_stale_pending_integration_denied",
            lambda *, existing, replay_context: (_ for _ in ()).throw(
                AssertionError("stale pending repair must fail closed before isolated sqlite mutation on file-backed sqlite")
            ),
        )

        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=service.parse(
                actor="alice",
                command="create task stale repair file backed lock guard",
                project_id=project_id,
            ),
            actor_trusted=True,
            idempotency_key=idempotency_key,
            execute_integration=True,
        )

        assert connection.in_transaction is True
        assert project.description == "caller-owned change"
        assert session.is_modified(project) is False
        assert execution.success is False
        assert execution.result == "rejected"
        assert execution.detail == rejection_detail
    finally:
        session.rollback()
        session.close()

    observer_session = database.SessionLocal()
    try:
        stored = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        observed_project = observer_session.get(models.Project, project_id)
    finally:
        observer_session.close()

    assert execute_counter == {"count": 0}
    assert stored.result == "pending_integration"
    assert stored.payload == "{legacy-pending-json"
    assert observed_project is not None
    assert observed_project.description is None



def test_command_service_stale_pending_replay_rejects_in_memory_sqlite_write_locked_caller_session():
    database, setup_session = _make_isolated_db_session()
    idempotency_key = "stale-repair-memory-write-lock"
    rejection_detail = "assisted create_task requires a clean caller transaction before durable reservation on sqlite"

    try:
        project = _create_project(
            setup_session,
            name="Stale repair in-memory sqlite lock guard project",
            slug=f"stale-repair-memory-lock-{uuid4().hex[:8]}",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    execute_counter = {"count": 0}
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        notion,
        "execute",
        lambda *, action, payload: execute_counter.__setitem__("count", execute_counter["count"] + 1)
        or IntegrationResult(ok=True, detail="unexpected stale in-memory sqlite execution"),
    )
    try:
        session = database.SessionLocal()
        try:
            project = session.get(models.Project, project_id)
            assert project is not None
            project.description = "caller-owned change"
            session.flush()

            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task stale repair memory lock guard",
                project_id=project_id,
            )

            connection = session.connection().connection
            assert connection.in_transaction is True
            assert session.is_modified(project) is False

            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key=idempotency_key,
                execute_integration=True,
            )

            assert connection.in_transaction is True
            assert project.description == "caller-owned change"
            assert session.is_modified(project) is False
            assert execution.success is False
            assert execution.result == "rejected"
            assert execution.detail == rejection_detail
        finally:
            session.rollback()
            session.close()
    finally:
        monkeypatch.undo()

    observer_session = database.SessionLocal()
    try:
        stored = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        observed_project = observer_session.get(models.Project, project_id)
    finally:
        observer_session.close()

    assert execute_counter == {"count": 0}
    assert stored.result == "pending_integration"
    assert stored.payload == "{legacy-pending-json"
    assert observed_project is not None
    assert observed_project.description is None


def test_command_service_stale_pending_replay_rejects_sqlite_url_write_locked_caller_session(monkeypatch):
    database, setup_session = _make_shared_connection_db_session()
    idempotency_key = "stale-repair-sqlite-url-write-lock"
    rejection_detail = "assisted create_task requires a clean caller transaction before durable reservation on sqlite"

    try:
        project = _create_project(
            setup_session,
            name="Stale repair sqlite url lock guard project",
            slug=f"stale-repair-sqlite-url-lock-{uuid4().hex[:8]}",
        )
        setup_session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="alice",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload="{legacy-pending-json",
                result="pending_integration",
                idempotency_key=idempotency_key,
                created_at=datetime.utcnow() - timedelta(minutes=10),
            )
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    execute_counter = {"count": 0}
    monkeypatch.setattr(
        INTEGRATIONS["notion"],
        "execute",
        lambda *, action, payload: execute_counter.__setitem__("count", execute_counter["count"] + 1)
        or IntegrationResult(ok=True, detail="unexpected stale sqlite:// execution"),
    )

    session = database.SessionLocal()
    try:
        project = session.get(models.Project, project_id)
        assert project is not None
        project.description = "caller-owned change"
        session.flush()

        service = CommandService(db_session=session)
        bind = session.get_bind()
        connection = session.connection().connection
        assert getattr(bind.url, "database", None) is None
        assert connection.in_transaction is True
        assert session.is_modified(project) is False

        execution = service.execute(
            actor="alice",
            role="admin",
            proposal=service.parse(
                actor="alice",
                command="create task stale repair sqlite url lock guard",
                project_id=project_id,
            ),
            actor_trusted=True,
            idempotency_key=idempotency_key,
            execute_integration=True,
        )

        assert connection.in_transaction is True
        assert project.description == "caller-owned change"
        assert session.is_modified(project) is False
        assert execution.success is False
        assert execution.result == "rejected"
        assert execution.detail == rejection_detail
    finally:
        session.rollback()
        session.close()

    observer_session = database.SessionLocal()
    try:
        stored = observer_session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        observed_project = observer_session.get(models.Project, project_id)
    finally:
        observer_session.close()

    assert execute_counter == {"count": 0}
    assert stored.result == "pending_integration"
    assert stored.payload == "{legacy-pending-json"
    assert observed_project is not None
    assert observed_project.description is None



def test_command_service_pending_integration_wait_budget_matches_reporting_baseline():
    assert CommandService.PENDING_INTEGRATION_WAIT_ATTEMPTS == 100
    assert CommandService.PENDING_INTEGRATION_WAIT_DELAY_SECONDS == 0.05



def test_command_service_create_task_assisted_execution_respects_policy_before_integrations_execute(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted execution policy project",
        slug="assisted-policy",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]

    def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
        raise AssertionError("integration execute must not run when policy rejects")

    monkeypatch.setattr(notion, "execute", execute_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task policy blocked assisted task",
        project_id=project.id,
    )
    execution = service.execute(
        actor="alice",
        role="operator",
        proposal=proposal,
        actor_trusted=False,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "untrusted actor blocked"


def test_command_service_create_task_assisted_execution_replay_preserves_integration_detail(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted execution replay project",
        slug="assisted-replay",
    )
    session.commit()
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="notion executed assisted replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task replay assisted detail",
        project_id=project.id,
    )
    expected_detail = "notion executed assisted replay"

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-replay",
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-replay",
        execute_integration=True,
    )

    assert first.success is True
    assert first.result == "executed"
    assert first.detail == expected_detail
    assert second.success is True
    assert second.result == "executed"
    assert second.detail == expected_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 1

    audit = session.query(models.AuditEvent).filter_by(id=first.audit_id).one()
    stored_payload = json.loads(audit.payload)
    assert stored_payload["integration"]["detail"] == expected_detail


def test_command_service_create_task_assisted_execution_recovers_stale_pending_idempotency_reservation_with_unreadable_payload(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale unreadable recovery",
        slug="assisted-stale-unreadable",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-unreadable"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale unreadable replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale unreadable recovery",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload="{legacy-pending-json",
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["replay_repair"] == {
        "source": "unreadable_pending_integration_payload",
    }
    assert "auth" not in stored_payload
    assert "proposal" not in stored_payload
    assert "name" not in stored_payload["integration"]
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_recovers_stale_pending_idempotency_reservation_with_empty_payload(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale empty payload recovery",
        slug="assisted-stale-empty",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-empty"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale empty replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale empty recovery",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps({}, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["replay_repair"] == {
        "source": "unreadable_pending_integration_payload",
    }
    assert "auth" not in stored_payload
    assert "proposal" not in stored_payload
    assert "name" not in stored_payload["integration"]
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_preserves_readable_partial_stale_payload_without_repair(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale partial payload recovery",
        slug="assisted-stale-partial",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-partial"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale partial replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale partial recovery",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(
                {
                    "actor": "alice",
                    "integration": {
                        "name": "legacy-partial-adapter",
                        "action": "create_task",
                        "status": "pending",
                        "detail": "integration execution pending",
                    },
                },
                ensure_ascii=False,
            ),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert "auth" not in stored_payload
    assert "proposal" not in stored_payload
    assert stored_payload["integration"]["name"] == "legacy-partial-adapter"
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_recovers_stale_pending_idempotency_reservation_with_malformed_integration_payload_shape(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale malformed integration payload",
        slug="assisted-stale-malformed-integration-payload",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-malformed-integration-payload"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale malformed integration replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale malformed integration payload",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "proposal": proposal.model_dump(),
        "integration": "legacy-malformed-integration-shape",
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["auth"] == original_payload["auth"]
    assert stored_payload["proposal"] == original_payload["proposal"]
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_recovers_stale_pending_idempotency_reservation_with_full_auth_partial_proposal_payload(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale full-auth partial-proposal payload",
        slug="assisted-stale-full-auth-partial-proposal",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-full-auth-partial-proposal"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale full-auth partial-proposal replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale full auth partial proposal payload",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "proposal": {
            "action": proposal.action,
            "payload": proposal.payload,
        },
        "integration": {
            "name": "legacy-partial-proposal-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["auth"] == original_payload["auth"]
    assert stored_payload["proposal"] == original_payload["proposal"]
    assert stored_payload["integration"]["name"] == "legacy-partial-proposal-adapter"
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_stale_auth_only_payload_returns_manual_reconciliation_rejection(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale auth-only payload",
        slug="assisted-stale-auth-only",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-auth-only"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale auth-only replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale auth only payload",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(
                {
                    "actor": "alice",
                    "auth": {
                        "role": "admin",
                        "actor_trusted": True,
                        "dry_run": False,
                        "validate_integration": False,
                        "execute_integration": True,
                    },
                    "integration": {
                        "name": "legacy-auth-only-adapter",
                        "action": "create_task",
                        "status": "pending",
                        "detail": "integration execution pending",
                    },
                },
                ensure_ascii=False,
            ),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert "proposal" not in stored_payload
    assert stored_payload["auth"] == {
        "role": "admin",
        "actor_trusted": True,
        "dry_run": False,
        "validate_integration": False,
        "execute_integration": True,
    }
    assert stored_payload["integration"]["name"] == "legacy-auth-only-adapter"
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail



def test_command_service_create_task_assisted_execution_different_actor_does_not_tombstone_readable_partial_stale_payload(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale partial payload different actor",
        slug="assisted-stale-partial-different-actor",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-partial-different-actor"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale partial mismatch replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    original_proposal = service.parse(
        actor="alice",
        command="create task original stale partial request",
        project_id=project.id,
    )
    replay_proposal = service.parse(
        actor="bob",
        command="create task replay stale partial request",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "integration": {
            "name": "legacy-partial-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="bob",
        role="operator",
        proposal=replay_proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored.actor == "alice"
    assert stored_payload == original_payload




def test_command_service_create_task_assisted_execution_stale_auth_only_payload_preserves_reservation_for_different_actor(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale auth-only different actor",
        slug="assisted-stale-auth-only-different-actor",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-auth-only-different-actor"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale auth-only different-actor replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "integration": {
            "name": "legacy-auth-only-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }
    replay_proposal = service.parse(
        actor="bob",
        command="create task stale auth only different actor",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="bob",
        role="admin",
        proposal=replay_proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored.actor == "alice"
    assert stored_payload == original_payload



def test_command_service_create_task_assisted_execution_stale_auth_only_payload_preserves_reservation_for_same_actor_different_project(
    db_session, monkeypatch
):
    session = db_session
    original_project = _create_project(
        session,
        name="Assisted stale auth-only original project",
        slug="assisted-stale-auth-only-original-project",
    )
    replay_project = _create_project(
        session,
        name="Assisted stale auth-only replay project",
        slug="assisted-stale-auth-only-replay-project",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-auth-only-same-actor-different-project"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale auth-only same-actor different-project replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "integration": {
            "name": "legacy-auth-only-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }
    replay_proposal = service.parse(
        actor="alice",
        command="create task stale auth only same actor different project",
        project_id=replay_project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=original_project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=original_project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=replay_proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored.project_id == original_project.id
    assert stored.target_id == original_project.id
    assert stored.actor == "alice"
    assert stored_payload == original_payload



@pytest.mark.parametrize(
    ("stored_payload", "expected_detail"),
    [
        pytest.param(
            "{legacy-pending-json",
            "idempotency key already used for unreadable stored request context",
            id="unreadable",
        ),
        pytest.param(
            {
                "actor": "alice",
                "auth": {
                    "dry_run": False,
                    "validate_integration": False,
                    "execute_integration": True,
                },
                "proposal": {},
                "integration": {
                    "name": "legacy-auth-incomplete-adapter",
                    "action": "create_task",
                    "status": "pending",
                    "detail": "integration execution pending",
                },
            },
            "idempotency key already used for different request context",
            id="auth-incomplete",
        ),
    ],
)
def test_command_service_create_task_assisted_execution_lower_trust_replay_does_not_tombstone_incomplete_stale_payload(
    db_session, monkeypatch, stored_payload, expected_detail
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale lower trust incomplete payload",
        slug=f"assisted-stale-lower-trust-incomplete-{uuid4().hex[:8]}",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = f"assisted-stale-lower-trust-incomplete-{uuid4().hex}"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after lower-trust stale replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task lower trust incomplete stale replay",
        project_id=project.id,
    )
    payload_to_store = stored_payload
    if isinstance(payload_to_store, dict):
        payload_to_store = {
            **payload_to_store,
            "proposal": {
                **proposal.model_dump(),
                **payload_to_store["proposal"],
                "project_id": project.id,
                "payload": proposal.payload,
            },
        }
        serialized_payload = json.dumps(payload_to_store, ensure_ascii=False)
    else:
        serialized_payload = payload_to_store

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=serialized_payload,
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="alice",
        role="operator",
        proposal=proposal,
        actor_trusted=False,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == expected_detail
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    assert stored.result == "pending_integration"
    if isinstance(payload_to_store, dict):
        assert json.loads(stored.payload) == payload_to_store
    else:
        assert stored.payload == payload_to_store



def test_command_service_create_task_assisted_execution_stale_proposal_only_payload_preserves_reservation_for_different_request(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale proposal-only different request",
        slug="assisted-stale-proposal-only-different-request",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-proposal-only-different-request"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale proposal-only different-request replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    original_proposal = service.parse(
        actor="alice",
        command="create task original stale proposal only request",
        project_id=project.id,
    )
    replay_proposal = service.parse(
        actor="alice",
        command="create task replay stale proposal only request",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "proposal": {
            "action": original_proposal.action,
            "project_id": original_proposal.project_id,
            "payload": {
                "title": original_proposal.payload["title"],
            },
        },
        "integration": {
            "name": "legacy-proposal-only-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=replay_proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored.actor == "alice"
    assert stored_payload == original_payload



def test_command_service_create_task_assisted_execution_same_mode_different_context_preserves_stale_pending_reservation(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale different context",
        slug="assisted-stale-different-context",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-different-context"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected execution after stale different-context replay")

    monkeypatch.setattr(notion, "execute", execute_stub)

    original_proposal = service.parse(
        actor="alice",
        command="create task original stale assisted request",
        project_id=project.id,
    )
    replay_proposal = service.parse(
        actor="bob",
        command="create task replay with different context",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "proposal": original_proposal.model_dump(),
        "policy": {
            "allowed": True,
            "reason": "policy accepted",
            "requires_approval": False,
            "safe_pause_blocked": False,
        },
        "integration": {
            "name": "legacy-notion-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="bob",
        role="operator",
        proposal=replay_proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored.actor == "alice"
    assert stored_payload == original_payload



def test_command_service_validate_mode_does_not_tombstone_stale_assisted_pending_idempotency_reservation(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Validate mode stale assisted idempotency",
        slug="validate-stale-assisted-idempotency",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "validate-stale-assisted-idempotency"
    validation_counter = {"validate": 0}

    def validate_stub(*, action: str, payload: dict) -> None:
        validation_counter["validate"] += 1

    monkeypatch.setattr(notion, "validate", validate_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale assisted validate mismatch",
        project_id=project.id,
    )
    original_payload = {
        "actor": "alice",
        "auth": {
            "role": "admin",
            "actor_trusted": True,
            "dry_run": False,
            "validate_integration": False,
            "execute_integration": True,
        },
        "proposal": proposal.model_dump(),
        "policy": {
            "allowed": True,
            "reason": "policy accepted",
            "requires_approval": False,
            "safe_pause_blocked": False,
        },
        "integration": {
            "name": "legacy-notion-adapter",
            "action": "create_task",
            "status": "pending",
            "detail": "integration execution pending",
        },
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(original_payload, ensure_ascii=False),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        validate_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"
    assert validation_counter["validate"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "pending_integration"
    assert stored_payload == original_payload



def test_command_service_create_task_replay_rejects_readable_malformed_nested_payload_shape(db_session):
    session = db_session
    project = _create_project(
        session,
        name="Readable malformed replay payload",
        slug="readable-malformed-replay-payload",
    )
    service = CommandService(db_session=session)
    idempotency_key = "readable-malformed-replay-payload"
    proposal = service.parse(
        actor="alice",
        command="create task readable malformed replay payload",
        project_id=project.id,
    )
    malformed_payload = {
        "actor": "alice",
        "auth": ["admin", True],
        "proposal": proposal.model_dump(),
        "policy": ["legacy-policy-shape"],
        "integration": "legacy-integration-shape",
        "replay_repair": ["legacy-repair-shape"],
    }

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(malformed_payload, ensure_ascii=False),
            result="denied",
            idempotency_key=idempotency_key,
        )
    )
    session.commit()

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "idempotency key already used for different request context"

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    assert json.loads(stored.payload) == malformed_payload



def test_command_service_create_task_assisted_execution_recovers_stale_pending_idempotency_reservation(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted stale pending recovery",
        slug="assisted-stale-pending",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-stale-pending"
    stale_detail = "stale pending integration request requires manual reconciliation before retry"
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected second execution")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task stale pending recovery",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(
                {
                    "actor": "alice",
                    "auth": {
                        "role": "admin",
                        "actor_trusted": True,
                        "dry_run": False,
                        "validate_integration": False,
                        "execute_integration": True,
                    },
                    "proposal": proposal.model_dump(),
                    "policy": {
                        "allowed": True,
                        "reason": "policy accepted",
                        "requires_approval": False,
                        "safe_pause_blocked": False,
                    },
                    "integration": {
                        "name": "legacy-notion-adapter",
                        "action": "create_task",
                        "status": "pending",
                        "detail": "integration execution pending",
                    },
                },
                ensure_ascii=False,
            ),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow() - timedelta(minutes=10),
        )
    )
    session.commit()

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == stale_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == stale_detail
    assert second.audit_id == first.audit_id
    assert call_counter["execute"] == 0

    stored = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
    stored_payload = json.loads(stored.payload)
    assert stored.result == "denied"
    assert stored_payload["actor"] == "alice"
    assert stored_payload["auth"] == {
        "role": "admin",
        "actor_trusted": True,
        "dry_run": False,
        "validate_integration": False,
        "execute_integration": True,
    }
    assert stored_payload["proposal"] == proposal.model_dump()
    assert stored_payload["integration"]["name"] == "legacy-notion-adapter"
    assert stored_payload["integration"]["action"] == "create_task"
    assert stored_payload["integration"]["status"] == "failed"
    assert stored_payload["integration"]["detail"] == stale_detail


def test_command_service_create_task_assisted_execution_waits_for_fresh_pending_idempotency_reservation(
    db_session, monkeypatch
):
    session = db_session
    project = _create_project(
        session,
        name="Assisted fresh pending wait",
        slug="assisted-fresh-pending",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    idempotency_key = "assisted-fresh-pending"
    call_counter = {"execute": 0, "wait": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        return IntegrationResult(ok=True, detail="unexpected live execution")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task fresh pending wait",
        project_id=project.id,
    )

    session.add(
        models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="create_task",
            target_type="proposal",
            target_id=project.id,
            payload=json.dumps(
                {
                    "actor": "alice",
                    "auth": {
                        "role": "admin",
                        "actor_trusted": True,
                        "dry_run": False,
                        "validate_integration": False,
                        "execute_integration": True,
                    },
                    "proposal": proposal.model_dump(),
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
                        "detail": "integration execution pending",
                    },
                },
                ensure_ascii=False,
            ),
            result="pending_integration",
            idempotency_key=idempotency_key,
            created_at=datetime.utcnow(),
        )
    )
    session.commit()

    def wait_stub(idempotency_key_arg: str | None, *, attempts: int = 50, delay_seconds: float = 0.01, wait_for_stable_result: bool = False):
        call_counter["wait"] += 1
        assert idempotency_key_arg == idempotency_key
        assert wait_for_stable_result is True
        existing = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        existing.result = "executed"
        existing.payload = json.dumps(
            {
                "actor": "alice",
                "auth": {
                    "role": "admin",
                    "actor_trusted": True,
                    "dry_run": False,
                    "validate_integration": False,
                    "execute_integration": True,
                },
                "proposal": proposal.model_dump(),
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
                    "detail": "notion executed after wait",
                },
            },
            ensure_ascii=False,
        )
        session.flush()
        return existing

    monkeypatch.setattr(service, "_wait_for_existing_idempotent_record", wait_stub)

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=True,
    )

    assert execution.success is True
    assert execution.result == "executed"
    assert execution.detail == "notion executed after wait"
    assert call_counter == {"execute": 0, "wait": 1}



def test_command_service_create_task_assisted_execution_returns_integration_error_detail(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted integration failure project",
        slug="assisted-failure",
    )
    session.commit()
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    failure_detail = "notion execute failed: api timeout"
    start_task_count = session.query(models.Task).count()
    start_audit_count = session.query(models.AuditEvent).count()

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        raise IntegrationError(failure_detail)

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task assisted integration failure",
        project_id=project.id,
    )
    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-integration-failure",
        execute_integration=True,
    )
    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == failure_detail
    assert session.query(models.Task).count() == start_task_count
    assert session.query(models.AuditEvent).count() == start_audit_count + 1


def test_command_service_create_task_assisted_execution_rejects_integration_result_false(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted integration result false project",
        slug="assisted-result-false",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    rejected_detail = "notion returned integration-level failure"

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "assisted rejected"
        return IntegrationResult(ok=False, detail=rejected_detail)

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task assisted rejected",
        project_id=project.id,
    )
    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == rejected_detail
    assert session.query(models.Task).count() == 0

    audit = session.query(models.AuditEvent).filter_by(id=execution.audit_id).one()
    assert audit.result == "denied"
    stored_payload = json.loads(audit.payload)
    assert stored_payload["integration"]["status"] == "rejected"
    assert stored_payload["integration"]["detail"] == rejected_detail


def test_command_service_create_task_assisted_execution_is_idempotency_mode_isolated_from_live_dry_run_validate(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted idempotency isolation",
        slug="assisted-idem",
    )
    session.commit()
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        return IntegrationResult(ok=True, detail="notion executed assisted")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task idempotency mode isolation",
        project_id=project.id,
    )

    assisted = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-mode-isolation",
        execute_integration=True,
    )
    live = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-mode-isolation",
    )
    dry_run = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-mode-isolation",
        dry_run=True,
    )
    validate = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-mode-isolation",
        validate_integration=True,
    )

    assert assisted.success is True
    assert assisted.result == "executed"
    assert live.success is False
    assert live.result == "rejected"
    assert live.detail == "idempotency key already used for different request context"
    assert live.audit_id == assisted.audit_id
    assert dry_run.success is False
    assert dry_run.result == "rejected"
    assert dry_run.detail == "idempotency key already used for different request context"
    assert dry_run.audit_id == assisted.audit_id
    assert validate.success is False
    assert validate.result == "rejected"
    assert validate.detail == "idempotency key already used for different request context"
    assert validate.audit_id == assisted.audit_id



def test_command_service_create_task_assisted_execution_rejects_missing_project_context(db_session, monkeypatch):
    session = db_session
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    call_counter = {"execute": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        raise AssertionError("integration execute should not run without project context")

    monkeypatch.setattr(notion, "execute", execute_stub)

    proposal = service.parse(
        actor="alice",
        command="create task requires project id",
        project_id=None,
    )
    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "project context required for assisted create_task"
    assert call_counter["execute"] == 0


def test_command_service_execute_integration_rejects_unsupported_action_without_execute_or_validate_call(db_session, monkeypatch):
    session = db_session
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    call_counter = {"execute": 0, "validate": 0}

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        raise AssertionError("integration execute should not run for unsupported action")

    def validate_stub(*, action: str, payload: dict) -> None:
        call_counter["validate"] += 1
        raise AssertionError("integration validate should not run for unsupported execute_integration request")

    monkeypatch.setattr(notion, "execute", execute_stub)
    monkeypatch.setattr(notion, "validate", validate_stub)

    proposal = service.parse(
        actor="alice",
        command="close task T-1",
        project_id=None,
    )
    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        execute_integration=True,
    )

    assert execution.success is False
    assert execution.result == "rejected"
    assert execution.detail == "execute_integration mode currently supports create_task only"
    assert call_counter["execute"] == 0
    assert call_counter["validate"] == 0


def test_command_service_create_task_execute_integration_and_validate_integration_precedence(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted validate precedence",
        slug="assisted-validate-precedence",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]

    calls = {"execute": 0, "validate": 0}

    def validate_stub(*, action: str, payload: dict) -> None:
        calls["validate"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project.id
        assert payload["title"] == "precedence test"

    def execute_forbidden(*, action: str, payload: dict) -> IntegrationResult:
        calls["execute"] += 1
        raise AssertionError("integration execute should not run in validate-only mode")

    monkeypatch.setattr(notion, "validate", validate_stub)
    monkeypatch.setattr(notion, "execute", execute_forbidden)

    proposal = service.parse(
        actor="alice",
        command="create task precedence test",
        project_id=project.id,
    )

    execution = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="assisted-validate-precedence",
        execute_integration=True,
        validate_integration=True,
    )

    assert execution.success is True
    assert execution.result == "validated"
    assert execution.detail == "policy accepted; notion validated create_task without execution"
    assert calls["validate"] == 1
    assert calls["execute"] == 0


def test_command_service_create_task_read_only_integration_idempotency_isolated_from_live_and_dry_run(db_session):
    session = db_session
    project = _create_project(session, name="Read-only idempotency project", slug="readonly-idem")
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command="create task release checklist",
        project_id=project.id,
    )

    read_only = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="integration-mode-isolation-key",
        validate_integration=True,
    )
    assert read_only.success is True
    assert read_only.result == "validated"

    live = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="integration-mode-isolation-key",
        dry_run=False,
    )
    assert live.success is False
    assert live.result == "rejected"
    assert live.detail == "idempotency key already used for different request context"
    assert live.audit_id == read_only.audit_id

    dry_run = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="integration-mode-isolation-key",
        dry_run=True,
    )
    assert dry_run.success is False
    assert dry_run.result == "rejected"
    assert dry_run.detail == "idempotency key already used for different request context"
    assert dry_run.audit_id == read_only.audit_id


def test_command_service_read_only_integration_validation_replay_preserves_validation_detail(db_session):
    session = db_session
    project = _create_project(
        session,
        name="Read-only validation replay detail",
        slug="readonly-validation-detail",
    )
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command="create task validate replay detail",
        project_id=project.id,
    )
    expected_detail = "policy accepted; notion validated create_task without execution"

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-replay-detail",
        validate_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-replay-detail",
        validate_integration=True,
    )

    assert first.success is True
    assert first.result == "validated"
    assert first.detail == expected_detail
    assert second.success is True
    assert second.result == "validated"
    assert second.detail == expected_detail
    assert second.audit_id == first.audit_id


def test_command_service_read_only_integration_validation_replay_preserves_unsupported_action_detail(db_session):
    session = db_session
    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command="close task t-1",
        project_id=None,
    )
    expected_detail = "integration validation mode currently supports create_task only"

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-unsupported-detail",
        validate_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-unsupported-detail",
        validate_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == expected_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == expected_detail
    assert second.audit_id == first.audit_id

    stored = session.query(models.AuditEvent).filter_by(id=first.audit_id).one()
    payload = json.loads(stored.payload)
    assert payload["integration"]["detail"] == expected_detail


def test_command_service_read_only_integration_validation_failure_replay_preserves_integration_error_detail(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Read-only validation failure detail",
        slug="readonly-validation-failure-detail",
    )
    service = CommandService(db_session=session)
    notion = INTEGRATIONS["notion"]
    failure_detail = "notion validate rejected missing required title field"

    def validate_stub(*, action: str, payload: dict) -> None:
        raise IntegrationError(failure_detail)

    monkeypatch.setattr(notion, "validate", validate_stub)

    proposal = service.parse(
        actor="alice",
        command="create task validate replay failure",
        project_id=project.id,
    )

    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-failure-detail",
        validate_integration=True,
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="readonly-validation-failure-detail",
        validate_integration=True,
    )

    assert first.success is False
    assert first.result == "rejected"
    assert first.detail == failure_detail
    assert second.success is False
    assert second.result == "rejected"
    assert second.detail == failure_detail
    assert second.audit_id == first.audit_id


def test_command_service_pause_and_unpause_toggle_project_safe_state(db_session):
    session = db_session
    project = _create_project(session, name="Safe project", slug="safe")

    service = CommandService(db_session=session)

    pause_proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    pause_result = service.execute(
        actor="alice",
        role="admin",
        proposal=pause_proposal,
        actor_trusted=True,
    )
    assert pause_result.success is True
    assert pause_result.result == "executed"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is True

    resume_proposal = service.parse(
        actor="alice",
        command=f"resume project {project.id}",
        project_id=project.id,
    )
    resume_result = service.execute(
        actor="alice",
        role="admin",
        proposal=resume_proposal,
        actor_trusted=True,
    )
    assert resume_result.success is True
    assert resume_result.result == "executed"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is False


def test_command_service_dry_run_and_live_idempotency_modes_are_isolated(db_session):
    session = db_session
    project = _create_project(session, name="Mode isolation", slug="mode-isolation")
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )

    dry_result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="mode-isolated-key",
        dry_run=True,
    )
    assert dry_result.success is True
    assert dry_result.result == "simulated"

    live_result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="mode-isolated-key",
        dry_run=False,
    )

    assert live_result.success is False
    assert live_result.result == "rejected"
    assert live_result.detail == "idempotency key already used for different request context"
    assert live_result.audit_id == dry_result.audit_id
    session.refresh(project)
    assert project.safe_paused is False



def test_command_service_rollback_of_pause_action_reverses_state_and_records_rollback(db_session):
    session = db_session
    project = _create_project(session, name="Rollback project", slug="rollback")
    service = CommandService(db_session=session)

    pause_proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    pause_result = service.execute(
        actor="alice",
        role="admin",
        proposal=pause_proposal,
        actor_trusted=True,
    )
    assert pause_result.success is True
    assert pause_result.result == "executed"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is True

    rollback_result = service.rollback(
        actor="alice",
        role="admin",
        audit_event_id=pause_result.audit_id,
        reason="mistaken command",
        actor_trusted=True,
    )
    assert rollback_result.success is True
    assert rollback_result.result == "executed"
    assert rollback_result.proposal.action == "rollback_action"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is False

    rollback_audit = session.query(models.AuditEvent).filter_by(id=rollback_result.audit_id).one()
    assert rollback_audit.action == "rollback_action"
    assert rollback_audit.result == "executed"
    rollback_record = session.query(models.RollbackRecord).filter_by(audit_event_id=pause_result.audit_id).one()
    assert rollback_record.actor == "alice"
    assert rollback_record.reason == "mistaken command"
    assert rollback_record.executed is True


def test_command_service_rejects_rollback_for_unauthorized_role(db_session):
    session = db_session
    project = _create_project(session, name="Rollback deny project", slug="rollback-deny")
    service = CommandService(db_session=session)

    pause_proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    pause_result = service.execute(
        actor="alice",
        role="admin",
        proposal=pause_proposal,
        actor_trusted=True,
    )
    assert pause_result.success is True

    rollback_result = service.rollback(
        actor="eve",
        role="viewer",
        audit_event_id=pause_result.audit_id,
        reason="attempted rollback by wrong role",
        actor_trusted=True,
    )
    assert rollback_result.success is False
    assert rollback_result.result == "rejected"
    assert rollback_result.detail == "rollback requires admin or owner"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is True


def test_command_service_rejects_rollback_for_non_executed_audit_event(db_session):
    session = db_session
    project = _create_project(session, name="Rollback pending project", slug="rollback-pending")
    service = CommandService(db_session=session)

    gated_proposal = CommandProposal(
        action="pause_project",
        project_id=project.id,
        reason="approval required before pause",
        payload={"note": "approval gate"},
        requires_approval=True,
    )
    gated_result = service.execute(
        actor="alice",
        role="admin",
        proposal=gated_proposal,
        actor_trusted=True,
    )
    assert gated_result.success is True
    assert gated_result.result == "requires_approval"

    rollback_result = service.rollback(
        actor="alice",
        role="admin",
        audit_event_id=gated_result.audit_id,
        reason="cannot roll back non-executed audit",
        actor_trusted=True,
    )

    assert rollback_result.success is False
    assert rollback_result.result == "rejected"
    assert rollback_result.detail == "only executed audit events can be rolled back"
    assert rollback_result.rollback_record_id == ""

    session.flush()
    session.refresh(project)
    assert project.safe_paused is False
    rollback_records = session.query(models.RollbackRecord).filter_by(audit_event_id=gated_result.audit_id).count()
    assert rollback_records == 0


def test_command_service_rejects_repeated_rollback_of_same_event(db_session):
    session = db_session
    project = _create_project(session, name="Rollback repeat project", slug="rollback-repeat")
    service = CommandService(db_session=session)

    pause_proposal = service.parse(
        actor="alice",
        command=f"pause project {project.id}",
        project_id=project.id,
    )
    pause_result = service.execute(
        actor="alice",
        role="admin",
        proposal=pause_proposal,
        actor_trusted=True,
    )
    assert pause_result.success is True
    assert pause_result.result == "executed"

    first_rollback = service.rollback(
        actor="alice",
        role="admin",
        audit_event_id=pause_result.audit_id,
        reason="undo accidental pause",
        actor_trusted=True,
    )
    assert first_rollback.success is True
    assert first_rollback.result == "executed"

    session.flush()
    session.refresh(project)
    assert project.safe_paused is False

    second_rollback = service.rollback(
        actor="alice",
        role="admin",
        audit_event_id=pause_result.audit_id,
        reason="repeat rollback attempt",
        actor_trusted=True,
    )
    assert second_rollback.success is False
    assert second_rollback.result == "rejected"
    assert second_rollback.detail == "audit event already rolled back"
    assert second_rollback.rollback_record_id == ""

    session.refresh(project)
    assert project.safe_paused is False
    rollback_records = (
        session.query(models.RollbackRecord)
        .filter_by(audit_event_id=pause_result.audit_id, executed=True)
        .count()
    )
    assert rollback_records == 1


def test_command_service_rejects_concurrent_rollback_of_same_event(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)

    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Rollback concurrent project",
            slug="rollback-concurrent",
        )
        setup_service = CommandService(db_session=setup_session)
        pause_proposal = setup_service.parse(
            actor="alice",
            command=f"pause project {project.id}",
            project_id=project.id,
        )
        pause_result = setup_service.execute(
            actor="alice",
            role="admin",
            proposal=pause_proposal,
            actor_trusted=True,
        )
        assert pause_result.success is True
        assert pause_result.result == "executed"
        setup_project_id = project.id
        setup_session.commit()
    finally:
        setup_session.close()

    start_barrier = Barrier(2)

    def _run_once():
        session = database.SessionLocal()
        try:
            service = CommandService(db_session=session)
            start_barrier.wait(timeout=5)
            execution = service.rollback(
                actor="alice",
                role="admin",
                audit_event_id=pause_result.audit_id,
                reason="duplicate concurrent rollback",
                actor_trusted=True,
            )
            session.commit()
            return {
                "status": "ok",
                "success": execution.success,
                "result": execution.result,
                "rollback_record_id": execution.rollback_record_id,
            }
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [future.result() for future in [executor.submit(_run_once), executor.submit(_run_once)]]

    assert all(outcome["status"] == "ok" for outcome in outcomes)
    assert {outcome["result"] for outcome in outcomes} == {"executed", "rejected"}

    verify_session = database.SessionLocal()
    try:
        verify_project = verify_session.get(models.Project, setup_project_id)
        assert verify_project is not None
        assert verify_project.safe_paused is False

        rollback_records = (
            verify_session.query(models.RollbackRecord)
            .filter_by(audit_event_id=pause_result.audit_id, executed=True)
            .count()
        )
        assert rollback_records == 1
    finally:
        verify_session.close()


def test_command_service_approval_required_path_does_not_apply_action(db_session):
    session = db_session
    project = _create_project(session, name="Approval project", slug="approval")
    service = CommandService(db_session=session)

    protected = CommandProposal(
        action="pause_project",
        project_id=project.id,
        reason="forced approval",
        payload={"note": "test"},
        requires_approval=True,
    )
    start_count = session.query(models.AuditEvent).count()
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=protected,
        actor_trusted=True,
    )
    end_count = session.query(models.AuditEvent).count()

    assert result.result == "requires_approval"
    assert end_count == start_count + 1

    session.refresh(project)
    assert project.safe_paused is False
    audit = session.query(models.AuditEvent).filter_by(id=result.audit_id).one()
    assert audit.result == "awaiting_approval"
    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=result.audit_id).one()
    assert execution_record.project_id == project.id
    assert execution_record.action == "pause_project"
    assert execution_record.status == "awaiting_approval"
    assert execution_record.requested_at is not None
    assert execution_record.awaiting_approval_at is not None
    assert execution_record.executed_at is None
    assert execution_record.verified_at is None
    approval_request = session.query(models.ApprovalRequest).filter_by(audit_event_id=result.audit_id).one()
    assert approval_request.project_id == project.id
    assert approval_request.action == "pause_project"
    assert approval_request.status == "pending"
    assert approval_request.requested_by == "alice"
    assert approval_request.requested_at is not None
    assert approval_request.expires_at is not None


def test_command_service_approved_action_requires_explicit_resume_and_executes_once(db_session):
    session = db_session
    project = _create_project(session, name="Approved resume project", slug="approved-resume")
    service = CommandService(db_session=session)

    protected = CommandProposal(
        action="pause_project",
        project_id=project.id,
        reason="forced approval",
        payload={"note": "bounded approval"},
        requires_approval=True,
    )
    gated = service.execute(
        actor="alice",
        role="admin",
        proposal=protected,
        actor_trusted=True,
        idempotency_key="approval-resume-once",
    )

    approval = service.decide_approval(
        audit_event_id=gated.audit_id,
        actor="boss-user",
        role="owner",
        approved=True,
        decision_text="approved",
    )

    session.refresh(project)
    assert project.safe_paused is False
    assert approval.status == "approved"
    assert approval.reviewer_actor == "boss-user"
    assert approval.reviewer_role == "owner"
    assert approval.decision_text == "approved"
    assert approval.decided_at is not None

    resumed = service.resume_approval(
        audit_event_id=gated.audit_id,
        actor="alice",
        role="admin",
        actor_trusted=True,
    )

    assert resumed.success is True
    assert resumed.result == "executed"
    assert resumed.audit_id == gated.audit_id

    session.refresh(project)
    assert project.safe_paused is True

    refreshed_approval = session.query(models.ApprovalRequest).filter_by(audit_event_id=gated.audit_id).one()
    assert refreshed_approval.status == "executed"
    assert refreshed_approval.resumed_at is not None

    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=gated.audit_id).one()
    assert execution_record.status == "verified"
    assert execution_record.executed_at is not None
    assert execution_record.verified_at is not None

    replay = service.resume_approval(
        audit_event_id=gated.audit_id,
        actor="alice",
        role="admin",
        actor_trusted=True,
    )
    assert replay.success is True
    assert replay.result == "executed"
    assert replay.audit_id == gated.audit_id


def test_command_service_rejected_approval_does_not_execute_downstream_action(db_session):
    session = db_session
    project = _create_project(session, name="Rejected approval project", slug="rejected-approval")
    service = CommandService(db_session=session)

    protected = CommandProposal(
        action="pause_project",
        project_id=project.id,
        reason="forced approval",
        payload={"note": "reject path"},
        requires_approval=True,
    )
    gated = service.execute(
        actor="alice",
        role="admin",
        proposal=protected,
        actor_trusted=True,
    )

    approval = service.decide_approval(
        audit_event_id=gated.audit_id,
        actor="boss-user",
        role="owner",
        approved=False,
        decision_text="rejected",
    )
    assert approval.status == "rejected"

    resumed = service.resume_approval(
        audit_event_id=gated.audit_id,
        actor="alice",
        role="admin",
        actor_trusted=True,
    )
    assert resumed.success is False
    assert resumed.result == "rejected"

    session.refresh(project)
    assert project.safe_paused is False
    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=gated.audit_id).one()
    assert execution_record.status == "rejected"


def test_command_service_idempotent_replay_prefers_durable_approval_state_after_restart(tmp_path):
    database, db_path = _make_file_backed_db_module(tmp_path)
    session = database.SessionLocal()
    try:
        project = _create_project(session, name="Durable replay approval", slug="durable-replay-approval")
        service = CommandService(db_session=session)
        proposal = CommandProposal(
            action="pause_project",
            project_id=project.id,
            reason="forced approval",
            payload={"note": "restart replay"},
            requires_approval=True,
        )
        first = service.execute(
            actor="alice",
            role="admin",
            proposal=proposal,
            actor_trusted=True,
            idempotency_key="durable-approval-replay",
        )
        service.decide_approval(
            audit_event_id=first.audit_id,
            actor="boss-user",
            role="owner",
            approved=True,
            decision_text="approved after restart",
        )

        audit = session.get(models.AuditEvent, first.audit_id)
        assert audit is not None
        payload = json.loads(audit.payload)
        payload.pop("approval", None)
        audit.payload = json.dumps(payload, ensure_ascii=False)
        audit.result = "awaiting_approval"
        execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=first.audit_id).one()
        execution_record.status = "awaiting_approval"
        session.commit()
    finally:
        session.close()

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(f"sqlite:///{db_path}")
    replay_session = database.SessionLocal()
    try:
        service = CommandService(db_session=replay_session)
        stored_project = replay_session.query(models.Project).filter_by(slug="durable-replay-approval").one()
        proposal = CommandProposal(
            action="pause_project",
            project_id=stored_project.id,
            reason="forced approval",
            payload={"note": "restart replay"},
            requires_approval=True,
        )
        replay = service.execute(
            actor="alice",
            role="admin",
            proposal=proposal,
            actor_trusted=True,
            idempotency_key="durable-approval-replay",
        )

        assert replay.success is True
        assert replay.result == "approved"
        approval = replay_session.query(models.ApprovalRequest).filter_by(audit_event_id=replay.audit_id).one()
        assert approval.status == "approved"
    finally:
        replay_session.close()


def test_command_service_draft_boss_escalation_is_audit_only_and_no_mutation(db_session):
    session = db_session
    project = _create_project(session, name="Draft escalation", slug="draft-escalation")
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation project telemetry blocked",
        project_id=project.id,
    )
    start_count = session.query(models.AuditEvent).count()
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )
    end_count = session.query(models.AuditEvent).count()

    assert result.success is True
    assert result.result == "requires_approval"
    assert end_count == start_count + 1

    session.refresh(project)
    assert project.safe_paused is False

    audit = session.query(models.AuditEvent).filter_by(id=result.audit_id).one()
    payload = json.loads(audit.payload)
    assert audit.action == "draft_boss_escalation"
    assert audit.result == "awaiting_approval"
    assert payload["proposal"]["payload"]["raw_command"] == "draft_boss_escalation project telemetry blocked"
    assert payload["proposal"]["payload"]["escalation_message"] == "project telemetry blocked"
    assert payload["proposal"]["payload"]["risk_level"] == "high"
    assert payload["proposal"]["payload"]["trace_label"] == "draft_boss_escalation"


def test_command_service_idempotent_replay_returns_approved_for_recorded_approval_reply(db_session):
    session = db_session
    project = _create_project(session, name="Replay approved escalation", slug="replay-approved-escalation")
    service = CommandService(db_session=session)
    idempotency_key = "replay-approved-escalation"

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation project telemetry blocked",
        project_id=project.id,
    )
    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
    )

    audit = session.get(models.AuditEvent, first.audit_id)
    assert audit is not None
    audit.result = "approved"
    payload = json.loads(audit.payload)
    payload["approval"] = {
        "status": "approved",
        "actor": "boss-user",
        "actor_role": "boss",
        "text": "approved",
    }
    audit.payload = json.dumps(payload, ensure_ascii=False)
    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=first.audit_id).one()
    execution_record.status = "approved"
    session.commit()

    replay = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
    )

    assert replay.success is True
    assert replay.result == "approved"
    assert replay.audit_id == first.audit_id

    refreshed = session.query(models.ActionExecution).filter_by(audit_event_id=first.audit_id).one()
    assert refreshed.status == "approved"


def test_command_service_idempotent_replay_returns_rejected_for_recorded_rejection_reply(db_session):
    session = db_session
    project = _create_project(session, name="Replay rejected escalation", slug="replay-rejected-escalation")
    service = CommandService(db_session=session)
    idempotency_key = "replay-rejected-escalation"

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation project telemetry blocked",
        project_id=project.id,
    )
    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
    )

    audit = session.get(models.AuditEvent, first.audit_id)
    assert audit is not None
    audit.result = "rejected"
    payload = json.loads(audit.payload)
    payload["approval"] = {
        "status": "rejected",
        "actor": "boss-user",
        "actor_role": "boss",
        "text": "rejected",
    }
    audit.payload = json.dumps(payload, ensure_ascii=False)
    execution_record = session.query(models.ActionExecution).filter_by(audit_event_id=first.audit_id).one()
    execution_record.status = "rejected"
    session.commit()

    replay = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
    )

    assert replay.success is False
    assert replay.result == "rejected"
    assert replay.audit_id == first.audit_id

    refreshed = session.query(models.ActionExecution).filter_by(audit_event_id=first.audit_id).one()
    assert refreshed.status == "rejected"


def test_command_service_draft_boss_escalation_is_allowed_in_safe_paused_project(db_session):
    session = db_session
    project = _create_project(session, name="Safe pause draft escalation", slug="safe-escalation")
    project.safe_paused = True
    session.flush()

    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation customer impact escalation",
        project_id=project.id,
    )
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )

    assert result.success is True
    assert result.result == "requires_approval"
    assert project.safe_paused is True


def test_command_service_draft_boss_escalation_rejects_viewer(db_session):
    session = db_session
    project = _create_project(session, name="Viewer blocked escalation", slug="viewer-escalation")
    service = CommandService(db_session=session)
    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation viewer blocked escalation",
        project_id=project.id,
    )
    result = service.execute(
        actor="alice",
        role="viewer",
        proposal=proposal,
        actor_trusted=True,
    )

    assert result.success is False
    assert result.result == "rejected"
    assert result.detail == "requires operator role"


def test_command_service_draft_boss_escalation_requires_project_context(db_session):
    service = CommandService(db_session=db_session)

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation missing project context",
        project_id=None,
    )
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )

    assert result.success is False
    assert result.result == "rejected"
    assert result.detail == "project context required for draft_boss_escalation"


def test_command_service_draft_boss_escalation_requires_existing_project(db_session):
    service = CommandService(db_session=db_session)

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation project missing",
        project_id="missing-project-id",
    )
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )

    assert result.success is False
    assert result.result == "rejected"
    assert result.detail == "project not found for draft_boss_escalation"


def test_command_service_draft_boss_escalation_requires_message(db_session):
    session = db_session
    project = _create_project(session, name="Message required escalation", slug="message-required")
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation   ",
        project_id=project.id,
    )
    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
    )

    assert result.success is False
    assert result.result == "rejected"
    assert result.detail == "escalation message required for draft_boss_escalation"


def test_command_service_draft_boss_escalation_idempotent_replay_reuses_audit_record(db_session):
    session = db_session
    project = _create_project(session, name="Idempotent escalation", slug="idem-escalation")
    service = CommandService(db_session=session)

    proposal = service.parse(
        actor="alice",
        command="draft_boss_escalation production incident escalation",
        project_id=project.id,
    )
    first = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="escalation-idem-key",
    )
    second = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="escalation-idem-key",
    )

    assert first.success is True
    assert second.success is True
    assert first.result == "requires_approval"
    assert second.result == "requires_approval"
    assert first.audit_id == second.audit_id


def test_command_service_replays_legacy_idempotent_record_without_dry_run_flag(db_session):
    session = db_session
    legacy_proposal = {
        "action": "close_task",
        "project_id": None,
        "reason": "parsed command",
        "payload": {
            "target_type": "task",
            "target_id": "t-1",
            "raw_command": "close task T-1",
        },
        "requires_approval": False,
    }
    session.add(
        models.AuditEvent(
            actor="alice",
            action="close_task",
            target_type="proposal",
            target_id="T-1",
            payload=json.dumps(
                {
                    "actor": "alice",
                    "auth": {"role": "admin", "actor_trusted": True},
                    "proposal": legacy_proposal,
                    "policy": {"reason": "approved with human confirmation"},
                }
            ),
            result="awaiting_approval",
            idempotency_key="legacy-idempotency-key",
        )
    )
    session.commit()

    service = CommandService(db_session=session)
    proposal = service.parse(actor="alice", command="close task T-1", project_id=None)

    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="legacy-idempotency-key",
    )

    assert result.success is True
    assert result.result == "requires_approval"
    assert result.audit_id


def test_command_service_rejects_idempotent_replay_when_stored_payload_is_invalid(db_session):
    session = db_session
    session.add(
        models.AuditEvent(
            actor="alice",
            action="close_task",
            target_type="proposal",
            target_id="T-1",
            payload="{not-json",
            result="awaiting_approval",
            idempotency_key="broken-idempotency-key",
        )
    )
    session.commit()

    service = CommandService(db_session=session)
    proposal = service.parse(actor="alice", command="close task T-1", project_id=None)

    result = service.execute(
        actor="alice",
        role="admin",
        proposal=proposal,
        actor_trusted=True,
        idempotency_key="broken-idempotency-key",
    )

    assert result.success is False
    assert result.result == "rejected"
    assert result.detail == "idempotency key already used for unreadable stored request context"


def test_command_service_idempotency_replays_under_concurrent_duplicate_requests(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)

    start_barrier = Barrier(2)
    idempotency_key = f"idem-{uuid4().hex[:8]}"

    def _run_once():
        session = database.SessionLocal()
        try:
            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="close task T-1",
                project_id=None,
            )
            start_barrier.wait(timeout=5)
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key=idempotency_key,
            )
            session.commit()
            return {"status": "ok", "result": execution.result, "audit_id": execution.audit_id}
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = [future.result() for future in [executor.submit(_run_once), executor.submit(_run_once)]]

    assert all(outcome["status"] == "ok" for outcome in outcomes)
    assert {outcome["result"] for outcome in outcomes} == {"requires_approval"}
    assert len({outcome["audit_id"] for outcome in outcomes}) == 1


def test_command_service_concurrent_assisted_create_task_execution_runs_integration_once_and_replays_result(tmp_path):
    database, _ = _make_file_backed_db_module(tmp_path)
    setup_session = database.SessionLocal()
    try:
        project = _create_project(
            setup_session,
            name="Concurrent assisted create_task project",
            slug="assisted-concurrent",
        )
        setup_session.commit()
        project_id = project.id
    finally:
        setup_session.close()

    notion = INTEGRATIONS["notion"]
    call_counter = {"execute": 0}
    start_barrier = Barrier(2)
    idempotency_key = "assisted-concurrent-duplicate-key"

    def execute_stub(*, action: str, payload: dict) -> IntegrationResult:
        call_counter["execute"] += 1
        assert action == "create_task"
        assert payload["project_id"] == project_id
        time.sleep(0.05)
        return IntegrationResult(ok=True, detail="notion executed assisted create_task")

    original_execute = notion.execute
    notion.execute = execute_stub

    def _run_once():
        session = database.SessionLocal()
        try:
            service = CommandService(db_session=session)
            proposal = service.parse(
                actor="alice",
                command="create task concurrent assisted duplicate",
                project_id=project_id,
            )
            start_barrier.wait(timeout=5)
            execution = service.execute(
                actor="alice",
                role="admin",
                proposal=proposal,
                actor_trusted=True,
                idempotency_key=idempotency_key,
                execute_integration=True,
            )
            session.commit()
            return {
                "status": "ok",
                "result": execution.result,
                "detail": execution.detail,
                "audit_id": execution.audit_id,
                "success": execution.success,
            }
        except Exception as exc:
            session.rollback()
            return {"status": "error", "error_type": type(exc).__name__, "detail": str(exc)}
        finally:
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = [future.result() for future in [executor.submit(_run_once), executor.submit(_run_once)]]
    finally:
        notion.execute = original_execute

    assert all(outcome["status"] == "ok" for outcome in outcomes)
    assert {outcome["result"] for outcome in outcomes} == {"executed"}
    assert {outcome["success"] for outcome in outcomes} == {True}
    assert len({outcome["detail"] for outcome in outcomes}) == 1
    assert outcomes[0]["detail"] == "notion executed assisted create_task"
    assert len({outcome["audit_id"] for outcome in outcomes}) == 1
    assert call_counter["execute"] == 1
