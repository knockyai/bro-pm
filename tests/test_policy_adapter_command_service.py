from __future__ import annotations

import json
import importlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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


def test_command_service_create_task_assisted_execution_calls_integrations_execute_not_validate(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted execution project",
        slug="assisted-execution",
    )
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


def test_command_service_create_task_assisted_execution_returns_integration_error_detail(db_session, monkeypatch):
    session = db_session
    project = _create_project(
        session,
        name="Assisted integration failure project",
        slug="assisted-failure",
    )
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
