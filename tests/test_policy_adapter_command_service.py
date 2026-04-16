from __future__ import annotations

import importlib
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest

from bro_pm.policy import PolicyEngine
from bro_pm.adapters.hermes_runtime import HermesAdapter
from bro_pm.schemas import CommandProposal
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

    assert blocked.allowed is False
    assert blocked.safe_pause_blocked is True
    assert blocked.reason == "project is safe-paused"

    assert allowed.allowed is True
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
