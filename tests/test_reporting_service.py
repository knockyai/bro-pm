from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime
from uuid import uuid4

import pytest

from bro_pm import models
from bro_pm.integrations import IntegrationError
from bro_pm.services.reporting_service import ReportingService


@pytest.fixture
def reporting_db(tmp_path):
    db_path = tmp_path / f"bro_pm_reporting_{uuid4().hex}.db"
    db_url = f"sqlite:///{db_path}"

    for mod_name in ("bro_pm.database",):
        sys.modules.pop(mod_name, None)

    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)
    yield database


def test_reporting_service_surfaces_timer_risk_trace_labels_as_project_risks(reporting_db):
    session = reporting_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={},
        )
        session.add(project)
        session.commit()

        for trace_label, description in (
            ("timer_executor_overload:alice", "Executor alice is overloaded against capacity."),
            ("timer_stalled_task:task-1", "Task is stalled and needs an unblock step."),
            ("timer_commitment_risk", "Commitment deadline is at risk from open work."),
        ):
            session.add(
                models.AuditEvent(
                    project_id=project.id,
                    actor="bro_pm_timer",
                    action="create_task",
                    target_type="proposal",
                    target_id=project.id,
                    payload=json.dumps(
                        {
                            "proposal": {
                                "payload": {
                                    "trace_label": trace_label,
                                    "description": description,
                                }
                            },
                            "policy": {"reason": "timer autonomy"},
                            "integration": {"detail": "notion executed: create_task"},
                        },
                        ensure_ascii=False,
                    ),
                    result="executed",
                    created_at=datetime.utcnow(),
                )
            )
        session.commit()

        service = ReportingService(db_session=session)
        response = service.generate_project_report(
            project=project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=False,
        )
    finally:
        session.close()

    assert {risk.kind for risk in response.risks} == {
        "commitment_risk",
        "executor_overload",
        "stalled_task",
    }
    risks_by_trace = {risk.trace_label: risk for risk in response.risks}
    assert risks_by_trace["timer_executor_overload:alice"].source == "audit_event"
    assert risks_by_trace["timer_executor_overload:alice"].lineage == (
        "mode=unknown -> trace=timer_executor_overload:alice -> audit=create_task:executed -> integration=notion executed: create_task"
    )
    assert risks_by_trace["timer_commitment_risk"].summary == "Commitment deadline is at risk from open work."


def test_reporting_service_surfaces_due_action_lineage_for_failure_escalation(reporting_db):
    session = reporting_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={},
        )
        session.add(project)
        session.commit()

        session.add(
            models.DueAction(
                project_id=project.id,
                channel="telegram",
                recipient="boss-user",
                kind="boss_escalation",
                payload_json={
                    "text": "Autonomous timer escalated repeated failures to the boss.",
                    "trace_label": "timer_failure_escalation",
                    "risk_level": "high",
                },
                due_at=datetime.utcnow(),
                status="delivered",
                actor="bro_pm_timer",
                idempotency_key="timer-decision:failure",
            )
        )
        session.commit()

        service = ReportingService(db_session=session)
        response = service.generate_project_report(
            project=project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=False,
        )
    finally:
        session.close()

    assert len(response.risks) == 1
    risk = response.risks[0]
    assert risk.kind == "boss_escalation"
    assert risk.source == "due_action"
    assert risk.trace_label == "timer_failure_escalation"
    assert risk.due_action_id is not None
    assert risk.summary == "Autonomous timer escalated repeated failures to the boss."
    assert risk.lineage == (
        "trace=timer_failure_escalation -> due_action=boss_escalation:delivered -> delivery=telegram:boss-user"
    )


def test_reporting_service_decisions_include_autonomy_reason_mode_and_lineage(reporting_db):
    session = reporting_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={},
        )
        session.add(project)
        session.commit()

        session.add(
            models.AuditEvent(
                project_id=project.id,
                actor="bro_pm_timer",
                action="create_task",
                target_type="proposal",
                target_id=project.id,
                payload=json.dumps(
                    {
                        "proposal": {
                            "reason": "10-minute autonomous decision timer detected commitment and deadline risk",
                            "payload": {
                                "mode": "timer_autonomy",
                                "trace_label": "timer_commitment_risk",
                                "title": "Reduce commitment risk",
                                "description": "Reduce scope, reassign work, or re-commit the deadline.",
                            },
                        },
                        "policy": {"reason": "policy accepted"},
                        "integration": {"detail": "notion executed: create_task"},
                    },
                    ensure_ascii=False,
                ),
                result="executed",
                created_at=datetime.utcnow(),
            )
        )
        session.commit()

        service = ReportingService(db_session=session)
        response = service.generate_project_report(
            project=project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=False,
        )
    finally:
        session.close()

    assert len(response.decisions) == 1
    decision = response.decisions[0]
    assert decision.reason == "10-minute autonomous decision timer detected commitment and deadline risk"
    assert decision.mode == "timer_autonomy"
    assert decision.trace_label == "timer_commitment_risk"
    assert decision.lineage == (
        "mode=timer_autonomy -> trace=timer_commitment_risk -> audit=create_task:executed -> integration=notion executed: create_task"
    )


def test_reporting_service_execute_publish_queues_hermes_due_action_and_audit(reporting_db):
    session = reporting_db.SessionLocal()
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={
                "onboarding": {
                    "status": "active",
                    "reporting_cadence": "weekly",
                    "board_integration": "notion",
                }
            },
        )
        session.add(project)
        session.commit()

        service = ReportingService(db_session=session)
        response = service.generate_project_report(
            project=project,
            actor="alice",
            role="admin",
            actor_trusted=True,
            execute_publish=True,
            idempotency_key="report-publish-hermes-queue",
        )

        due_action = (
            session.query(models.DueAction)
            .filter_by(project_id=project.id, kind="project_report_publish")
            .one()
        )
        audit = session.query(models.AuditEvent).filter_by(idempotency_key="report-publish-hermes-queue").one()
        audit_payload = json.loads(audit.payload)
    finally:
        session.close()

    assert response.publish.integration == "hermes"
    assert response.publish.owner == "hermes"
    assert response.publish.status == "queued"
    assert response.publish.target == f"Bro-PM/Reports/internal/Projects/{project.slug}"
    assert due_action.channel == "hermes"
    assert due_action.recipient == "knowledge-writer"
    assert due_action.status == "pending"
    assert due_action.idempotency_key == "report-publish-hermes-queue:due-action"
    assert due_action.payload_json["publish_contract"]["report"]["project_id"] == project.id
    assert due_action.payload_json["ownership"] == {
        "operational_truth": "bro_pm",
        "knowledge_outputs": "hermes",
    }
    assert audit.result == "queued"
    assert audit_payload["integration"]["name"] == "hermes"
    assert audit_payload["integration"]["detail"] == (
        "Queued Hermes knowledge handoff; Bro-PM kept operational truth and did not write Notion directly"
    )


def test_reporting_service_execute_publish_failure_marks_hermes_audit_failed_and_replayable(reporting_db, monkeypatch):
    session = reporting_db.SessionLocal()
    idempotency_key = "report-publish-hermes-queue-failure"
    project_id = None
    seen_pending_integration = None
    try:
        project = models.Project(
            name=f"Project {uuid4().hex[:8]}",
            slug=f"project-{uuid4().hex[:8]}",
            timezone="UTC",
            created_by="alice",
            metadata_json={
                "onboarding": {
                    "status": "active",
                    "reporting_cadence": "weekly",
                    "board_integration": "notion",
                }
            },
        )
        session.add(project)
        session.commit()
        project_id = project.id

        service = ReportingService(db_session=session)

        def fail_enqueue(**kwargs):
            nonlocal seen_pending_integration
            reserved = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
            seen_pending_integration = json.loads(reserved.payload)["integration"]
            raise RuntimeError("queue unavailable")

        monkeypatch.setattr(service, "_enqueue_publish_due_action", fail_enqueue)

        with pytest.raises(RuntimeError, match="queue unavailable"):
            service.generate_project_report(
                project=project,
                actor="alice",
                role="admin",
                actor_trusted=True,
                execute_publish=True,
                idempotency_key=idempotency_key,
            )

        audit = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one()
        audit_payload = json.loads(audit.payload)
    finally:
        session.close()

    assert seen_pending_integration == {
        "name": "hermes",
        "action": "publish_report",
        "status": "pending",
        "detail": "report publish handoff pending",
    }
    assert audit.result == "failed"
    assert audit_payload["integration"] == {
        "name": "hermes",
        "action": "publish_report",
        "status": "failed",
        "detail": "queue unavailable",
    }
    assert audit_payload["idempotency"]["replay"] == {
        "kind": "error",
        "detail": "queue unavailable",
    }

    replay_session = reporting_db.SessionLocal()
    try:
        replay_project = replay_session.get(models.Project, project_id)
        assert replay_project is not None
        replay_service = ReportingService(db_session=replay_session)
        with pytest.raises(IntegrationError, match="queue unavailable"):
            replay_service.generate_project_report(
                project=replay_project,
                actor="alice",
                role="admin",
                actor_trusted=True,
                execute_publish=True,
                idempotency_key=idempotency_key,
            )
    finally:
        replay_session.close()
