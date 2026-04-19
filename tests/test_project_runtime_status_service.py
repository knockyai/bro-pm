from __future__ import annotations

from datetime import datetime, timedelta

from bro_pm import models
from bro_pm.services.project_runtime_status_service import ProjectRuntimeStatusService

from tests.test_policy_adapter_command_service import _create_project, db_session


def test_project_runtime_status_service_treats_failed_and_archived_tasks_as_closed(db_session):
    session = db_session
    project = _create_project(session, name="Runtime terminal statuses", slug="runtime-terminal-statuses")
    project.created_at = datetime(2026, 4, 19, 8, 59, 0)
    project.updated_at = datetime(2026, 4, 19, 9, 0, 0)

    goal = models.Goal(
        project_id=project.id,
        title="Close stale work",
        status="active",
        created_at=datetime(2026, 4, 19, 9, 1, 0),
        updated_at=datetime(2026, 4, 19, 9, 1, 0),
    )
    session.add(goal)
    session.flush()

    session.add_all(
        [
            models.Task(
                project_id=project.id,
                goal_id=goal.id,
                title="Verification failed task",
                status="failed",
                created_at=datetime(2026, 4, 19, 9, 2, 0),
                updated_at=datetime(2026, 4, 19, 9, 2, 0),
            ),
            models.Task(
                project_id=project.id,
                goal_id=goal.id,
                title="Archived task",
                status="archived",
                created_at=datetime(2026, 4, 19, 9, 3, 0),
                updated_at=datetime(2026, 4, 19, 9, 3, 0),
            ),
        ]
    )
    session.commit()

    summary = ProjectRuntimeStatusService(db_session=session).get_project_status(project_id=project.id)

    assert summary.task_counts.total == 2
    assert summary.task_counts.open == 0
    assert summary.revision_at == datetime(2026, 4, 19, 9, 3, 0)


def test_project_runtime_status_service_summarizes_durable_runtime_state(db_session):
    session = db_session
    project = _create_project(session, name="Runtime project", slug="runtime-project")
    project.created_at = datetime(2026, 4, 19, 9, 59, 0)
    project.safe_paused = True
    project.updated_at = datetime(2026, 4, 19, 10, 0, 0)

    active_goal = models.Goal(
        project_id=project.id,
        title="Ship runtime API",
        status=" active ",
        created_at=datetime(2026, 4, 19, 10, 1, 0),
        updated_at=datetime(2026, 4, 19, 10, 1, 0),
    )
    session.add(active_goal)
    session.flush()

    session.add_all(
        [
            models.Task(
                project_id=project.id,
                goal_id=active_goal.id,
                title="Investigate durable state",
                status="todo",
                created_at=datetime(2026, 4, 19, 10, 2, 0),
                updated_at=datetime(2026, 4, 19, 10, 2, 0),
            ),
            models.Task(
                project_id=project.id,
                goal_id=active_goal.id,
                title="Close old follow-up",
                status="done",
                created_at=datetime(2026, 4, 19, 10, 3, 0),
                updated_at=datetime(2026, 4, 19, 10, 3, 0),
            ),
        ]
    )

    pending_audit = models.AuditEvent(
        project_id=project.id,
        actor="alice",
        action="pause_project",
        target_type="proposal",
        target_id=project.id,
        payload="{}",
        result="awaiting_approval",
        created_at=datetime(2026, 4, 19, 10, 4, 0),
    )
    failed_audit = models.AuditEvent(
        project_id=project.id,
        actor="alice",
        action="create_task",
        target_type="proposal",
        target_id=project.id,
        payload="{}",
        result="denied",
        created_at=datetime(2026, 4, 19, 10, 5, 0),
    )
    queued_audit = models.AuditEvent(
        project_id=project.id,
        actor="alice",
        action="create_task",
        target_type="proposal",
        target_id=project.id,
        payload="{}",
        result="pending_integration",
        created_at=datetime(2026, 4, 19, 10, 6, 0),
    )
    session.add_all([pending_audit, failed_audit, queued_audit])
    session.flush()

    session.add(
        models.ApprovalRequest(
            audit_event_id=pending_audit.id,
            project_id=project.id,
            action="pause_project",
            status="pending",
            requested_by="alice",
            requested_at=datetime(2026, 4, 19, 10, 4, 0),
            expires_at=datetime(2026, 4, 26, 10, 4, 0),
            updated_at=datetime(2026, 4, 19, 10, 7, 0),
        )
    )
    session.add(
        models.ExecutionOutbox(
            audit_event_id=failed_audit.id,
            project_id=project.id,
            integration_name="notion",
            integration_action="create_task",
            payload_json={},
            status="failed",
            available_at=datetime(2026, 4, 19, 10, 5, 0),
            failed_at=datetime(2026, 4, 19, 10, 8, 0),
            last_error="verification failed",
            updated_at=datetime(2026, 4, 19, 10, 8, 0),
        )
    )
    session.add(
        models.ExecutionOutbox(
            audit_event_id=queued_audit.id,
            project_id=project.id,
            integration_name="notion",
            integration_action="create_task",
            payload_json={},
            status="queued",
            available_at=datetime(2026, 4, 19, 10, 9, 0),
            updated_at=datetime(2026, 4, 19, 10, 9, 0),
        )
    )
    session.commit()

    before_call = datetime.utcnow() - timedelta(seconds=1)
    summary = ProjectRuntimeStatusService(db_session=session).get_project_status(project_id=project.id)
    after_call = datetime.utcnow() + timedelta(seconds=1)

    assert summary.project_id == project.id
    assert summary.safe_paused is True
    assert summary.active_goal_count == 1
    assert summary.task_counts.total == 2
    assert summary.task_counts.open == 1
    assert summary.approvals.pending == 1
    assert summary.executions.pending == 1
    assert summary.executions.failed == 1
    assert summary.executions.last_failure_at == datetime(2026, 4, 19, 10, 8, 0)
    assert summary.revision_at == datetime(2026, 4, 19, 10, 9, 0)
    assert before_call <= summary.generated_at <= after_call
