from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from .. import models
from ..schemas import CommandProposal
from .command_service import CommandService
from .reporting_service import ReportingService


AUTONOMOUS_ACTOR = "bro_pm_timer"
AUTONOMOUS_ROLE = "admin"
SUPPORTED_CADENCES = frozenset({"daily", "weekly"})
DECISION_INTERVAL = timedelta(minutes=10)
DECISION_FAILURE_LOOKBACK = timedelta(hours=24)
AUTONOMY_HEURISTIC_COOLDOWN = timedelta(hours=24)
FAILURE_ESCALATION_THRESHOLD = 2
OVERDUE_REPLAN_THRESHOLD = 3
FAILURE_ACTIONS = frozenset({"create_task", "publish_report", "onboard_project"})
TERMINAL_TASK_STATUSES = frozenset({"done", "closed", "cancelled", "failed", "archived"})


@dataclass(frozen=True)
class CadenceWindow:
    cadence: str
    start: datetime
    end: datetime
    key: str


@dataclass(frozen=True)
class DecisionWindow:
    start: datetime
    end: datetime
    key: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(value: datetime | None) -> datetime:
    if value is None:
        return _utc_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _project_timezone(project: models.Project) -> ZoneInfo:
    timezone_name = (project.timezone or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def _normalize_reporting_cadence(project: models.Project) -> str:
    metadata = project.metadata_json or {}
    onboarding = metadata.get("onboarding") or {}
    cadence = onboarding.get("reporting_cadence", "manual")
    if not isinstance(cadence, str):
        return "manual"
    return cadence.strip().lower() or "manual"


def _cadence_window_for(project: models.Project, *, now: datetime) -> CadenceWindow | None:
    cadence = _normalize_reporting_cadence(project)
    if cadence == "manual":
        return None
    if cadence not in SUPPORTED_CADENCES:
        return None

    project_now = now.astimezone(_project_timezone(project))
    if cadence == "daily":
        start = project_now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        key = start.date().isoformat()
    else:
        start = (project_now - timedelta(days=project_now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
        iso_year, iso_week, _ = start.isocalendar()
        key = f"{iso_year}-W{iso_week:02d}"

    return CadenceWindow(
        cadence=cadence,
        start=start.astimezone(timezone.utc),
        end=end.astimezone(timezone.utc),
        key=key,
    )


def _scheduled_publish_idempotency_key(project: models.Project, *, window: CadenceWindow) -> str:
    return f"timer-report:{project.id}:{window.cadence}:{window.key}"


def _decision_window_for(*, now: datetime) -> DecisionWindow:
    start = now.astimezone(timezone.utc).replace(second=0, microsecond=0)
    bucket_minute = (start.minute // 10) * 10
    start = start.replace(minute=bucket_minute)
    end = start + DECISION_INTERVAL
    return DecisionWindow(start=start, end=end, key=start.strftime("%Y-%m-%dT%H:%MZ"))


def _decision_idempotency_key(project_id: str, *, trace_label: str, window: DecisionWindow) -> str:
    return f"timer-decision:{project_id}:{trace_label}:{window.key}"


def _is_open_task(task: models.Task) -> bool:
    status = (task.status or "").strip().lower()
    return status not in TERMINAL_TASK_STATUSES


def _load_payload(text: str | None) -> dict:
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except (TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_trace_label(payload: dict) -> str | None:
    proposal = payload.get("proposal") or {}
    proposal_payload = proposal.get("payload") or {}
    trace_label = proposal_payload.get("trace_label")
    if isinstance(trace_label, str) and trace_label.strip():
        return trace_label.strip()
    return None


def _recent_autonomy_action_exists(session: Session, *, project_id: str, trace_label: str, now: datetime) -> bool:
    cutoff = now - AUTONOMY_HEURISTIC_COOLDOWN
    events = (
        session.query(models.AuditEvent)
        .filter(
            models.AuditEvent.project_id == project_id,
            models.AuditEvent.actor == AUTONOMOUS_ACTOR,
            models.AuditEvent.created_at >= cutoff,
        )
        .order_by(models.AuditEvent.created_at.desc())
        .all()
    )
    for event in events:
        if _payload_trace_label(_load_payload(event.payload)) == trace_label:
            return True
    return False


def _decision_already_exists_in_window(session: Session, *, project_id: str, window: DecisionWindow) -> bool:
    pattern = f"timer-decision:{project_id}:%:{window.key}"
    existing = (
        session.query(models.AuditEvent.id)
        .filter(
            models.AuditEvent.project_id == project_id,
            models.AuditEvent.actor == AUTONOMOUS_ACTOR,
            models.AuditEvent.idempotency_key.like(pattern),
        )
        .first()
    )
    return existing is not None


def _count_recent_failures(session: Session, *, project_id: str, now: datetime) -> int:
    cutoff = now - DECISION_FAILURE_LOOKBACK
    return (
        session.query(func.count(models.AuditEvent.id))
        .filter(
            models.AuditEvent.project_id == project_id,
            models.AuditEvent.created_at >= cutoff,
            models.AuditEvent.action.in_(tuple(FAILURE_ACTIONS)),
            models.AuditEvent.result.in_(("failed", "denied")),
        )
        .scalar()
        or 0
    )


def _active_goal(session: Session, *, project_id: str) -> models.Goal | None:
    return (
        session.query(models.Goal)
        .filter(
            models.Goal.project_id == project_id,
            func.lower(func.trim(models.Goal.status)) == "active",
        )
        .order_by(models.Goal.created_at.asc(), models.Goal.id.asc())
        .first()
    )


def _project_tasks(session: Session, *, project_id: str) -> list[models.Task]:
    return (
        session.query(models.Task)
        .filter(models.Task.project_id == project_id)
        .order_by(models.Task.created_at.asc(), models.Task.id.asc())
        .all()
    )


def _open_task_count(tasks: list[models.Task]) -> int:
    return sum(1 for task in tasks if _is_open_task(task))


def _overdue_open_task_count(tasks: list[models.Task], *, now: datetime) -> int:
    overdue = 0
    for task in tasks:
        if not _is_open_task(task):
            continue
        if task.due_at is None:
            continue
        if _normalize_timestamp(task.due_at) < now:
            overdue += 1
    return overdue


def _execute_autonomous_proposal(
    session: Session,
    *,
    project_id: str,
    proposal: CommandProposal,
    idempotency_key: str,
    execute_integration: bool = False,
) -> bool:
    service = CommandService(db_session=session)
    execution = service.execute(
        actor=AUTONOMOUS_ACTOR,
        role=AUTONOMOUS_ROLE,
        proposal=proposal,
        actor_trusted=True,
        idempotency_key=idempotency_key,
        execute_integration=execute_integration,
    )
    session.commit()
    return execution.success


def _build_failure_escalation_proposal(project: models.Project, *, failure_count: int) -> CommandProposal:
    return CommandProposal(
        action="draft_boss_escalation",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected repeated recent failures",
        requires_approval=True,
        payload={
            "mode": "timer_autonomy",
            "trace_label": "timer_failure_escalation",
            "risk_level": "high",
            "operator_confirmation": True,
            "escalation_message": (
                f"Autonomous 10-minute decision review detected {failure_count} recent failures for project {project.slug or project.id}. "
                "Further action likely requires boss attention."
            ),
        },
    )


def _build_followup_task_proposal(project: models.Project, *, goal: models.Goal) -> CommandProposal:
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer found active goal with no open tasks",
        payload={
            "title": f"Define next concrete step for goal: {goal.title}",
            "description": (
                f"Autonomous 10-minute decision review found active goal '{goal.title}' but no open local tasks. "
                "Create the next concrete execution step."
            ),
            "mode": "timer_autonomy",
            "trace_label": "timer_goal_without_open_tasks",
        },
    )


def _build_overdue_replan_proposal(project: models.Project, *, overdue_count: int) -> CommandProposal:
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected overdue open tasks",
        payload={
            "title": f"Replan overdue tasks for {project.name}",
            "description": (
                f"Autonomous 10-minute decision review found {overdue_count} overdue open tasks for project {project.slug or project.id}. "
                "Create a concrete replan / recovery step."
            ),
            "mode": "timer_autonomy",
            "trace_label": "timer_overdue_replan",
        },
    )


def _publish_due_project(project_id: str, *, session_factory: sessionmaker, now: datetime) -> bool:
    session: Session = session_factory()
    try:
        project = session.get(models.Project, project_id)
        if project is None or project.safe_paused:
            return False

        window = _cadence_window_for(project, now=now)
        if window is None:
            return False

        idempotency_key = _scheduled_publish_idempotency_key(project, window=window)
        existing = session.query(models.AuditEvent).filter_by(idempotency_key=idempotency_key).one_or_none()
        if existing is not None:
            return False

        service = ReportingService(db_session=session)
        service.generate_project_report(
            project=project,
            actor=AUTONOMOUS_ACTOR,
            role=AUTONOMOUS_ROLE,
            actor_trusted=True,
            execute_publish=True,
            idempotency_key=idempotency_key,
        )
        return True
    finally:
        session.close()


def _run_due_project_decision(project_id: str, *, session_factory: sessionmaker, now: datetime) -> bool:
    session: Session = session_factory()
    try:
        project = session.get(models.Project, project_id)
        if project is None or project.safe_paused:
            return False

        window = _decision_window_for(now=now)
        if _decision_already_exists_in_window(session, project_id=project.id, window=window):
            return False

        failure_count = _count_recent_failures(session, project_id=project.id, now=now)
        if failure_count >= FAILURE_ESCALATION_THRESHOLD and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_failure_escalation",
            now=now,
        ):
            proposal = _build_failure_escalation_proposal(project, failure_count=failure_count)
            return _execute_autonomous_proposal(
                session,
                project_id=project.id,
                proposal=proposal,
                idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_failure_escalation", window=window),
            )

        tasks = _project_tasks(session, project_id=project.id)
        overdue_open_tasks = _overdue_open_task_count(tasks, now=now)
        if overdue_open_tasks >= OVERDUE_REPLAN_THRESHOLD and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_overdue_replan",
            now=now,
        ):
            proposal = _build_overdue_replan_proposal(project, overdue_count=overdue_open_tasks)
            return _execute_autonomous_proposal(
                session,
                project_id=project.id,
                proposal=proposal,
                idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_overdue_replan", window=window),
                execute_integration=True,
            )

        goal = _active_goal(session, project_id=project.id)
        if goal is not None and _open_task_count(tasks) == 0 and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_goal_without_open_tasks",
            now=now,
        ):
            proposal = _build_followup_task_proposal(project, goal=goal)
            return _execute_autonomous_proposal(
                session,
                project_id=project.id,
                proposal=proposal,
                idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_goal_without_open_tasks", window=window),
                execute_integration=True,
            )

        return False
    finally:
        session.close()


def run_due_once(*, session_factory: sessionmaker, now: datetime | None = None) -> int:
    run_started_at = _normalize_timestamp(now)
    selection_session: Session = session_factory()
    try:
        project_ids = [project_id for (project_id,) in selection_session.query(models.Project.id).order_by(models.Project.id.asc()).all()]
    finally:
        selection_session.close()

    published = 0
    for project_id in project_ids:
        published += int(_publish_due_project(project_id, session_factory=session_factory, now=run_started_at))
    return published


def run_due_decisions_once(*, session_factory: sessionmaker, now: datetime | None = None) -> int:
    run_started_at = _normalize_timestamp(now)
    selection_session: Session = session_factory()
    try:
        project_ids = [project_id for (project_id,) in selection_session.query(models.Project.id).order_by(models.Project.id.asc()).all()]
    finally:
        selection_session.close()

    decisions_taken = 0
    for project_id in project_ids:
        decisions_taken += int(_run_due_project_decision(project_id, session_factory=session_factory, now=run_started_at))
    return decisions_taken


async def poll_due_forever(*, session_factory: sessionmaker, poll_interval_seconds: float) -> None:
    interval = max(float(poll_interval_seconds), 0.1)
    while True:
        try:
            run_due_once(session_factory=session_factory)
            run_due_decisions_once(session_factory=session_factory)
        except Exception:
            # Keep the in-process timer alive; timer actions use idempotent/cooldown guards.
            pass
        await asyncio.sleep(interval)


def start_polling_task(*, session_factory: sessionmaker, poll_interval_seconds: float) -> asyncio.Task[None]:
    return asyncio.create_task(
        poll_due_forever(session_factory=session_factory, poll_interval_seconds=poll_interval_seconds)
    )


async def stop_polling_task(task: asyncio.Task[None] | None) -> None:
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
