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
from .planning_state import sync_executor_load
from .command_service import CommandService
from .gateway_service import GatewayService
from .reporting_service import ReportingService


AUTONOMOUS_ACTOR = "bro_pm_timer"
AUTONOMOUS_ROLE = "admin"
SUPPORTED_CADENCES = frozenset({"daily", "weekly"})
DECISION_INTERVAL = timedelta(minutes=10)
DECISION_FAILURE_LOOKBACK = timedelta(hours=24)
AUTONOMY_HEURISTIC_COOLDOWN = timedelta(hours=24)
FAILURE_ESCALATION_THRESHOLD = 2
OVERDUE_REPLAN_THRESHOLD = 3
STALL_LOOKBACK = timedelta(days=2)
COMMITMENT_SOON_WINDOW = timedelta(days=3)
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


def _decision_trace_already_exists_in_window(
    session: Session,
    *,
    project_id: str,
    trace_label: str,
    window: DecisionWindow,
) -> bool:
    existing = (
        session.query(models.AuditEvent.id)
        .filter(
            models.AuditEvent.project_id == project_id,
            models.AuditEvent.actor == AUTONOMOUS_ACTOR,
            models.AuditEvent.idempotency_key == _decision_idempotency_key(project_id, trace_label=trace_label, window=window),
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


def _open_tasks(tasks: list[models.Task]) -> list[models.Task]:
    return [task for task in tasks if _is_open_task(task)]


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


def _executor_capacity_profiles(session: Session, *, project_id: str) -> list[models.ExecutorCapacityProfile]:
    return (
        session.query(models.ExecutorCapacityProfile)
        .filter(models.ExecutorCapacityProfile.project_id == project_id)
        .order_by(models.ExecutorCapacityProfile.actor.asc(), models.ExecutorCapacityProfile.id.asc())
        .all()
    )


def _first_overloaded_profile(
    profiles: list[models.ExecutorCapacityProfile],
) -> models.ExecutorCapacityProfile | None:
    for profile in profiles:
        if profile.capacity_units > 0 and profile.load_units > profile.capacity_units:
            return profile
    return None


def _first_idle_profile(
    profiles: list[models.ExecutorCapacityProfile],
    *,
    open_tasks: list[models.Task],
) -> models.ExecutorCapacityProfile | None:
    has_unassigned_open_task = any(task.assignee is None for task in open_tasks)
    if not has_unassigned_open_task:
        return None
    for profile in profiles:
        if profile.capacity_units <= 0:
            continue
        if profile.load_units == 0:
            return profile
    return None


def _stalled_reference_at(task: models.Task) -> datetime:
    return _normalize_timestamp(task.last_progress_at or task.updated_at or task.created_at)


def _first_stalled_task(tasks: list[models.Task], *, now: datetime) -> models.Task | None:
    cutoff = now - STALL_LOOKBACK
    for task in tasks:
        if _stalled_reference_at(task) <= cutoff:
            return task
    return None


def _commitment_target(
    project: models.Project,
    *,
    goal: models.Goal | None,
) -> tuple[datetime | None, str]:
    if goal is not None and goal.commitment_due_at is not None:
        return _normalize_timestamp(goal.commitment_due_at), f"goal '{goal.title}'"
    if project.commitment_due_at is not None:
        return _normalize_timestamp(project.commitment_due_at), "project commitment"
    return None, ""


def _commitment_risk_context(
    project: models.Project,
    *,
    goal: models.Goal | None,
    open_tasks: list[models.Task],
    profiles: list[models.ExecutorCapacityProfile],
    now: datetime,
) -> dict | None:
    commitment_due_at, commitment_label = _commitment_target(project, goal=goal)
    if commitment_due_at is None:
        return None

    remaining = commitment_due_at - now
    if remaining > COMMITMENT_SOON_WINDOW:
        return None

    total_remaining_capacity = sum(max(profile.capacity_units - profile.load_units, 0) for profile in profiles)
    open_task_count = len(open_tasks)
    overdue_open_tasks = _overdue_open_task_count(open_tasks, now=now)
    if remaining <= timedelta(0):
        if open_task_count == 0:
            return None
    elif open_task_count <= total_remaining_capacity and overdue_open_tasks == 0:
        return None

    return {
        "due_at": commitment_due_at,
        "label": commitment_label,
        "open_task_count": open_task_count,
        "remaining_capacity": total_remaining_capacity,
        "overdue_open_tasks": overdue_open_tasks,
    }


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


def _onboarding_metadata(project: models.Project) -> dict:
    metadata = project.metadata_json or {}
    onboarding = metadata.get("onboarding") or {}
    return onboarding if isinstance(onboarding, dict) else {}


def _preferred_gateway_channel(project: models.Project) -> str | None:
    integrations = _onboarding_metadata(project).get("communication_integrations")
    if not isinstance(integrations, list):
        return None

    normalized = [value.strip().lower() for value in integrations if isinstance(value, str) and value.strip()]
    if "telegram" in normalized:
        return "telegram"
    return normalized[0] if normalized else None


def _failure_escalation_recipient(project: models.Project) -> str | None:
    onboarding = _onboarding_metadata(project)
    for field_name in ("boss", "admin"):
        value = onboarding.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if isinstance(project.created_by, str) and project.created_by.strip():
        return project.created_by.strip()
    return None


def _enqueue_failure_escalation_due_action(
    session: Session,
    *,
    project: models.Project,
    proposal: CommandProposal,
    window: DecisionWindow,
    now: datetime,
) -> bool:
    channel = _preferred_gateway_channel(project)
    recipient = _failure_escalation_recipient(project)
    if channel is None or recipient is None:
        return False

    idempotency_key = _decision_idempotency_key(project.id, trace_label="timer_failure_escalation", window=window)
    existing = session.query(models.DueAction).filter_by(idempotency_key=idempotency_key).one_or_none()
    if existing is not None:
        return False

    payload = {
        "text": proposal.payload.get("escalation_message") or proposal.reason,
        "risk_level": proposal.payload.get("risk_level", "high"),
        "requires_approval": True,
        "trace_label": "timer_failure_escalation",
        "project_id": project.id,
        "project_slug": project.slug,
        "proposal_action": proposal.action,
    }
    GatewayService(db_session=session).enqueue_due_action(
        project_id=project.id,
        channel=channel,
        recipient=recipient,
        kind="boss_escalation",
        payload=payload,
        due_at=now,
        actor=AUTONOMOUS_ACTOR,
        idempotency_key=idempotency_key,
    )
    return True


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


def _build_executor_overload_proposal(
    project: models.Project,
    *,
    profile: models.ExecutorCapacityProfile,
) -> CommandProposal:
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected executor overload against durable capacity",
        payload={
            "title": f"Rebalance overloaded executor: {profile.actor}",
            "description": (
                f"Autonomous 10-minute decision review found executor {profile.actor} carrying load "
                f"{profile.load_units} against capacity {profile.capacity_units} for project {project.slug or project.id}. "
                "Reassign work or reduce inflight scope."
            ),
            "mode": "timer_autonomy",
            "trace_label": f"timer_executor_overload:{profile.actor}",
        },
    )


def _build_idle_executor_proposal(
    project: models.Project,
    *,
    profile: models.ExecutorCapacityProfile,
    unassigned_open_tasks: int,
) -> CommandProposal:
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected idle executor with spare capacity",
        payload={
            "title": f"Assign next task to idle executor: {profile.actor}",
            "description": (
                f"Autonomous 10-minute decision review found idle executor {profile.actor} with capacity "
                f"{profile.capacity_units} and {unassigned_open_tasks} unassigned open tasks in project {project.slug or project.id}. "
                "Assign the next concrete work item."
            ),
            "mode": "timer_autonomy",
            "trace_label": f"timer_idle_executor:{profile.actor}",
        },
    )


def _build_stalled_task_proposal(project: models.Project, *, task: models.Task, stalled_since: datetime) -> CommandProposal:
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected stalled task from progress timestamp",
        payload={
            "title": f"Unblock stalled task: {task.title}",
            "description": (
                f"Autonomous 10-minute decision review found task '{task.title}' stalled since "
                f"{stalled_since.isoformat()} for project {project.slug or project.id}. "
                "Follow up with the assignee and define the unblock or next progress step."
            ),
            "mode": "timer_autonomy",
            "trace_label": f"timer_stalled_task:{task.id}",
        },
    )


def _build_commitment_risk_proposal(project: models.Project, *, context: dict) -> CommandProposal:
    due_at = context["due_at"]
    label = context["label"]
    open_task_count = context["open_task_count"]
    remaining_capacity = context["remaining_capacity"]
    overdue_open_tasks = context["overdue_open_tasks"]
    return CommandProposal(
        action="create_task",
        project_id=project.id,
        reason="10-minute autonomous decision timer detected commitment and deadline risk",
        payload={
            "title": f"Reduce commitment risk for {project.name}",
            "description": (
                f"Autonomous 10-minute decision review found {open_task_count} open tasks against "
                f"{remaining_capacity} remaining capacity before the {label} deadline at {due_at.isoformat()} "
                f"for project {project.slug or project.id}. Overdue open tasks: {overdue_open_tasks}. "
                "Reduce scope, reassign work, or re-commit the deadline."
            ),
            "mode": "timer_autonomy",
            "trace_label": "timer_commitment_risk",
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


def _run_due_project_decision(project_id: str, *, session_factory: sessionmaker, now: datetime) -> int:
    session: Session = session_factory()
    try:
        project = session.get(models.Project, project_id)
        if project is None or project.safe_paused:
            return 0

        window = _decision_window_for(now=now)
        decisions_taken = 0

        failure_count = _count_recent_failures(session, project_id=project.id, now=now)
        if failure_count >= FAILURE_ESCALATION_THRESHOLD and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_failure_escalation",
            now=now,
        ) and not _decision_trace_already_exists_in_window(
            session,
            project_id=project.id,
            trace_label="timer_failure_escalation",
            window=window,
        ):
            proposal = _build_failure_escalation_proposal(project, failure_count=failure_count)
            decisions_taken += int(
                _enqueue_failure_escalation_due_action(
                session,
                project=project,
                proposal=proposal,
                window=window,
                now=now,
            )
            )
            if decisions_taken:
                return decisions_taken

        sync_executor_load(session, project_id=project.id)
        session.commit()
        project = session.get(models.Project, project_id)
        if project is None or project.safe_paused:
            return decisions_taken
        tasks = _project_tasks(session, project_id=project.id)
        open_tasks = _open_tasks(tasks)
        profiles = _executor_capacity_profiles(session, project_id=project.id)
        overdue_open_tasks = _overdue_open_task_count(tasks, now=now)
        if overdue_open_tasks >= OVERDUE_REPLAN_THRESHOLD and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_overdue_replan",
            now=now,
        ) and not _decision_trace_already_exists_in_window(
            session,
            project_id=project.id,
            trace_label="timer_overdue_replan",
            window=window,
        ):
            proposal = _build_overdue_replan_proposal(project, overdue_count=overdue_open_tasks)
            decisions_taken += int(
                _execute_autonomous_proposal(
                session,
                project_id=project.id,
                proposal=proposal,
                idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_overdue_replan", window=window),
                execute_integration=True,
            )
            )
            if decisions_taken:
                return decisions_taken

        stalled_task = _first_stalled_task(open_tasks, now=now)
        if stalled_task is not None:
            stalled_trace_label = f"timer_stalled_task:{stalled_task.id}"
            if not _recent_autonomy_action_exists(
                session,
                project_id=project.id,
                trace_label=stalled_trace_label,
                now=now,
            ) and not _decision_trace_already_exists_in_window(
                session,
                project_id=project.id,
                trace_label=stalled_trace_label,
                window=window,
            ):
                proposal = _build_stalled_task_proposal(
                    project,
                    task=stalled_task,
                    stalled_since=_stalled_reference_at(stalled_task),
                )
                decisions_taken += int(
                    _execute_autonomous_proposal(
                        session,
                        project_id=project.id,
                        proposal=proposal,
                        idempotency_key=_decision_idempotency_key(project.id, trace_label=stalled_trace_label, window=window),
                        execute_integration=True,
                    )
                )
                if decisions_taken:
                    return decisions_taken

        overloaded_profile = _first_overloaded_profile(profiles)
        if overloaded_profile is not None:
            overload_trace_label = f"timer_executor_overload:{overloaded_profile.actor}"
            if not _recent_autonomy_action_exists(
                session,
                project_id=project.id,
                trace_label=overload_trace_label,
                now=now,
            ) and not _decision_trace_already_exists_in_window(
                session,
                project_id=project.id,
                trace_label=overload_trace_label,
                window=window,
            ):
                proposal = _build_executor_overload_proposal(project, profile=overloaded_profile)
                decisions_taken += int(
                    _execute_autonomous_proposal(
                        session,
                        project_id=project.id,
                        proposal=proposal,
                        idempotency_key=_decision_idempotency_key(project.id, trace_label=overload_trace_label, window=window),
                        execute_integration=True,
                    )
                )
                if decisions_taken:
                    return decisions_taken

        goal = _active_goal(session, project_id=project.id)
        commitment_risk = _commitment_risk_context(
            project,
            goal=goal,
            open_tasks=open_tasks,
            profiles=profiles,
            now=now,
        )
        if commitment_risk is not None and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_commitment_risk",
            now=now,
        ) and not _decision_trace_already_exists_in_window(
            session,
            project_id=project.id,
            trace_label="timer_commitment_risk",
            window=window,
        ):
            proposal = _build_commitment_risk_proposal(project, context=commitment_risk)
            decisions_taken += int(
                _execute_autonomous_proposal(
                    session,
                    project_id=project.id,
                    proposal=proposal,
                    idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_commitment_risk", window=window),
                    execute_integration=True,
                )
            )
            if decisions_taken:
                return decisions_taken

        idle_profile = _first_idle_profile(profiles, open_tasks=open_tasks)
        if idle_profile is not None:
            idle_trace_label = f"timer_idle_executor:{idle_profile.actor}"
            if not _recent_autonomy_action_exists(
                session,
                project_id=project.id,
                trace_label=idle_trace_label,
                now=now,
            ) and not _decision_trace_already_exists_in_window(
                session,
                project_id=project.id,
                trace_label=idle_trace_label,
                window=window,
            ):
                proposal = _build_idle_executor_proposal(
                    project,
                    profile=idle_profile,
                    unassigned_open_tasks=sum(1 for task in open_tasks if task.assignee is None),
                )
                decisions_taken += int(
                    _execute_autonomous_proposal(
                        session,
                        project_id=project.id,
                        proposal=proposal,
                        idempotency_key=_decision_idempotency_key(project.id, trace_label=idle_trace_label, window=window),
                        execute_integration=True,
                    )
                )
                if decisions_taken:
                    return decisions_taken

        if goal is not None and len(open_tasks) == 0 and not _recent_autonomy_action_exists(
            session,
            project_id=project.id,
            trace_label="timer_goal_without_open_tasks",
            now=now,
        ) and not _decision_trace_already_exists_in_window(
            session,
            project_id=project.id,
            trace_label="timer_goal_without_open_tasks",
            window=window,
        ):
            proposal = _build_followup_task_proposal(project, goal=goal)
            decisions_taken += int(
                _execute_autonomous_proposal(
                session,
                project_id=project.id,
                proposal=proposal,
                idempotency_key=_decision_idempotency_key(project.id, trace_label="timer_goal_without_open_tasks", window=window),
                execute_integration=True,
            )
            )
            if decisions_taken:
                return decisions_taken

        return decisions_taken
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
