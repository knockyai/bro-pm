from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy.orm import Session, sessionmaker

from .. import models
from .reporting_service import ReportingService


AUTONOMOUS_ACTOR = "bro_pm_timer"
AUTONOMOUS_ROLE = "admin"
SUPPORTED_CADENCES = frozenset({"daily", "weekly"})


@dataclass(frozen=True)
class CadenceWindow:
    cadence: str
    start: datetime
    end: datetime
    key: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_now(now: datetime | None) -> datetime:
    if now is None:
        return _utc_now()
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


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


def run_due_once(*, session_factory: sessionmaker, now: datetime | None = None) -> int:
    run_started_at = _normalize_now(now)
    selection_session: Session = session_factory()
    try:
        project_ids = [project_id for (project_id,) in selection_session.query(models.Project.id).order_by(models.Project.id.asc()).all()]
    finally:
        selection_session.close()

    published = 0
    for project_id in project_ids:
        published += int(_publish_due_project(project_id, session_factory=session_factory, now=run_started_at))
    return published


async def poll_due_forever(*, session_factory: sessionmaker, poll_interval_seconds: float) -> None:
    interval = max(float(poll_interval_seconds), 0.1)
    while True:
        try:
            run_due_once(session_factory=session_factory)
        except Exception:
            # Keep the in-process timer alive; publishing is idempotent per window.
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
