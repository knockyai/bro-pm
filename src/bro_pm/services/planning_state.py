from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models


_FINAL_TASK_STATUSES = {"done", "closed", "cancelled"}


def seed_capacity_profiles(
    db: Session,
    *,
    project_id: str,
    team_entries: Iterable[dict],
    source: str = "onboarding",
) -> None:
    for team in team_entries:
        actor = str(team.get("owner", "")).strip()
        team_name = str(team.get("name", "")).strip()
        capacity_units = int(team.get("capacity", 0) or 0)
        if not actor or not team_name:
            continue
        db.add(
            models.ExecutorCapacityProfile(
                project_id=project_id,
                actor=actor,
                team_name=team_name,
                capacity_units=capacity_units,
                load_units=0,
                source=source,
            )
        )


def sync_executor_load(db: Session, *, project_id: str) -> None:
    load_rows = (
        db.query(
            models.Task.assignee,
            func.count(models.Task.id),
        )
        .filter(
            models.Task.project_id == project_id,
            models.Task.assignee.isnot(None),
            ~func.lower(func.trim(models.Task.status)).in_(_FINAL_TASK_STATUSES),
        )
        .group_by(models.Task.assignee)
        .all()
    )
    loads_by_actor = {str(actor): int(count) for actor, count in load_rows if actor}

    profiles = (
        db.query(models.ExecutorCapacityProfile)
        .filter(models.ExecutorCapacityProfile.project_id == project_id)
        .all()
    )
    for profile in profiles:
        profile.load_units = loads_by_actor.get(profile.actor, 0)
