from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from .planning_state import sync_executor_load


_DECOMPOSITION_PHASES = (
    ("Clarify scope for {focus}", "Clarify scope, constraints, and acceptance details for '{source_title}'."),
    ("Execute next slice for {focus}", "Complete the next concrete delivery step for '{source_title}'."),
    ("Verify and report for {focus}", "Verify the outcome for '{source_title}' and publish a concise status update."),
)
_FINAL_TASK_STATUSES = {"done", "closed", "cancelled"}


@dataclass(frozen=True)
class PlannedTask:
    title: str
    description: str | None
    status: str
    assignee: str | None
    priority: str
    goal_id: str | None


class PlannerService:
    """Deterministic MVP planner for narrow next-step decomposition."""

    def __init__(self, db_session: Session):
        self.db = db_session

    def recommend_goal_tasks(self, *, goal_id: str, max_tasks: int = 3) -> list[PlannedTask]:
        goal = self.db.get(models.Goal, goal_id)
        if goal is None:
            raise ValueError("goal not found")
        return self._recommend_for_source(
            project_id=goal.project_id,
            source_title=goal.title,
            goal_id=goal.id,
            priority="high",
            max_tasks=max_tasks,
        )

    def recommend_task_tasks(self, *, task_id: str, max_tasks: int = 3) -> list[PlannedTask]:
        task = self.db.get(models.Task, task_id)
        if task is None:
            raise ValueError("task not found")
        return self._recommend_for_source(
            project_id=task.project_id,
            source_title=task.title,
            goal_id=task.goal_id or self._active_goal_id(project_id=task.project_id),
            priority=task.priority or "medium",
            max_tasks=max_tasks,
        )

    def create_goal_tasks(self, *, goal_id: str, max_tasks: int = 3) -> list[models.Task]:
        recommendations = self.recommend_goal_tasks(goal_id=goal_id, max_tasks=max_tasks)
        return self._persist_recommendations(
            project_id=self._goal(goal_id).project_id,
            recommendations=recommendations,
        )

    def create_task_follow_ups(self, *, task_id: str, max_tasks: int = 3) -> list[models.Task]:
        task = self._task(task_id)
        recommendations = self.recommend_task_tasks(task_id=task_id, max_tasks=max_tasks)
        return self._persist_recommendations(project_id=task.project_id, recommendations=recommendations)

    def _goal(self, goal_id: str) -> models.Goal:
        goal = self.db.get(models.Goal, goal_id)
        if goal is None:
            raise ValueError("goal not found")
        return goal

    def _task(self, task_id: str) -> models.Task:
        task = self.db.get(models.Task, task_id)
        if task is None:
            raise ValueError("task not found")
        return task

    def _recommend_for_source(
        self,
        *,
        project_id: str,
        source_title: str,
        goal_id: str | None,
        priority: str,
        max_tasks: int,
    ) -> list[PlannedTask]:
        focus = self._focus_fragment(source_title)
        profiles = self._candidate_profiles(project_id=project_id)
        pending_assignments: dict[str, int] = {}
        recommendations: list[PlannedTask] = []

        for template_title, template_description in _DECOMPOSITION_PHASES[:max_tasks]:
            assignee = self._choose_assignee(profiles=profiles, pending_assignments=pending_assignments)
            if assignee is not None:
                pending_assignments[assignee] = pending_assignments.get(assignee, 0) + 1
            recommendations.append(
                PlannedTask(
                    title=template_title.format(focus=focus),
                    description=template_description.format(source_title=source_title),
                    status="todo",
                    assignee=assignee,
                    priority=priority,
                    goal_id=goal_id,
                )
            )

        return recommendations

    def _persist_recommendations(
        self,
        *,
        project_id: str,
        recommendations: list[PlannedTask],
    ) -> list[models.Task]:
        created: list[models.Task] = []
        for recommendation in recommendations:
            task = models.Task(
                project_id=project_id,
                goal_id=recommendation.goal_id,
                title=recommendation.title,
                description=recommendation.description,
                status=recommendation.status,
                assignee=recommendation.assignee,
                priority=recommendation.priority,
            )
            self.db.add(task)
            created.append(task)

        self.db.flush()
        sync_executor_load(self.db, project_id=project_id)
        for task in created:
            self.db.refresh(task)
        return created

    def _candidate_profiles(self, *, project_id: str) -> list[models.ExecutorCapacityProfile]:
        sync_executor_load(self.db, project_id=project_id)
        return (
            self.db.query(models.ExecutorCapacityProfile)
            .filter(models.ExecutorCapacityProfile.project_id == project_id)
            .order_by(models.ExecutorCapacityProfile.actor.asc(), models.ExecutorCapacityProfile.id.asc())
            .all()
        )

    def _choose_assignee(
        self,
        *,
        profiles: list[models.ExecutorCapacityProfile],
        pending_assignments: dict[str, int],
    ) -> str | None:
        best_actor: str | None = None
        best_score: tuple[int, int, str] | None = None

        for profile in profiles:
            if profile.capacity_units <= 0:
                continue
            effective_load = profile.load_units + pending_assignments.get(profile.actor, 0)
            if effective_load >= profile.capacity_units:
                continue
            remaining_capacity = profile.capacity_units - effective_load
            score = (effective_load, -remaining_capacity, profile.actor)
            if best_score is None or score < best_score:
                best_score = score
                best_actor = profile.actor

        return best_actor

    @staticmethod
    def _focus_fragment(source_title: str) -> str:
        compact = " ".join((source_title or "").split()).strip()
        if not compact:
            return "the current work item"
        return compact[:96]

    def _active_goal_id(self, *, project_id: str) -> str | None:
        active_goal = (
            self.db.query(models.Goal)
            .filter(
                models.Goal.project_id == project_id,
                func.lower(func.trim(models.Goal.status)) == "active",
            )
            .order_by(models.Goal.created_at.desc(), models.Goal.id.desc())
            .first()
        )
        return active_goal.id if active_goal is not None else None
