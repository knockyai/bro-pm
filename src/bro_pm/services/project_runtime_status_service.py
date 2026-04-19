from __future__ import annotations

from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import models
from ..schemas import (
    ProjectRuntimeApprovalSummary,
    ProjectRuntimeExecutionSummary,
    ProjectRuntimeStatusResponse,
    ProjectRuntimeTaskCounts,
)

_CLOSED_TASK_STATUSES = frozenset({
    "done",
    "completed",
    "closed",
    "cancelled",
    "canceled",
    "failed",
    "archived",
})


class ProjectRuntimeStatusService:
    def __init__(self, *, db_session: Session) -> None:
        self.db_session = db_session

    def get_project_status(self, *, project_id: str) -> ProjectRuntimeStatusResponse:
        project = self.db_session.get(models.Project, project_id)
        if project is None:
            raise ValueError("project not found")

        active_goal_count = (
            self.db_session.query(func.count(models.Goal.id))
            .filter(
                models.Goal.project_id == project_id,
                func.lower(func.trim(models.Goal.status)) == "active",
            )
            .scalar()
            or 0
        )
        total_task_count = (
            self.db_session.query(func.count(models.Task.id))
            .filter(models.Task.project_id == project_id)
            .scalar()
            or 0
        )
        open_task_count = (
            self.db_session.query(func.count(models.Task.id))
            .filter(
                models.Task.project_id == project_id,
                ~func.lower(func.trim(models.Task.status)).in_(_CLOSED_TASK_STATUSES),
            )
            .scalar()
            or 0
        )
        pending_approval_count = (
            self.db_session.query(func.count(models.ApprovalRequest.id))
            .filter(
                models.ApprovalRequest.project_id == project_id,
                models.ApprovalRequest.status == "pending",
            )
            .scalar()
            or 0
        )
        pending_execution_count = (
            self.db_session.query(func.count(models.ExecutionOutbox.id))
            .filter(
                models.ExecutionOutbox.project_id == project_id,
                models.ExecutionOutbox.status.in_(("queued", "claimed")),
            )
            .scalar()
            or 0
        )
        failed_execution_count = (
            self.db_session.query(func.count(models.ExecutionOutbox.id))
            .filter(
                models.ExecutionOutbox.project_id == project_id,
                models.ExecutionOutbox.status == "failed",
            )
            .scalar()
            or 0
        )
        last_failure_at = (
            self.db_session.query(
                func.max(
                    func.coalesce(
                        models.ExecutionOutbox.failed_at,
                        models.ExecutionOutbox.updated_at,
                        models.ExecutionOutbox.created_at,
                    )
                )
            )
            .filter(
                models.ExecutionOutbox.project_id == project_id,
                models.ExecutionOutbox.status == "failed",
            )
            .scalar()
        )

        revision_candidates = [
            project.updated_at,
            project.created_at,
            self._max_timestamp(models.Goal, project_id=project_id),
            self._max_timestamp(models.Task, project_id=project_id),
            self._max_timestamp(models.ApprovalRequest, project_id=project_id),
            self._max_timestamp(models.ExecutionOutbox, project_id=project_id),
            self._max_timestamp(models.ActionExecution, project_id=project_id),
        ]
        revision_at = max(candidate for candidate in revision_candidates if candidate is not None)

        return ProjectRuntimeStatusResponse(
            project_id=project.id,
            safe_paused=bool(project.safe_paused),
            active_goal_count=int(active_goal_count),
            task_counts=ProjectRuntimeTaskCounts(
                total=int(total_task_count),
                open=int(open_task_count),
            ),
            approvals=ProjectRuntimeApprovalSummary(
                pending=int(pending_approval_count),
            ),
            executions=ProjectRuntimeExecutionSummary(
                pending=int(pending_execution_count),
                failed=int(failed_execution_count),
                last_failure_at=last_failure_at,
            ),
            revision_at=revision_at,
            generated_at=datetime.utcnow(),
        )

    def _max_timestamp(self, model: type[models.Base], *, project_id: str) -> datetime | None:
        return (
            self.db_session.query(
                func.max(
                    func.coalesce(
                        model.updated_at,
                        model.created_at,
                    )
                )
            )
            .filter(model.project_id == project_id)
            .scalar()
        )
