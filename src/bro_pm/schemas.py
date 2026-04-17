from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=3, max_length=120)
    slug: str = Field(min_length=3, max_length=120)
    description: str | None = None
    created_by: str | None = None
    visibility: str = "internal"
    safe_paused: bool = False
    metadata: dict[str, Any] | None = None

    @field_validator("slug")
    @classmethod
    def validate_slug(cls, value: str) -> str:
        if isinstance(value, str) and "/" in value:
            raise ValueError("slug must not contain '/'")
        return value

    @field_validator("visibility")
    @classmethod
    def validate_visibility(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            return "internal"
        if "/" in normalized:
            raise ValueError("visibility must not contain '/'")
        return normalized


class ProjectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    slug: str
    description: str | None
    safe_paused: bool
    created_by: str | None
    visibility: str
    metadata: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=140)
    description: str | None = None
    status: str = "todo"
    assignee: str | None = None
    priority: str = "medium"
    policy_flags: list[str] | None = None
    due_at: datetime | None = None


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    goal_id: str | None = None
    title: str
    description: str | None
    status: str
    assignee: str | None
    priority: str
    policy_flags: list[str] | None = None
    due_at: datetime | None
    created_at: datetime
    updated_at: datetime


class GoalTaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=140)
    description: str | None = None
    status: str = "todo"
    assignee: str | None = None
    priority: str = "medium"
    policy_flags: list[str] | None = None
    due_at: datetime | None = None


class GoalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    status: str = "draft"
    tasks: list[GoalTaskCreate] = Field(default_factory=list)

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        if isinstance(value, str):
            return value.strip().lower()
        return value


class GoalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    title: str
    description: str | None
    status: str
    created_at: datetime
    updated_at: datetime
    tasks: list[TaskResponse] = Field(default_factory=list)


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_text: str = Field(min_length=3, max_length=2000)
    project_id: str | None = None
    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")
    idempotency_key: str | None = None
    dry_run: bool = False


class CommandProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str
    project_id: str | None = None
    reason: str
    payload: dict[str, Any] = Field(default_factory=dict)
    requires_approval: bool = False


class ActionExecuteRequest(BaseModel):
    proposal_id: str
    actor: str = Field(min_length=2, max_length=120)
    approved: bool = False


class CommandResponse(BaseModel):
    accepted: bool
    result: str
    action: str
    target: str | None = None
    detail: str
    audit_id: str
    proposal_id: str


class AuditResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str | None
    actor: str
    action: str
    target_type: str
    target_id: str | None
    result: str
    created_at: datetime


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")
    audit_event_id: str
    reason: str = Field(min_length=5, max_length=1000)


class ProjectReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")


class RollbackResponse(BaseModel):
    accepted: bool
    result: str
    action: str
    target: str | None = None
    detail: str
    audit_id: str
    rollback_record_id: str


class ProjectReportKpis(BaseModel):
    total_tasks: int
    completed_tasks: int
    open_tasks: int
    active_goals: int
    audit_events: int


class ProjectReportRisk(BaseModel):
    kind: str
    audit_id: str | None = None
    action: str | None = None
    status: str | None = None
    summary: str


class ProjectReportDecision(BaseModel):
    audit_id: str
    action: str
    result: str
    summary: str


class ProjectReportLinks(BaseModel):
    project: str
    tasks: str
    audit_events: str
    report: str
    notion_parent: str
    notion_project: str


class ReportPublishResult(BaseModel):
    integration: str
    action: str
    status: str
    target: str
    detail: str
    visibility: str


class ProjectReportResponse(BaseModel):
    project_id: str
    report_type: str
    visibility: str
    summary: str
    kpis: ProjectReportKpis
    risks: list[ProjectReportRisk] = Field(default_factory=list)
    decisions: list[ProjectReportDecision] = Field(default_factory=list)
    action_ids: list[str] = Field(default_factory=list)
    links: ProjectReportLinks
    publish: ReportPublishResult
