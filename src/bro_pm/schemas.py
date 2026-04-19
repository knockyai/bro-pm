from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=3, max_length=120)
    slug: str = Field(min_length=3, max_length=120)
    description: str | None = None
    timezone: str | None = None
    commitment_due_at: datetime | None = None
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

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("timezone must not be empty")
        try:
            ZoneInfo(normalized)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone") from exc
        return normalized


class ProjectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    slug: str
    description: str | None
    timezone: str | None = None
    commitment_due_at: datetime | None = None
    safe_paused: bool
    created_by: str | None
    visibility: str
    metadata: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime


class ProjectMembershipResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    actor: str
    role: str


class OnboardingTeamInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=2, max_length=120)
    owner: str = Field(min_length=2, max_length=120)
    capacity: int = Field(ge=1, le=1000)


class ProjectOnboardingCreate(ProjectCreate):
    boss: str = Field(min_length=2, max_length=120)
    admin: str = Field(min_length=2, max_length=120)
    reporting_cadence: str = Field(min_length=3, max_length=80, default="weekly")
    communication_integrations: list[str] = Field(default_factory=list)
    board_integration: str = Field(min_length=2, max_length=80)
    team: list[OnboardingTeamInput] = Field(default_factory=list)

    @field_validator("communication_integrations")
    @classmethod
    def validate_communication_integrations(cls, value: list[str]) -> list[str]:
        normalized = [item.strip() for item in value if item and item.strip()]
        if not normalized:
            raise ValueError("at least one communication integration is required")
        return normalized

    @field_validator("board_integration", "reporting_cadence")
    @classmethod
    def normalize_non_empty_string(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be empty")
        return normalized

    @field_validator("team")
    @classmethod
    def validate_unique_capacity_profiles(cls, value: list[OnboardingTeamInput]) -> list[OnboardingTeamInput]:
        seen: set[tuple[str, str]] = set()
        for entry in value:
            key = (entry.name.strip().lower(), entry.owner.strip().lower())
            if key in seen:
                raise ValueError("team entries must be unique by name and owner")
            seen.add(key)
        return value


class OnboardingGateChecks(BaseModel):
    policy_attached: bool
    communication_ready: bool
    board_sync_healthy: bool
    safe_pause_default_off: bool


class OnboardingSmokeCheck(BaseModel):
    status: str
    detail: str


class ProjectOnboardingResponse(BaseModel):
    project: ProjectResponse
    timezone: str
    policy: str
    reporting_cadence: str
    memberships: list[ProjectMembershipResponse] = Field(default_factory=list)
    gate_checks: OnboardingGateChecks
    smoke_check: OnboardingSmokeCheck
    status: str


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=140)
    description: str | None = None
    status: str = "todo"
    assignee: str | None = None
    priority: str = "medium"
    policy_flags: list[str] | None = None
    due_at: datetime | None = None
    last_progress_at: datetime | None = None


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
    last_progress_at: datetime | None
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
    last_progress_at: datetime | None = None


class GoalCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    status: str = "draft"
    commitment_due_at: datetime | None = None
    auto_decompose: bool = False
    max_generated_tasks: int = Field(default=3, ge=1, le=3)
    tasks: list[GoalTaskCreate] = Field(default_factory=list)

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, value: str) -> str:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("tasks")
    @classmethod
    def validate_task_list(cls, value: list[GoalTaskCreate]) -> list[GoalTaskCreate]:
        return value


class TaskDecompositionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_generated_tasks: int = Field(default=3, ge=1, le=3)


class GoalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    title: str
    description: str | None
    status: str
    commitment_due_at: datetime | None
    created_at: datetime
    updated_at: datetime
    tasks: list[TaskResponse] = Field(default_factory=list)


class ExecutorCapacityProfileResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str
    team_name: str
    actor: str
    capacity_units: int
    load_units: int
    source: str
    created_at: datetime
    updated_at: datetime


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_text: str = Field(min_length=3, max_length=2000)
    project_id: str | None = None
    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")
    idempotency_key: str | None = None
    dry_run: bool = False
    validate_integration: bool = False
    execute_integration: bool = False


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


class ApprovalDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")
    approved: bool
    decision_text: str | None = Field(default=None, max_length=1000)


class ApprovalDecisionResponse(BaseModel):
    audit_id: str
    approval_id: str
    status: str
    reviewer_actor: str | None = None
    reviewer_role: str | None = None
    decision_text: str | None = None


class ApprovalResumeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")


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
    detail: str
    created_at: datetime


class AuditEventDetailResponse(AuditResponse):
    payload: dict[str, Any] = Field(default_factory=dict)


class ProjectRuntimeTaskCounts(BaseModel):
    total: int
    open: int


class ProjectRuntimeApprovalSummary(BaseModel):
    pending: int


class ProjectRuntimeExecutionSummary(BaseModel):
    pending: int
    failed: int
    last_failure_at: datetime | None = None


class ProjectRuntimeStatusResponse(BaseModel):
    project_id: str
    safe_paused: bool
    active_goal_count: int
    task_counts: ProjectRuntimeTaskCounts
    approvals: ProjectRuntimeApprovalSummary
    executions: ProjectRuntimeExecutionSummary
    revision_at: datetime
    generated_at: datetime


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
    execute_publish: bool = False
    idempotency_key: str | None = Field(default=None, max_length=120)


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
    source: str | None = None
    audit_id: str | None = None
    due_action_id: str | None = None
    action: str | None = None
    status: str | None = None
    trace_label: str | None = None
    summary: str
    lineage: str | None = None


class ProjectReportDecision(BaseModel):
    audit_id: str
    action: str
    result: str
    summary: str
    reason: str | None = None
    mode: str | None = None
    trace_label: str | None = None
    lineage: str | None = None


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


class DueActionClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    gateway: str = Field(min_length=2, max_length=120)
    limit: int = Field(default=10, ge=1, le=100)


class DueActionDeliveryUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str = Field(pattern="^(delivered|failed|acked)$")
    claim_token: str = Field(min_length=8, max_length=255)
    external_delivery_id: str | None = Field(default=None, max_length=255)
    error_detail: str | None = Field(default=None, max_length=4000)


class DueActionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    project_id: str | None
    channel: str
    recipient: str
    kind: str
    payload_json: dict[str, Any] = Field(default_factory=dict)
    due_at: datetime
    status: str
    actor: str | None = None
    idempotency_key: str | None = None
    claim_token: str | None = None
    claimed_by: str | None = None
    claimed_at: datetime | None = None
    delivery_attempted_at: datetime | None = None
    delivered_at: datetime | None = None
    failed_at: datetime | None = None
    acked_at: datetime | None = None
    external_delivery_id: str | None = None
    last_error: str | None = None
    created_at: datetime
    updated_at: datetime


class DueActionClaimResponse(BaseModel):
    items: list[DueActionResponse] = Field(default_factory=list)


class InboundEventIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    platform: str = Field(min_length=2, max_length=80)
    chat_id: str | None = Field(default=None, max_length=255)
    thread_id: str | None = Field(default=None, max_length=255)
    actor: str = Field(min_length=2, max_length=120)
    actor_role: str | None = Field(default=None, max_length=80)
    project_id: str | None = None
    text: str = Field(min_length=1, max_length=4000)
    normalized_intent: str | None = Field(default=None, max_length=120)
    due_action_id: str | None = None
    pending_audit_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class InboundEventDispositionResponse(BaseModel):
    event_id: str
    disposition: str
    reason: str
    due_action_id: str | None = None
    pending_audit_id: str | None = None
    project_id: str | None = None
