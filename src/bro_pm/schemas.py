from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ProjectCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=3, max_length=120)
    slug: str = Field(min_length=3, max_length=120)
    description: str | None = None
    created_by: str | None = None
    visibility: str = "internal"
    safe_paused: bool = False
    metadata: dict[str, Any] | None = None


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
    title: str
    description: str | None
    status: str
    assignee: str | None
    priority: str
    policy_flags: list[str] | None = None
    due_at: datetime | None
    created_at: datetime
    updated_at: datetime


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_text: str = Field(min_length=3, max_length=2000)
    project_id: str | None = None
    actor: str = Field(min_length=2, max_length=120)
    role: str = Field(pattern="^(owner|admin|operator|viewer)$")
    idempotency_key: str | None = None


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


class RollbackResponse(BaseModel):
    accepted: bool
    result: str
    action: str
    target: str | None = None
    detail: str
    audit_id: str
    rollback_record_id: str
