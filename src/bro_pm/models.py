from __future__ import annotations

from datetime import datetime
import uuid

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, text
from sqlalchemy import Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), unique=True)
    slug: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(120), nullable=True)
    commitment_due_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    safe_paused: Mapped[bool] = mapped_column(Boolean, default=False)
    created_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    visibility: Mapped[str] = mapped_column(String(40), default="internal")
    metadata_json: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    tasks: Mapped[list["Task"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    goals: Mapped[list["Goal"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    memberships: Mapped[list["ProjectMembership"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    executor_capacity_profiles: Mapped[list["ExecutorCapacityProfile"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )
    tracker_credentials: Mapped[list["TrackerCredential"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )
    due_actions: Mapped[list["DueAction"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    conversation_events: Mapped[list["ConversationEvent"]] = relationship(
        back_populates="project",
        cascade="all, delete-orphan",
    )


class ProjectMembership(Base):
    __tablename__ = "project_memberships"
    __table_args__ = (
        UniqueConstraint("project_id", "actor", name="uq_project_memberships_project_actor"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String, ForeignKey("projects.id"), index=True)
    actor: Mapped[str] = mapped_column(String(120))
    role: Mapped[str] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    project: Mapped[Project] = relationship(back_populates="memberships")


class Goal(Base):
    __tablename__ = "goals"
    __table_args__ = (
        Index(
            "uq_goals_project_active",
            "project_id",
            unique=True,
            sqlite_where=text("lower(trim(status)) = 'active'"),
            postgresql_where=text("lower(trim(status)) = 'active'"),
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String, ForeignKey("projects.id"), index=True)
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="draft")
    commitment_due_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    project: Mapped[Project] = relationship(back_populates="goals")
    tasks: Mapped[list["Task"]] = relationship(back_populates="goal", cascade="all, delete-orphan")


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String, ForeignKey("projects.id"), index=True)
    goal_id: Mapped[str | None] = mapped_column(String, ForeignKey("goals.id"), nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(30), default="todo")
    assignee: Mapped[str | None] = mapped_column(String(120), nullable=True)
    priority: Mapped[str] = mapped_column(String(30), default="medium")
    policy_flags: Mapped[list[str] | None] = mapped_column("policy_flags", JSON, nullable=True)
    due_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_progress_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    project: Mapped[Project] = relationship(back_populates="tasks")
    goal: Mapped[Goal | None] = relationship(back_populates="tasks")


class ExecutorCapacityProfile(Base):
    __tablename__ = "executor_capacity_profiles"
    __table_args__ = (
        UniqueConstraint("project_id", "actor", "team_name", name="uq_executor_capacity_profiles_project_actor_team"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String, ForeignKey("projects.id"), index=True)
    team_name: Mapped[str] = mapped_column(String(120))
    actor: Mapped[str] = mapped_column(String(120), index=True)
    capacity_units: Mapped[int] = mapped_column(Integer, default=0)
    load_units: Mapped[int] = mapped_column(Integer, default=0)
    source: Mapped[str] = mapped_column(String(40), default="manual")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    project: Mapped[Project] = relationship(back_populates="executor_capacity_profiles")


class TrackerCredential(Base):
    __tablename__ = "tracker_credentials"
    __table_args__ = (
        UniqueConstraint("project_id", "provider", name="uq_tracker_credentials_project_provider"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str] = mapped_column(String, ForeignKey("projects.id"), index=True)
    provider: Mapped[str] = mapped_column(String(80), index=True)
    config_json: Mapped[dict] = mapped_column("config", JSON, default=dict)
    secret_json: Mapped[dict] = mapped_column("secrets", JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    project: Mapped[Project] = relationship(back_populates="tracker_credentials")


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_audit_events_idempotency_key"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str | None] = mapped_column(String, ForeignKey("projects.id"), nullable=True)
    actor: Mapped[str] = mapped_column(String(120))
    action: Mapped[str] = mapped_column(String(120))
    target_type: Mapped[str] = mapped_column(String(50))
    target_id: Mapped[str | None] = mapped_column(String)
    payload: Mapped[str] = mapped_column(Text)
    result: Mapped[str] = mapped_column(String(40), default="pending")
    idempotency_key: Mapped[str | None] = mapped_column(String(120), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    action_execution: Mapped["ActionExecution | None"] = relationship(
        back_populates="audit_event",
        cascade="all, delete-orphan",
        uselist=False,
    )


class ActionExecution(Base):
    __tablename__ = "action_executions"
    __table_args__ = (
        UniqueConstraint("audit_event_id", name="uq_action_executions_audit_event_id"),
        Index("ix_action_executions_project_status", "project_id", "status"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    audit_event_id: Mapped[str] = mapped_column(String, ForeignKey("audit_events.id"), index=True)
    project_id: Mapped[str | None] = mapped_column(String, ForeignKey("projects.id"), nullable=True, index=True)
    actor: Mapped[str] = mapped_column(String(120))
    action: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(40), default="requested")
    requested_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    awaiting_approval_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    audit_event: Mapped[AuditEvent] = relationship(back_populates="action_execution")
    project: Mapped[Project | None] = relationship()


class RollbackRecord(Base):
    __tablename__ = "rollback_records"

    __table_args__ = (
        UniqueConstraint("audit_event_id", name="uq_rollback_records_audit_event_id"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    audit_event_id: Mapped[str] = mapped_column(String, ForeignKey("audit_events.id"))
    actor: Mapped[str] = mapped_column(String(120))
    reason: Mapped[str] = mapped_column(Text)
    executed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class PolicyRule(Base):
    __tablename__ = "policy_rules"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(120), unique=True)
    scope: Mapped[str] = mapped_column(String(60), default="global")
    role_required: Mapped[str] = mapped_column(String(60), default="operator")
    allow_when_safe_paused: Mapped[bool] = mapped_column(Boolean, default=False)
    deny_when_untrusted_actor: Mapped[bool] = mapped_column(Boolean, default=True)


class DueAction(Base):
    __tablename__ = "due_actions"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_due_actions_idempotency_key"),
        Index("ix_due_actions_status_due_at", "status", "due_at"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str | None] = mapped_column(String, ForeignKey("projects.id"), nullable=True, index=True)
    channel: Mapped[str] = mapped_column(String(80))
    recipient: Mapped[str] = mapped_column(String(255))
    kind: Mapped[str] = mapped_column(String(80))
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict)
    due_at: Mapped[datetime] = mapped_column(DateTime, index=True)
    status: Mapped[str] = mapped_column(String(40), default="pending")
    actor: Mapped[str | None] = mapped_column(String(120), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    claim_token: Mapped[str | None] = mapped_column(String(255), nullable=True)
    claimed_by: Mapped[str | None] = mapped_column(String(120), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    delivery_attempted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    acked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    external_delivery_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    project: Mapped[Project | None] = relationship(back_populates="due_actions")


class ConversationEvent(Base):
    __tablename__ = "conversation_events"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id: Mapped[str | None] = mapped_column(String, ForeignKey("projects.id"), nullable=True, index=True)
    due_action_id: Mapped[str | None] = mapped_column(String, ForeignKey("due_actions.id"), nullable=True, index=True)
    pending_audit_id: Mapped[str | None] = mapped_column(String, ForeignKey("audit_events.id"), nullable=True, index=True)
    platform: Mapped[str] = mapped_column(String(80))
    chat_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    thread_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actor: Mapped[str] = mapped_column(String(120))
    actor_role: Mapped[str | None] = mapped_column(String(80), nullable=True)
    text: Mapped[str] = mapped_column(Text)
    normalized_intent: Mapped[str | None] = mapped_column(String(120), nullable=True)
    metadata_json: Mapped[dict] = mapped_column("metadata", JSON, default=dict)
    disposition: Mapped[str] = mapped_column(String(80), default="ignore")
    decision_reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    project: Mapped[Project | None] = relationship(back_populates="conversation_events")
