from __future__ import annotations

import re
from copy import deepcopy
from typing import Iterator

from datetime import datetime

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .models import Base, PolicyVersion
from .policy import DEFAULT_POLICY_RULES


_ENGINE_OPTIONS = {"echo": False, "future": True}

_engine = create_engine(settings.database_url, **_ENGINE_OPTIONS)
SessionLocal = sessionmaker(bind=_engine, class_=Session, autocommit=False, autoflush=False, future=True)
_ACTIVE_GOAL_INDEX_NAME = "uq_goals_project_active"
_ACTIVE_GOAL_PARTIAL_INDEX_DIALECTS = frozenset({"sqlite", "postgresql"})
_ACTIVE_GOAL_INDEX_PREDICATE = "lower(trim(status)) = 'active'"
_POLICY_VERSION_ACTIVE_INDEX_NAME = "uq_policy_versions_active_key"
_POLICY_VERSION_PARTIAL_INDEX_DIALECTS = frozenset({"sqlite", "postgresql"})
_POLICY_VERSION_ACTIVE_INDEX_PREDICATE = {
    "sqlite": "is_active = 1",
    "postgresql": "is_active = true",
}
_CONVERSATION_EVENT_CORRELATION_INDEX_NAME = "ix_conversation_events_correlation_key"
_CONVERSATION_EVENT_SOURCE_EVENT_INDEX_NAME = "uq_conversation_events_source_event_key"


def _compact_sql(sql: str) -> str:
    return re.sub(r"\s+", "", sql.lower())


def assert_active_goal_uniqueness_dialect_supported(dialect_name: str | None = None) -> None:
    """Guard against dialects that cannot model partial unique indexes reliably."""

    dialect = dialect_name or _engine.dialect.name
    if dialect not in _ACTIVE_GOAL_PARTIAL_INDEX_DIALECTS:
        supported = ", ".join(sorted(_ACTIVE_GOAL_PARTIAL_INDEX_DIALECTS))
        raise RuntimeError(
            f"active-goal uniqueness is only supported on dialects: {supported}"
            f"; {dialect} does not support required partial unique index semantics"
        )


def assert_active_policy_version_dialect_supported(dialect_name: str | None = None) -> None:
    dialect = dialect_name or _engine.dialect.name
    if dialect not in _POLICY_VERSION_PARTIAL_INDEX_DIALECTS:
        supported = ", ".join(sorted(_POLICY_VERSION_PARTIAL_INDEX_DIALECTS))
        raise RuntimeError(
            f"active policy-version uniqueness is only supported on dialects: {supported}"
            f"; {dialect} does not support required partial unique index semantics"
        )


def _initialize_engine(database_url: str) -> None:
    """(Re)initialize the SQLAlchemy engine and session factory for tests/runtime."""

    global _engine
    global SessionLocal

    _engine = create_engine(database_url, **_ENGINE_OPTIONS)
    SessionLocal.configure(bind=_engine)


def _has_index(inspector, table_name: str, index_name: str) -> bool:
    indexes = inspector.get_indexes(table_name)
    return any(index["name"] == index_name for index in indexes)


def _active_goal_index_definition() -> str | None:
    if _engine.dialect.name == "sqlite":
        with _engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name = :index_name AND tbl_name='goals'"
                ),
                {"index_name": _ACTIVE_GOAL_INDEX_NAME},
            ).first()
        if row is None:
            return None
        return row[0]

    if _engine.dialect.name == "postgresql":
        with _engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT indexdef FROM pg_indexes WHERE tablename='goals' AND indexname=:index_name"
                ),
                {"index_name": _ACTIVE_GOAL_INDEX_NAME},
            ).first()
        if row is None:
            return None
        return row[0]

    return None


def _active_goal_index_has_expected_shape() -> bool:
    definition = _active_goal_index_definition()
    if definition is None:
        return False

    compact = _compact_sql(definition)
    if "createuniqueindex" not in compact:
        return False

    if not re.search(r"on(?:\"?[a-z0-9_]+\"?\.)?\"?goals\"?\([^\)]*project_id[^\)]*\)", compact):
        return False

    if not re.search(r"where[^)]*lower\(trim\((?:\"?status\"?)\)\)=\'active\'", compact):
        return False

    return True


def _assert_active_goal_index_shape() -> None:
    definition = _active_goal_index_definition()
    if definition is None:
        return
    if _active_goal_index_has_expected_shape():
        return

    raise RuntimeError(
        f"active-goal unique index {_ACTIVE_GOAL_INDEX_NAME} has unexpected shape; "
        f"definition={definition}; expected unique predicate `{_ACTIVE_GOAL_INDEX_PREDICATE}`"
    )


def _policy_version_active_index_definition() -> str | None:
    if _engine.dialect.name == "sqlite":
        with _engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT sql FROM sqlite_master WHERE type='index' AND name = :index_name AND tbl_name='policy_versions'"
                ),
                {"index_name": _POLICY_VERSION_ACTIVE_INDEX_NAME},
            ).first()
        if row is None:
            return None
        return row[0]

    if _engine.dialect.name == "postgresql":
        with _engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT indexdef FROM pg_indexes WHERE tablename='policy_versions' AND indexname=:index_name"
                ),
                {"index_name": _POLICY_VERSION_ACTIVE_INDEX_NAME},
            ).first()
        if row is None:
            return None
        return row[0]

    return None


def _policy_version_active_index_has_expected_shape() -> bool:
    definition = _policy_version_active_index_definition()
    if definition is None:
        return False

    compact = _compact_sql(definition)
    if "createuniqueindex" not in compact:
        return False

    if not re.search(
        r"on(?:\"?[a-z0-9_]+\"?\.)?\"?policy_versions\"?\([^\)]*policy_key[^\)]*\)",
        compact,
    ):
        return False

    expected_predicate = _compact_sql(_POLICY_VERSION_ACTIVE_INDEX_PREDICATE[_engine.dialect.name])
    return expected_predicate in compact


def _assert_policy_version_active_index_shape() -> None:
    definition = _policy_version_active_index_definition()
    if definition is None:
        return
    if _policy_version_active_index_has_expected_shape():
        return

    raise RuntimeError(
        f"policy-version active unique index {_POLICY_VERSION_ACTIVE_INDEX_NAME} has unexpected shape; "
        f"definition={definition}; expected predicate `{_POLICY_VERSION_ACTIVE_INDEX_PREDICATE[_engine.dialect.name]}`"
    )


def _legacy_active_policy_version_duplicates() -> list[tuple[str, int]]:
    predicate = _POLICY_VERSION_ACTIVE_INDEX_PREDICATE[_engine.dialect.name]
    rows = []
    with _engine.connect() as connection:
        results = connection.execute(
            text(
                f"""
                SELECT policy_key, COUNT(*) AS active_policy_count
                FROM policy_versions
                WHERE {predicate}
                GROUP BY policy_key
                HAVING COUNT(*) > 1
                """
            )
        ).mappings().all()
    for row in results:
        rows.append((row["policy_key"], row["active_policy_count"]))
    return rows


def _assert_no_legacy_active_policy_version_duplicates() -> None:
    duplicates = _legacy_active_policy_version_duplicates()
    if not duplicates:
        return

    duplicates_summary = ", ".join(f"{policy_key} ({count})" for policy_key, count in duplicates)
    raise RuntimeError(
        "duplicate active policy versions detected for legacy migration: "
        f"unique active policy constraint would be violated for {duplicates_summary}"
    )


def _legacy_active_goal_duplicates() -> list[tuple[str, int]]:
    rows = []
    with _engine.connect() as connection:
        results = connection.execute(
            text(
                """
                SELECT project_id, COUNT(*) AS active_goal_count
                FROM goals
                WHERE lower(trim(status)) = 'active'
                GROUP BY project_id
                HAVING COUNT(*) > 1
                """
            )
        ).mappings().all()
    for row in results:
        rows.append((row["project_id"], row["active_goal_count"]))
    return rows


def _assert_no_legacy_active_goal_duplicates() -> None:
    duplicates = _legacy_active_goal_duplicates()
    if not duplicates:
        return

    duplicates_summary = ", ".join(
        f"{project_id} ({count})" for project_id, count in duplicates
    )
    raise RuntimeError(
        "duplicate active goals detected for legacy migration: "
        f"active goal constraint would be violated for {duplicates_summary}"
    )


def _legacy_rollback_record_duplicates() -> list[tuple[str, int]]:
    rows = []
    with _engine.connect() as connection:
        results = connection.execute(
            text(
                """
                SELECT audit_event_id, COUNT(*) AS rollback_count
                FROM rollback_records
                GROUP BY audit_event_id
                HAVING COUNT(*) > 1
                """
            )
        ).mappings().all()
    for row in results:
        rows.append((row["audit_event_id"], row["rollback_count"]))
    return rows


def _assert_no_legacy_rollback_record_duplicates() -> None:
    duplicates = _legacy_rollback_record_duplicates()
    if not duplicates:
        return

    duplicates_summary = ", ".join(
        f"{audit_event_id} ({count})" for audit_event_id, count in duplicates
    )
    raise RuntimeError(
        "duplicate rollback records detected for legacy migration: "
        f"unique rollback constraint would be violated for {duplicates_summary}"
    )


def _upgrade_legacy_schema() -> None:
    inspector = inspect(_engine)

    if "projects" in inspector.get_table_names():
        project_columns = {column["name"] for column in inspector.get_columns("projects")}
        if "timezone" not in project_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE projects ADD COLUMN timezone VARCHAR(120)"))
        if "commitment_due_at" not in project_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE projects ADD COLUMN commitment_due_at DATETIME"))

    if "tasks" in inspector.get_table_names():
        task_columns = {column["name"] for column in inspector.get_columns("tasks")}
        if "goal_id" not in task_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE tasks ADD COLUMN goal_id VARCHAR"))
                if _engine.dialect.name == "sqlite":
                    connection.execute(text("CREATE INDEX IF NOT EXISTS ix_tasks_goal_id ON tasks (goal_id)"))
        if "last_progress_at" not in task_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE tasks ADD COLUMN last_progress_at DATETIME"))

    if "goals" in inspector.get_table_names():
        goal_columns = {column["name"] for column in inspector.get_columns("goals")}
        if "commitment_due_at" not in goal_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE goals ADD COLUMN commitment_due_at DATETIME"))
        if _has_index(inspector, "goals", _ACTIVE_GOAL_INDEX_NAME):
            _assert_active_goal_index_shape()
        else:
            _assert_no_legacy_active_goal_duplicates()
            with _engine.begin() as connection:
                if _engine.dialect.name in {"sqlite", "postgresql"}:
                    connection.execute(
                        text(
                            f"CREATE UNIQUE INDEX IF NOT EXISTS {_ACTIVE_GOAL_INDEX_NAME} "
                            "ON goals (project_id) WHERE lower(trim(status)) = 'active'"
                        )
                    )

    if "policy_versions" in inspector.get_table_names():
        if _has_index(inspector, "policy_versions", _POLICY_VERSION_ACTIVE_INDEX_NAME):
            _assert_policy_version_active_index_shape()
        else:
            _assert_no_legacy_active_policy_version_duplicates()
            with _engine.begin() as connection:
                predicate = _POLICY_VERSION_ACTIVE_INDEX_PREDICATE[_engine.dialect.name]
                connection.execute(
                    text(
                        f"CREATE UNIQUE INDEX IF NOT EXISTS {_POLICY_VERSION_ACTIVE_INDEX_NAME} "
                        f"ON policy_versions (policy_key) WHERE {predicate}"
                    )
                )

    if "conversation_events" in inspector.get_table_names():
        conversation_event_columns = {column["name"] for column in inspector.get_columns("conversation_events")}
        if "source_event_key" not in conversation_event_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE conversation_events ADD COLUMN source_event_key VARCHAR(255)"))
        if "correlation_key" not in conversation_event_columns:
            with _engine.begin() as connection:
                connection.execute(text("ALTER TABLE conversation_events ADD COLUMN correlation_key VARCHAR(255)"))
        with _engine.begin() as connection:
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {_CONVERSATION_EVENT_SOURCE_EVENT_INDEX_NAME} "
                    "ON conversation_events (source_event_key)"
                )
            )
            connection.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS {_CONVERSATION_EVENT_CORRELATION_INDEX_NAME} "
                    "ON conversation_events (correlation_key)"
                )
            )

    if "rollback_records" in inspector.get_table_names():
        rollback_columns = {column["name"] for column in inspector.get_columns("rollback_records")}
        _assert_no_legacy_rollback_record_duplicates()
        with _engine.begin() as connection:
            if "rollback_root_audit_event_id" not in rollback_columns:
                connection.execute(text("ALTER TABLE rollback_records ADD COLUMN rollback_root_audit_event_id VARCHAR"))
            if "plan" not in rollback_columns:
                connection.execute(text("ALTER TABLE rollback_records ADD COLUMN plan JSON"))
            if "verification_detail" not in rollback_columns:
                connection.execute(
                    text("ALTER TABLE rollback_records ADD COLUMN verification_detail TEXT NOT NULL DEFAULT ''")
                )
            if "remediation_detail" not in rollback_columns:
                connection.execute(
                    text("ALTER TABLE rollback_records ADD COLUMN remediation_detail TEXT NOT NULL DEFAULT ''")
                )
            connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_rollback_records_audit_event_id "
                    "ON rollback_records (audit_event_id)"
                )
            )
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_rollback_records_rollback_root_audit_event_id "
                    "ON rollback_records (rollback_root_audit_event_id)"
                )
            )


def _seed_default_policy_version() -> None:
    with SessionLocal() as session:
        default_rows = session.query(PolicyVersion).filter_by(policy_key="default").order_by(PolicyVersion.version.asc()).all()
        active_rows = [row for row in default_rows if row.is_active]
        if len(active_rows) > 1:
            raise RuntimeError("multiple active policy versions detected for default policy")
        if active_rows:
            return
        if default_rows:
            raise RuntimeError("default policy rows exist but no active default policy version is present")

        other_rows_exist = session.query(PolicyVersion.id).first() is not None
        if other_rows_exist:
            raise RuntimeError("policy_versions table is populated but the default active policy row is missing")

        session.add(
            PolicyVersion(
                policy_key="default",
                version=1,
                description="Default deterministic MVP policy",
                rules_json=deepcopy(DEFAULT_POLICY_RULES),
                is_active=True,
                activated_at=datetime.utcnow(),
            )
        )
        session.commit()


def init_db(database_url: str | None = None) -> None:
    """Initialize or reinitialize DB schema for a concrete database URL."""

    if database_url is not None and database_url != settings.database_url:
        _initialize_engine(database_url)
    assert_active_goal_uniqueness_dialect_supported()
    assert_active_policy_version_dialect_supported()
    _upgrade_legacy_schema()
    Base.metadata.create_all(bind=_engine)
    _seed_default_policy_version()


def get_db_session() -> Iterator[Session]:
    """FastAPI dependency: yields a SQLAlchemy session with transaction handling."""

    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class Database:
    """Compatibility wrapper for non-HTTP usage paths."""

    def __init__(self, session_factory: sessionmaker | None = None) -> None:
        self.session_factory = session_factory or SessionLocal

    def session(self) -> Session:
        return self.session_factory()
