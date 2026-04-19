from __future__ import annotations

import re
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .models import Base


_ENGINE_OPTIONS = {"echo": False, "future": True}

_engine = create_engine(settings.database_url, **_ENGINE_OPTIONS)
SessionLocal = sessionmaker(bind=_engine, class_=Session, autocommit=False, autoflush=False, future=True)
_ACTIVE_GOAL_INDEX_NAME = "uq_goals_project_active"
_ACTIVE_GOAL_PARTIAL_INDEX_DIALECTS = frozenset({"sqlite", "postgresql"})
_ACTIVE_GOAL_INDEX_PREDICATE = "lower(trim(status)) = 'active'"
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


def _initialize_engine(database_url: str) -> None:
    """(Re)initialize the SQLAlchemy engine and session factory for tests/runtime."""

    global _engine
    global SessionLocal

    _engine.dispose()
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


def init_db(database_url: str | None = None) -> None:
    """Initialize or reinitialize DB schema for a concrete database URL."""

    if database_url is not None and database_url != settings.database_url:
        _initialize_engine(database_url)
    assert_active_goal_uniqueness_dialect_supported()
    _upgrade_legacy_schema()
    Base.metadata.create_all(bind=_engine)


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
