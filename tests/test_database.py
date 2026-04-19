from __future__ import annotations

import importlib
import sys

import pytest

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.schema import CreateIndex

from bro_pm import database
from bro_pm import models


def _create_legacy_schema(db_url: str) -> None:
    legacy_engine = create_engine(db_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    slug TEXT NOT NULL UNIQUE,
                    description TEXT,
                    safe_paused INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    visibility TEXT NOT NULL DEFAULT 'internal',
                    "metadata" TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE goals (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'draft',
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE tasks (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'todo',
                    assignee TEXT,
                    priority TEXT NOT NULL DEFAULT 'medium',
                    policy_flags TEXT,
                    due_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE audit_events (
                    id TEXT PRIMARY KEY,
                    project_id TEXT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    target_id TEXT,
                    payload TEXT NOT NULL,
                    result TEXT NOT NULL DEFAULT 'pending',
                    idempotency_key TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (project_id) REFERENCES projects(id)
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE rollback_records (
                    id TEXT PRIMARY KEY,
                    audit_event_id TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    executed INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (audit_event_id) REFERENCES audit_events(id)
                )
                """,
            ),
        )
        connection.execute(
            text(
                """
                CREATE TABLE policy_rules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    scope TEXT NOT NULL DEFAULT 'global',
                    role_required TEXT NOT NULL DEFAULT 'operator',
                    allow_when_safe_paused INTEGER NOT NULL DEFAULT 0,
                    deny_when_untrusted_actor INTEGER NOT NULL DEFAULT 1
                )
                """,
            ),
        )


def test_database_module_import_and_init_db_with_memory_url(monkeypatch):
    # Ensure module uses an isolated in-memory DB during the import path under test.
    monkeypatch.setenv("BRO_PM_DATABASE_URL", "sqlite:///:memory:")
    sys.modules.pop("bro_pm.database", None)

    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")

    inspector = inspect(database._engine)
    tables = set(inspector.get_table_names())

    assert "projects" in tables
    assert "tasks" in tables
    assert "audit_events" in tables
    assert "action_executions" in tables
    assert "execution_outbox" in tables
    assert "policy_versions" in tables

    db_session = database.SessionLocal()
    try:
        active_policy = db_session.query(models.PolicyVersion).filter_by(is_active=True).one()
        assert active_policy.policy_key == "default"
        assert active_policy.version == 1
        assert active_policy.rules_json["role_order"] == ["viewer", "operator", "admin", "owner"]
    finally:
        db_session.close()


def test_policy_version_active_unique_index_compiles_for_sqlite_and_postgresql():
    index = next(index for index in models.PolicyVersion.__table__.indexes if index.name == "uq_policy_versions_active_key")

    sqlite_sql = str(CreateIndex(index).compile(dialect=sqlite.dialect())).lower()
    postgres_sql = str(CreateIndex(index).compile(dialect=postgresql.dialect())).lower()

    assert "create unique index uq_policy_versions_active_key" in sqlite_sql
    assert "where is_active = 1" in sqlite_sql
    assert "create unique index uq_policy_versions_active_key" in postgres_sql
    assert "where is_active = true" in postgres_sql


def test_policy_versions_allow_only_one_active_row_per_policy_key(monkeypatch):
    monkeypatch.setenv("BRO_PM_DATABASE_URL", "sqlite:///:memory:")
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")

    db_session = database.SessionLocal()
    try:
        db_session.add(
            models.PolicyVersion(
                policy_key="default",
                version=2,
                description="conflicting active policy",
                rules_json={"role_order": ["viewer", "operator", "admin", "owner"]},
                is_active=True,
            )
        )
        with pytest.raises(IntegrityError):
            db_session.commit()
    finally:
        db_session.rollback()
        db_session.close()


def test_init_db_seeds_active_default_stalled_task_heuristic(monkeypatch):
    monkeypatch.setenv("BRO_PM_DATABASE_URL", "sqlite:///:memory:")
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db("sqlite:///:memory:")

    inspector = inspect(database._engine)
    tables = set(inspector.get_table_names())
    assert "heuristic_versions" in tables

    db_session = database.SessionLocal()
    try:
        active_heuristic = (
            db_session.query(models.HeuristicVersion)
            .filter_by(heuristic_key="stalled_task", is_active=True)
            .one()
        )
        assert active_heuristic.version == 1
        assert active_heuristic.family == "decision_timer"
        assert active_heuristic.config_json == {"lookback_hours": 48}
    finally:
        db_session.close()



def test_init_db_adds_missing_heuristic_key_version_uniqueness_to_legacy_table(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'legacy_heuristic_versions.db'}"
    legacy_engine = create_engine(db_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE heuristic_versions (
                    id TEXT PRIMARY KEY,
                    family TEXT NOT NULL,
                    heuristic_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    config JSON NOT NULL DEFAULT '{}',
                    is_active INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    activated_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO heuristic_versions (
                    id,
                    family,
                    heuristic_key,
                    version,
                    description,
                    config,
                    is_active,
                    activated_at
                ) VALUES (
                    'heuristic-1',
                    'decision_timer',
                    'stalled_task',
                    1,
                    'legacy default stalled-task heuristic',
                    '{"lookback_hours": 48}',
                    1,
                    CURRENT_TIMESTAMP
                )
                """
            )
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)

    db_session = database.SessionLocal()
    try:
        db_session.add(
            models.HeuristicVersion(
                id="heuristic-duplicate",
                family="decision_timer",
                heuristic_key="stalled_task",
                version=1,
                description="duplicate version should be rejected",
                config_json={"lookback_hours": 24},
                is_active=False,
            )
        )
        with pytest.raises(IntegrityError):
            db_session.commit()
    finally:
        db_session.rollback()
        db_session.close()



def test_init_db_fails_closed_when_active_stalled_task_heuristic_config_is_invalid(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'invalid_stalled_task_heuristic.db'}"
    legacy_engine = create_engine(db_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE heuristic_versions (
                    id TEXT PRIMARY KEY,
                    family TEXT NOT NULL,
                    heuristic_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    config JSON NOT NULL DEFAULT '{}',
                    is_active INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    activated_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO heuristic_versions (
                    id,
                    family,
                    heuristic_key,
                    version,
                    description,
                    config,
                    is_active,
                    activated_at
                ) VALUES (
                    'heuristic-invalid',
                    'decision_timer',
                    'stalled_task',
                    1,
                    'legacy stalled-task heuristic missing lookback',
                    '{}',
                    1,
                    CURRENT_TIMESTAMP
                )
                """
            )
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="positive integer lookback_hours"):
        database.init_db(db_url)



def test_init_db_fails_closed_when_heuristic_key_version_uniqueness_has_wrong_shape(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'wrong_shape_heuristic_uniqueness.db'}"
    legacy_engine = create_engine(db_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE heuristic_versions (
                    id TEXT PRIMARY KEY,
                    family TEXT NOT NULL,
                    heuristic_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    config JSON NOT NULL DEFAULT '{}',
                    is_active INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    activated_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE UNIQUE INDEX uq_heuristic_versions_key_version
                ON heuristic_versions (family, version)
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO heuristic_versions (
                    id,
                    family,
                    heuristic_key,
                    version,
                    description,
                    config,
                    is_active,
                    activated_at
                ) VALUES (
                    'heuristic-1',
                    'decision_timer',
                    'stalled_task',
                    1,
                    'legacy default stalled-task heuristic',
                    '{"lookback_hours": 48}',
                    1,
                    CURRENT_TIMESTAMP
                )
                """
            )
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="unexpected shape"):
        database.init_db(db_url)



def test_init_db_fails_closed_when_default_policy_rows_exist_but_none_is_active(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'inactive_default_policy.db'}"
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(db_url)

    db_session = database.SessionLocal()
    try:
        active_policy = db_session.query(models.PolicyVersion).filter_by(policy_key="default", is_active=True).one()
        active_policy.is_active = False
        db_session.commit()
    finally:
        db_session.close()

    with pytest.raises(RuntimeError, match="no active default policy version"):
        database.init_db(db_url)


def test_init_db_adds_goal_id_to_legacy_tasks_schema(tmp_path):
    """Legacy databases without tasks.goal_id should be migrated safely."""

    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_tasks_goal_id.db'}"

    _create_legacy_schema(legacy_db_url)
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(legacy_db_url)

    inspector = inspect(database._engine)
    task_columns = {column["name"] for column in inspector.get_columns("tasks")}
    assert "goal_id" in task_columns

    db_session = database.SessionLocal()
    try:
        project = models.Project(
            id="project-legacy-1",
            name="Legacy Project",
            slug="legacy-project",
            visibility="internal",
        )
        db_session.add(project)
        db_session.flush()

        goal = models.Goal(
            project_id=project.id,
            title="Legacy goal",
            status="active",
        )
        db_session.add(goal)
        db_session.flush()

        task = models.Task(
            project_id=project.id,
            goal_id=goal.id,
            title="Goal task",
            status="todo",
        )
        db_session.add(task)
        db_session.flush()

        assert task.id
        assert task.goal_id == goal.id
    finally:
        db_session.rollback()
        db_session.close()


def test_init_db_upgrades_legacy_schema_for_autonomy_state(tmp_path):
    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_autonomy_state.db'}"

    _create_legacy_schema(legacy_db_url)
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(legacy_db_url)

    inspector = inspect(database._engine)
    project_columns = {column["name"] for column in inspector.get_columns("projects")}
    goal_columns = {column["name"] for column in inspector.get_columns("goals")}
    task_columns = {column["name"] for column in inspector.get_columns("tasks")}
    tables = set(inspector.get_table_names())

    assert "commitment_due_at" in project_columns
    assert "commitment_due_at" in goal_columns
    assert "last_progress_at" in task_columns
    assert "executor_capacity_profiles" in tables
    assert "action_executions" in tables


def test_init_db_upgrades_legacy_rollback_records_schema_for_dependency_plans(tmp_path):
    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_rollback_records.db'}"

    _create_legacy_schema(legacy_db_url)
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(legacy_db_url)

    inspector = inspect(database._engine)
    rollback_columns = {column["name"] for column in inspector.get_columns("rollback_records")}

    assert "rollback_root_audit_event_id" in rollback_columns
    assert "plan" in rollback_columns
    assert "verification_detail" in rollback_columns
    assert "remediation_detail" in rollback_columns


def test_init_db_upgrades_legacy_rollback_records_schema_preserves_unique_audit_event_id(tmp_path):
    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_rollback_records_unique.db'}"

    _create_legacy_schema(legacy_db_url)
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(legacy_db_url)

    db_session = database.SessionLocal()
    try:
        project = models.Project(
            id="project-legacy-rollback-1",
            name="Legacy rollback project",
            slug="legacy-rollback-project",
            visibility="internal",
        )
        db_session.add(project)
        db_session.flush()

        audit = models.AuditEvent(
            project_id=project.id,
            actor="alice",
            action="pause_project",
            target_type="project",
            target_id=project.id,
            payload="{}",
            result="executed",
        )
        db_session.add(audit)
        db_session.flush()

        db_session.add(
            models.RollbackRecord(
                audit_event_id=audit.id,
                rollback_root_audit_event_id=audit.id,
                actor="alice",
                reason="first rollback",
                plan_json={},
                verification_detail="verify",
                remediation_detail="",
                executed=True,
            )
        )
        db_session.flush()

        db_session.add(
            models.RollbackRecord(
                audit_event_id=audit.id,
                rollback_root_audit_event_id=audit.id,
                actor="bob",
                reason="duplicate rollback",
                plan_json={},
                verification_detail="verify",
                remediation_detail="",
                executed=True,
            )
        )
        with pytest.raises(IntegrityError):
            db_session.flush()
    finally:
        db_session.rollback()
        db_session.close()


def test_init_db_rejects_legacy_duplicate_rollback_records_before_creating_unique_index(tmp_path):
    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_rollback_records_duplicates.db'}"

    _create_legacy_schema(legacy_db_url)
    legacy_engine = create_engine(legacy_db_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO projects (id, name, slug, visibility) VALUES (:id, :name, :slug, :visibility)"
            ),
            {
                "id": "project-legacy-rollback-dup",
                "name": "Legacy rollback duplicate project",
                "slug": "legacy-rollback-duplicate-project",
                "visibility": "internal",
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO audit_events (id, project_id, actor, action, target_type, target_id, payload, result)
                VALUES (:id, :project_id, :actor, :action, :target_type, :target_id, :payload, :result)
                """
            ),
            {
                "id": "audit-legacy-rollback-dup",
                "project_id": "project-legacy-rollback-dup",
                "actor": "alice",
                "action": "pause_project",
                "target_type": "project",
                "target_id": "project-legacy-rollback-dup",
                "payload": "{}",
                "result": "executed",
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO rollback_records (id, audit_event_id, actor, reason, executed)
                VALUES (:id, :audit_event_id, :actor, :reason, :executed)
                """
            ),
            [
                {
                    "id": "rollback-legacy-dup-1",
                    "audit_event_id": "audit-legacy-rollback-dup",
                    "actor": "alice",
                    "reason": "first legacy rollback",
                    "executed": 1,
                },
                {
                    "id": "rollback-legacy-dup-2",
                    "audit_event_id": "audit-legacy-rollback-dup",
                    "actor": "bob",
                    "reason": "duplicate legacy rollback",
                    "executed": 1,
                },
            ],
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="duplicate rollback records detected for legacy migration"):
        database.init_db(legacy_db_url)


def test_init_db_rejects_legacy_duplicates_before_creating_active_goal_index(tmp_path):
    """Legacy active-goal duplicates should fail with a clear migration preflight error."""

    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_active_goal_duplicates.db'}"

    _create_legacy_schema(legacy_db_url)
    with create_engine(legacy_db_url).begin() as connection:
        connection.execute(
            text(
                "INSERT INTO projects (id, name, slug, visibility) VALUES (:id, :name, :slug, :visibility)"
            ),
            {
                "id": "project-legacy-2",
                "name": "Legacy Active Project",
                "slug": "legacy-active-project",
                "visibility": "internal",
            },
        )
        connection.execute(
            text(
                "INSERT INTO goals (id, project_id, title, status) VALUES (:id, :project_id, :title, :status)"
            ),
            [
                {
                    "id": "goal-legacy-active-1",
                    "project_id": "project-legacy-2",
                    "title": "Primary active goal",
                    "status": "active",
                },
                {
                    "id": "goal-legacy-active-2",
                    "project_id": "project-legacy-2",
                    "title": "Conflicting active goal",
                    "status": "active",
                },
            ],
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="duplicate active goals detected for legacy migration"):
        database.init_db(legacy_db_url)


def test_init_db_rejects_legacy_duplicates_before_creating_active_goal_index_with_normalized_status(tmp_path):
    """Case/whitespace variants in existing goal status should still count as active."""

    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_active_goal_duplicates_normalized.db'}"

    _create_legacy_schema(legacy_db_url)
    with create_engine(legacy_db_url).begin() as connection:
        connection.execute(
            text(
                "INSERT INTO projects (id, name, slug, visibility) VALUES (:id, :name, :slug, :visibility)"
            ),
            {
                "id": "project-legacy-3",
                "name": "Legacy Normalized Active Project",
                "slug": "legacy-normalized-active-project",
                "visibility": "internal",
            },
        )
        connection.execute(
            text(
                "INSERT INTO goals (id, project_id, title, status) VALUES (:id, :project_id, :title, :status)"
            ),
            [
                {
                    "id": "goal-legacy-active-normalized-1",
                    "project_id": "project-legacy-3",
                    "title": "Primary active goal",
                    "status": " Active ",
                },
                {
                    "id": "goal-legacy-active-normalized-2",
                    "project_id": "project-legacy-3",
                    "title": "Conflicting active goal",
                    "status": "active",
                },
            ],
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="duplicate active goals detected for legacy migration"):
        database.init_db(legacy_db_url)


def test_init_db_rejects_wrong_shape_active_goal_index_and_does_not_assume_shape_by_name(tmp_path):
    """A pre-existing index with the right name but wrong predicate must fail migration."""

    legacy_db_url = f"sqlite:///{tmp_path / 'legacy_active_goal_wrong_index_shape.db'}"

    _create_legacy_schema(legacy_db_url)
    with create_engine(legacy_db_url).begin() as connection:
        connection.execute(
            text(
                "CREATE UNIQUE INDEX uq_goals_project_active ON goals (project_id) WHERE status = 'active'"
            )
        )

    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")

    with pytest.raises(RuntimeError, match="has unexpected shape"):
        database.init_db(legacy_db_url)


def test_init_db_enforces_single_active_goal_with_active_goal_index(tmp_path):
    """Goal intake should be guarded by a DB-level uniqueness guarantee."""

    database_url = f"sqlite:///{tmp_path / 'legacy_goals_active_index.db'}"
    sys.modules.pop("bro_pm.database", None)
    database = importlib.import_module("bro_pm.database")
    database.init_db(database_url)

    session = database.SessionLocal()
    try:
        project = models.Project(
            id="project-active-1",
            name="Active Project",
            slug="active-project",
            visibility="internal",
        )
        session.add(project)
        session.flush()

        session.add(
            models.Goal(
                id="goal-active-1",
                project_id=project.id,
                title="First active goal",
                status="active",
            )
        )
        session.commit()

        session.add(
            models.Goal(
                id="goal-active-2",
                project_id=project.id,
                title="Second active goal",
                status="active",
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()
    finally:
        session.rollback()
        session.close()



def test_goal_active_index_compiles_with_partial_predicate_for_sqlite_and_postgresql():
    """Active-goal uniqueness index should be conditional on both SQLite and PostgreSQL."""

    active_goal_index = next(
        index for index in models.Goal.__table__.indexes if index.name == "uq_goals_project_active"
    )

    sqlite_ddl = str(CreateIndex(active_goal_index).compile(dialect=sqlite.dialect()))
    postgres_ddl = str(CreateIndex(active_goal_index).compile(dialect=postgresql.dialect()))

    assert "CREATE UNIQUE INDEX uq_goals_project_active ON goals (project_id)" in sqlite_ddl
    assert "WHERE lower(trim(status)) = 'active'" in sqlite_ddl

    assert "CREATE UNIQUE INDEX uq_goals_project_active ON goals (project_id)" in postgres_ddl
    assert "WHERE lower(trim(status)) = 'active'" in postgres_ddl


def test_active_goal_uniqueness_only_supported_on_sqlite_and_postgresql():
    """Unsupported DB dialects should fail fast instead of changing semantics silently."""

    database.assert_active_goal_uniqueness_dialect_supported("sqlite")
    database.assert_active_goal_uniqueness_dialect_supported("postgresql")

    with pytest.raises(RuntimeError, match="active-goal uniqueness is only supported on dialects"):
        database.assert_active_goal_uniqueness_dialect_supported("mysql")
