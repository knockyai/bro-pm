from __future__ import annotations

import importlib
import sys

from sqlalchemy import inspect


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
