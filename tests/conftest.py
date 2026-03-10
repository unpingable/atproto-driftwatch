"""Shared fixtures for driftwatch test suite.

Isolates the SQLite database per test so tests don't contaminate each other.
"""
import pytest


@pytest.fixture(autouse=True)
def isolate_db(tmp_path, monkeypatch):
    """Give each test its own fresh SQLite database."""
    import labeler.db as db_mod
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    db_mod.DATA_DIR = tmp_path
    db_mod.init_db()
    yield
