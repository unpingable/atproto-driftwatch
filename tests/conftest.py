"""Shared fixtures for driftwatch test suite.

Isolates the SQLite database per test so tests don't contaminate each other.
"""
import pytest


@pytest.fixture(autouse=True)
def isolate_db(tmp_path, monkeypatch):
    """Give each test its own fresh SQLite database."""
    import labeler.db as db_mod
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    # Producer-side recheck gating is now keyed off these env vars
    # (see db.py _add_recheck_txn / enqueue_claim_recheck). Tests were
    # written before the gate existed; default both to enabled so the
    # historical contracts hold. Tests that exercise the gate itself
    # should override with monkeypatch.setenv(..., "0").
    monkeypatch.setenv("ENABLE_LONGITUDINAL_RECHECK", "1")
    monkeypatch.setenv("ENABLE_CLAIM_RECHECK", "1")
    db_mod.DATA_DIR = tmp_path
    db_mod.init_db()
    yield
