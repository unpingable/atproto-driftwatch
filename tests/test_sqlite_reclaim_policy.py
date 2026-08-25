"""auto_vacuum policy, reclaim-status telemetry, recovery-capacity invariant."""

import os
import sqlite3
import time

import pytest

from labeler import maintenance as M
from labeler.retention_scheduler import (
    DISK_RUNWAY_CRITICAL_DAYS,
    DISK_RUNWAY_MIN_FREE_BYTES,
    RetentionScheduler,
)

GIB = 1024 ** 3
DAY = 86400.0


@pytest.fixture(autouse=True)
def _clean_brake():
    M._reset_brake_for_test()
    yield
    M._reset_brake_for_test()


def _build_db(path, mode_incremental, rows=4000, keep=1000):
    c = sqlite3.connect(str(path))
    if mode_incremental:
        c.execute("PRAGMA auto_vacuum=INCREMENTAL")
    c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, blob TEXT)")
    c.executemany("INSERT INTO t (blob) VALUES (?)", [("x" * 2000,) for _ in range(rows)])
    c.commit()
    c.execute("DELETE FROM t WHERE id > ?", (keep,))
    c.commit()
    return c


class TestIncrementalVacuumStatus:
    def test_mode_zero_is_reported_as_structural_fault(self, tmp_path):
        c = _build_db(tmp_path / "m0.sqlite", mode_incremental=False)
        try:
            r = M.incremental_vacuum_chunk(c, pages=1000)
            assert r["status"] == "mode_incompatible"
            assert r["auto_vacuum"] == M.AUTO_VACUUM_NONE
            assert r["freelist_count"] > 0
            assert "rebuilt" in r["detail"]
        finally:
            c.close()

    def test_mode_two_reclaims_the_requested_page_count(self, tmp_path):
        """The executescript regression test.

        conn.execute("PRAGMA incremental_vacuum(1000)") reclaims exactly ONE
        page. If this test ever sees 1, the helper has regressed to the broken
        invocation and the whole permanent fix is inert.
        """
        c = _build_db(tmp_path / "m2.sqlite", mode_incremental=True)
        try:
            before = c.execute("PRAGMA freelist_count").fetchone()[0]
            assert before > 1000, "test fixture must have a freelist worth draining"
            r = M.incremental_vacuum_chunk(c, pages=1000)
            assert r["status"] == "ok"
            assert r["reclaimed_pages"] == 1000, (
                "expected 1000 pages; got %s — the one-page-per-call bug is back"
                % r["reclaimed_pages"]
            )
        finally:
            c.close()

    def test_mode_two_empty_freelist_is_noop_not_fault(self, tmp_path):
        path = tmp_path / "m2empty.sqlite"
        c = sqlite3.connect(str(path))
        c.execute("PRAGMA auto_vacuum=INCREMENTAL")
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        c.commit()
        try:
            r = M.incremental_vacuum_chunk(c, pages=1000)
            assert r["status"] == "noop"
            assert r["reclaimed_pages"] == 0
        finally:
            c.close()

    def test_reclaim_physically_shrinks_the_file(self, tmp_path):
        path = tmp_path / "shrink.sqlite"
        c = _build_db(path, mode_incremental=True)
        try:
            size_before = path.stat().st_size
            M.incremental_vacuum_chunk(c, pages=100000)
            c.commit()
            assert path.stat().st_size < size_before
        finally:
            c.close()


# ---------------------------------------------------------------------------
# Phase 5 — recovery-capacity invariant, visible before physical exhaustion.
# ---------------------------------------------------------------------------


class TestRecoveryCapacity:
    def test_required_bytes_applies_margin(self):
        assert M.rebuild_required_bytes(100, margin=1.2) == 120

    def test_current_production_geometry_is_a_deficit(self, tmp_path, monkeypatch):
        """live ~57.2 GiB vs ~28 GiB free on / -> insufficient.

        This is the state the invariant should have been reporting for weeks
        before the volume actually filled.
        """
        import shutil as _sh

        class _Root:
            total, used, free = 157 * GIB, 129 * GIB, 28 * GIB

        ws = tmp_path / "scratch"
        ws.mkdir()
        monkeypatch.setattr(_sh, "disk_usage", lambda p: _Root)
        monkeypatch.setattr(M, "DATA_DIR", tmp_path / "data")
        (tmp_path / "data").mkdir()

        class _Conn:
            def execute(self, sql):
                val = {
                    "PRAGMA page_size": 4096,
                    "PRAGMA page_count": 50518271,
                    "PRAGMA freelist_count": 35536691,
                    "PRAGMA auto_vacuum": 0,
                }[sql]
                return type("R", (), {"fetchone": lambda self: (val,)})()

        state = M.recovery_capacity_state(_Conn(), paths=[str(ws)])
        assert state["live_bytes"] == pytest.approx(57.2 * GIB, rel=0.01)
        assert state["required_bytes"] > state["workspace_bytes"]
        assert state["ok"] is False
        assert state["reason"] == "insufficient_recovery_workspace"
        assert state["deficit_bytes"] > 0

    def test_unconfigured_workspace_is_zero_not_root_free_space(self, tmp_path):
        """Free bytes somewhere is not qualified recovery workspace."""
        class _Conn:
            def execute(self, sql):
                val = {
                    "PRAGMA page_size": 4096,
                    "PRAGMA page_count": 1000,
                    "PRAGMA freelist_count": 0,
                    "PRAGMA auto_vacuum": 2,
                }[sql]
                return type("R", (), {"fetchone": lambda self: (val,)})()

        state = M.recovery_capacity_state(_Conn(), paths=[])
        assert state["workspace_bytes"] == 0
        assert state["ok"] is False
        assert state["reason"] == "no_qualified_recovery_workspace_configured"

    def test_same_filesystem_as_database_does_not_qualify(self, tmp_path, monkeypatch):
        """You cannot rebuild a full volume onto itself."""
        data = tmp_path / "data"
        data.mkdir()
        monkeypatch.setattr(M, "DATA_DIR", data)
        ws = M.qualified_recovery_workspace(paths=[str(tmp_path)], db_dir=data)
        assert ws["workspace_bytes"] == 0
        assert ws["candidates"][0]["reason"] == "same_filesystem_as_database"

    def test_missing_path_does_not_qualify(self, tmp_path):
        ws = M.qualified_recovery_workspace(
            paths=[str(tmp_path / "nope")], db_dir=tmp_path
        )
        assert ws["workspace_bytes"] == 0
        assert ws["candidates"][0]["reason"] == "missing_or_not_a_directory"


# ---------------------------------------------------------------------------
# Phase 7 — retention lag must not manufacture epoch-scale nonsense.
# ---------------------------------------------------------------------------
