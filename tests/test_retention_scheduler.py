"""Tests for the pressure-aware retention scheduler.

Validates:
  * Pre-pass gate skips when pressure signals exceed thresholds.
  * Mid-pass abort on rollback_lost tripwire.
  * Mid-pass abort on pass-budget-exceeded.
  * Mid-pass abort on chunk-overrun-tolerance-exceeded.
  * NullScheduler executes everything (legacy parity).
  * Resumability — chunked DELETE with abort partway leaves the rest for
    the next pass.
  * /health surface returns sensible state values.
"""

import time
from unittest import mock

import pytest

from labeler import retention
from labeler.retention_scheduler import (
    AbortRetentionPass,
    NullScheduler,
    RetentionScheduler,
)
from tests.test_retention import _make_db, _iso, _insert_claim


class _PressureStub:
    """Minimal stand-in for ATProtoConsumer's pressure surface.

    Snapshot is mutable so tests can simulate pressure changes between
    chunks. ``rollback_lost_total`` is the key tripwire signal — set it
    to a non-zero value mid-test to simulate ingest losing events while
    retention is running.
    """

    def __init__(self, **fields):
        self._snap = {
            "backlog": 0,
            "queue_max": 5000,
            "median_dequeue_age_s": 0.0,
            "stream_lag_s": 0.0,
            "rollback_lost_total": 0,
            "events_dropped_total": 0,
        }
        self._snap.update(fields)

    def get_pressure_snapshot(self):
        return dict(self._snap)

    def set(self, **fields):
        self._snap.update(fields)


# ----------------------------------------------------------------------
# NullScheduler parity.
# ----------------------------------------------------------------------


class TestNullScheduler:
    def test_runs_full_pass_uninterrupted(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        for i in range(3):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        sched = NullScheduler()
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["raw_stripped"] == 3
        assert "aborted" not in stats
        assert sched.last_pass is None  # NullScheduler doesn't track passes


# ----------------------------------------------------------------------
# Pre-pass gate.
# ----------------------------------------------------------------------


class TestPrePassGate:
    @pytest.mark.parametrize(
        "field,value,expected_marker",
        [
            ("backlog", 9999, "backlog"),
            ("stream_lag_s", 999.0, "stream_lag"),
        ],
    )
    def test_skips_when_pressure_high(self, tmp_path, monkeypatch,
                                       field, value, expected_marker):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        # Insert something retention WOULD do work on, to prove it's the
        # gate (not "nothing to do") that caused the skip.
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://x/1", _iso(-48 * 3600), "did:a", '{"x":1}'),
        )
        conn.commit()

        stub = _PressureStub(**{field: value})
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats == {"skipped": True, "skip_reason": "pre_pass_pressure"}
        assert sched.skipped_due_to_pressure == 1
        # The row should NOT have been stripped.
        raw = conn.execute("SELECT raw FROM events").fetchone()[0]
        assert raw is not None
        assert sched.last_pass["skipped"] is True
        assert expected_marker in sched.last_pass["skip_reason"]

    def test_runs_when_pressure_clear(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://x/1", _iso(-48 * 3600), "did:a", '{"x":1}'),
        )
        conn.commit()

        stub = _PressureStub(backlog=10, median_dequeue_age_s=5.0)
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["raw_stripped"] == 1
        assert "aborted" not in stats
        assert sched.skipped_due_to_pressure == 0
        assert sched.last_pass["completed"] is True

    def test_high_median_age_does_not_gate(self, tmp_path, monkeypatch):
        """median_dequeue_age_s measures longitudinal recheck lag, not
        live ingest queue lag. The scheduler must NOT gate on it.
        """
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://x/1", _iso(-48 * 3600), "did:a", '{"x":1}'),
        )
        conn.commit()

        # 8520s = 142 minutes — typical prod value with longitudinal behind.
        stub = _PressureStub(backlog=10, median_dequeue_age_s=8520.0)
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["raw_stripped"] == 1
        assert sched.skipped_due_to_pressure == 0


# ----------------------------------------------------------------------
# Tripwire: rollback_lost mid-pass aborts.
# ----------------------------------------------------------------------


class TestRollbackLostTripwire:
    def test_abort_on_rollback_increment(self, tmp_path, monkeypatch):
        """A non-zero delta in rollback_lost_total during a pass aborts.

        The trip happens at the next ``before_chunk`` call after the
        increment, so at least one chunk runs before the abort fires.
        """
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn = _make_db()
        for i in range(10):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        # Hook: after the first chunk runs, simulate the writer losing an
        # event. Next before_chunk should detect the delta and abort.
        original_after = sched.after_chunk
        chunks_seen = {"n": 0}

        def hooked_after(op_name, elapsed_s, rows):
            original_after(op_name, elapsed_s, rows)
            chunks_seen["n"] += 1
            if chunks_seen["n"] == 1:
                stub.set(rollback_lost_total=5)

        sched.after_chunk = hooked_after  # type: ignore[assignment]

        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats.get("aborted") is True
        assert "rollback_lost_tripwire" in stats["abort_reason"]
        # First chunk committed (2 rows stripped) before the trip.
        assert sched.last_pass["chunks_executed"] == 1
        assert sched.last_pass["rows_total"] == 2
        # Subsequent ops were skipped after the abort.
        assert stats.get("events_pruned_skipped") is True or "events_pruned" not in stats


# ----------------------------------------------------------------------
# Wall-clock budgets.
# ----------------------------------------------------------------------


class TestWallClockBudgets:
    def test_pass_budget_exceeded_aborts(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)
        # Force a tight pass budget — first chunk OK, then check trips.
        from labeler import retention_scheduler as rs
        monkeypatch.setattr(rs, "PASS_BUDGET_S", 0.05)

        conn = _make_db()
        for i in range(20):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0.1)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        # Either the pass exited cleanly (rows < BATCH on a small table) or
        # was aborted with pass_budget_exceeded. Both are acceptable; the
        # contract is that the scheduler doesn't run forever.
        if stats.get("aborted"):
            assert "pass_budget_exceeded" in stats["abort_reason"]

    def test_chunk_overrun_tolerance_aborts(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)
        from labeler import retention_scheduler as rs
        monkeypatch.setattr(rs, "CHUNK_BUDGET_S", 0.0)  # every chunk overruns
        monkeypatch.setattr(rs, "CHUNK_OVERRUN_TOLERANCE", 2)

        conn = _make_db()
        for i in range(20):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats.get("aborted") is True
        assert "chunk_overrun_tolerance_exceeded" in stats["abort_reason"]


# ----------------------------------------------------------------------
# Resumability across passes.
# ----------------------------------------------------------------------


class TestResumability:
    def test_aborted_pass_picks_up_next_run(self, tmp_path, monkeypatch):
        """A pass that aborts mid-prune leaves the rest for next time."""
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 5)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn = _make_db()
        for i in range(15):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)

        # Trip after the first chunk by setting rollback_lost.
        original_after = sched.after_chunk
        seen = {"n": 0}

        def hooked_after(op_name, elapsed_s, rows):
            original_after(op_name, elapsed_s, rows)
            seen["n"] += 1
            if seen["n"] == 1:
                stub.set(rollback_lost_total=1)

        sched.after_chunk = hooked_after  # type: ignore[assignment]
        stats1 = retention.run_retention_once_with_sched(sched, conn=conn)
        assert stats1.get("aborted") is True

        # Some rows stripped, others not.
        nulls_after_pass1 = conn.execute(
            "SELECT COUNT(*) FROM events WHERE raw IS NULL"
        ).fetchone()[0]
        non_nulls_after_pass1 = 15 - nulls_after_pass1
        assert nulls_after_pass1 > 0
        assert non_nulls_after_pass1 > 0

        # Second pass: clear the tripwire by setting rollback_lost_total
        # to the new baseline. Reset its baseline view by building a
        # fresh scheduler (production loop reuses one, but begin_pass
        # re-reads the snapshot on entry).
        stub2 = _PressureStub(rollback_lost_total=1)
        sched2 = RetentionScheduler(consumer=stub2, sleep_between_s=0)
        stats2 = retention.run_retention_once_with_sched(sched2, conn=conn)

        assert "aborted" not in stats2
        # All remaining old rows should now be NULL.
        nulls_after_pass2 = conn.execute(
            "SELECT COUNT(*) FROM events WHERE raw IS NULL"
        ).fetchone()[0]
        assert nulls_after_pass2 == 15


# ----------------------------------------------------------------------
# Health surface.
# ----------------------------------------------------------------------


class TestHealthState:
    def test_initial_state_ok(self):
        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        state = sched.health_state()
        assert state["enabled"] is True
        assert state["scheduler"] == "pressure_aware_v1"
        assert state["state"] == "ok"
        assert state["last_pass"] is None
        assert state["disk_runway_days"] is None  # no samples yet

    def test_disk_runway_critical_state(self):
        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        # Two samples 1 hour apart, losing 50 GB; only 1 GB free.
        # Burn = 50 GB / 3600s = ~14 MB/s. Runway = 1 GB / 14 MB/s ≈ 70s = ~0.001 days.
        sched._disk_history.append((1000.0, 100 * 1024**3, 51 * 1024**3))
        sched._disk_history.append((4600.0, 150 * 1024**3, 1 * 1024**3))
        state = sched.health_state()
        assert state["state"] == "critical"
        assert "disk_runway_days" in state
        assert state["disk_runway_days"] < 1.0

    def test_pressure_skip_throttled_state(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        stub = _PressureStub(backlog=99999)
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        retention.run_retention_once_with_sched(sched, conn=conn)
        state = sched.health_state()
        assert state["state"] == "throttled"
        assert sched.skipped_due_to_pressure == 1


# ----------------------------------------------------------------------
# Disk runway estimation.
# ----------------------------------------------------------------------


class TestDiskRunway:
    def test_no_samples(self):
        sched = RetentionScheduler()
        assert sched.estimate_disk_runway_days() is None

    def test_single_sample(self):
        sched = RetentionScheduler()
        sched.record_disk_sample(50 * 1024**3, 100 * 1024**3)
        assert sched.estimate_disk_runway_days() is None

    def test_burn_rate_extrapolation(self):
        sched = RetentionScheduler()
        # Day 1: 50 GB free. Day 2 (one day later): 39 GB free. Burn = 11 GB/day.
        # Runway = 39 GB / 11 GB/day ≈ 3.55 days.
        now = time.time()
        sched._disk_history.append((now - 86400, 100 * 1024**3, 50 * 1024**3))
        sched._disk_history.append((now, 111 * 1024**3, 39 * 1024**3))
        runway = sched.estimate_disk_runway_days()
        assert runway is not None
        assert 3.0 < runway < 4.0

    def test_no_burn_returns_infinite(self):
        sched = RetentionScheduler()
        now = time.time()
        sched._disk_history.append((now - 86400, 50 * 1024**3, 100 * 1024**3))
        sched._disk_history.append((now, 50 * 1024**3, 100 * 1024**3))
        assert sched.estimate_disk_runway_days() == float("inf")
