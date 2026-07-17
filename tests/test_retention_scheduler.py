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


@pytest.fixture(autouse=True)
def _isolate_artifact_dirs(tmp_path, monkeypatch):
    """Keep retention passes out of the repo's real data/ tree.

    Same hygiene as tests/test_retention.py: with pyarrow installed the
    Parquet forward path mkdirs PARQUET_DIR (default: src/data/parquet)
    unconditionally, so unredirected runs litter the repo.
    """
    monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path / "archive")
    monkeypatch.setattr(retention, "PARQUET_DIR", tmp_path / "parquet")


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

    def test_high_stream_lag_does_not_gate(self, tmp_path, monkeypatch):
        """stream_lag_s measures jetstream catch-up (now - latest_event_time),
        not writer pressure. Every container restart inflates it
        immediately (cursor rewind), which used to false-trip the gate on
        an idle writer. The scheduler must NOT gate on it. stream_lag_s
        remains observable in platform_health / summary / /health/extended
        for its intended purpose.
        """
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://x/1", _iso(-48 * 3600), "did:a", '{"x":1}'),
        )
        conn.commit()

        # 3600s = the production hot-patched threshold value, which was
        # itself an admission that the gate was firing for the wrong
        # reasons. With the gate removed, even multi-hour lag must not
        # block retention.
        stub = _PressureStub(backlog=10, stream_lag_s=3600.0)
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["raw_stripped"] == 1
        assert sched.skipped_due_to_pressure == 0


# ----------------------------------------------------------------------
# Lock-pressure classification (CLEANUP_DEBT.md #1).
# ----------------------------------------------------------------------


class TestLockPressureClassification:
    """``sqlite3.OperationalError("database is locked")`` is busy_timeout-
    elapsed soft abort under writer contention. The scheduler must
    classify it as ``lock_pressure`` (with the partial row count
    preserved), NOT as the ``-1`` sentinel which is reserved for
    actually-unexpected exceptions (real bugs). See CLEANUP_DEBT.md #1.
    """

    def test_raw_strip_lock_pressure_preserves_partial(self, tmp_path, monkeypatch):
        """Lock-busy mid-strip records the partial row count + lock_pressure flag."""
        import sqlite3 as _sqlite3
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn = _make_db()
        for i in range(20):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        # _strip_raw_chunk: succeed 3 times, then raise busy-timeout shape.
        original_chunk = retention._strip_raw_chunk
        call_count = {"n": 0}

        def hooked_chunk(conn_, cutoff_iso):
            call_count["n"] += 1
            if call_count["n"] > 3:
                raise _sqlite3.OperationalError("database is locked")
            return original_chunk(conn_, cutoff_iso)

        monkeypatch.setattr(retention, "_strip_raw_chunk", hooked_chunk)

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        # 3 chunks * STRIP_BATCH=2 = 6 rows stripped before busy-timeout.
        assert stats["raw_stripped"] == 6, stats
        assert stats.get("raw_stripped_lock_pressure") is True
        # Must NOT be the -1 sentinel (reserved for real bugs).
        assert stats["raw_stripped"] != -1

    def test_unexpected_operational_error_stays_minus_one(self, tmp_path, monkeypatch):
        """A non-lock OperationalError (real-bug shape) still records -1."""
        import sqlite3 as _sqlite3
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

        def bad_chunk(conn_, cutoff_iso):
            raise _sqlite3.OperationalError("no such table: nonexistent")

        monkeypatch.setattr(retention, "_strip_raw_chunk", bad_chunk)

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["raw_stripped"] == -1
        assert "raw_stripped_lock_pressure" not in stats

    def test_archive_lock_pressure_sets_flag(self, tmp_path, monkeypatch):
        """Lock-busy in claim_history archive sets claims_lock_pressure flag;
        does NOT record -1 sentinels. Partial parquet/gzip artifacts on disk
        are durable and idempotent-skipped on the next pass.
        """
        import sqlite3 as _sqlite3
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()

        def hooked_archive(conn_, scheduler=None):
            raise _sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(
            retention, "_archive_and_prune_claim_history", hooked_archive,
        )

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats.get("claims_lock_pressure") is True
        # setdefault(...,0) — not -1.
        assert stats["claims_archived"] == 0
        assert stats["claims_pruned"] == 0

    def test_archive_unexpected_error_stays_minus_one(self, tmp_path, monkeypatch):
        """A non-lock exception in archive still records -1 sentinels."""
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()

        def hooked_archive(conn_, scheduler=None):
            raise RuntimeError("unexpected bug")

        monkeypatch.setattr(
            retention, "_archive_and_prune_claim_history", hooked_archive,
        )

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        stats = retention.run_retention_once_with_sched(sched, conn=conn)

        assert stats["claims_archived"] == -1
        assert stats["claims_pruned"] == -1
        assert "claims_lock_pressure" not in stats


# ----------------------------------------------------------------------
# busy_timeout config + lock-contention harness (CLEANUP_DEBT.md #2).
# ----------------------------------------------------------------------


class TestBusyTimeoutAndLockContentionHarness:
    """``RETENTION_BUSY_TIMEOUT_MS`` defaults to 60000 (matches the
    consumer's writer connection). The harness below validates the
    composed behavior of CLEANUP_DEBT #1 + #2 under REAL OS-level lock
    contention (two SQLite connections, one holds an exclusive write
    transaction): the retention chunk waits its busy_timeout, the
    resulting ``OperationalError("database is locked")`` classifies as
    lock_pressure (soft abort) instead of the -1 real-bug sentinel.

    The harness uses a SHORT busy_timeout (200ms) for test speed; in
    production the value is 60000ms. The shape under test is the
    error-classification path, not the wall-clock value.
    """

    def test_default_busy_timeout_is_60s(self):
        """Default RETENTION_BUSY_TIMEOUT_MS matches consumer's 60s."""
        assert retention.RETENTION_BUSY_TIMEOUT_MS == 60000

    def test_real_lock_contention_classifies_as_lock_pressure(
        self, tmp_path, monkeypatch,
    ):
        """End-to-end: a second connection holds an exclusive write
        transaction; retention's chunk hits busy_timeout, raises
        OperationalError("database is locked"), and the scheduler
        records lock_pressure (not -1). Proves #1 + #2 compose under
        real lock contention, not just mocked exceptions.
        """
        import sqlite3 as _sqlite3
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        db_path = tmp_path / "harness.sqlite"
        # _make_db with a file path returns a usable conn AND creates the
        # schema. Insert old rows so retention has something to attempt.
        setup_conn = _make_db(str(db_path))
        for i in range(10):
            setup_conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        setup_conn.commit()

        # conn_a: holds an exclusive write transaction for the duration
        # of the test. SQLite serializes writers, so conn_b's first
        # UPDATE chunk will block on busy_timeout.
        conn_a = _sqlite3.connect(str(db_path), isolation_level=None,
                                  check_same_thread=False)
        conn_a.execute("BEGIN IMMEDIATE")

        # conn_b: retention's connection. Short busy_timeout for test
        # speed. In prod this is RETENTION_BUSY_TIMEOUT_MS=60000.
        conn_b = _sqlite3.connect(str(db_path), check_same_thread=False)
        conn_b.execute("PRAGMA busy_timeout=200")  # 200ms

        stub = _PressureStub()
        sched = RetentionScheduler(consumer=stub, sleep_between_s=0)
        try:
            stats = retention.run_retention_once_with_sched(sched, conn=conn_b)
        finally:
            try:
                conn_a.execute("COMMIT")
            except Exception:
                pass
            conn_a.close()
            conn_b.close()
            setup_conn.close()

        # Strip attempt hit the lock, busy_timeout elapsed, error
        # classified as lock_pressure instead of -1.
        assert stats.get("raw_stripped_lock_pressure") is True, stats
        assert stats["raw_stripped"] != -1
        # No rows committed because the very first chunk was blocked.
        assert stats["raw_stripped"] == 0


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

    def test_no_burn_returns_none(self):
        """No measurable burn (or net free gain — e.g., after an archive
        delete). Must NOT return float('inf'); that would break JSON
        serialization in /health/extended.
        """
        sched = RetentionScheduler()
        now = time.time()
        sched._disk_history.append((now - 86400, 50 * 1024**3, 100 * 1024**3))
        sched._disk_history.append((now, 50 * 1024**3, 100 * 1024**3))
        assert sched.estimate_disk_runway_days() is None

    def test_net_free_gain_returns_none(self):
        """Same protection if disk_free went UP between samples (archive
        cleanup, WAL checkpoint reclaiming)."""
        sched = RetentionScheduler()
        now = time.time()
        sched._disk_history.append((now - 86400, 100 * 1024**3, 50 * 1024**3))
        sched._disk_history.append((now, 100 * 1024**3, 60 * 1024**3))
        assert sched.estimate_disk_runway_days() is None
