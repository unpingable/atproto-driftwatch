"""Tests for labeler.facts_export — driftwatch sidecar exporter."""

import os
import sqlite3
import time

import pytest

from labeler.facts_export import (
    BATCH_LIMIT,
    OVERLAP_HOURS,
    RETENTION_DAYS,
    _ensure_tables,
    _get_meta_int,
    _recompute_bounds,
    _recompute_hourly,
    _set_meta,
    _upsert_uri_fingerprints,
    export_once,
)


def _make_source(rows=None):
    """Create an in-memory claim_history table with optional seed rows."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE claim_history (
            authorDid TEXT,
            claim_fingerprint TEXT,
            createdAt TIMESTAMP,
            confidence REAL,
            provenance TEXT,
            evidence_hash TEXT,
            post_uri TEXT,
            post_cid TEXT,
            fingerprint_version TEXT
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_claim_history_created ON claim_history(createdAt)"
    )
    if rows:
        conn.executemany(
            "INSERT INTO claim_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        conn.commit()
    return conn


def _ts(offset_hours=0):
    """ISO timestamp offset_hours from now (recent enough to survive 30d pruning)."""
    import datetime
    base = datetime.datetime.now(datetime.timezone.utc)
    dt = base + datetime.timedelta(hours=offset_hours)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _export(source, tmp_path):
    """Helper: run export_once with proper paths and forced snapshot."""
    facts_path = str(tmp_path / "facts.sqlite")
    work_path = str(tmp_path / "facts_work.sqlite")
    export_once(source, facts_path, work_path, force_snapshot=True)
    return facts_path, work_path


# -------------------------------------------------------------------
# 1. Export from empty claim_history → empty tables, meta populated
# -------------------------------------------------------------------
class TestEmptyExport:
    def test_empty_source(self, tmp_path):
        source = _make_source()
        facts_path, _ = _export(source, tmp_path)

        assert os.path.exists(facts_path)
        sidecar = sqlite3.connect(facts_path)
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 0
        assert sidecar.execute("SELECT COUNT(*) FROM fingerprint_hourly").fetchone()[0] == 0
        assert sidecar.execute("SELECT COUNT(*) FROM fingerprint_bounds").fetchone()[0] == 0
        # meta should have last_export_epoch
        row = sidecar.execute("SELECT value FROM meta WHERE key='last_export_epoch'").fetchone()
        assert row is not None
        assert int(row[0]) > 0
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 2. Export with sample data → all three tables correct
# -------------------------------------------------------------------
class TestSampleData:
    def test_basic_export(self, tmp_path):
        ts = _ts(-1)
        rows = [
            ("did:alice", "fp_abc", ts, None, "", "", "at://did:alice/post/1", "cid1", "v1"),
            ("did:bob", "fp_abc", ts, None, "", "", "at://did:bob/post/2", "cid2", "v1"),
            ("did:carol", "fp_xyz", ts, None, "", "", "at://did:carol/post/3", "cid3", "v1"),
        ]
        source = _make_source(rows)
        facts_path, _ = _export(source, tmp_path)

        sidecar = sqlite3.connect(facts_path)
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 3
        assert sidecar.execute("SELECT COUNT(*) FROM fingerprint_bounds").fetchone()[0] == 2
        fp_abc = sidecar.execute(
            "SELECT total_claims FROM fingerprint_bounds WHERE fingerprint='fp_abc'"
        ).fetchone()
        assert fp_abc[0] == 2
        hourly_count = sidecar.execute("SELECT COUNT(*) FROM fingerprint_hourly").fetchone()[0]
        assert hourly_count >= 2
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 3. Second export preserves old data + adds new
# -------------------------------------------------------------------
class TestCopyForward:
    def test_second_export_preserves(self, tmp_path):
        now_ts = _ts(0)
        rows1 = [
            ("did:alice", "fp_abc", now_ts, None, "", "", "at://did:alice/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows1)
        facts_path = str(tmp_path / "facts.sqlite")
        work_path = str(tmp_path / "facts_work.sqlite")

        export_once(source, facts_path, work_path, force_snapshot=True)

        # Check working DB directly (snapshot is compacted copy)
        work = sqlite3.connect(work_path)
        assert work.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 1
        checkpoint_1 = _get_meta_int(work, "last_checkpoint_rowid")
        work.close()

        # Add more data
        source.execute(
            "INSERT INTO claim_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("did:bob", "fp_xyz", now_ts, None, "", "", "at://did:bob/post/2", "cid2", "v1"),
        )
        source.commit()

        export_once(source, facts_path, work_path, force_snapshot=True)

        work = sqlite3.connect(work_path)
        assert work.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 2
        checkpoint_2 = _get_meta_int(work, "last_checkpoint_rowid")
        assert checkpoint_2 > checkpoint_1
        work.close()

        # Snapshot should also have 2 rows
        snap = sqlite3.connect(facts_path)
        assert snap.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 2
        snap.close()
        source.close()


# -------------------------------------------------------------------
# 4. Batch loop: processes multiple batches, checkpoint advances
# -------------------------------------------------------------------
class TestBatchLoop:
    def test_multiple_batches(self, tmp_path, monkeypatch):
        import labeler.facts_export as fe
        monkeypatch.setattr(fe, "BATCH_LIMIT", 2)

        now_ts = _ts(0)
        rows = [
            ("did:a", "fp1", now_ts, None, "", "", f"at://did:a/post/{i}", f"cid{i}", "v1")
            for i in range(5)
        ]
        source = _make_source(rows)
        facts_path, work_path = _export(source, tmp_path)

        snap = sqlite3.connect(facts_path)
        assert snap.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 5
        cp = _get_meta_int(snap, "last_checkpoint_rowid")
        assert cp == 5
        snap.close()
        source.close()


# -------------------------------------------------------------------
# 5. Overlap: hourly bins in 72h window delete/replaced
# -------------------------------------------------------------------
class TestOverlapHourly:
    def test_no_double_counting(self, tmp_path):
        ts = _ts(-1)
        rows = [
            ("did:a", "fp1", ts, None, "", "", "at://did:a/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows)
        facts_path = str(tmp_path / "facts.sqlite")
        work_path = str(tmp_path / "facts_work.sqlite")

        export_once(source, facts_path, work_path, force_snapshot=True)

        snap = sqlite3.connect(facts_path)
        hourly_before = snap.execute(
            "SELECT SUM(event_count) FROM fingerprint_hourly"
        ).fetchone()[0] or 0
        snap.close()

        # Re-export without new data
        export_once(source, facts_path, work_path, force_snapshot=True)

        snap = sqlite3.connect(facts_path)
        hourly_after = snap.execute(
            "SELECT SUM(event_count) FROM fingerprint_hourly"
        ).fetchone()[0] or 0
        snap.close()

        assert hourly_after == hourly_before
        source.close()


# -------------------------------------------------------------------
# 6. Pruning: rows older than 30d removed
# -------------------------------------------------------------------
class TestPruning:
    def test_old_rows_pruned(self, tmp_path):
        import datetime
        old_ts = (
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        ).strftime("%Y-%m-%d %H:%M:%S")
        recent_ts = _ts(0)

        rows = [
            ("did:old", "fp1", old_ts, None, "", "", "at://did:old/post/1", "cid1", "v1"),
            ("did:new", "fp2", recent_ts, None, "", "", "at://did:new/post/2", "cid2", "v1"),
        ]
        source = _make_source(rows)
        facts_path, _ = _export(source, tmp_path)

        sidecar = sqlite3.connect(facts_path)
        count = sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0]
        assert count == 1
        uri = sidecar.execute("SELECT post_uri FROM uri_fingerprint").fetchone()[0]
        assert uri == "at://did:new/post/2"
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 7. Bounds recomputed after prune (reflect retained data only)
# -------------------------------------------------------------------
class TestBoundsAfterPrune:
    def test_bounds_exclude_pruned(self, tmp_path):
        import datetime
        old_ts = (
            datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        ).strftime("%Y-%m-%d %H:%M:%S")
        recent_ts = _ts(0)

        rows = [
            ("did:old", "fp_shared", old_ts, None, "", "", "at://did:old/post/1", "cid1", "v1"),
            ("did:new", "fp_shared", recent_ts, None, "", "", "at://did:new/post/2", "cid2", "v1"),
        ]
        source = _make_source(rows)
        facts_path, _ = _export(source, tmp_path)

        sidecar = sqlite3.connect(facts_path)
        bounds = sidecar.execute(
            "SELECT total_claims FROM fingerprint_bounds WHERE fingerprint='fp_shared'"
        ).fetchone()
        assert bounds[0] == 1
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 8. Dedup: multiple rows per post_uri → highest rowid wins
# -------------------------------------------------------------------
class TestDedup:
    def test_highest_rowid_wins(self, tmp_path):
        now_ts = _ts(0)
        rows = [
            ("did:a", "fp_old", now_ts, None, "", "", "at://did:a/post/1", "cid1", "v1"),
            ("did:a", "fp_new", now_ts, None, "", "", "at://did:a/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows)
        facts_path, _ = _export(source, tmp_path)

        sidecar = sqlite3.connect(facts_path)
        count = sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0]
        assert count == 1
        fp = sidecar.execute(
            "SELECT fingerprint FROM uri_fingerprint WHERE post_uri='at://did:a/post/1'"
        ).fetchone()[0]
        assert fp == "fp_new"
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 9. Snapshot has DELETE journal mode
# -------------------------------------------------------------------
class TestJournalMode:
    def test_delete_journal(self, tmp_path):
        source = _make_source()
        facts_path, _ = _export(source, tmp_path)

        sidecar = sqlite3.connect(facts_path)
        mode = sidecar.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "delete"
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 10. Working DB uses WAL, snapshot uses DELETE
# -------------------------------------------------------------------
class TestWorkingDbWal:
    def test_work_wal_snap_delete(self, tmp_path):
        source = _make_source()
        facts_path = str(tmp_path / "facts.sqlite")
        work_path = str(tmp_path / "facts_work.sqlite")
        export_once(source, facts_path, work_path, force_snapshot=True)

        work = sqlite3.connect(work_path)
        work_mode = work.execute("PRAGMA journal_mode").fetchone()[0]
        assert work_mode == "wal"
        work.close()

        snap = sqlite3.connect(facts_path)
        snap_mode = snap.execute("PRAGMA journal_mode").fetchone()[0]
        assert snap_mode == "delete"
        snap.close()
        source.close()


# -------------------------------------------------------------------
# 11. Hourly overlap covers recent late arrivals
# -------------------------------------------------------------------
class TestHourlyOverlap:
    def test_late_arrival_within_window(self, tmp_path):
        """A late arrival within HOURLY_OVERLAP_HOURS is counted in hourly bins."""
        recent_ts = _ts(0)
        rows = [
            ("did:a", "fp1", recent_ts, None, "", "", "at://did:a/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows)
        facts_path = str(tmp_path / "facts.sqlite")
        work_path = str(tmp_path / "facts_work.sqlite")

        # First export establishes last_export_epoch
        export_once(source, facts_path, work_path, force_snapshot=True)

        # Insert a late arrival within the 6h hourly overlap window
        late_ts = _ts(-3)
        source.execute(
            "INSERT INTO claim_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("did:b", "fp_late", late_ts, None, "", "", "at://did:b/post/2", "cid2", "v1"),
        )
        source.commit()

        export_once(source, facts_path, work_path, force_snapshot=True)

        snap = sqlite3.connect(facts_path)
        # Late arrival within window should appear in hourly bins
        late_hourly = snap.execute(
            "SELECT SUM(event_count) FROM fingerprint_hourly WHERE fingerprint='fp_late'"
        ).fetchone()[0]
        assert late_hourly is not None and late_hourly >= 1
        # Its uri_fingerprint mapping should also be correct
        uri_count = snap.execute(
            "SELECT COUNT(*) FROM uri_fingerprint WHERE post_uri='at://did:b/post/2'"
        ).fetchone()[0]
        assert uri_count == 1
        snap.close()
        source.close()


# -------------------------------------------------------------------
# 12. No snapshot when interval not elapsed
# -------------------------------------------------------------------
class TestSnapshotInterval:
    def test_no_snapshot_when_recent(self, tmp_path):
        source = _make_source()
        facts_path = str(tmp_path / "facts.sqlite")
        work_path = str(tmp_path / "facts_work.sqlite")

        # First export: force snapshot
        export_once(source, facts_path, work_path, force_snapshot=True)
        mtime_1 = os.path.getmtime(facts_path)

        # Second export: no force, interval not elapsed
        import time as _time
        _time.sleep(0.1)
        export_once(source, facts_path, work_path, force_snapshot=False)

        # Snapshot file should not have been updated
        mtime_2 = os.path.getmtime(facts_path)
        assert mtime_2 == mtime_1
        source.close()
