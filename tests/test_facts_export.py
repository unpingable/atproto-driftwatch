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


# -------------------------------------------------------------------
# 1. Export from empty claim_history → empty tables, meta populated
# -------------------------------------------------------------------
class TestEmptyExport:
    def test_empty_source(self, tmp_path):
        source = _make_source()
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

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
        # Use timestamps 1h ago so they fall within the 72h overlap window
        # but aren't at the exact edge of the upper bound
        ts = _ts(-1)
        rows = [
            ("did:alice", "fp_abc", ts, None, "", "", "at://did:alice/post/1", "cid1", "v1"),
            ("did:bob", "fp_abc", ts, None, "", "", "at://did:bob/post/2", "cid2", "v1"),
            ("did:carol", "fp_xyz", ts, None, "", "", "at://did:carol/post/3", "cid3", "v1"),
        ]
        source = _make_source(rows)
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        # uri_fingerprint: 3 unique posts
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 3

        # fingerprint_bounds: 2 fingerprints
        assert sidecar.execute("SELECT COUNT(*) FROM fingerprint_bounds").fetchone()[0] == 2
        fp_abc = sidecar.execute(
            "SELECT total_claims FROM fingerprint_bounds WHERE fingerprint='fp_abc'"
        ).fetchone()
        assert fp_abc[0] == 2  # alice + bob

        # fingerprint_hourly: at least 2 rows (one per fingerprint for the hour)
        hourly_count = sidecar.execute("SELECT COUNT(*) FROM fingerprint_hourly").fetchone()[0]
        assert hourly_count >= 2

        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 3. Copy-forward: second export preserves old data + adds new
# -------------------------------------------------------------------
class TestCopyForward:
    def test_second_export_preserves(self, tmp_path):
        now_ts = _ts(0)
        rows1 = [
            ("did:alice", "fp_abc", now_ts, None, "", "", "at://did:alice/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows1)
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 1
        checkpoint_1 = int(sidecar.execute(
            "SELECT value FROM meta WHERE key='last_checkpoint_rowid'"
        ).fetchone()[0])
        sidecar.close()

        # Add more data
        source.execute(
            "INSERT INTO claim_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("did:bob", "fp_xyz", now_ts, None, "", "", "at://did:bob/post/2", "cid2", "v1"),
        )
        source.commit()

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 2
        checkpoint_2 = int(sidecar.execute(
            "SELECT value FROM meta WHERE key='last_checkpoint_rowid'"
        ).fetchone()[0])
        assert checkpoint_2 > checkpoint_1
        sidecar.close()
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
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        assert sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 5
        # checkpoint should be at max rowid (5)
        cp = _get_meta_int(sidecar, "last_checkpoint_rowid")
        assert cp == 5
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 5. Overlap: hourly bins in 72h window delete/replaced
# -------------------------------------------------------------------
class TestOverlapHourly:
    def test_no_double_counting(self, tmp_path):
        ts = _ts(-1)  # 1h ago, safely within overlap window
        rows = [
            ("did:a", "fp1", ts, None, "", "", "at://did:a/post/1", "cid1", "v1"),
        ]
        source = _make_source(rows)
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        hourly_before = sidecar.execute(
            "SELECT SUM(event_count) FROM fingerprint_hourly"
        ).fetchone()[0] or 0
        sidecar.close()

        # Re-export without new data — hourly should not double count
        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        hourly_after = sidecar.execute(
            "SELECT SUM(event_count) FROM fingerprint_hourly"
        ).fetchone()[0] or 0
        sidecar.close()

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
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        count = sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0]
        # Old row should be pruned (>30d), only recent should remain
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
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        bounds = sidecar.execute(
            "SELECT total_claims FROM fingerprint_bounds WHERE fingerprint='fp_shared'"
        ).fetchone()
        # Only 1 should remain after prune (old one pruned)
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
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        count = sidecar.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0]
        assert count == 1
        fp = sidecar.execute(
            "SELECT fingerprint FROM uri_fingerprint WHERE post_uri='at://did:a/post/1'"
        ).fetchone()[0]
        assert fp == "fp_new"  # second insert has higher rowid
        sidecar.close()
        source.close()


# -------------------------------------------------------------------
# 9. Journal mode is DELETE
# -------------------------------------------------------------------
class TestJournalMode:
    def test_delete_journal(self, tmp_path):
        source = _make_source()
        facts_path = str(tmp_path / "facts.sqlite")

        export_once(source, facts_path)

        sidecar = sqlite3.connect(facts_path)
        mode = sidecar.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "delete"
        sidecar.close()
        source.close()
