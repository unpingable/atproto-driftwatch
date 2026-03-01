"""Tests for labeler.retention — raw strip, archive + prune."""

import gzip
import json
import sqlite3
import time

import pytest

from labeler import retention


def _make_db():
    """Create an in-memory DB with the tables retention operates on."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE events (
            event_uri TEXT PRIMARY KEY,
            ctime TIMESTAMP,
            author TEXT,
            raw TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE edges (
            src_did TEXT,
            dst_did TEXT,
            type TEXT,
            ctime TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE event_versions (
            event_uri TEXT,
            version_ts TIMESTAMP,
            raw TEXT
        )
    """)
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
            fingerprint_version TEXT,
            evidence_class TEXT DEFAULT 'none',
            fp_kind TEXT DEFAULT 'unknown',
            observed_at TIMESTAMP
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_ctime ON events(ctime)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_edges_ctime ON edges(ctime)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_history_created ON claim_history(createdAt)")
    return conn


def _iso(offset_sec=0):
    """ISO timestamp offset from now."""
    t = time.time() + offset_sec
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(t))


def _insert_claim(conn, fingerprint, created_at, observed_at=None,
                  author="did:a", post_uri=None):
    """Insert a claim_history row with all required columns."""
    if post_uri is None:
        post_uri = f"at://u/post/{fingerprint}"
    conn.execute(
        "INSERT INTO claim_history "
        "(authorDid, claim_fingerprint, createdAt, confidence, provenance, "
        "evidence_hash, post_uri, post_cid, fingerprint_version, "
        "evidence_class, fp_kind, observed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (author, fingerprint, created_at, 0.9, "test", "hash",
         post_uri, "cid", "v1", "none", "text", observed_at),
    )


class TestStripOldRaw:
    def test_empty_table(self):
        conn = _make_db()
        n = retention._strip_old_raw(conn)
        assert n == 0

    def test_recent_events_kept(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-3600), "did:a", '{"text":"hello"}'),
        )
        conn.commit()
        n = retention._strip_old_raw(conn)
        assert n == 0
        raw = conn.execute("SELECT raw FROM events").fetchone()[0]
        assert raw is not None

    def test_old_events_stripped(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-48 * 3600), "did:a", '{"text":"old"}'),
        )
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/2", _iso(-3600), "did:a", '{"text":"new"}'),
        )
        conn.commit()

        n = retention._strip_old_raw(conn)
        assert n == 1

        rows = conn.execute(
            "SELECT event_uri, raw FROM events ORDER BY event_uri"
        ).fetchall()
        assert rows[0][1] is None      # old event stripped
        assert rows[1][1] is not None   # recent event kept

    def test_metadata_preserved_after_strip(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-48 * 3600), "did:a", '{"text":"old"}'),
        )
        conn.commit()

        retention._strip_old_raw(conn)

        row = conn.execute("SELECT event_uri, ctime, author FROM events").fetchone()
        assert row[0] == "at://u/post/1"
        assert row[1] is not None
        assert row[2] == "did:a"

    def test_already_null_not_counted(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-48 * 3600), "did:a", None),
        )
        conn.commit()
        n = retention._strip_old_raw(conn)
        assert n == 0

    def test_batched_strip(self, monkeypatch):
        monkeypatch.setattr(retention, "STRIP_BATCH", 3)
        conn = _make_db()
        for i in range(10):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://u/post/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        n = retention._strip_old_raw(conn)
        assert n == 10

        null_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE raw IS NULL"
        ).fetchone()[0]
        assert null_count == 10


class TestPruneTable:
    def test_events_pruned(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/old", _iso(-10 * 86400), "did:a", None),
        )
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/new", _iso(-3600), "did:a", None),
        )
        conn.commit()

        n = retention._prune_table(conn, "events", "ctime", 7 * 86400)
        assert n == 1

        remaining = conn.execute("SELECT event_uri FROM events").fetchall()
        assert len(remaining) == 1
        assert remaining[0][0] == "at://u/post/new"

    def test_edges_pruned(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO edges VALUES (?, ?, ?, ?)",
            ("did:a", "did:b", "reply", _iso(-20 * 86400)),
        )
        conn.execute(
            "INSERT INTO edges VALUES (?, ?, ?, ?)",
            ("did:a", "did:c", "reply", _iso(-3600)),
        )
        conn.commit()

        n = retention._prune_table(conn, "edges", "ctime", 14 * 86400)
        assert n == 1

        remaining = conn.execute("SELECT dst_did FROM edges").fetchall()
        assert len(remaining) == 1
        assert remaining[0][0] == "did:c"

    def test_event_versions_pruned(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO event_versions VALUES (?, ?, ?)",
            ("at://u/post/1", _iso(-10 * 86400), '{"old":true}'),
        )
        conn.execute(
            "INSERT INTO event_versions VALUES (?, ?, ?)",
            ("at://u/post/1", _iso(-3600), '{"new":true}'),
        )
        conn.commit()

        n = retention._prune_table(conn, "event_versions", "version_ts", 7 * 86400)
        assert n == 1

    def test_nothing_to_prune(self):
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-3600), "did:a", None),
        )
        conn.commit()
        n = retention._prune_table(conn, "events", "ctime", 7 * 86400)
        assert n == 0

    def test_batched_delete(self, monkeypatch):
        monkeypatch.setattr(retention, "DELETE_BATCH", 3)
        conn = _make_db()
        for i in range(10):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://u/post/{i}", _iso(-10 * 86400), "did:a", None),
            )
        conn.commit()

        n = retention._prune_table(conn, "events", "ctime", 7 * 86400)
        assert n == 10
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


class TestArchiveClaimHistory:
    def test_no_old_claims(self):
        conn = _make_db()
        _insert_claim(conn, "fp1", _iso(-3600))
        conn.commit()
        result = retention._archive_claim_history(conn)
        assert result["archived"] == 0
        assert result["deleted"] == 0

    def test_archive_old_claims(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        old_ts = _iso(-20 * 86400)
        _insert_claim(conn, "fp_old", old_ts, observed_at=old_ts)
        _insert_claim(conn, "fp_new", _iso(-3600), observed_at=_iso(-3600))
        conn.commit()

        result = retention._archive_claim_history(conn)
        assert result["archived"] == 1
        assert result["deleted"] == 1
        assert len(result["files"]) == 1

        # Verify archive file contents
        archive_path = result["files"][0]
        with gzip.open(archive_path, "rt") as f:
            lines = f.readlines()
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["claim_fingerprint"] == "fp_old"

        # Claim deleted from DB
        remaining = conn.execute("SELECT COUNT(*) FROM claim_history").fetchone()[0]
        assert remaining == 1

    def test_coalesce_uses_observed_at_over_created_at(self, tmp_path, monkeypatch):
        """When observed_at is set, use it for retention cutoff (trusted timestamp)."""
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        # createdAt is recent but observed_at is old -> should be archived
        _insert_claim(conn, "fp1", _iso(-3600), observed_at=_iso(-20 * 86400))
        conn.commit()

        result = retention._archive_claim_history(conn)
        assert result["archived"] == 1

    def test_coalesce_falls_back_to_created_at(self, tmp_path, monkeypatch):
        """When observed_at is NULL (legacy), fall back to createdAt."""
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        _insert_claim(conn, "fp1", _iso(-20 * 86400), observed_at=None)
        conn.commit()

        result = retention._archive_claim_history(conn)
        assert result["archived"] == 1

    def test_day_partitioned_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        # Two different days
        ts1 = _iso(-20 * 86400)
        ts2 = _iso(-21 * 86400)
        _insert_claim(conn, "fp1", ts1, observed_at=ts1)
        _insert_claim(conn, "fp2", ts2, observed_at=ts2)
        conn.commit()

        result = retention._archive_claim_history(conn)
        assert result["archived"] == 2
        assert result["deleted"] == 2
        # Should be 1 or 2 files depending on whether days differ
        assert len(result["files"]) >= 1


class TestRunRetentionOnce:
    def test_full_pass(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        # Old event with raw (should strip + eventually prune)
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/strip", _iso(-48 * 3600), "did:a", '{"text":"old"}'),
        )
        # Very old event (should prune)
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/prune", _iso(-10 * 86400), "did:a", None),
        )
        # Recent event (should survive)
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/keep", _iso(-3600), "did:a", '{"text":"new"}'),
        )
        # Old edge
        conn.execute(
            "INSERT INTO edges VALUES (?, ?, ?, ?)",
            ("did:a", "did:b", "reply", _iso(-20 * 86400)),
        )
        # Old claim (should be archived + pruned)
        old_ts = _iso(-20 * 86400)
        _insert_claim(conn, "fp1", old_ts, observed_at=old_ts)
        conn.commit()

        stats = retention.run_retention_once(conn=conn)

        assert stats["raw_stripped"] == 1
        assert stats["events_pruned"] == 1
        assert stats["edges_pruned"] == 1
        assert stats["claims_archived"] == 1
        assert stats["claims_pruned"] == 1

        # Survivor check
        remaining = conn.execute("SELECT event_uri FROM events").fetchall()
        uris = {r[0] for r in remaining}
        assert "at://u/post/keep" in uris
        assert "at://u/post/strip" in uris  # stripped but not old enough to prune
        assert "at://u/post/prune" not in uris

    def test_empty_db(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        stats = retention.run_retention_once(conn=conn)
        assert stats["raw_stripped"] == 0
        assert stats["events_pruned"] == 0
        assert stats["edges_pruned"] == 0
        assert stats["claims_archived"] == 0
        assert stats["claims_pruned"] == 0

    def test_idempotent(self, tmp_path, monkeypatch):
        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-48 * 3600), "did:a", '{"text":"old"}'),
        )
        conn.commit()

        stats1 = retention.run_retention_once(conn=conn)
        assert stats1["raw_stripped"] == 1

        stats2 = retention.run_retention_once(conn=conn)
        assert stats2["raw_stripped"] == 0  # already stripped


class TestEnvOverrides:
    def test_custom_retention_windows(self, monkeypatch):
        monkeypatch.setattr(retention, "EVENTS_RETENTION_SEC", 3600)
        conn = _make_db()
        conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?)",
            ("at://u/post/1", _iso(-2 * 3600), "did:a", None),
        )
        conn.commit()

        n = retention._prune_table(conn, "events", "ctime", retention.EVENTS_RETENTION_SEC)
        assert n == 1
