"""Tests for labeler.retention — raw strip, archive + prune."""

import gzip
import json
import sqlite3
import time

import pytest

from labeler import retention


def _make_db(path=":memory:"):
    """Create a DB with the tables retention operates on.

    Default in-memory; pass a file path for tests that need multiple
    connections to see the same data (e.g. the writer-thread path).
    check_same_thread=False so connections can be shared across the
    asyncio default executor's threads.
    """
    conn = sqlite3.connect(path, check_same_thread=False)
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


# --- Writer-thread path tests (single-writer invariant) ---

class _StubConsumer:
    """Minimal stand-in for ATProtoConsumer.

    submit_mutation runs the callable synchronously on a real connection,
    serializing through the test's event loop. get_ingest_backlog is fixed.
    """

    def __init__(self, conn, backlog=0):
        self._conn = conn
        self._backlog = backlog

    async def submit_mutation(self, fn, *args, **kwargs):
        return fn(self._conn, *args, **kwargs)

    def get_ingest_backlog(self):
        return self._backlog


def _setup_writer_path_db(tmp_path, monkeypatch):
    """Set up a file-backed DB and monkey-patch get_conn so the async
    retention path's short-lived read connections see the same data.
    Returns (writer_conn, db_path)."""
    db_path = str(tmp_path / "writer-path.sqlite")
    conn = _make_db(db_path)
    from labeler import db as _db

    def _fresh_conn():
        rc = sqlite3.connect(db_path, check_same_thread=False)
        rc.execute("PRAGMA journal_mode=WAL")
        return rc

    monkeypatch.setattr(_db, "get_conn", _fresh_conn)
    return conn, db_path


class TestRunRetentionOnceAsync:
    def test_strips_via_writer_thread(self, tmp_path, monkeypatch):
        import asyncio

        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn, _ = _setup_writer_path_db(tmp_path, monkeypatch)
        for i in range(5):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://u/post/{i}", _iso(-48 * 3600), "did:a", f'{{"x":{i}}}'),
            )
        conn.commit()

        consumer = _StubConsumer(conn)
        stats = asyncio.run(retention.run_retention_once_async(consumer))

        assert stats["raw_stripped"] == 5
        assert stats["raw_stripped_chunks"]["failed"] == 0
        assert stats["raw_stripped_chunks"]["completed"] >= 3  # ceil(5/2) chunks

        # Verify all rows actually stripped
        nulls = conn.execute(
            "SELECT COUNT(*) FROM events WHERE raw IS NULL"
        ).fetchone()[0]
        assert nulls == 5

    def test_prunes_events_and_edges_via_writer_thread(self, tmp_path, monkeypatch):
        import asyncio

        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "DELETE_BATCH", 2)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn, _ = _setup_writer_path_db(tmp_path, monkeypatch)
        for i in range(3):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://e/{i}", _iso(-10 * 86400), "did:a", None),
            )
        for i in range(4):
            conn.execute(
                "INSERT INTO edges VALUES (?, ?, ?, ?)",
                ("did:a", f"did:b{i}", "reply", _iso(-30 * 86400)),
            )
        conn.commit()

        consumer = _StubConsumer(conn)
        stats = asyncio.run(retention.run_retention_once_async(consumer))

        assert stats["events_pruned"] == 3
        assert stats["edges_pruned"] == 4
        assert stats["events_pruned_chunks"]["failed"] == 0
        assert stats["edges_pruned_chunks"]["failed"] == 0

    def test_honest_stats_under_chunk_failure(self, tmp_path, monkeypatch):
        """A failing chunk increments failed counter; no -1 sentinels.

        Under the single-writer invariant, lock conflicts should not happen
        in steady state — the writer thread is the sole writer. So a
        chunk_failed > 0 is a real signal (cron overlap, CLI conflict, bug)
        rather than a transient. The current design honors that by ending
        the op on first failure (n=0 < BATCH triggers loop exit), with
        chunks_failed as the honest counter.
        """
        import asyncio

        monkeypatch.setattr(retention, "ARCHIVE_DIR", tmp_path)
        monkeypatch.setattr(retention, "STRIP_BATCH", 5)
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 0.0)

        conn, _ = _setup_writer_path_db(tmp_path, monkeypatch)
        for i in range(10):
            conn.execute(
                "INSERT INTO events VALUES (?, ?, ?, ?)",
                (f"at://x/{i}", _iso(-48 * 3600), "did:a", '{"x":1}'),
            )
        conn.commit()

        # First chunk raises; remaining iterations don't run because the
        # loop exits when n < STRIP_BATCH (n=0 in the except branch).
        def always_fails(conn, cutoff_iso):
            raise RuntimeError("simulated lock conflict")

        monkeypatch.setattr(retention, "_strip_raw_chunk", always_fails)

        consumer = _StubConsumer(conn)
        stats = asyncio.run(retention.run_retention_once_async(consumer))

        chunks = stats["raw_stripped_chunks"]
        assert chunks["failed"] == 1
        assert chunks["attempted"] == 1
        assert chunks["completed"] == 0
        assert stats["raw_stripped"] == 0
        # No negative sentinels in any int field.
        for k, v in stats.items():
            if isinstance(v, int):
                assert v >= 0, f"{k} should not be negative; got {v}"

    def test_adaptive_sleep_respects_backlog(self, monkeypatch):
        """High backlog -> longer sleep. Low backlog -> base sleep."""
        monkeypatch.setattr(retention, "BATCH_SLEEP_SEC", 1.0)
        monkeypatch.setattr(retention, "BACKLOG_MED", 100)
        monkeypatch.setattr(retention, "BACKLOG_HIGH", 500)

        assert retention._adaptive_sleep(lambda: 0) == 1.0
        assert retention._adaptive_sleep(lambda: 50) == 1.0
        assert retention._adaptive_sleep(lambda: 200) == 2.0
        assert retention._adaptive_sleep(lambda: 1000) == 4.0
        # None get_backlog falls back to base
        assert retention._adaptive_sleep(None) == 1.0
