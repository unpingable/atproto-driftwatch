"""Tests for labeler.seed_targets — resolver queue seeding from labelwatch."""

import sqlite3
import time

from labeler.seed_targets import fetch_top_target_dids, seed_resolver_queue


def _make_labelwatch_db(tmp_path, events=None):
    """Create a minimal labelwatch-like DB with label_events."""
    path = str(tmp_path / "labelwatch.db")
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE label_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labeler_did TEXT NOT NULL,
            uri TEXT NOT NULL,
            val TEXT NOT NULL,
            ts TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE,
            target_did TEXT
        )
    """)
    if events:
        conn.executemany(
            "INSERT INTO label_events (labeler_did, uri, val, ts, event_hash, target_did) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            events,
        )
    conn.commit()
    conn.close()
    return path


def _make_driftwatch_db(tmp_path, existing_dids=None):
    """Create a minimal driftwatch-like DB with actor_identity_current."""
    path = str(tmp_path / "driftwatch.sqlite")
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE actor_identity_current (
            did TEXT PRIMARY KEY,
            handle TEXT,
            is_active INTEGER DEFAULT 1,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            last_event_time_us INTEGER,
            last_event_kind TEXT,
            reducer_version INTEGER NOT NULL DEFAULT 1,
            pds_endpoint TEXT,
            pds_host TEXT,
            resolver_status TEXT,
            resolver_last_attempt_at TIMESTAMP,
            resolver_last_success_at TIMESTAMP,
            resolver_error TEXT,
            identity_source TEXT DEFAULT 'live'
        )
    """)
    if existing_dids:
        for did, source in existing_dids:
            conn.execute(
                "INSERT INTO actor_identity_current (did, identity_source, reducer_version) VALUES (?, ?, 1)",
                (did, source),
            )
    conn.commit()
    conn.close()
    return path


class TestFetchTopTargetDids:
    def test_basic(self, tmp_path):
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        lw_path = _make_labelwatch_db(tmp_path, events=[
            ("lab1", "at://a/post/1", "spam", now, "h1", "did:plc:alice"),
            ("lab1", "at://a/post/2", "spam", now, "h2", "did:plc:alice"),
            ("lab1", "at://b/post/3", "spam", now, "h3", "did:plc:bob"),
            ("lab1", "at://a/post/4", "nsfw", now, "h4", "did:plc:alice"),
        ])
        targets = fetch_top_target_dids(lw_path, days=7, limit=10)
        assert len(targets) == 2
        # alice should be first (3 labels)
        assert targets[0][0] == "did:plc:alice"
        assert targets[0][1] == 3
        assert targets[1][0] == "did:plc:bob"
        assert targets[1][1] == 1

    def test_limit(self, tmp_path):
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        events = [(f"lab1", f"at://d{i}/post/1", "spam", now, f"h{i}", f"did:plc:user{i}")
                   for i in range(100)]
        lw_path = _make_labelwatch_db(tmp_path, events=events)
        targets = fetch_top_target_dids(lw_path, days=7, limit=10)
        assert len(targets) == 10


class TestSeedResolverQueue:
    def test_insert_new(self, tmp_path):
        dw_path = _make_driftwatch_db(tmp_path)
        targets = [("did:plc:alice", 10), ("did:plc:bob", 5)]
        stats = seed_resolver_queue(dw_path, targets)
        assert stats["inserted"] == 2
        assert stats["upgraded"] == 0
        assert stats["skipped"] == 0

        # Verify rows exist with correct source and NULL resolver_status
        conn = sqlite3.connect(dw_path)
        rows = conn.execute(
            "SELECT did, identity_source, resolver_status FROM actor_identity_current ORDER BY did"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("did:plc:alice", "labelwatch_seed", None)
        assert rows[1] == ("did:plc:bob", "labelwatch_seed", None)
        conn.close()

    def test_upgrade_live_to_both(self, tmp_path):
        dw_path = _make_driftwatch_db(tmp_path, existing_dids=[
            ("did:plc:alice", "live"),
        ])
        targets = [("did:plc:alice", 10)]
        stats = seed_resolver_queue(dw_path, targets)
        assert stats["upgraded"] == 1
        assert stats["inserted"] == 0

        conn = sqlite3.connect(dw_path)
        row = conn.execute(
            "SELECT identity_source FROM actor_identity_current WHERE did = ?",
            ("did:plc:alice",),
        ).fetchone()
        assert row[0] == "both"
        conn.close()

    def test_skip_already_seeded(self, tmp_path):
        dw_path = _make_driftwatch_db(tmp_path, existing_dids=[
            ("did:plc:alice", "labelwatch_seed"),
        ])
        targets = [("did:plc:alice", 10)]
        stats = seed_resolver_queue(dw_path, targets)
        assert stats["skipped"] == 1
        assert stats["inserted"] == 0
        assert stats["upgraded"] == 0

    def test_mixed(self, tmp_path):
        dw_path = _make_driftwatch_db(tmp_path, existing_dids=[
            ("did:plc:alice", "live"),
            ("did:plc:carol", "labelwatch_seed"),
        ])
        targets = [
            ("did:plc:alice", 10),  # upgrade to both
            ("did:plc:bob", 5),     # new insert
            ("did:plc:carol", 3),   # skip
        ]
        stats = seed_resolver_queue(dw_path, targets)
        assert stats["inserted"] == 1
        assert stats["upgraded"] == 1
        assert stats["skipped"] == 1
