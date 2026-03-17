"""Tests for identity event capture and current-state reduction."""

import json
import pytest
from labeler.identity import (
    MISSING,
    IdentityDelta,
    parse_identity_event,
    apply_identity_event,
    _event_is_newer,
)
from labeler.db import get_conn, init_db


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    """Isolate DB to tmp_path."""
    import labeler.db as db_mod
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    init_db()


def _identity_event(did="did:plc:abc123", handle="user.bsky.social",
                     time_us=1000000, seq=100):
    return {
        "did": did,
        "time_us": time_us,
        "kind": "identity",
        "identity": {
            "did": did,
            "handle": handle,
            "seq": seq,
            "time": "2024-09-05T06:11:04.870Z",
        },
    }


def _account_event(did="did:plc:abc123", active=True, time_us=1000000, seq=100):
    return {
        "did": did,
        "time_us": time_us,
        "kind": "account",
        "account": {
            "active": active,
            "did": did,
            "seq": seq,
            "time": "2024-09-05T06:11:04.870Z",
        },
    }


class TestParsing:
    def test_parse_identity_event(self):
        js = _identity_event(handle="alice.bsky.social")
        delta = parse_identity_event(js)
        assert delta is not None
        assert delta.did == "did:plc:abc123"
        assert delta.event_kind == "identity"
        assert delta.handle == "alice.bsky.social"
        assert delta.active is MISSING

    def test_parse_account_event(self):
        js = _account_event(active=True)
        delta = parse_identity_event(js)
        assert delta is not None
        assert delta.event_kind == "account"
        assert delta.active is True
        assert delta.handle is MISSING

    def test_parse_commit_returns_none(self):
        js = {"did": "did:plc:abc", "time_us": 100, "kind": "commit",
              "commit": {"operation": "create"}}
        assert parse_identity_event(js) is None

    def test_parse_unknown_kind_returns_none(self):
        js = {"did": "did:plc:abc", "time_us": 100, "kind": "something_new"}
        assert parse_identity_event(js) is None

    def test_parse_missing_did_returns_none(self):
        js = {"time_us": 100, "kind": "identity", "identity": {"handle": "x"}}
        assert parse_identity_event(js) is None

    def test_parse_missing_time_us_returns_none(self):
        js = {"did": "did:plc:abc", "kind": "identity",
              "identity": {"handle": "x"}}
        assert parse_identity_event(js) is None


class TestMissingSentinel:
    def test_missing_is_not_none(self):
        assert MISSING is not None

    def test_missing_is_not_empty_string(self):
        assert MISSING != ""

    def test_missing_is_not_false(self):
        assert MISSING is not False


class TestEventIsNewer:
    def test_newer_timestamp(self):
        assert _event_is_newer(2000, 1000) is True

    def test_older_timestamp(self):
        assert _event_is_newer(1000, 2000) is False

    def test_equal_timestamp(self):
        assert _event_is_newer(1000, 1000) is True

    def test_none_existing(self):
        assert _event_is_newer(1000, None) is True


class TestApplyCreatesNew:
    def test_first_identity_event_creates_row(self):
        js = _identity_event(handle="alice.example")
        delta = parse_identity_event(js)
        apply_identity_event(delta)

        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM actor_identity_current WHERE did = ?",
            (delta.did,),
        ).fetchone()
        conn.close()
        assert row is not None

    def test_new_row_has_handle(self):
        delta = parse_identity_event(_identity_event(handle="bob.test"))
        apply_identity_event(delta)

        conn = get_conn()
        row = conn.execute(
            "SELECT handle FROM actor_identity_current WHERE did = ?",
            (delta.did,),
        ).fetchone()
        conn.close()
        assert row[0] == "bob.test"

    def test_new_row_defaults_active(self):
        """Identity event has no active field — should default to 1."""
        delta = parse_identity_event(_identity_event())
        apply_identity_event(delta)

        conn = get_conn()
        row = conn.execute(
            "SELECT is_active FROM actor_identity_current WHERE did = ?",
            (delta.did,),
        ).fetchone()
        conn.close()
        assert row[0] == 1


class TestSparseOverwriteProtection:
    def test_account_event_does_not_erase_handle(self):
        """Identity sets handle; later account event must not erase it."""
        apply_identity_event(
            parse_identity_event(_identity_event(handle="alice.test", time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_account_event(active=True, time_us=2000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT handle, is_active FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == "alice.test"  # handle preserved
        assert row[1] == 1  # active updated

    def test_identity_event_does_not_erase_active(self):
        """Account sets active=False; later identity must not reset it."""
        apply_identity_event(
            parse_identity_event(_account_event(active=False, time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_identity_event(handle="bob.test", time_us=2000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT handle, is_active FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == "bob.test"
        assert row[1] == 0  # active preserved from earlier event


class TestOutOfOrderEvents:
    def test_older_event_does_not_overwrite_handle(self):
        """Newer event sets handle; older event arriving later must not replace it."""
        apply_identity_event(
            parse_identity_event(_identity_event(handle="newer.handle", time_us=2000))
        )
        apply_identity_event(
            parse_identity_event(_identity_event(handle="older.handle", time_us=1000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT handle FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == "newer.handle"

    def test_older_event_updates_first_seen(self):
        """Out-of-order event with earlier timestamp should move first_seen_at earlier."""
        apply_identity_event(
            parse_identity_event(_identity_event(time_us=2000000))
        )
        apply_identity_event(
            parse_identity_event(_identity_event(time_us=1000000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT first_seen_at, last_seen_at FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        # first_seen should be the earlier timestamp
        assert row[0] < row[1]


class TestTombstone:
    def test_tombstone_marks_inactive(self):
        apply_identity_event(
            parse_identity_event(_identity_event(handle="alive.user", time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_account_event(active=False, time_us=2000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT is_active FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == 0

    def test_tombstone_preserves_handle(self):
        apply_identity_event(
            parse_identity_event(_identity_event(handle="alive.user", time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_account_event(active=False, time_us=2000))
        )

        conn = get_conn()
        row = conn.execute(
            "SELECT handle FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == "alive.user"

    def test_row_not_deleted_on_tombstone(self):
        apply_identity_event(
            parse_identity_event(_identity_event(time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_account_event(active=False, time_us=2000))
        )

        conn = get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()[0]
        conn.close()
        assert count == 1


class TestReplayIdempotency:
    def test_same_event_twice_produces_same_state(self):
        js = _identity_event(handle="alice.test", time_us=1000)
        apply_identity_event(parse_identity_event(js))
        apply_identity_event(parse_identity_event(js))

        conn = get_conn()
        row = conn.execute(
            "SELECT handle, first_seen_at, last_seen_at FROM actor_identity_current"
            " WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == "alice.test"
        assert row[1] == row[2]  # same timestamp

    def test_replay_appends_to_log(self):
        """Raw log is append-only — replays create duplicate rows (acceptable)."""
        js = _identity_event(time_us=1000)
        apply_identity_event(parse_identity_event(js))
        apply_identity_event(parse_identity_event(js))

        conn = get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM identity_events WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()[0]
        conn.close()
        assert count == 2


class TestAppendOnlyLog:
    def test_events_appended(self):
        apply_identity_event(
            parse_identity_event(_identity_event(time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_account_event(time_us=2000))
        )

        conn = get_conn()
        rows = conn.execute(
            "SELECT event_kind FROM identity_events WHERE did = ? ORDER BY time_us",
            ("did:plc:abc123",),
        ).fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0][0] == "identity"
        assert rows[1][0] == "account"

    def test_raw_payload_preserved(self):
        js = _identity_event(handle="raw.test", time_us=1000)
        apply_identity_event(parse_identity_event(js))

        conn = get_conn()
        raw = conn.execute(
            "SELECT raw FROM identity_events WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()[0]
        conn.close()
        parsed = json.loads(raw)
        assert parsed["identity"]["handle"] == "raw.test"


class TestReducerVersion:
    def test_reducer_version_set(self):
        apply_identity_event(
            parse_identity_event(_identity_event(time_us=1000))
        )
        conn = get_conn()
        row = conn.execute(
            "SELECT reducer_version FROM actor_identity_current WHERE did = ?",
            ("did:plc:abc123",),
        ).fetchone()
        conn.close()
        assert row[0] == 1


class TestMultipleDids:
    def test_different_dids_get_separate_rows(self):
        apply_identity_event(
            parse_identity_event(_identity_event(did="did:plc:aaa", handle="a.test", time_us=1000))
        )
        apply_identity_event(
            parse_identity_event(_identity_event(did="did:plc:bbb", handle="b.test", time_us=1000))
        )

        conn = get_conn()
        count = conn.execute("SELECT COUNT(*) FROM actor_identity_current").fetchone()[0]
        conn.close()
        assert count == 2
