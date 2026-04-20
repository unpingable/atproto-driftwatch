"""Tests for DID vintage (genesis timestamp) capture and storage."""

import json
from unittest.mock import patch, MagicMock
import urllib.error

import pytest

from labeler.vintage import (
    DIDVintage,
    _extract_genesis_created_at,
    fetch_did_genesis,
    set_vintage,
    set_vintage_many,
    start_export_run,
    finish_export_run,
)
from labeler.db import get_conn, init_db


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    import labeler.db as db_mod
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    init_db()


def _seed_actor(did: str, has_vintage: bool = False):
    conn = get_conn()
    conn.execute(
        "INSERT INTO actor_identity_current "
        "(did, first_seen_at, last_seen_at, reducer_version, resolver_status, "
        " did_created_at, vintage_source) "
        "VALUES (?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 'ok', ?, ?)",
        (
            did,
            "2024-06-01T00:00:00Z" if has_vintage else None,
            "plc_export" if has_vintage else None,
        ),
    )
    conn.commit()
    conn.close()


class TestExtractGenesis:
    def test_returns_first_entry_created_at(self):
        entries = [
            {"createdAt": "2024-06-01T00:00:00Z", "operation": {"type": "plc_operation"}},
            {"createdAt": "2025-01-01T00:00:00Z", "operation": {"type": "plc_operation"}},
        ]
        assert _extract_genesis_created_at(entries) == "2024-06-01T00:00:00Z"

    def test_returns_none_for_empty(self):
        assert _extract_genesis_created_at([]) is None

    def test_returns_none_for_non_list(self):
        assert _extract_genesis_created_at({"not": "a list"}) is None

    def test_returns_none_when_first_has_no_created_at(self):
        assert _extract_genesis_created_at([{"operation": {}}]) is None


class TestFetchDIDGenesis:
    def test_unsupported_method_for_did_web(self):
        result = fetch_did_genesis("did:web:example.com")
        assert result.source == "unsupported_method"
        assert result.created_at is None

    @patch("labeler.vintage.urllib.request.urlopen")
    def test_extracts_genesis_from_log(self, mock_urlopen):
        body = json.dumps([
            {"createdAt": "2024-06-01T00:00:00Z", "operation": {}},
            {"createdAt": "2025-01-01T00:00:00Z", "operation": {}},
        ]).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = fetch_did_genesis("did:plc:abc123")
        assert result.created_at == "2024-06-01T00:00:00Z"
        assert result.source == "plc_audit_log"

    @patch("labeler.vintage.urllib.request.urlopen")
    def test_404_returns_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "u", 404, "Not Found", {}, None,
        )
        result = fetch_did_genesis("did:plc:abc123")
        assert result.created_at is None
        assert result.error == "http_404"


class TestSetVintage:
    def test_writes_when_null(self):
        _seed_actor("did:plc:a")
        conn = get_conn()
        try:
            status = set_vintage(conn, "did:plc:a", "2024-01-01T00:00:00Z", "plc_export", "run1")
            conn.commit()
            assert status == "written"
            row = conn.execute(
                "SELECT did_created_at, vintage_source, vintage_run_id "
                "FROM actor_identity_current WHERE did = ?",
                ("did:plc:a",),
            ).fetchone()
            assert row[0] == "2024-01-01T00:00:00Z"
            assert row[1] == "plc_export"
            assert row[2] == "run1"
        finally:
            conn.close()

    def test_skips_when_already_set(self):
        _seed_actor("did:plc:a", has_vintage=True)
        conn = get_conn()
        try:
            status = set_vintage(conn, "did:plc:a", "2099-01-01T00:00:00Z", "plc_export", "run2")
            conn.commit()
            assert status == "skipped_existing"
            row = conn.execute(
                "SELECT did_created_at FROM actor_identity_current WHERE did = ?",
                ("did:plc:a",),
            ).fetchone()
            # Original value preserved
            assert row[0] == "2024-06-01T00:00:00Z"
        finally:
            conn.close()

    def test_skips_missing_row(self):
        conn = get_conn()
        try:
            status = set_vintage(conn, "did:plc:nope", "2024-01-01T00:00:00Z", "plc_export")
            conn.commit()
            assert status == "skipped_missing_row"
        finally:
            conn.close()

    def test_repair_override_requires_flag(self):
        _seed_actor("did:plc:a", has_vintage=True)
        conn = get_conn()
        try:
            with pytest.raises(ValueError):
                set_vintage(conn, "did:plc:a", "2099-01-01T00:00:00Z", "repair_override")
        finally:
            conn.close()

    def test_repair_override_writes_when_allowed(self):
        _seed_actor("did:plc:a", has_vintage=True)
        conn = get_conn()
        try:
            status = set_vintage(
                conn, "did:plc:a", "2020-01-01T00:00:00Z",
                "repair_override", "repair1", allow_repair=True,
            )
            conn.commit()
            assert status == "repaired"
            row = conn.execute(
                "SELECT did_created_at, vintage_source FROM actor_identity_current WHERE did = ?",
                ("did:plc:a",),
            ).fetchone()
            assert row[0] == "2020-01-01T00:00:00Z"
            assert row[1] == "repair_override"
        finally:
            conn.close()

    def test_invalid_source_rejected(self):
        _seed_actor("did:plc:a")
        conn = get_conn()
        try:
            with pytest.raises(ValueError):
                set_vintage(conn, "did:plc:a", "2024-01-01T00:00:00Z", "bogus_source")
        finally:
            conn.close()


class TestSetVintageMany:
    def test_mixed_batch(self):
        _seed_actor("did:plc:a")
        _seed_actor("did:plc:b", has_vintage=True)
        conn = get_conn()
        try:
            stats = set_vintage_many(
                conn,
                [
                    ("did:plc:a", "2024-01-01T00:00:00Z"),
                    ("did:plc:b", "2099-01-01T00:00:00Z"),
                    ("did:plc:missing", "2024-01-01T00:00:00Z"),
                ],
                source="plc_export",
                run_id="run1",
            )
            conn.commit()
            assert stats == {"written": 1, "skipped_existing": 1, "skipped_missing_row": 1}
            # b should be untouched
            row = conn.execute(
                "SELECT did_created_at FROM actor_identity_current WHERE did = 'did:plc:b'",
            ).fetchone()
            assert row[0] == "2024-06-01T00:00:00Z"
        finally:
            conn.close()

    def test_empty_batch(self):
        conn = get_conn()
        try:
            stats = set_vintage_many(conn, [], source="plc_export")
            assert stats == {"written": 0, "skipped_existing": 0, "skipped_missing_row": 0}
        finally:
            conn.close()


class TestExportRuns:
    def test_run_lifecycle(self):
        conn = get_conn()
        try:
            start_export_run(conn, "run-abc", note="pilot")
            finish_export_run(
                conn, "run-abc",
                first_cursor="2022-11-17T00:00:00Z",
                last_cursor="2026-04-19T00:00:00Z",
                ops_processed=1000, dids_seen=800, genesis_ops_written=800,
                status="completed",
            )
            conn.commit()
            row = conn.execute(
                "SELECT run_id, status, ops_processed, genesis_ops_written, "
                "first_cursor, last_cursor, note "
                "FROM plc_export_runs WHERE run_id = 'run-abc'"
            ).fetchone()
            assert row[0] == "run-abc"
            assert row[1] == "completed"
            assert row[2] == 1000
            assert row[3] == 800
            assert row[4] == "2022-11-17T00:00:00Z"
            assert row[5] == "2026-04-19T00:00:00Z"
            assert row[6] == "pilot"
        finally:
            conn.close()
