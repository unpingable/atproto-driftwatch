"""Tests for DID document resolver sidecar."""

import json
import time
from unittest.mock import patch, MagicMock
import urllib.error

import pytest
from labeler.resolver import (
    resolve_did,
    ResolvedDID,
    _write_resolution,
    fetch_unresolved_batch,
    resolve_batch,
    STALE_AFTER_S,
    RETRY_BACKOFF_S,
)
from labeler.identity import parse_identity_event, apply_identity_event
from labeler.db import get_conn, init_db
from labeler import timeutil


@pytest.fixture(autouse=True)
def setup_db(tmp_path, monkeypatch):
    """Isolate DB to tmp_path."""
    import labeler.db as db_mod
    monkeypatch.setattr(db_mod, "DATA_DIR", tmp_path)
    init_db()


def _mock_did_doc(did="did:plc:test123", handle="alice.bsky.social",
                  pds="https://pds.example.com"):
    return json.dumps({
        "id": did,
        "alsoKnownAs": [f"at://{handle}"],
        "service": [
            {
                "id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": pds,
            }
        ],
    }).encode()


class TestResolveDID:
    @patch("labeler.resolver.urllib.request.urlopen")
    def test_resolve_plc_did(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = _mock_did_doc()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = resolve_did("did:plc:test123")
        assert result.status == "ok"
        assert result.pds_endpoint == "https://pds.example.com"
        assert result.pds_host == "pds.example.com"
        assert result.handle == "alice.bsky.social"

    @patch("labeler.resolver.urllib.request.urlopen")
    def test_resolve_web_did(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = _mock_did_doc(
            did="did:web:example.com",
            handle="example.com",
            pds="https://pds.third-party.com",
        )
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = resolve_did("did:web:example.com")
        assert result.status == "ok"
        assert result.pds_host == "pds.third-party.com"

    @patch("labeler.resolver.urllib.request.urlopen")
    def test_resolve_404(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 404, "Not Found", {}, None
        )
        result = resolve_did("did:plc:nonexistent")
        assert result.status == "not_found"
        assert result.error == "http_404"

    @patch("labeler.resolver.urllib.request.urlopen")
    def test_resolve_server_error(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.HTTPError(
            "url", 500, "Server Error", {}, None
        )
        result = resolve_did("did:plc:broken")
        assert result.status == "error"
        assert result.error == "http_500"

    @patch("labeler.resolver.urllib.request.urlopen")
    def test_resolve_timeout(self, mock_urlopen):
        mock_urlopen.side_effect = TimeoutError()
        result = resolve_did("did:plc:slow")
        assert result.status == "error"
        assert result.error == "TimeoutError"

    def test_unsupported_method(self):
        result = resolve_did("did:key:z6Mk...")
        assert result.status == "error"
        assert "unsupported_method" in result.error

    @patch("labeler.resolver.urllib.request.urlopen")
    def test_no_pds_service(self, mock_urlopen):
        doc = json.dumps({
            "id": "did:plc:nopds",
            "alsoKnownAs": ["at://nopds.test"],
            "service": [],
        }).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = doc
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = resolve_did("did:plc:nopds")
        assert result.status == "ok"
        assert result.pds_endpoint is None
        assert result.pds_host is None


class TestWriteResolution:
    def test_write_creates_new_row(self):
        result = ResolvedDID(
            did="did:plc:new1", pds_endpoint="https://pds.example.com",
            pds_host="pds.example.com", handle="new.user", status="ok",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT pds_endpoint, pds_host, resolver_status FROM actor_identity_current WHERE did = ?",
            ("did:plc:new1",),
        ).fetchone()
        conn.close()
        assert row[0] == "https://pds.example.com"
        assert row[1] == "pds.example.com"
        assert row[2] == "ok"

    def test_write_updates_existing_row(self):
        # Create row via identity event first
        apply_identity_event(parse_identity_event({
            "did": "did:plc:existing",
            "time_us": 1000000,
            "kind": "identity",
            "identity": {"did": "did:plc:existing", "handle": "id-handle.test"},
        }))

        # Resolve and write back
        result = ResolvedDID(
            did="did:plc:existing", pds_endpoint="https://pds.example.com",
            pds_host="pds.example.com", handle="resolver-handle.test", status="ok",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT handle, pds_endpoint, resolver_status FROM actor_identity_current WHERE did = ?",
            ("did:plc:existing",),
        ).fetchone()
        conn.close()
        assert row[0] == "id-handle.test"  # identity event handle preserved
        assert row[1] == "https://pds.example.com"  # PDS filled in
        assert row[2] == "ok"

    def test_resolver_handle_does_not_overwrite_identity_handle(self):
        """Identity events set handle; resolver should not overwrite it."""
        apply_identity_event(parse_identity_event({
            "did": "did:plc:hashandle",
            "time_us": 1000000,
            "kind": "identity",
            "identity": {"did": "did:plc:hashandle", "handle": "real.handle"},
        }))

        result = ResolvedDID(
            did="did:plc:hashandle", pds_endpoint="https://pds.test",
            pds_host="pds.test", handle="resolver.handle", status="ok",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT handle FROM actor_identity_current WHERE did = ?",
            ("did:plc:hashandle",),
        ).fetchone()
        conn.close()
        assert row[0] == "real.handle"

    def test_resolver_handle_fills_null(self):
        """If identity event didn't set handle, resolver should fill it."""
        # Account event — no handle
        apply_identity_event(parse_identity_event({
            "did": "did:plc:nohandle",
            "time_us": 1000000,
            "kind": "account",
            "account": {"did": "did:plc:nohandle", "active": True},
        }))

        result = ResolvedDID(
            did="did:plc:nohandle", pds_endpoint="https://pds.test",
            pds_host="pds.test", handle="resolved.handle", status="ok",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT handle FROM actor_identity_current WHERE did = ?",
            ("did:plc:nohandle",),
        ).fetchone()
        conn.close()
        assert row[0] == "resolved.handle"

    def test_write_not_found(self):
        result = ResolvedDID(
            did="did:plc:gone", pds_endpoint=None, pds_host=None,
            handle=None, status="not_found", error="http_404",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT resolver_status, resolver_error FROM actor_identity_current WHERE did = ?",
            ("did:plc:gone",),
        ).fetchone()
        conn.close()
        assert row[0] == "not_found"
        assert row[1] == "http_404"

    def test_write_error(self):
        result = ResolvedDID(
            did="did:plc:err", pds_endpoint=None, pds_host=None,
            handle=None, status="error", error="http_500",
        )
        _write_resolution(result)

        conn = get_conn()
        row = conn.execute(
            "SELECT resolver_status, resolver_error, resolver_last_success_at"
            " FROM actor_identity_current WHERE did = ?",
            ("did:plc:err",),
        ).fetchone()
        conn.close()
        assert row[0] == "error"
        assert row[2] is None  # no success timestamp


class TestFetchUnresolvedBatch:
    def test_unresolved_dids_returned(self):
        # Create rows via identity events (no resolver_status set)
        for i in range(5):
            apply_identity_event(parse_identity_event({
                "did": f"did:plc:unresolved{i}",
                "time_us": 1000000 + i,
                "kind": "identity",
                "identity": {"did": f"did:plc:unresolved{i}"},
            }))

        batch = fetch_unresolved_batch(limit=3)
        assert len(batch) == 3

    def test_resolved_dids_skipped(self):
        apply_identity_event(parse_identity_event({
            "did": "did:plc:resolved1",
            "time_us": 1000000,
            "kind": "identity",
            "identity": {"did": "did:plc:resolved1"},
        }))
        # Mark as resolved
        result = ResolvedDID(
            did="did:plc:resolved1", pds_endpoint="https://pds.test",
            pds_host="pds.test", handle="a.test", status="ok",
        )
        _write_resolution(result)

        batch = fetch_unresolved_batch()
        assert "did:plc:resolved1" not in batch

    def test_empty_table(self):
        batch = fetch_unresolved_batch()
        assert batch == []


class TestResolveBatch:
    @patch("labeler.resolver.resolve_did")
    def test_batch_resolves_and_writes(self, mock_resolve):
        mock_resolve.return_value = ResolvedDID(
            did="did:plc:batch1", pds_endpoint="https://pds.test",
            pds_host="pds.test", handle="batch.user", status="ok",
        )

        apply_identity_event(parse_identity_event({
            "did": "did:plc:batch1",
            "time_us": 1000000,
            "kind": "identity",
            "identity": {"did": "did:plc:batch1"},
        }))

        stats = resolve_batch()
        assert stats["resolved"] >= 1
        assert stats["ok"] >= 1

        conn = get_conn()
        row = conn.execute(
            "SELECT pds_host, resolver_status FROM actor_identity_current WHERE did = ?",
            ("did:plc:batch1",),
        ).fetchone()
        conn.close()
        assert row[0] == "pds.test"
        assert row[1] == "ok"

    def test_empty_batch_returns_zeros(self):
        stats = resolve_batch()
        assert stats == {"resolved": 0, "ok": 0, "not_found": 0, "error": 0}
