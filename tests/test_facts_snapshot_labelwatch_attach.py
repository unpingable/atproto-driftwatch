"""Acceptance test 6 of gap-spec-facts-export-duckdb-snapshot-001.

Runs the REAL labelwatch consumer (`scan._sync_driftwatch_facts`, the derive
pass) against a snapshot produced by ``export_snapshot_once`` — not against a
hand-rolled facts.sqlite. This is the receipt that the Phase 3 writer serves
the existing ATTACH contract unchanged:

  - labelwatch derives ``derived_label_fp`` rows from the snapshot without
    raising, and ``lag_sec_claimed`` lands in sensible bounds;
  - rows the writer quarantined (bogus ``created_epoch``) never become
    derived rows — the silently-wrong 25-year lag the ratification calls out
    cannot reach the consumer;
  - the attach dance leaves no temp tables and no lingering ``drift`` attach.

Cross-repo: imports labelwatch from the sibling checkout
(``<workbench>/labelwatch/src``). Skips cleanly when the sibling is absent
(e.g. driftwatch cloned standalone) — the in-repo snapshot tests
(test_facts_export_duckdb_snapshot.py) still cover the producer alone.
"""

import pathlib
import sqlite3
import sys
import time

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from labeler.facts_export_duckdb_snapshot import export_snapshot_once  # noqa: E402

_LABELWATCH_SRC = pathlib.Path(__file__).resolve().parents[2] / "labelwatch" / "src"
if _LABELWATCH_SRC.exists() and str(_LABELWATCH_SRC) not in sys.path:
    sys.path.insert(0, str(_LABELWATCH_SRC))

labelwatch_scan = pytest.importorskip(
    "labelwatch.scan",
    reason="labelwatch sibling checkout not available",
)
# Guard against exercising a stray installed/cached labelwatch instead of
# the sibling checkout this receipt is about. (sys.path stays prepended for
# the session — intra-labelwatch lazy imports must keep resolving — but the
# module origin is pinned.)
if not str(labelwatch_scan.__file__).startswith(str(_LABELWATCH_SRC)):
    pytest.skip(
        f"labelwatch imported from {labelwatch_scan.__file__}, "
        f"not the sibling checkout {_LABELWATCH_SRC}",
        allow_module_level=True,
    )
from labelwatch import db as lw_db  # noqa: E402
from labelwatch.config import Config as LwConfig  # noqa: E402
from labelwatch.scan import _sync_driftwatch_facts  # noqa: E402


GOOD_URI = "at://did:plc:author/app.bsky.feed.post/good1"
BOGUS_URI = "at://did:plc:author/app.bsky.feed.post/bogus1997"


def _iso_z(epoch: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def _iso_offset(epoch: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(epoch))


def _make_identity_db(path: pathlib.Path) -> pathlib.Path:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
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
        """
    )
    conn.execute(
        "INSERT INTO actor_identity_current "
        "(did, handle, pds_endpoint, pds_host, resolver_status, "
        " resolver_last_success_at, is_active, identity_source) "
        "VALUES ('did:plc:author', 'author.test', 'https://pds1.example', "
        "        'pds1.example', 'ok', '2026-06-01T00:00:00Z', 1, 'live')"
    )
    conn.commit()
    conn.close()
    return path


def _claim_row(post_uri: str, fingerprint: str, created_epoch: int) -> dict:
    created = _iso_offset(created_epoch)
    return {
        "authorDid": "did:plc:author",
        "claim_fingerprint": fingerprint,
        "createdAt": created,
        "confidence": 0.9,
        "provenance": "test",
        "evidence_hash": f"eh:{fingerprint}",
        "post_uri": post_uri,
        "post_cid": f"cid:{fingerprint}",
        "fingerprint_version": "v1",
        "evidence_class": "none",
        "fp_kind": "claim",
        "observed_at": created,
    }


def _write_claim_parquet(parquet_root: pathlib.Path, rows: list[dict]) -> None:
    day = rows[0]["createdAt"][:10]
    out_dir = parquet_root / "claim_history" / f"date={day}"
    out_dir.mkdir(parents=True)
    schema = pa.schema(
        [
            pa.field(name, pa.float64() if name == "confidence" else pa.string())
            for name in rows[0]
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out_dir / "part-00.parquet")


def _insert_label_event(conn, uri: str, ts: str, val: str) -> None:
    import hashlib

    event_hash = hashlib.sha256(f"{uri}:{ts}:{val}".encode()).hexdigest()[:16]
    conn.execute(
        "INSERT INTO label_events(labeler_did, src, uri, cid, val, neg, exp, sig, ts, event_hash) "
        "VALUES('did:plc:labeler', 'test', ?, 'cid1', ?, 0, NULL, NULL, ?, ?)",
        (uri, val, ts, event_hash),
    )
    conn.commit()


def test_labelwatch_derive_pass_consumes_writer_snapshot(tmp_path):
    now = int(time.time())
    post_epoch = now - 7200          # post created 2h ago
    label_epoch = now - 3600         # labeled 1h ago → lag 3600s
    bogus_epoch = 852_076_800        # 1997-01-01 — writer must quarantine

    # --- producer side: real snapshot writer over parquet + identity ---
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    parquet_root = tmp_path / "parquet"
    _write_claim_parquet(
        parquet_root,
        [
            _claim_row(GOOD_URI, "fp_good", post_epoch),
            _claim_row(BOGUS_URI, "fp_bogus", bogus_epoch),
        ],
    )
    facts_path = tmp_path / "facts.sqlite"
    manifest = export_snapshot_once(
        parquet_root=parquet_root,
        identity_source_path=identity_db,
        output_path=facts_path,
    )
    assert manifest["row_counts"]["uri_fingerprint"] == 1
    assert manifest["uri_fingerprint_rows_quarantined_bogus_created_epoch"] == 1

    # --- consumer side: real labelwatch derive pass, unchanged ---
    lw_conn = lw_db.connect(str(tmp_path / "labelwatch.db"))
    lw_db.init_db(lw_conn)
    _insert_label_event(lw_conn, GOOD_URI, _iso_z(label_epoch), "spam")
    # A label event on the quarantined post: without the writer-side
    # quarantine this would derive lag ≈ 29 years — silently wrong.
    _insert_label_event(lw_conn, BOGUS_URI, _iso_z(label_epoch), "spam")

    config = LwConfig(driftwatch_facts_path=str(facts_path))
    _sync_driftwatch_facts(lw_conn, config)  # must not raise

    rows = lw_conn.execute(
        "SELECT uri, claim_fingerprint, lag_sec_claimed FROM derived_label_fp"
    ).fetchall()
    by_uri = {r["uri"]: r for r in rows}

    # Good row derived with exact, sensible lag.
    assert GOOD_URI in by_uri
    good = by_uri[GOOD_URI]
    assert good["claim_fingerprint"] == "fp_good"
    assert good["lag_sec_claimed"] == label_epoch - post_epoch
    assert 0 <= good["lag_sec_claimed"] <= 86_400

    # Quarantined row never reaches the consumer: no derived row, no
    # decades-long lag.
    assert BOGUS_URI not in by_uri
    assert all(abs(r["lag_sec_claimed"]) < 10 * 365 * 86_400 for r in rows)

    # Attach hygiene: drift detached, temp tables cleaned.
    attached = {r[1] for r in lw_conn.execute("PRAGMA database_list").fetchall()}
    assert "drift" not in attached
    temp_tables = {
        r[0]
        for r in lw_conn.execute(
            "SELECT name FROM sqlite_temp_master WHERE type='table'"
        ).fetchall()
    }
    assert not {"tmp_drift_fp", "tmp_candidate_uris"} & temp_tables


def test_labelwatch_identity_query_shapes_against_writer_snapshot(tmp_path):
    """The hosting.py query shapes from the consumer inventory run unchanged
    against a writer-produced snapshot (coverage probe, denominator count,
    per-DID join key, host-family enumeration)."""
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    facts_path = tmp_path / "facts.sqlite"
    export_snapshot_once(
        parquet_root=tmp_path / "no-parquet",
        identity_source_path=identity_db,
        output_path=facts_path,
    )

    conn = sqlite3.connect(":memory:")
    conn.execute(f"ATTACH DATABASE 'file:{facts_path}?mode=ro' AS drift")

    # Coverage sanity probe (hosting.py L118 et al.)
    assert conn.execute("SELECT 1 FROM drift.actor_identity_facts LIMIT 1").fetchone()
    # Population denominator (L517)
    assert conn.execute("SELECT COUNT(*) FROM drift.actor_identity_facts").fetchone()[0] == 1
    # Host-family enumeration (L505)
    hosts = [r[0] for r in conn.execute(
        "SELECT DISTINCT pds_host FROM drift.actor_identity_facts"
    )]
    assert hosts == ["pds1.example"]
    # JOIN-key shape (L133 et al.): did is usable as the join key
    row = conn.execute(
        "SELECT handle, pds_host, resolver_status FROM drift.actor_identity_facts "
        "WHERE did = 'did:plc:author'"
    ).fetchone()
    assert row == ("author.test", "pds1.example", "ok")
    conn.execute("DETACH DATABASE drift")
    conn.close()
