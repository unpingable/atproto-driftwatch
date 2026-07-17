"""Tests for DuckDB-backed facts.sqlite snapshot export."""

import datetime as dt
import json
import os
import sqlite3
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from labeler.facts_export import _ensure_tables
from labeler.facts_export_duckdb_snapshot import export_snapshot_once


GENERATED_AT = "2026-06-10T12:00:00Z"


def _make_identity_db(path, rows=None):
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
    if rows is None:
        rows = [
            (
                "did:plc:aaa",
                "alice.test",
                "https://pds1.example",
                "pds1.example",
                "ok",
                "2026-06-01T00:00:00Z",
                1,
                "live",
            ),
            (
                "did:plc:bbb",
                "bob.test",
                "https://pds2.example",
                "pds2.example",
                "ok",
                "2026-06-02T00:00:00Z",
                1,
                "live",
            ),
        ]
    conn.executemany(
        "INSERT INTO actor_identity_current "
        "(did, handle, pds_endpoint, pds_host, resolver_status, "
        "resolver_last_success_at, is_active, identity_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()
    return path


def _write_claim_parquet(parquet_root, date_str="2026-06-09", rows=None):
    if rows is None:
        rows = [
            {
                "authorDid": "did:a",
                "claim_fingerprint": "fp_old",
                "createdAt": "2026-06-09T00:00:00+00:00",
                "confidence": 0.9,
                "provenance": "test",
                "evidence_hash": "eh1",
                "post_uri": "at://did:a/post/1",
                "post_cid": "cid1",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": "2026-06-09T00:01:00+00:00",
            },
            {
                "authorDid": "did:a",
                "claim_fingerprint": "fp_new",
                "createdAt": "2026-06-09T01:00:00+00:00",
                "confidence": 0.9,
                "provenance": "test",
                "evidence_hash": "eh2",
                "post_uri": "at://did:a/post/1",
                "post_cid": "cid1b",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": "2026-06-09T01:01:00+00:00",
            },
            {
                "authorDid": "did:b",
                "claim_fingerprint": "fp_b",
                "createdAt": "2026-06-09T02:00:00+00:00",
                "confidence": 0.7,
                "provenance": "test",
                "evidence_hash": "eh3",
                "post_uri": "at://did:b/post/2",
                "post_cid": "cid2",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": "2026-06-09T02:01:00+00:00",
            },
            {
                "authorDid": "did:old",
                "claim_fingerprint": "fp_oldtime",
                "createdAt": "1997-01-01T00:00:00+00:00",
                "confidence": 0.1,
                "provenance": "test",
                "evidence_hash": "eh4",
                "post_uri": "at://did:old/post/1",
                "post_cid": "cid4",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": "1997-01-01T00:01:00+00:00",
            },
            {
                "authorDid": "did:future",
                "claim_fingerprint": "fp_future",
                "createdAt": "2199-01-01T00:00:00+00:00",
                "confidence": 0.1,
                "provenance": "test",
                "evidence_hash": "eh5",
                "post_uri": "at://did:future/post/1",
                "post_cid": "cid5",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": "2199-01-01T00:01:00+00:00",
            },
        ]
    out_dir = parquet_root / "claim_history" / f"date={date_str}"
    out_dir.mkdir(parents=True)
    schema = pa.schema(
        [
            pa.field("authorDid", pa.string()),
            pa.field("claim_fingerprint", pa.string()),
            pa.field("createdAt", pa.string()),
            pa.field("confidence", pa.float64()),
            pa.field("provenance", pa.string()),
            pa.field("evidence_hash", pa.string()),
            pa.field("post_uri", pa.string()),
            pa.field("post_cid", pa.string()),
            pa.field("fingerprint_version", pa.string()),
            pa.field("evidence_class", pa.string()),
            pa.field("fp_kind", pa.string()),
            pa.field("observed_at", pa.string()),
        ]
    )
    path = out_dir / "part-00.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _schema(conn, table):
    return conn.execute(f"PRAGMA table_info({table})").fetchall()


def _index_names(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA index_list({table})").fetchall()}


def test_snapshot_schema_counts_quarantine_manifest_and_attach(tmp_path):
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    parquet_root = tmp_path / "parquet"
    parquet_path = _write_claim_parquet(parquet_root)
    out = tmp_path / "facts.sqlite"

    manifest = export_snapshot_once(
        parquet_root=parquet_root,
        identity_source_path=identity_db,
        output_path=out,
        generated_at=GENERATED_AT,
    )

    assert out.exists()
    assert not list(tmp_path.glob("facts.sqlite.tmp*")), "writer tmp leaked"
    assert not list(tmp_path.glob("facts.sqlite.manifest.json.tmp*")), "manifest tmp leaked"
    manifest_path = tmp_path / "facts.sqlite.manifest.json"
    assert json.loads(manifest_path.read_text()) == manifest

    conn = sqlite3.connect(str(out))
    expected = sqlite3.connect(":memory:")
    _ensure_tables(expected)
    assert _schema(conn, "actor_identity_facts") == _schema(expected, "actor_identity_facts")
    assert _schema(conn, "uri_fingerprint") == _schema(expected, "uri_fingerprint")
    assert "idx_uri_fp" in _index_names(conn, "uri_fingerprint")

    assert conn.execute("SELECT COUNT(*) FROM actor_identity_facts").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 2
    rows = conn.execute(
        "SELECT post_uri, fingerprint FROM uri_fingerprint ORDER BY post_uri"
    ).fetchall()
    assert rows == [("at://did:a/post/1", "fp_new"), ("at://did:b/post/2", "fp_b")]
    assert conn.execute(
        "SELECT COUNT(*) FROM uri_fingerprint WHERE created_epoch < ?",
        (int(dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc).timestamp()),),
    ).fetchone()[0] == 0
    conn.close()
    expected.close()

    assert manifest["input_parquet_paths"] == [str(parquet_path)]
    assert manifest["input_partition_window"] == {"min": "2026-06-09", "max": "2026-06-09"}
    assert manifest["row_counts"] == {
        "actor_identity_facts": 2,
        "uri_fingerprint": 2,
        "fingerprint_hourly": None,
        "fingerprint_bounds": None,
        "meta": None,
    }
    assert manifest["uri_fingerprint_rows_quarantined_bogus_created_epoch"] == 2
    assert manifest["uri_fingerprint_min_created_epoch_written"] is not None
    assert manifest["uri_fingerprint_max_created_epoch_written"] is not None
    assert manifest["generated_at"] == GENERATED_AT
    assert manifest["output_path"] == str(out)
    assert manifest["writer_version"]

    local = sqlite3.connect(":memory:")
    local.execute(f"ATTACH DATABASE '{out}' AS drift")
    joined = local.execute(
        """
        SELECT uf.fingerprint, aif.pds_host
        FROM drift.uri_fingerprint uf
        LEFT JOIN drift.actor_identity_facts aif ON aif.did = 'did:plc:aaa'
        WHERE uf.post_uri = 'at://did:a/post/1'
        """
    ).fetchone()
    assert joined == ("fp_new", "pds1.example")
    local.close()


def test_cross_partition_dedup_later_date_wins(tmp_path):
    """A post_uri appearing in two date partitions resolves to the later
    partition's row.

    Precise semantics being pinned: latest createdAt-day partition wins;
    within a day, file scan order (which is rowid order — the capture's
    per-day SELECT is ORDER BY rowid). This matches legacy's
    highest-rowid-wins EXCEPT for backdated corrections: a row ingested
    later but carrying an older createdAt lands in an older partition and
    loses here while winning under legacy. Known, accepted V0 divergence —
    partition date is the writer's only ordering signal across days.

    The older partition carries a control row so this test fails if that
    partition is silently never read (vacuous-pass hazard flagged by codex
    review, 2026-07-16).
    """
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    parquet_root = tmp_path / "parquet"
    dup_uri = "at://did:a/post/dup"
    control_uri = "at://did:a/post/only-day1"

    def row(uri, fp, created):
        return {
            "authorDid": "did:a",
            "claim_fingerprint": fp,
            "createdAt": created,
            "confidence": 0.9,
            "provenance": "test",
            "evidence_hash": f"eh:{fp}",
            "post_uri": uri,
            "post_cid": f"cid:{fp}",
            "fingerprint_version": "v1",
            "evidence_class": "none",
            "fp_kind": "claim",
            "observed_at": created,
        }

    _write_claim_parquet(
        parquet_root, date_str="2026-06-08",
        rows=[
            row(dup_uri, "fp_day1", "2026-06-08T10:00:00+00:00"),
            row(control_uri, "fp_control", "2026-06-08T11:00:00+00:00"),
        ],
    )
    _write_claim_parquet(
        parquet_root, date_str="2026-06-09",
        rows=[row(dup_uri, "fp_day2", "2026-06-09T10:00:00+00:00")],
    )
    out = tmp_path / "facts.sqlite"

    manifest = export_snapshot_once(
        parquet_root=parquet_root,
        identity_source_path=identity_db,
        output_path=out,
        generated_at=GENERATED_AT,
    )

    assert manifest["input_partition_window"] == {"min": "2026-06-08", "max": "2026-06-09"}
    conn = sqlite3.connect(str(out))
    rows = conn.execute(
        "SELECT post_uri, fingerprint FROM uri_fingerprint ORDER BY post_uri"
    ).fetchall()
    conn.close()
    # Control row proves the older partition was read at all; dup row proves
    # the later partition's version won.
    assert rows == [
        (dup_uri, "fp_day2"),
        (control_uri, "fp_control"),
    ]


def test_missing_parquet_publishes_controlled_identity_only_snapshot(tmp_path):
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    out = tmp_path / "facts.sqlite"

    manifest = export_snapshot_once(
        parquet_root=tmp_path / "missing-parquet",
        identity_source_path=identity_db,
        output_path=out,
        generated_at=GENERATED_AT,
    )

    conn = sqlite3.connect(str(out))
    assert conn.execute("SELECT COUNT(*) FROM actor_identity_facts").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM uri_fingerprint").fetchone()[0] == 0
    conn.close()

    assert manifest["input_parquet_paths"] == []
    assert manifest["input_partition_window"] is None
    assert manifest["row_counts"]["uri_fingerprint"] == 0
    assert manifest["uri_fingerprint_rows_quarantined_bogus_created_epoch"] == 0
    assert manifest["uri_fingerprint_min_created_epoch_written"] is None
    assert manifest["uri_fingerprint_max_created_epoch_written"] is None


def test_low_memory_limit_and_spill_dir_still_correct(tmp_path, monkeypatch):
    """Dedup runs inside DuckDB with a bounded memory_limit + on-disk spill;
    a tight limit must not change consumer-visible output (this is the
    property that keeps the writer from OOMing the production tree — 202M
    rows on a 7.8GB box, 2026-07-17)."""
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    parquet_root = tmp_path / "parquet"
    _write_claim_parquet(parquet_root)
    spill = tmp_path / "duckdb-spill"
    monkeypatch.setenv("DRIFTWATCH_FACTS_DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("DRIFTWATCH_FACTS_DUCKDB_THREADS", "1")
    monkeypatch.setenv("DRIFTWATCH_FACTS_DUCKDB_TEMP_DIR", str(spill))
    out = tmp_path / "facts.sqlite"

    manifest = export_snapshot_once(
        parquet_root=parquet_root,
        identity_source_path=identity_db,
        output_path=out,
        generated_at=GENERATED_AT,
    )

    assert spill.exists()  # temp_directory was configured
    conn = sqlite3.connect(str(out))
    rows = conn.execute(
        "SELECT post_uri, fingerprint FROM uri_fingerprint ORDER BY post_uri"
    ).fetchall()
    conn.close()
    # Same result as the default-memory run: dedup keeps latest fingerprint,
    # bogus epochs quarantined.
    assert rows == [("at://did:a/post/1", "fp_new"), ("at://did:b/post/2", "fp_b")]
    assert manifest["row_counts"]["uri_fingerprint"] == 2
    assert manifest["uri_fingerprint_rows_quarantined_bogus_created_epoch"] == 2


def test_overlapping_run_tmps_are_isolated(tmp_path):
    """PID-suffixed tmps: a concurrent run's fresh tmp is never touched;
    a crashed run's stale orphan is swept. Overlap can no longer publish
    another run's partial file (codex review finding, 2026-07-16)."""
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    out = tmp_path / "facts.sqlite"

    fresh = tmp_path / "facts.sqlite.tmp.99999"
    fresh.write_text("other run in progress")
    stale = tmp_path / "facts.sqlite.tmp.11111"
    stale.write_text("crashed run leftover")
    old = time.time() - 7200
    os.utime(stale, (old, old))

    export_snapshot_once(
        parquet_root=tmp_path / "missing-parquet",
        identity_source_path=identity_db,
        output_path=out,
        generated_at=GENERATED_AT,
    )

    assert out.exists()
    assert fresh.exists(), "live concurrent tmp must not be touched"
    assert fresh.read_text() == "other run in progress"
    assert not stale.exists(), "stale orphan tmp must be swept"


def test_manifest_failure_leaves_no_final_snapshot(tmp_path, monkeypatch):
    identity_db = _make_identity_db(tmp_path / "identity.sqlite")
    out = tmp_path / "facts.sqlite"

    import labeler.facts_export_duckdb_snapshot as mod

    def fail_manifest(_manifest_path, _manifest):
        raise RuntimeError("injected manifest failure")

    monkeypatch.setattr(mod, "_write_manifest_atomic", fail_manifest)

    with pytest.raises(RuntimeError, match="injected manifest failure"):
        export_snapshot_once(
            parquet_root=tmp_path / "missing-parquet",
            identity_source_path=identity_db,
            output_path=out,
            generated_at=GENERATED_AT,
        )

    assert not out.exists()
    assert not list(tmp_path.glob("facts.sqlite.tmp*")), "writer tmp leaked on failure"
