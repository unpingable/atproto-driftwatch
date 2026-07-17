"""Fixture-level parity: legacy facts_export vs Phase 3 snapshot writer.

Phase 3 exit criterion (gap-spec-log-structured-artifact-system § Phase 3)
requires "output parity acceptable." This is the offline half of that
receipt: identical logical claim/identity rows through both producers must
yield identical consumer-visible output, modulo exactly two documented
divergences, pinned by these tests:

1. **Quarantine** (writer-only filter): bogus ``created_epoch`` rows are
   excluded by the snapshot writer; legacy passes future-dated ones through.
2. **Retention horizon** (legacy-only filter): legacy prunes
   ``uri_fingerprint`` rows older than 30 days; the snapshot carries the
   full Parquet-retained history. Consumer impact is bounded: labelwatch's
   derive JOIN is driven by candidate URIs from recent label events (72h
   overlap window), so extra old rows are dead weight, not wrong answers.

Parity here is defined by the consumer inventory
(gap-spec-facts-export-consumer-inventory.md), not by byte equality:
  - ``uri_fingerprint``: (post_uri, fingerprint, created_epoch) — the three
    columns labelwatch reads. ``rowid_src`` is producer-internal and
    intentionally differs (legacy: source rowid; V0 writer: scan ordinal).
  - ``actor_identity_facts``: all 8 columns.
  - The three tables labelwatch does not read (``fingerprint_hourly``,
    ``fingerprint_bounds``, ``meta``) are absent from the snapshot by
    documented decision (snapshot-001 spec § scope item 4).

This also pins a subtle cross-engine agreement: the legacy producer derives
``created_epoch`` via SQLite ``strftime('%s', createdAt)`` while the writer
uses DuckDB ``epoch(try_cast(createdAt AS TIMESTAMPTZ))``. Parity on real
timestamp shapes is evidence the two parsers agree on the wire format.
"""

import pathlib
import sqlite3
import time

import pytest

pa = pytest.importorskip("pyarrow")
import pyarrow.parquet as pq  # noqa: E402

from labeler.facts_export import export_once  # noqa: E402
from labeler.facts_export_duckdb_snapshot import export_snapshot_once  # noqa: E402


GENERATED_AT = "2026-07-16T12:00:00Z"
NOW_EPOCH = 1_784_203_200  # == GENERATED_AT: both producers share one "now"


def _iso_offset(epoch: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(epoch))


IDENTITY_ROWS = [
    ("did:plc:aaa", "alice.test", "https://pds1.example", "pds1.example",
     "ok", "2026-06-01T00:00:00Z", 1, "live"),
    ("did:plc:bbb", "bob.test", None, None, "pending", None, 1, "labelwatch_seed"),
    ("did:plc:ccc", None, "https://pds3.example", "pds3.example",
     "error", "2026-05-01T12:34:56Z", 0, "both"),
]

# (post_uri, fingerprint, created_epoch). Good rows sit inside legacy's
# 30-day retention window relative to NOW_EPOCH. Includes a duplicate
# post_uri — later row must win under both producers' dedup — and one
# FUTURE-dated bogus row (2199). The specimen choice matters: legacy's
# retention prune (created_epoch < now - 30d) coincidentally deletes
# ancient-past bogus rows like the 1997 class, so the leak the quarantine
# demonstrably closes at this layer is the future-dated one, which sails
# straight through legacy retention.
GOOD_EPOCH_1 = NOW_EPOCH - 5 * 86_400
GOOD_EPOCH_2 = NOW_EPOCH - 5 * 86_400 + 3_600
GOOD_EPOCH_3 = NOW_EPOCH - 4 * 86_400
BOGUS_EPOCH = 7_226_582_400  # 2199-01-01: > generated_at + 1d → quarantined
HORIZON_EPOCH = NOW_EPOCH - 45 * 86_400  # valid, but beyond legacy's 30d prune

CLAIM_ROWS = [
    ("at://did:a/app.bsky.feed.post/1", "fp_old", GOOD_EPOCH_1),
    ("at://did:a/app.bsky.feed.post/1", "fp_new", GOOD_EPOCH_2),  # dup: wins
    ("at://did:b/app.bsky.feed.post/2", "fp_b", GOOD_EPOCH_3),
    ("at://did:future/app.bsky.feed.post/3", "fp_bogus", BOGUS_EPOCH),
    ("at://did:c/app.bsky.feed.post/4", "fp_horizon", HORIZON_EPOCH),
]


def _make_hot_db(path: pathlib.Path) -> pathlib.Path:
    """A labeler.sqlite-shaped source with claim_history + identity."""
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
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
        );
        CREATE TABLE actor_identity_current (
            did TEXT PRIMARY KEY,
            handle TEXT,
            is_active INTEGER DEFAULT 1,
            pds_endpoint TEXT,
            pds_host TEXT,
            resolver_status TEXT,
            resolver_last_success_at TIMESTAMP,
            identity_source TEXT DEFAULT 'live'
        );
        """
    )
    for post_uri, fingerprint, epoch in CLAIM_ROWS:
        created = _iso_offset(epoch)
        conn.execute(
            "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt, "
            " confidence, provenance, evidence_hash, post_uri, post_cid, "
            " fingerprint_version, evidence_class, fp_kind, observed_at) "
            "VALUES ('did:author', ?, ?, 0.9, 'test', 'eh', ?, 'cid', 'v1', "
            "        'none', 'claim', ?)",
            (fingerprint, created, post_uri, created),
        )
    conn.executemany(
        "INSERT INTO actor_identity_current "
        "(did, handle, pds_endpoint, pds_host, resolver_status, "
        " resolver_last_success_at, is_active, identity_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        IDENTITY_ROWS,
    )
    conn.commit()
    conn.close()
    return path


def _make_parquet(parquet_root: pathlib.Path) -> None:
    """The same claim rows as a claim_history Parquet partition."""
    rows = []
    for post_uri, fingerprint, epoch in CLAIM_ROWS:
        created = _iso_offset(epoch)
        rows.append(
            {
                "authorDid": "did:author",
                "claim_fingerprint": fingerprint,
                "createdAt": created,
                "confidence": 0.9,
                "provenance": "test",
                "evidence_hash": "eh",
                "post_uri": post_uri,
                "post_cid": "cid",
                "fingerprint_version": "v1",
                "evidence_class": "none",
                "fp_kind": "claim",
                "observed_at": created,
            }
        )
    out_dir = parquet_root / "claim_history" / "date=2026-06-09"
    out_dir.mkdir(parents=True)
    schema = pa.schema(
        [
            pa.field(name, pa.float64() if name == "confidence" else pa.string())
            for name in rows[0]
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), out_dir / "part-00.parquet")


def _consumer_visible_uri_rows(path: pathlib.Path) -> set:
    conn = sqlite3.connect(str(path))
    try:
        return set(
            conn.execute(
                "SELECT post_uri, fingerprint, created_epoch FROM uri_fingerprint"
            ).fetchall()
        )
    finally:
        conn.close()


def _identity_rows(path: pathlib.Path) -> set:
    conn = sqlite3.connect(str(path))
    try:
        return set(
            conn.execute(
                "SELECT did, handle, pds_endpoint, pds_host, resolver_status, "
                "       resolver_last_success_at, is_active, identity_source "
                "FROM actor_identity_facts"
            ).fetchall()
        )
    finally:
        conn.close()


@pytest.fixture()
def outputs(tmp_path, monkeypatch):
    """Run both producers over the same logical rows; return output paths."""
    # NOT tmp_path/"labeler.sqlite" — the autouse isolate_db fixture already
    # init_db()s that path with the real schema.
    hot_db = _make_hot_db(tmp_path / "hot_source.sqlite")
    parquet_root = tmp_path / "parquet"
    _make_parquet(parquet_root)

    # Legacy producer. Freeze its notion of "now" so the 30-day retention
    # prune bites deterministically (HORIZON row pruned, good rows kept).
    # `legacy.time` is the global time module, so the patch is process-wide —
    # bound it with monkeypatch.context() to exactly the legacy export call
    # instead of leaking a frozen clock across the whole fixture.
    import labeler.facts_export as legacy

    legacy_facts = tmp_path / "legacy" / "facts.sqlite"
    legacy_facts.parent.mkdir()
    source_factory = lambda: sqlite3.connect(str(hot_db))  # noqa: E731
    with monkeypatch.context() as m:
        m.setattr(legacy.time, "time", lambda: float(NOW_EPOCH))
        export_once(
            source_factory,
            facts_path=str(legacy_facts),
            work_path=str(tmp_path / "legacy" / "facts_work.sqlite"),
            force_snapshot=True,
        )

    # Snapshot writer.
    snapshot_facts = tmp_path / "snapshot" / "facts.sqlite"
    manifest = export_snapshot_once(
        parquet_root=parquet_root,
        identity_source_path=hot_db,
        output_path=snapshot_facts,
        generated_at=GENERATED_AT,
    )
    return legacy_facts, snapshot_facts, manifest


def test_uri_fingerprint_parity_modulo_documented_divergences(outputs):
    legacy_facts, snapshot_facts, manifest = outputs
    legacy_rows = _consumer_visible_uri_rows(legacy_facts)
    snapshot_rows = _consumer_visible_uri_rows(snapshot_facts)

    bogus_row = ("at://did:future/app.bsky.feed.post/3", "fp_bogus", BOGUS_EPOCH)
    horizon_row = ("at://did:c/app.bsky.feed.post/4", "fp_horizon", HORIZON_EPOCH)

    # Divergence 1 — quarantine: legacy leaks the future-dated row, the
    # snapshot excludes it and counts it in the manifest.
    assert bogus_row in legacy_rows
    assert bogus_row not in snapshot_rows
    assert manifest["uri_fingerprint_rows_quarantined_bogus_created_epoch"] == 1

    # Divergence 2 — retention horizon: legacy's 30d prune drops the old-but-
    # valid row; the snapshot carries the full Parquet-retained history.
    assert horizon_row not in legacy_rows
    assert horizon_row in snapshot_rows

    # And NOTHING else differs.
    assert legacy_rows - {bogus_row} == snapshot_rows - {horizon_row}

    # Dedup semantics agree: latest row for the duplicated post_uri won in both.
    dup = [r for r in snapshot_rows if r[0] == "at://did:a/app.bsky.feed.post/1"]
    assert dup == [("at://did:a/app.bsky.feed.post/1", "fp_new", GOOD_EPOCH_2)]


def test_identity_facts_parity(outputs):
    legacy_facts, snapshot_facts, _ = outputs
    legacy = _identity_rows(legacy_facts)
    snapshot = _identity_rows(snapshot_facts)
    assert legacy == snapshot
    assert len(snapshot) == len(IDENTITY_ROWS)


def test_unread_tables_absent_by_decision(outputs):
    """Snapshot ships only the two consumer-read tables; the legacy extras
    are absent by documented decision (snapshot-001 § scope item 4), which
    labelwatch tolerates (missing table → caveat, not 5xx)."""
    legacy_facts, snapshot_facts, manifest = outputs

    def tables(path):
        conn = sqlite3.connect(str(path))
        try:
            return {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        finally:
            conn.close()

    assert {"uri_fingerprint", "actor_identity_facts"} <= tables(legacy_facts)
    assert {"fingerprint_hourly", "fingerprint_bounds", "meta"} <= tables(legacy_facts)
    assert tables(snapshot_facts) == {"uri_fingerprint", "actor_identity_facts"}
    # ...and the manifest says so, not silently.
    assert manifest["row_counts"]["fingerprint_hourly"] is None
    assert manifest["row_counts"]["fingerprint_bounds"] is None
    assert manifest["row_counts"]["meta"] is None
