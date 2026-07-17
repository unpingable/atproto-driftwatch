"""DuckDB-backed facts.sqlite snapshot writer for labelwatch compatibility.

Parquet is authoritative past. DuckDB is the question engine.
``facts.sqlite`` is a compatibility projection/cache, not the source of custody.

V0 source split:
- ``uri_fingerprint`` is materialized from existing ``claim_history`` Parquet
  partitions through DuckDB.
- ``actor_identity_facts`` is materialized from Driftwatch SQLite
  ``actor_identity_current`` because V0 has no identity Parquet stream yet.

Missing or empty Parquet is controlled: the writer still publishes an
identity-only snapshot with zero URI rows, zero quarantine count, an empty input
path list, and a null partition window.

``rowid_src`` is a compatibility-only field in this writer. The historical
SQLite producer stored source ``claim_history.rowid`` there; Parquet does not
carry that rowid. V0 writes a deterministic ordinal — ``row_number()`` over
the deduped rows ordered by ``post_uri``.

Forward note for Phase 3.5 (`gap-spec-facts-export-duckdb-snapshot-001.md
§ Phase 3.5 handle`): V0 ``rowid_src`` is a producer-local ordinal,
NOT ``claim_history.rowid``. Labelwatch consumers must not treat it as
stable source-table identity (per the consumer inventory, labelwatch
does not read this column at all). If Phase 3.5 surfaces a need for
stable source identity, rename or replace this column then; do not
back-fit semantics into the V0 field.

Memory: the dedup + quarantine run inside DuckDB (streaming hash-aggregate,
spills to ``temp_directory`` at ``memory_limit``), so RAM is bounded by the
result set, not by history depth. This replaced a Python-dict accumulation
that OOM'd on the production tree (202M rows). See ``_duckdb_runtime_config``.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import pathlib
import sqlite3
import subprocess
import time
from typing import Any


WRITER_VERSION = "0.1.1"
_BOGUS_MIN_EPOCH = 1_577_836_800  # 2020-01-01T00:00:00Z
_STALE_TMP_MAX_AGE_S = 3_600  # orphaned .tmp.<pid> files older than this are swept


def _default_parquet_root() -> pathlib.Path:
    from .retention import PARQUET_DIR

    return pathlib.Path(PARQUET_DIR)


def _default_identity_source_path() -> pathlib.Path:
    from .db import DATA_DIR

    return pathlib.Path(DATA_DIR) / "labeler.sqlite"


def _default_output_path() -> pathlib.Path:
    from .db import DATA_DIR

    return pathlib.Path(DATA_DIR) / "facts.sqlite"


def _iso_utc(dt: _dt.datetime) -> str:
    return dt.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_generated_at(generated_at: Any | None) -> _dt.datetime:
    if generated_at is None:
        return _dt.datetime.now(_dt.timezone.utc)
    if isinstance(generated_at, (int, float)):
        return _dt.datetime.fromtimestamp(generated_at, tz=_dt.timezone.utc)
    if isinstance(generated_at, _dt.datetime):
        if generated_at.tzinfo is None:
            return generated_at.replace(tzinfo=_dt.timezone.utc)
        return generated_at.astimezone(_dt.timezone.utc)
    if isinstance(generated_at, str):
        text = generated_at.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = _dt.datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.astimezone(_dt.timezone.utc)
    raise TypeError(f"unsupported generated_at: {type(generated_at)!r}")


def _producer_git_sha() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=pathlib.Path(__file__).resolve().parents[2],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return None
    sha = result.stdout.strip()
    return sha or None


def _ensure_snapshot_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE actor_identity_facts (
            did                     TEXT PRIMARY KEY,
            handle                  TEXT,
            pds_endpoint            TEXT,
            pds_host                TEXT,
            resolver_status         TEXT,
            resolver_last_success_at TEXT,
            is_active               INTEGER,
            identity_source         TEXT
        );

        CREATE TABLE uri_fingerprint (
            post_uri       TEXT PRIMARY KEY,
            fingerprint    TEXT NOT NULL,
            created_epoch  INTEGER NOT NULL,
            rowid_src      INTEGER NOT NULL
        );
        CREATE INDEX idx_uri_fp ON uri_fingerprint(fingerprint);
        """
    )


def _copy_identity(identity_source_path: pathlib.Path, out_conn: sqlite3.Connection) -> int:
    # Open the identity DB read-only via the SQLite URI form. If the snapshot
    # writer ever runs against the live ``labeler.sqlite``, a read-only
    # connection prevents accidental contention with the writer's hot path.
    source = sqlite3.connect(
        f"file:{identity_source_path}?mode=ro",
        uri=True,
    )
    try:
        rows = source.execute(
            """
            SELECT did, handle, pds_endpoint, pds_host,
                   resolver_status, resolver_last_success_at, is_active,
                   identity_source
            FROM actor_identity_current
            """
        ).fetchall()
    finally:
        source.close()

    out_conn.executemany(
        "INSERT INTO actor_identity_facts "
        "(did, handle, pds_endpoint, pds_host, resolver_status, "
        " resolver_last_success_at, is_active, identity_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def _resolve_parquet_paths(parquet_root: pathlib.Path) -> list[pathlib.Path]:
    claim_root = parquet_root / "claim_history"
    if not claim_root.exists():
        return []
    return sorted(claim_root.glob("date=*/part-*.parquet"))


def _partition_window(paths: list[pathlib.Path]) -> dict[str, str] | None:
    parts = []
    for path in paths:
        for parent in path.parents:
            if parent.name.startswith("date="):
                parts.append(parent.name.removeprefix("date="))
                break
    if not parts:
        return None
    return {"min": min(parts), "max": max(parts)}


def _duckdb_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _duckdb_runtime_config() -> tuple[str, int, str | None]:
    """Memory limit, thread count, spill directory for the dedup query.

    Production reality forced this: the V0 writer accumulated every parquet
    row into a Python dict and OOM'd on the real tree (202M rows → ~15-21GB
    on a 7.8GB box). The dedup now runs inside DuckDB, which streams the
    hash-aggregate and spills partitions to ``temp_directory`` when it hits
    ``memory_limit`` — bounded RAM regardless of history depth. Defaults are
    conservative for a small shared VM; override via env for a bigger host.
    """
    mem = os.getenv("DRIFTWATCH_FACTS_DUCKDB_MEMORY_LIMIT", "2GB")
    try:
        threads = int(os.getenv("DRIFTWATCH_FACTS_DUCKDB_THREADS", "2"))
    except ValueError:
        threads = 2
    temp_dir = os.getenv("DRIFTWATCH_FACTS_DUCKDB_TEMP_DIR") or None
    return mem, max(1, threads), temp_dir


_STREAM_BATCH = 100_000


def _populate_uri_fingerprints(
    out_conn: sqlite3.Connection,
    parquet_paths: list[pathlib.Path],
    generated_epoch: int,
) -> tuple[int, int, int | None, int | None]:
    """Dedup claim_history Parquet into ``out_conn.uri_fingerprint``, streamed.

    Returns (written, quarantined, min_epoch, max_epoch).

    Neither DuckDB nor Python ever holds the full row set:
    - DuckDB does the dedup as a streaming hash-aggregate that spills to
      ``temp_directory`` under ``memory_limit`` (``preserve_insertion_order``
      is off — the output PK makes order irrelevant and preserving it forces
      DuckDB to buffer, which is exactly what OOM'd at scale).
    - The deduped result is pulled with ``fetchmany`` and inserted into SQLite
      in ``_STREAM_BATCH`` chunks; at most one batch lives in Python.

    This is the fix for the production tree (202M rows on a 7.8GB box): the
    prior implementation accumulated a Python dict of every URI and then
    materialized the whole result via fetchall + executemany — unbounded on
    both the dedup and the insert side.

    Dedup rule: one fingerprint per post_uri, greatest ``created_epoch`` wins
    (most recent claim). Reproduces the V0 "later partition wins" outcome —
    later date ⇒ larger createdAt — and the pinned cross-partition/duplicate
    test expectations, without depending on physical scan order.
    """
    if not parquet_paths:
        return 0, 0, None, None

    import duckdb

    valid_max_epoch = generated_epoch + 86_400
    mem_limit, threads, temp_dir = _duckdb_runtime_config()
    path_list = "[" + ", ".join(_duckdb_literal(str(p)) for p in parquet_paths) + "]"

    # Reusable projection: null-filtered, createdAt → epoch. Inlined into both
    # the quarantine-count scan and the dedup scan rather than materialized as
    # a temp table (a 202M-row temp table in an in-memory DuckDB would itself
    # OOM). Two bounded streaming scans of the parquet, no full materialization.
    classified = f"""
        SELECT
            post_uri,
            claim_fingerprint AS fingerprint,
            CAST(epoch(try_cast(createdAt AS TIMESTAMPTZ)) AS BIGINT) AS created_epoch
        FROM read_parquet({path_list})
        WHERE post_uri IS NOT NULL
          AND claim_fingerprint IS NOT NULL
    """

    con = duckdb.connect(database=":memory:")
    written = 0
    min_epoch: int | None = None
    max_epoch: int | None = None
    try:
        con.execute(f"SET threads TO {threads}")
        con.execute(f"SET memory_limit = {_duckdb_literal(mem_limit)}")
        con.execute("SET preserve_insertion_order = false")
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
            con.execute(f"SET temp_directory = {_duckdb_literal(temp_dir)}")

        quarantined = con.execute(
            f"""
            SELECT COUNT(*) FROM ({classified})
            WHERE created_epoch IS NULL
               OR created_epoch < ?
               OR created_epoch > ?
            """,
            [_BOGUS_MIN_EPOCH, valid_max_epoch],
        ).fetchone()[0]

        cur = con.execute(
            f"""
            SELECT
                post_uri,
                arg_max(fingerprint, created_epoch) AS fingerprint,
                max(created_epoch) AS created_epoch
            FROM ({classified})
            WHERE created_epoch IS NOT NULL
              AND created_epoch >= ?
              AND created_epoch <= ?
            GROUP BY post_uri
            """,
            [_BOGUS_MIN_EPOCH, valid_max_epoch],
        )

        insert = (
            "INSERT INTO uri_fingerprint (post_uri, fingerprint, created_epoch, rowid_src) "
            "VALUES (?, ?, ?, ?)"
        )
        while True:
            batch = cur.fetchmany(_STREAM_BATCH)
            if not batch:
                break
            out_rows = []
            for post_uri, fingerprint, created_epoch in batch:
                ce = int(created_epoch)
                written += 1
                # rowid_src: compatibility-only running ordinal, never read by
                # labelwatch (see module docstring). Not a source rowid.
                out_rows.append((str(post_uri), str(fingerprint), ce, written))
                if min_epoch is None or ce < min_epoch:
                    min_epoch = ce
                if max_epoch is None or ce > max_epoch:
                    max_epoch = ce
            out_conn.executemany(insert, out_rows)
    finally:
        con.close()

    return written, int(quarantined), min_epoch, max_epoch


def _write_manifest_atomic(manifest_path: pathlib.Path, manifest: dict[str, Any]) -> None:
    # PID-suffixed tmp: overlapping writer runs must never rename each
    # other's partially-written files into place. See _sweep_stale_tmps.
    tmp_path = manifest_path.with_name(f"{manifest_path.name}.tmp.{os.getpid()}")
    tmp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    os.replace(str(tmp_path), str(manifest_path))


def _sweep_stale_tmps(out_path: pathlib.Path) -> None:
    """Remove orphaned .tmp.<pid> files left by crashed/killed prior runs.

    Only sweeps tmps older than _STALE_TMP_MAX_AGE_S so a concurrent live
    run's in-progress tmp is never touched. (Concurrent runs should be
    prevented at the invocation layer — flock in the cron line, see
    RUNBOOK — but the writer must not corrupt output even without it.)
    """
    cutoff = time.time() - _STALE_TMP_MAX_AGE_S
    for pattern in (f"{out_path.name}.tmp.*", f"{out_path.name}.manifest.json.tmp.*"):
        for stale in out_path.parent.glob(pattern):
            try:
                if stale.stat().st_mtime < cutoff:
                    stale.unlink()
            except OSError:
                continue


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def export_snapshot_once(
    parquet_root: str | os.PathLike[str] | None = None,
    identity_source_path: str | os.PathLike[str] | None = None,
    output_path: str | os.PathLike[str] | None = None,
    generated_at: Any | None = None,
) -> dict[str, Any]:
    """Build and atomically publish a V0 labelwatch-compatible facts snapshot."""

    t0 = time.monotonic()
    generated_dt = _parse_generated_at(generated_at)
    generated_epoch = int(generated_dt.timestamp())
    generated_at_iso = _iso_utc(generated_dt)

    parquet_root_path = pathlib.Path(parquet_root) if parquet_root is not None else _default_parquet_root()
    identity_path = (
        pathlib.Path(identity_source_path)
        if identity_source_path is not None
        else _default_identity_source_path()
    )
    out_path = pathlib.Path(output_path) if output_path is not None else _default_output_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # PID-suffixed tmp: a fixed ".tmp" name lets an overlapping run unlink or
    # rename THIS run's partial file into production. With unique names each
    # run only ever renames its own complete output; overlap degrades to
    # last-writer-wins of complete snapshots.
    tmp_path = out_path.with_name(f"{out_path.name}.tmp.{os.getpid()}")
    manifest_path = out_path.with_name(out_path.name + ".manifest.json")
    _sweep_stale_tmps(out_path)
    if tmp_path.exists():
        tmp_path.unlink()

    parquet_paths = _resolve_parquet_paths(parquet_root_path)
    input_paths = [str(p) for p in parquet_paths]

    conn = sqlite3.connect(str(tmp_path))
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        _ensure_snapshot_schema(conn)
        identity_count = _copy_identity(identity_path, conn)
        uri_written, quarantined, min_epoch, max_epoch = _populate_uri_fingerprints(
            conn, parquet_paths, generated_epoch
        )
        conn.commit()

        row_counts = {
            "actor_identity_facts": _count(conn, "actor_identity_facts"),
            "uri_fingerprint": _count(conn, "uri_fingerprint"),
            "fingerprint_hourly": None,
            "fingerprint_bounds": None,
            "meta": None,
        }
        if row_counts["actor_identity_facts"] != identity_count:
            raise RuntimeError("actor_identity_facts row count verification failed")
        if row_counts["uri_fingerprint"] != uri_written:
            raise RuntimeError("uri_fingerprint row count verification failed")
    except Exception:
        conn.close()
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
    else:
        conn.close()

    manifest = {
        "generated_at": generated_at_iso,
        "producer_git_sha": _producer_git_sha(),
        "input_parquet_paths": input_paths,
        "input_partition_window": _partition_window(parquet_paths),
        "output_path": str(out_path),
        "row_counts": row_counts,
        "uri_fingerprint_rows_quarantined_bogus_created_epoch": quarantined,
        "uri_fingerprint_min_created_epoch_written": min_epoch,
        "uri_fingerprint_max_created_epoch_written": max_epoch,
        "duration_seconds": round(time.monotonic() - t0, 6),
        "writer_version": WRITER_VERSION,
    }

    try:
        _write_manifest_atomic(manifest_path, manifest)
        os.replace(str(tmp_path), str(out_path))
    except Exception:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise
    return manifest
