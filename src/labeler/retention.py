"""Retention loop — strip raw JSON from old events, archive + prune expired rows.

Runs hourly by default. Gated by ENABLE_RETENTION env var.

Retention policy:
  - events.raw:     NULLed after 24 hours (metadata preserved)
  - events rows:    deleted after 7 days
  - edges rows:     deleted after 14 days
  - event_versions: deleted after 7 days
  - claim_history:  archived to gzipped JSONL, then deleted after 14 days

Timestamp trust: claim_history uses COALESCE(observed_at, createdAt) for
retention cutoffs. observed_at is set by our insert path (trusted wall clock);
createdAt comes from the firehose (untrusted but usually fine). Legacy rows
without observed_at fall back to createdAt.

Architecture
------------
Retention runs on its own SQLite write connection — it does **not** route
through the consumer's writer thread. The 5850d01 attempt at writer-thread
routing (commit ``5850d01``, see ``specs/gaps/gap-spec-single-writer-invariant.md``
footer) failed acceptance under firehose load. The async writer-thread
helpers below remain in the file as documented historical scar but are not
wired into ``run_periodic`` from production.

Pressure-aware scheduling lives in ``retention_scheduler.py``. The leaves
below take an optional ``scheduler`` argument; ``NullScheduler`` is the
default, giving the same shape as the legacy code. ``run_periodic`` builds
a ``RetentionScheduler`` when a consumer is provided.
"""

import asyncio
import gzip
import json
import logging
import os
import pathlib
import sqlite3
import time

LOG = logging.getLogger("labeler.retention")


class _LockPressure(Exception):
    """Raised by a retention leaf when a chunk hits ``sqlite3.OperationalError(
    "database is locked")`` — i.e. busy_timeout elapsed under writer contention.

    Carries the partial row total committed by prior chunks of the same leaf,
    so the scheduler caller can record honest stats instead of the ``-1``
    sentinel that conflates "lock pressure" with "actual bug." See
    ``docs/CLEANUP_DEBT.md`` #1.
    """

    def __init__(self, partial_total: int):
        super().__init__(f"lock_pressure (partial={partial_total})")
        self.partial_total = partial_total


def _is_lock_pressure(exc: sqlite3.OperationalError) -> bool:
    """True when an OperationalError message indicates busy-timeout pressure.

    SQLite raises "database is locked" or "database table is locked" after
    PRAGMA ``busy_timeout`` elapses with the lock still held by another
    writer. Other OperationalError messages (syntax, missing table, …)
    indicate real bugs and must NOT be classified as soft aborts.
    """
    msg = str(exc).lower()
    return "database is locked" in msg or "database table is locked" in msg

DEFAULT_INTERVAL_SEC = int(os.getenv("RETENTION_INTERVAL_SEC", str(3600)))  # 1h

# Retention windows (seconds)
RAW_STRIP_AGE_SEC = int(os.getenv("RAW_STRIP_AGE_SEC", str(24 * 3600)))      # 24h
EVENTS_RETENTION_SEC = int(os.getenv("EVENTS_RETENTION_SEC", str(7 * 86400)))  # 7d
EDGES_RETENTION_SEC = int(os.getenv("EDGES_RETENTION_SEC", str(14 * 86400)))   # 14d
CLAIM_RETENTION_SEC = int(os.getenv("CLAIM_RETENTION_SEC", str(14 * 86400)))   # 14d

# Batch sizes for UPDATE/DELETE — kept small so each chunk releases the
# writer lock quickly. Retention competes with the persistent event-writer
# thread (consumer.py) for the single SQLite write lock; large chunks
# starve the writer, which then loses event batches to "database is locked".
STRIP_BATCH = int(os.getenv("RETENTION_STRIP_BATCH", "1000"))
DELETE_BATCH = int(os.getenv("RETENTION_DELETE_BATCH", "1000"))
ARCHIVE_BATCH = 50_000

# Sleep between batches to yield the writer lock. Tuned long enough that
# the persistent writer can drain a couple of its own batches between
# retention chunks. Total pass time grows; that's acceptable — losing
# events to lock-conflict is not.
BATCH_SLEEP_SEC = float(os.getenv("RETENTION_BATCH_SLEEP_SEC", "5.0"))

# Busy timeout for retention's own SQLite write connection. Raised
# 2026-06-02 from 30000 → 60000 to match the consumer's writer (which
# uses 60s). The earlier 30s caused spurious soft-aborts on modest
# writer-thread contention bursts that resolved within 30-60s. The
# longer wait is acceptable because:
#   - The scheduler's per-chunk gate fires BETWEEN chunks, not during
#     a chunk's busy-wait. A chunk that waits the full 60s does not
#     give the scheduler a chance to bail mid-wait — backlog
#     accumulated during that window is not detected until the next
#     chunk's pre-call gate. This is a documented trade-off; the win
#     is fewer pointless aborts during transient contention.
#   - A busy-wait that exhausts the timeout still raises
#     ``sqlite3.OperationalError("database is locked")``, which the
#     leaves now classify as ``_LockPressure`` (CLEANUP_DEBT #1) —
#     soft abort with partial-progress preservation, not the -1
#     real-bug sentinel.
# See ``CLEANUP_DEBT.md`` #2 for the full design note.
RETENTION_BUSY_TIMEOUT_MS = int(os.getenv("RETENTION_BUSY_TIMEOUT_MS", "60000"))

# Cooperative scheduling thresholds for the writer-thread path. When ingest
# backlog grows, retention sleeps longer between chunks so the writer can
# drain. Multiplies BATCH_SLEEP_SEC.
BACKLOG_MED = int(os.getenv("RETENTION_BACKLOG_MED", "1000"))
BACKLOG_HIGH = int(os.getenv("RETENTION_BACKLOG_HIGH", "3000"))

# Archive directory (legacy JSONL path; kept for backward compatibility)
ARCHIVE_DIR = pathlib.Path(os.getenv(
    "RETENTION_ARCHIVE_DIR",
    str(pathlib.Path(__file__).resolve().parents[1] / "data" / "archive"),
))

# Parquet capture directory (forward path; written directly by retention)
PARQUET_DIR = pathlib.Path(os.getenv(
    "RETENTION_PARQUET_DIR",
    str(pathlib.Path(__file__).resolve().parents[1] / "data" / "parquet"),
))

# Forward path toggle. When set, retention writes Parquet partitions instead
# of gzipped JSONL archives. Default on. The JSONL path is kept reachable for
# rollback, gated by setting this to "0".
PARQUET_CAPTURE_ENABLED = os.getenv("ENABLE_RETENTION_PARQUET_CAPTURE", "1") == "1"

try:
    import pyarrow as _pa  # noqa: F401
    import pyarrow.parquet as _pq  # noqa: F401
    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False

# Claim history columns to archive
_CLAIM_COLS = (
    "authorDid", "claim_fingerprint", "createdAt", "confidence",
    "provenance", "evidence_hash", "post_uri", "post_cid",
    "fingerprint_version", "evidence_class", "fp_kind", "observed_at",
)


def _strip_old_raw(conn, scheduler=None):
    """NULL out events.raw for events older than RAW_STRIP_AGE_SEC.

    Batched to avoid long-held write locks. Returns total rows stripped.
    Pass a ``RetentionScheduler`` to enable pressure-aware gating; the
    default ``NullScheduler`` gives legacy behavior.

    Raises ``_LockPressure(total)`` if a chunk hits "database is locked"
    after busy_timeout. Prior chunks' rows remain committed in the DB.
    """
    if scheduler is None:
        from .retention_scheduler import NullScheduler
        scheduler = NullScheduler(sleep_between_s=BATCH_SLEEP_SEC)
    cutoff = time.time() - RAW_STRIP_AGE_SEC
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))
    total = 0
    while True:
        scheduler.before_chunk("raw_strip")
        t0 = time.monotonic()
        try:
            n = _strip_raw_chunk(conn, cutoff_iso)
        except sqlite3.OperationalError as e:
            if _is_lock_pressure(e):
                raise _LockPressure(total) from e
            raise
        elapsed = time.monotonic() - t0
        scheduler.after_chunk("raw_strip", elapsed, n)
        total += n
        if n < STRIP_BATCH:
            break
        scheduler.sleep_between_chunks()
    return total


def _prune_table(conn, table, time_col, retention_sec, scheduler=None):
    """Delete rows older than retention window. Returns rows deleted.

    Raises ``_LockPressure(total)`` if a chunk hits "database is locked"
    after busy_timeout. Prior chunks' deletions remain committed in the DB.
    """
    if scheduler is None:
        from .retention_scheduler import NullScheduler
        scheduler = NullScheduler(sleep_between_s=BATCH_SLEEP_SEC)
    cutoff = time.time() - retention_sec
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))
    total = 0
    op_label = f"prune_{table}"
    while True:
        scheduler.before_chunk(op_label)
        t0 = time.monotonic()
        try:
            n = _prune_table_chunk(conn, table, time_col, cutoff_iso, DELETE_BATCH)
        except sqlite3.OperationalError as e:
            if _is_lock_pressure(e):
                raise _LockPressure(total) from e
            raise
        elapsed = time.monotonic() - t0
        scheduler.after_chunk(op_label, elapsed, n)
        total += n
        if n < DELETE_BATCH:
            break
        scheduler.sleep_between_chunks()
    return total


def _archive_claim_history(conn, scheduler=None) -> dict:
    """Archive old claim_history rows to gzipped JSONL, then delete.

    Uses COALESCE(observed_at, createdAt) as the retention timestamp.
    Archives one day at a time for partitioned files.
    Returns {"archived": N, "deleted": N, "files": [...]}

    The scheduler is consulted before starting each day's read+write (a
    gzip blob, atomic in spirit) and before each delete chunk.
    """
    if scheduler is None:
        from .retention_scheduler import NullScheduler
        scheduler = NullScheduler(sleep_between_s=BATCH_SLEEP_SEC)
    cutoff = time.time() - CLAIM_RETENTION_SEC
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))

    # Find distinct days that have rows to archive
    retention_col = "COALESCE(observed_at, createdAt)"
    days_rows = conn.execute(
        f"SELECT DISTINCT DATE({retention_col}) as d "
        f"FROM claim_history "
        f"WHERE {retention_col} < ? "
        f"ORDER BY d",
        (cutoff_iso,),
    ).fetchall()

    if not days_rows:
        return {"archived": 0, "deleted": 0, "files": []}

    archive_dir = ARCHIVE_DIR / "claim_history"
    archive_dir.mkdir(parents=True, exist_ok=True)

    total_archived = 0
    total_deleted = 0
    files = []

    for (day_str,) in days_rows:
        if not day_str:
            continue

        day_start = f"{day_str}T00:00:00+00:00"
        day_end = f"{day_str}T23:59:59.999999+00:00"

        # Pre-day gate: bail before opening the gzip if pressure has returned.
        scheduler.before_chunk(f"archive_day_{day_str}")

        # Count rows for this day
        count_row = conn.execute(
            f"SELECT COUNT(*) FROM claim_history "
            f"WHERE {retention_col} >= ? AND {retention_col} <= ?",
            (day_start, day_end),
        ).fetchone()
        day_count = count_row[0] if count_row else 0

        if day_count == 0:
            continue

        # Archive to gzipped JSONL
        cols_sql = ", ".join(_CLAIM_COLS)
        archive_path = archive_dir / f"{day_str}.jsonl.gz"

        # If file exists, we're resuming — append mode
        mode = "ab" if archive_path.exists() else "wb"
        written = 0

        t0 = time.monotonic()
        with gzip.open(str(archive_path), mode) as gz:
            offset = 0
            while offset < day_count:
                rows = conn.execute(
                    f"SELECT rowid, {cols_sql} FROM claim_history "
                    f"WHERE {retention_col} >= ? AND {retention_col} <= ? "
                    f"ORDER BY rowid LIMIT ? OFFSET ?",
                    (day_start, day_end, ARCHIVE_BATCH, offset),
                ).fetchall()

                if not rows:
                    break

                for row in rows:
                    rowid = row[0]
                    record = dict(zip(_CLAIM_COLS, row[1:]))
                    # Convert None to null in JSON
                    line = json.dumps(record, sort_keys=True, ensure_ascii=True)
                    gz.write(line.encode("utf-8"))
                    gz.write(b"\n")
                    written += 1

                offset += len(rows)
        scheduler.after_chunk(f"archive_day_{day_str}", time.monotonic() - t0, written)

        # Verify: written count should match day_count (or exceed if appending)
        if written < day_count:
            LOG.warning("archive count mismatch for %s: wrote %d, expected %d — skipping delete",
                        day_str, written, day_count)
            continue

        # Delete archived rows
        deleted_day = 0
        while True:
            scheduler.before_chunk(f"delete_archived_{day_str}")
            tc = time.monotonic()
            n = _delete_archived_day_chunk(
                conn, retention_col, day_start, day_end, DELETE_BATCH,
            )
            scheduler.after_chunk(
                f"delete_archived_{day_str}", time.monotonic() - tc, n,
            )
            deleted_day += n
            if n < DELETE_BATCH:
                break
            scheduler.sleep_between_chunks()

        total_archived += written
        total_deleted += deleted_day
        files.append(str(archive_path))
        LOG.info("archived %s: %d rows → %s, deleted %d",
                 day_str, written, archive_path.name, deleted_day)

    return {"archived": total_archived, "deleted": total_deleted, "files": files}


# ----------------------------------------------------------------------------
# Writer-thread path: chunk functions + async wrappers.
#
# Each chunk function below runs inside the persistent writer thread (via
# consumer.submit_mutation) and operates on the writer's shared connection.
# It must commit before returning. The async wrappers issue chunks one at a
# time and yield between them — single-writer invariant from gap-spec, with
# cooperation against ingest backlog.
# ----------------------------------------------------------------------------


def _strip_raw_chunk(conn, cutoff_iso):
    """One UPDATE chunk for raw-stripping. Returns rows updated."""
    c = conn.execute(
        "UPDATE events SET raw = NULL "
        "WHERE rowid IN ("
        "  SELECT rowid FROM events "
        "  WHERE raw IS NOT NULL AND ctime < ? "
        "  LIMIT ?"
        ")",
        (cutoff_iso, STRIP_BATCH),
    )
    n = c.rowcount
    conn.commit()
    return n


def _archive_claim_history_to_parquet(conn, scheduler=None) -> dict:
    """Archive expired claim_history rows to date-partitioned Parquet, then delete.

    Forward path that replaces the JSONL gzip archive in production. Each
    eligible day becomes one ``date=YYYY-MM-DD/part-00.parquet`` file under
    PARQUET_DIR/claim_history/. Writes go to ``part-00.parquet.tmp`` and are
    atomically renamed only after the file's row count matches the source
    count for that day. Idempotent: skips days whose final partition already
    has at least the expected row count.

    Returns {"archived": N, "deleted": N, "files": [...]} with the same shape
    as ``_archive_claim_history``. Receipts are written per partition under
    PARQUET_DIR/.receipts/retention_capture_YYYY-MM-DD.json.

    Per-day reads are bounded to the partition window. No global scans.
    """
    if not _HAS_PYARROW:
        LOG.error(
            "ENABLE_RETENTION_PARQUET_CAPTURE=1 but pyarrow not importable; "
            "skipping claim_history capture this pass"
        )
        return {"archived": 0, "deleted": 0, "files": []}

    if scheduler is None:
        from .retention_scheduler import NullScheduler
        scheduler = NullScheduler(sleep_between_s=BATCH_SLEEP_SEC)

    cutoff = time.time() - CLAIM_RETENTION_SEC
    cutoff_iso = time.strftime(
        "%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff)
    )
    retention_col = "COALESCE(observed_at, createdAt)"

    # No discovery scan. SCAN_DISTINCT_DATE on a 30M-row claim_history holds
    # a read snapshot for many minutes; even on its own connection it pins
    # WAL against the live writer (2026-05-08 incident: 28-minute scan, WAL
    # to 3.3 GB, drop_frac to 12% before we aborted).
    #
    # Instead: iterate candidate dates from a fixed lookback window, ordered
    # oldest-first. For each date, skip if a complete partition already
    # exists. Otherwise let per-day work figure out via an INDEXED bounded
    # query whether there are rows to capture. The createdAt column has an
    # index (idx_claim_history_created); the per-day query uses it.
    #
    # Edge case: rows with bogus user-supplied createdAt (year 1997 or 2123,
    # ~25k of them per maintenance log) are missed by createdAt-based
    # filtering. They accumulate as garbage and need a separate cleanup
    # pass. Documented as known leak.
    import datetime as _dt
    today = _dt.datetime.now(_dt.timezone.utc).date()
    cutoff_date = today - _dt.timedelta(seconds=CLAIM_RETENTION_SEC)
    lookback_days = int(os.getenv("RETENTION_PARQUET_LOOKBACK_DAYS", "21"))
    earliest_date = cutoff_date - _dt.timedelta(days=lookback_days)

    parquet_root_for_skip = PARQUET_DIR / "claim_history"

    candidate_dates = []
    d = earliest_date
    while d <= cutoff_date:
        existing = parquet_root_for_skip / f"date={d.isoformat()}" / "part-00.parquet"
        if not existing.exists():
            candidate_dates.append(d.isoformat())
        d += _dt.timedelta(days=1)

    if not candidate_dates:
        LOG.info(
            "parquet capture: nothing to do (lookback %dd from cutoff %s, "
            "all partitions present)",
            lookback_days, cutoff_date.isoformat(),
        )
        return {"archived": 0, "deleted": 0, "files": []}

    # Cap per-pass work. Drain backlog across passes.
    max_days = int(os.getenv("RETENTION_PARQUET_MAX_DAYS_PER_PASS", "1"))
    if max_days > 0:
        candidate_dates = candidate_dates[:max_days]
    LOG.info(
        "parquet capture: %d candidate date(s) for this pass: %s",
        len(candidate_dates), ", ".join(candidate_dates),
    )

    # Build the (day_str,) tuples the per-day loop expects.
    days_rows = [(d,) for d in candidate_dates]

    parquet_root = PARQUET_DIR / "claim_history"
    parquet_root.mkdir(parents=True, exist_ok=True)
    receipts_dir = PARQUET_DIR / ".receipts"
    receipts_dir.mkdir(parents=True, exist_ok=True)

    pa_schema = _pa.schema([
        _pa.field("authorDid", _pa.string()),
        _pa.field("claim_fingerprint", _pa.string()),
        _pa.field("createdAt", _pa.string()),
        _pa.field("confidence", _pa.float64()),
        _pa.field("provenance", _pa.string()),
        _pa.field("evidence_hash", _pa.string()),
        _pa.field("post_uri", _pa.string()),
        _pa.field("post_cid", _pa.string()),
        _pa.field("fingerprint_version", _pa.string()),
        _pa.field("evidence_class", _pa.string()),
        _pa.field("fp_kind", _pa.string()),
        _pa.field("observed_at", _pa.string()),
    ])

    total_archived = 0
    total_deleted = 0
    files = []

    # Per-day queries use createdAt (indexed) rather than the COALESCE
    # expression. Bogus-createdAt rows are missed; see comment above.
    per_day_filter = "createdAt >= ? AND createdAt <= ?"

    for (day_str,) in days_rows:
        if not day_str:
            continue

        day_start = f"{day_str}T00:00:00+00:00"
        day_end = f"{day_str}T23:59:59.999999+00:00"

        scheduler.before_chunk(f"parquet_day_{day_str}")

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM claim_history WHERE {per_day_filter}",
            (day_start, day_end),
        ).fetchone()
        day_count = count_row[0] if count_row else 0

        if day_count == 0:
            continue

        out_dir = parquet_root / f"date={day_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part-00.parquet"
        tmp_path = out_dir / "part-00.parquet.tmp"

        # Idempotent: if the final partition exists with at least the expected
        # row count, just clean up the source rows. Don't re-write.
        if out_path.exists():
            try:
                pf = _pq.ParquetFile(str(out_path))
                if pf.metadata.num_rows >= day_count:
                    deleted_day = 0
                    while True:
                        scheduler.before_chunk(f"delete_archived_{day_str}")
                        tc = time.monotonic()
                        n = _delete_archived_day_chunk(
                            conn, "createdAt", day_start, day_end, DELETE_BATCH,
                        )
                        scheduler.after_chunk(
                            f"delete_archived_{day_str}", time.monotonic() - tc, n,
                        )
                        deleted_day += n
                        if n < DELETE_BATCH:
                            break
                        scheduler.sleep_between_chunks()
                    total_deleted += deleted_day
                    files.append(str(out_path))
                    LOG.info(
                        "parquet capture %s: existing partition kept (%d rows), deleted %d source rows",
                        day_str, pf.metadata.num_rows, deleted_day,
                    )
                    continue
            except Exception:
                LOG.exception(
                    "parquet inspect failed for %s, will rewrite", out_path,
                )

        # Clean stale tmp from any previous interrupted run.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

        t0 = time.monotonic()
        written = 0
        batch_pylist = []
        PYLIST_BATCH = 50_000
        writer = _pq.ParquetWriter(
            str(tmp_path), pa_schema, compression="zstd",
        )
        day_aborted = False
        try:
            cols_sql = ", ".join(_CLAIM_COLS)
            offset = 0
            while offset < day_count:
                # Per-batch scheduler gate: lets the scheduler intervene
                # mid-day if writer pressure rises. A heavy day (e.g., a
                # backfill catching up 1M+ postdated rows) without this
                # would block writer attention for minutes.
                t_chunk = time.monotonic()
                try:
                    scheduler.before_chunk(f"parquet_day_{day_str}_batch")
                except Exception:
                    # AbortRetentionPass propagates -- but we want to clean
                    # up the open writer + tmp file first.
                    day_aborted = True
                    raise
                rows = conn.execute(
                    f"SELECT {cols_sql} FROM claim_history "
                    f"WHERE {per_day_filter} "
                    f"ORDER BY rowid LIMIT ? OFFSET ?",
                    (day_start, day_end, ARCHIVE_BATCH, offset),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(_CLAIM_COLS, row))
                    if record["confidence"] is not None:
                        record["confidence"] = float(record["confidence"])
                    batch_pylist.append(record)
                    written += 1
                    if len(batch_pylist) >= PYLIST_BATCH:
                        table = _pa.Table.from_pylist(batch_pylist, schema=pa_schema)
                        writer.write_table(table)
                        batch_pylist = []
                offset += len(rows)
                scheduler.after_chunk(
                    f"parquet_day_{day_str}_batch",
                    time.monotonic() - t_chunk, len(rows),
                )
                scheduler.sleep_between_chunks()
            if batch_pylist:
                table = _pa.Table.from_pylist(batch_pylist, schema=pa_schema)
                writer.write_table(table)
        finally:
            writer.close()
            if day_aborted and tmp_path.exists():
                # Tmp will be cleaned on next pass start; nothing else to do.
                pass

        # Verify file row count matches what we wrote.
        try:
            pf = _pq.ParquetFile(str(tmp_path))
            file_rows = pf.metadata.num_rows
        except Exception:
            file_rows = -1

        if written != day_count or file_rows != day_count:
            LOG.warning(
                "parquet capture mismatch for %s: source=%d wrote=%d file=%d "
                "— skipping rename and delete",
                day_str, day_count, written, file_rows,
            )
            try:
                tmp_path.unlink()
            except OSError:
                pass
            continue

        # Atomic rename only after verification.
        os.replace(str(tmp_path), str(out_path))

        receipt = {
            "date": day_str,
            "rows": written,
            "produced_at": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(),
            ),
            "duration_s": round(time.monotonic() - t0, 2),
            "src_rows": day_count,
            "dst_path": str(out_path),
            "dst_bytes": out_path.stat().st_size,
            "source": "retention_capture",
        }
        (receipts_dir / f"retention_capture_{day_str}.json").write_text(
            json.dumps(receipt, indent=2)
        )

        # Source rows preserved until receipt validates. Now delete using
        # the same indexed createdAt filter that drove the capture.
        deleted_day = 0
        while True:
            scheduler.before_chunk(f"delete_archived_{day_str}")
            tc = time.monotonic()
            n = _delete_archived_day_chunk(
                conn, "createdAt", day_start, day_end, DELETE_BATCH,
            )
            scheduler.after_chunk(
                f"delete_archived_{day_str}", time.monotonic() - tc, n,
            )
            deleted_day += n
            if n < DELETE_BATCH:
                break
            scheduler.sleep_between_chunks()

        total_archived += written
        total_deleted += deleted_day
        files.append(str(out_path))
        LOG.info(
            "parquet capture %s: %d rows -> %s, deleted %d source rows in %.1fs",
            day_str, written, out_path.name, deleted_day,
            time.monotonic() - t0,
        )

    return {
        "archived": total_archived,
        "deleted": total_deleted,
        "files": files,
    }


def _archive_and_prune_claim_history(conn, scheduler=None):
    """Dispatch claim_history archive to the configured cold-path backend.

    Default (PARQUET_CAPTURE_ENABLED=1): write date-partitioned Parquet
    directly under PARQUET_DIR. Same shape as ``_archive_claim_history`` but
    different storage format, and runs early in the pass (see
    ``run_retention_once_with_sched``) so it does not get starved by
    raw_strip / events_prune budget.

    Rollback (PARQUET_CAPTURE_ENABLED=0 or pyarrow missing): fall back to the
    legacy gzipped JSONL writer.
    """
    if PARQUET_CAPTURE_ENABLED and _HAS_PYARROW:
        return _archive_claim_history_to_parquet(conn, scheduler=scheduler)
    return _archive_claim_history(conn, scheduler=scheduler)


def _prune_table_chunk(conn, table, time_col, cutoff_iso, batch):
    """One DELETE chunk for prune. Returns rows deleted."""
    c = conn.execute(
        f"DELETE FROM {table} "
        f"WHERE rowid IN ("
        f"  SELECT rowid FROM {table} "
        f"  WHERE {time_col} < ? "
        f"  LIMIT ?"
        f")",
        (cutoff_iso, batch),
    )
    n = c.rowcount
    conn.commit()
    return n


def _delete_archived_day_chunk(conn, retention_col, day_start, day_end, batch):
    """One DELETE chunk for an already-archived day's claim_history rows."""
    c = conn.execute(
        f"DELETE FROM claim_history "
        f"WHERE rowid IN ("
        f"  SELECT rowid FROM claim_history "
        f"  WHERE {retention_col} >= ? AND {retention_col} <= ? "
        f"  LIMIT ?"
        f")",
        (day_start, day_end, batch),
    )
    n = c.rowcount
    conn.commit()
    return n


def _incremental_vacuum_chunk(conn):
    """Reclaim freelist pages. Delegates to the shared, correct implementation.

    See maintenance.incremental_vacuum_chunk: ``conn.execute("PRAGMA
    incremental_vacuum(N)")`` reclaims exactly one page regardless of N, which
    is what this function used to do.
    """
    from .maintenance import incremental_vacuum_chunk
    result = incremental_vacuum_chunk(conn, pages=1000)
    conn.commit()
    return result


def _adaptive_sleep(get_backlog) -> float:
    """How long to sleep between retention chunks based on ingest pressure."""
    base = BATCH_SLEEP_SEC
    if get_backlog is None:
        return base
    try:
        backlog = get_backlog()
    except Exception:
        return base
    if backlog > BACKLOG_HIGH:
        return base * 4
    if backlog > BACKLOG_MED:
        return base * 2
    return base


async def _strip_old_raw_via_writer(submit_mutation, get_backlog):
    """Strip raw JSON via writer thread. Cooperative; honest stats."""
    cutoff = time.time() - RAW_STRIP_AGE_SEC
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))

    rows = 0
    chunks_attempted = 0
    chunks_completed = 0
    chunks_failed = 0

    while True:
        chunks_attempted += 1
        try:
            n = await submit_mutation(_strip_raw_chunk, cutoff_iso)
            chunks_completed += 1
            rows += n
        except Exception:
            LOG.exception("strip raw chunk failed")
            chunks_failed += 1
            n = 0

        if n < STRIP_BATCH:
            break
        await asyncio.sleep(_adaptive_sleep(get_backlog))

    return {
        "rows": rows,
        "chunks_attempted": chunks_attempted,
        "chunks_completed": chunks_completed,
        "chunks_failed": chunks_failed,
    }


async def _prune_table_via_writer(submit_mutation, get_backlog,
                                   table, time_col, retention_sec):
    cutoff = time.time() - retention_sec
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))

    rows = 0
    chunks_attempted = 0
    chunks_completed = 0
    chunks_failed = 0

    while True:
        chunks_attempted += 1
        try:
            n = await submit_mutation(
                _prune_table_chunk, table, time_col, cutoff_iso, DELETE_BATCH,
            )
            chunks_completed += 1
            rows += n
        except Exception:
            LOG.exception("prune chunk failed (table=%s)", table)
            chunks_failed += 1
            n = 0

        if n < DELETE_BATCH:
            break
        await asyncio.sleep(_adaptive_sleep(get_backlog))

    return {
        "rows": rows,
        "chunks_attempted": chunks_attempted,
        "chunks_completed": chunks_completed,
        "chunks_failed": chunks_failed,
    }


def _archive_day_read_and_write(day_str, day_start, day_end):
    """Read claim_history rows for one day and append-or-write JSONL.gz.

    Sync, runs in default executor (off the event loop). No DB writes;
    only reads on a short-lived connection. Returns (written, day_count)
    so the async caller can decide whether to issue DELETEs.
    """
    from .db import get_conn

    retention_col = "COALESCE(observed_at, createdAt)"
    archive_dir = ARCHIVE_DIR / "claim_history"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{day_str}.jsonl.gz"

    cols_sql = ", ".join(_CLAIM_COLS)
    mode = "ab" if archive_path.exists() else "wb"
    written = 0

    rconn = get_conn()
    try:
        count_row = rconn.execute(
            f"SELECT COUNT(*) FROM claim_history "
            f"WHERE {retention_col} >= ? AND {retention_col} <= ?",
            (day_start, day_end),
        ).fetchone()
        day_count = count_row[0] if count_row else 0
        if day_count == 0:
            return (0, 0, str(archive_path))

        with gzip.open(str(archive_path), mode) as gz:
            offset = 0
            while offset < day_count:
                rows = rconn.execute(
                    f"SELECT rowid, {cols_sql} FROM claim_history "
                    f"WHERE {retention_col} >= ? AND {retention_col} <= ? "
                    f"ORDER BY rowid LIMIT ? OFFSET ?",
                    (day_start, day_end, ARCHIVE_BATCH, offset),
                ).fetchall()
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(_CLAIM_COLS, row[1:]))
                    line = json.dumps(record, sort_keys=True, ensure_ascii=True)
                    gz.write(line.encode("utf-8"))
                    gz.write(b"\n")
                    written += 1
                offset += len(rows)
    finally:
        rconn.close()

    return (written, day_count, str(archive_path))


async def _archive_claim_history_via_writer(submit_mutation, get_backlog):
    """Archive + prune claim_history. Reads on a short-lived connection
    (off the event loop via default executor); deletes via writer thread.
    """
    from .db import get_conn

    cutoff = time.time() - CLAIM_RETENTION_SEC
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))
    retention_col = "COALESCE(observed_at, createdAt)"

    loop = asyncio.get_event_loop()

    def _list_days():
        rconn = get_conn()
        try:
            return rconn.execute(
                f"SELECT DISTINCT DATE({retention_col}) as d "
                f"FROM claim_history "
                f"WHERE {retention_col} < ? "
                f"ORDER BY d",
                (cutoff_iso,),
            ).fetchall()
        finally:
            rconn.close()

    days_rows = await loop.run_in_executor(None, _list_days)
    if not days_rows:
        return {
            "archived": 0, "deleted": 0, "files": [],
            "chunks_attempted": 0, "chunks_completed": 0, "chunks_failed": 0,
        }

    total_archived = 0
    total_deleted = 0
    files = []
    chunks_attempted = 0
    chunks_completed = 0
    chunks_failed = 0

    for (day_str,) in days_rows:
        if not day_str:
            continue
        day_start = f"{day_str}T00:00:00+00:00"
        day_end = f"{day_str}T23:59:59.999999+00:00"

        written, day_count, archive_path = await loop.run_in_executor(
            None, _archive_day_read_and_write, day_str, day_start, day_end,
        )
        if day_count == 0:
            continue
        if written < day_count:
            LOG.warning(
                "archive count mismatch for %s: wrote %d, expected %d "
                "— skipping delete",
                day_str, written, day_count,
            )
            continue

        deleted_day = 0
        while True:
            chunks_attempted += 1
            try:
                n = await submit_mutation(
                    _delete_archived_day_chunk,
                    retention_col, day_start, day_end, DELETE_BATCH,
                )
                chunks_completed += 1
                deleted_day += n
            except Exception:
                LOG.exception(
                    "delete archived-day chunk failed (day=%s)", day_str,
                )
                chunks_failed += 1
                n = 0
            if n < DELETE_BATCH:
                break
            await asyncio.sleep(_adaptive_sleep(get_backlog))

        total_archived += written
        total_deleted += deleted_day
        files.append(archive_path)
        LOG.info(
            "archived %s: %d rows -> %s, deleted %d",
            day_str, written, archive_path.rsplit("/", 1)[-1], deleted_day,
        )

    return {
        "archived": total_archived,
        "deleted": total_deleted,
        "files": files,
        "chunks_attempted": chunks_attempted,
        "chunks_completed": chunks_completed,
        "chunks_failed": chunks_failed,
    }


async def run_retention_once_async(consumer):
    """Run retention via the persistent writer thread (single-writer
    invariant). All mutations route through consumer.submit_mutation.
    Returns stats dict shaped to be a strict superset of the legacy sync
    path's stats — same top-level keys (raw_stripped, events_pruned, ...)
    with new ``*_chunks`` sub-dicts that report attempted/completed/failed
    counts instead of -1 sentinels.
    """
    submit_mutation = consumer.submit_mutation
    get_backlog = consumer.get_ingest_backlog

    t0 = time.monotonic()
    stats = {}

    def _record_op(key, result, label):
        stats[key] = result["rows"]
        stats[f"{key}_chunks"] = {
            "attempted": result["chunks_attempted"],
            "completed": result["chunks_completed"],
            "failed": result["chunks_failed"],
        }
        if result["rows"] > 0 or result["chunks_failed"] > 0:
            LOG.info(
                "%s: rows=%d chunks=a%d/c%d/f%d",
                label, result["rows"],
                result["chunks_attempted"],
                result["chunks_completed"],
                result["chunks_failed"],
            )

    # 1. Strip raw JSON
    try:
        result = await _strip_old_raw_via_writer(submit_mutation, get_backlog)
        _record_op("raw_stripped", result, "raw_strip")
    except Exception:
        LOG.exception("raw strip wrapper failed")
        stats["raw_stripped"] = 0
        stats["raw_stripped_chunks"] = {"attempted": 0, "completed": 0, "failed": 1}

    # 2. Prune events
    try:
        result = await _prune_table_via_writer(
            submit_mutation, get_backlog, "events", "ctime", EVENTS_RETENTION_SEC,
        )
        _record_op("events_pruned", result, "events_prune")
    except Exception:
        LOG.exception("events prune wrapper failed")
        stats["events_pruned"] = 0
        stats["events_pruned_chunks"] = {"attempted": 0, "completed": 0, "failed": 1}

    # 3. Prune event_versions
    try:
        result = await _prune_table_via_writer(
            submit_mutation, get_backlog,
            "event_versions", "version_ts", EVENTS_RETENTION_SEC,
        )
        _record_op("event_versions_pruned", result, "event_versions_prune")
    except Exception:
        LOG.exception("event_versions prune wrapper failed")
        stats["event_versions_pruned"] = 0
        stats["event_versions_pruned_chunks"] = {"attempted": 0, "completed": 0, "failed": 1}

    # 4. Prune edges
    try:
        result = await _prune_table_via_writer(
            submit_mutation, get_backlog,
            "edges", "ctime", EDGES_RETENTION_SEC,
        )
        _record_op("edges_pruned", result, "edges_prune")
    except Exception:
        LOG.exception("edges prune wrapper failed")
        stats["edges_pruned"] = 0
        stats["edges_pruned_chunks"] = {"attempted": 0, "completed": 0, "failed": 1}

    # 5. Archive + prune claim_history
    try:
        result = await _archive_claim_history_via_writer(submit_mutation, get_backlog)
        stats["claims_archived"] = result["archived"]
        stats["claims_pruned"] = result["deleted"]
        stats["archive_files"] = result["files"]
        stats["claims_archive_chunks"] = {
            "attempted": result["chunks_attempted"],
            "completed": result["chunks_completed"],
            "failed": result["chunks_failed"],
        }
        if result["archived"] > 0 or result["chunks_failed"] > 0:
            LOG.info(
                "claims archive: archived=%d deleted=%d chunks=a%d/c%d/f%d",
                result["archived"], result["deleted"],
                result["chunks_attempted"],
                result["chunks_completed"],
                result["chunks_failed"],
            )
    except Exception:
        LOG.exception("claim_history archive wrapper failed")
        stats["claims_archived"] = 0
        stats["claims_pruned"] = 0
        stats["claims_archive_chunks"] = {"attempted": 0, "completed": 0, "failed": 1}

    # 6. Incremental vacuum (writer-thread, no-op when mode != 2)
    try:
        stats["incremental_vacuum"] = await submit_mutation(_incremental_vacuum_chunk)
    except Exception:
        LOG.exception("incremental_vacuum failed")
        stats["incremental_vacuum"] = {"failed": True}

    # 7. DB geometry snapshot — read-only, short-lived connection
    try:
        from .db import get_conn, DATA_DIR

        def _read_geometry():
            rconn = get_conn()
            try:
                page_count = rconn.execute("PRAGMA page_count").fetchone()[0]
                page_size = rconn.execute("PRAGMA page_size").fetchone()[0]
                freelist_count = rconn.execute("PRAGMA freelist_count").fetchone()[0]
            finally:
                rconn.close()
            db_size_mb = round(page_count * page_size / (1024 * 1024), 1)
            freelist_mb = round(freelist_count * page_size / (1024 * 1024), 1)
            freelist_pct = round(100 * freelist_count / max(page_count, 1), 1)
            wal_path = DATA_DIR / "labeler.sqlite-wal"
            wal_mb = (
                round(wal_path.stat().st_size / (1024 * 1024), 1)
                if wal_path.exists() else 0.0
            )
            return {
                "db_size_mb": db_size_mb,
                "freelist_mb": freelist_mb,
                "freelist_pct": freelist_pct,
                "wal_mb": wal_mb,
            }

        stats["db_geometry"] = await asyncio.get_event_loop().run_in_executor(
            None, _read_geometry,
        )
        g = stats["db_geometry"]
        LOG.info(
            "db geometry: size=%.0fMB freelist=%.0fMB(%.1f%%) wal=%.1fMB",
            g["db_size_mb"], g["freelist_mb"], g["freelist_pct"], g["wal_mb"],
        )
    except Exception:
        LOG.exception("db geometry check failed")

    elapsed = time.monotonic() - t0
    LOG.info(
        "retention pass complete in %.1fs: %s", elapsed,
        {k: v for k, v in stats.items() if k not in ("archive_files", "db_geometry")},
    )
    if elapsed > DEFAULT_INTERVAL_SEC:
        LOG.warning(
            "retention fell behind: pass took %.0fs > interval %ds — "
            "passes will overlap, sustained writer contention likely",
            elapsed, DEFAULT_INTERVAL_SEC,
        )

    try:
        from .bake_gate import record_retention_stats
        record_retention_stats(stats)
    except Exception:
        pass

    return stats


def run_retention_once(conn=None):
    """Run all retention tasks once. Returns stats dict."""
    from .db import get_conn

    own_conn = conn is None
    if own_conn:
        conn = get_conn()
        conn.execute(f"PRAGMA busy_timeout={RETENTION_BUSY_TIMEOUT_MS}")

    t0 = time.monotonic()
    stats = {}

    # 1. Archive + prune old claim_history (14d default).
    # Runs FIRST so the cold-path producer is not starved by raw_strip /
    # events_prune budget. See run_retention_once_with_sched comment for
    # background.
    try:
        t = time.monotonic()
        archive_stats = _archive_and_prune_claim_history(conn)
        stats["claims_archived"] = archive_stats["archived"]
        stats["claims_pruned"] = archive_stats["deleted"]
        stats["archive_files"] = archive_stats["files"]
        if archive_stats["archived"] > 0:
            LOG.info("archived %d + pruned %d claim_history rows in %.1fs",
                     archive_stats["archived"], archive_stats["deleted"],
                     time.monotonic() - t)
    except Exception:
        LOG.exception("claim_history archive/prune failed")
        stats["claims_archived"] = -1
        stats["claims_pruned"] = -1

    # 2. Strip raw JSON from old events (24h default)
    try:
        t = time.monotonic()
        n = _strip_old_raw(conn)
        stats["raw_stripped"] = n
        if n > 0:
            LOG.info("stripped raw from %d events in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("raw strip failed")
        stats["raw_stripped"] = -1

    # 3. Prune old events (7d default)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "events", "ctime", EVENTS_RETENTION_SEC)
        stats["events_pruned"] = n
        if n > 0:
            LOG.info("pruned %d events in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("events prune failed")
        stats["events_pruned"] = -1

    # 4. Prune old event_versions (same window as events)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "event_versions", "version_ts", EVENTS_RETENTION_SEC)
        stats["event_versions_pruned"] = n
        if n > 0:
            LOG.info("pruned %d event_versions in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("event_versions prune failed")
        stats["event_versions_pruned"] = -1

    # 5. Prune old edges (14d default)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "edges", "ctime", EDGES_RETENTION_SEC)
        stats["edges_pruned"] = n
        if n > 0:
            LOG.info("pruned %d edges in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("edges prune failed")
        stats["edges_pruned"] = -1

    # 6. WAL truncation is owned by the persistent writer thread (see
    # consumer.py _maybe_wal_truncate). Retention's chunked DELETE/UPDATE
    # batches make poor TRUNCATE candidates because they're contended; the
    # writer's post-commit moment is the cleanest restart point. Calling
    # TRUNCATE here would also be a single-writer-invariant violation —
    # mutation paths must converge through the writer thread.

    # 6b. Incremental vacuum: reclaim freelist pages without needing temp space.
    # Only effective when the DB is auto_vacuum=INCREMENTAL (mode 2). On a
    # mode-0 DB nothing can be reclaimed in place — that is now reported as
    # status "mode_incompatible" rather than a bare {"skipped": true}, because
    # a database that structurally cannot reclaim space is a fault, not an
    # idle pass. /health/extended surfaces it under "reclaim".
    try:
        from .maintenance import incremental_vacuum_chunk
        stats["incremental_vacuum"] = incremental_vacuum_chunk(conn, pages=1000)
        if stats["incremental_vacuum"].get("status") == "mode_incompatible":
            LOG.warning(
                "incremental_vacuum: %s",
                stats["incremental_vacuum"].get("detail", "mode incompatible"),
            )
    except Exception:
        LOG.exception("incremental_vacuum failed")
        stats["incremental_vacuum"] = {"status": "failed"}

    # 7. DB geometry snapshot (for VACUUM planning and growth monitoring)
    try:
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0]
        db_size_mb = round(page_count * page_size / (1024 * 1024), 1)
        freelist_mb = round(freelist_count * page_size / (1024 * 1024), 1)
        freelist_pct = round(100 * freelist_count / max(page_count, 1), 1)

        # WAL file size
        import pathlib as _pl
        from .db import DATA_DIR
        wal_path = DATA_DIR / "labeler.sqlite-wal"
        wal_mb = round(wal_path.stat().st_size / (1024 * 1024), 1) if wal_path.exists() else 0.0

        stats["db_geometry"] = {
            "db_size_mb": db_size_mb,
            "freelist_mb": freelist_mb,
            "freelist_pct": freelist_pct,
            "wal_mb": wal_mb,
        }
        LOG.info("db geometry: size=%.0fMB freelist=%.0fMB(%.1f%%) wal=%.1fMB",
                 db_size_mb, freelist_mb, freelist_pct, wal_mb)
    except Exception:
        LOG.exception("db geometry check failed")

    elapsed = time.monotonic() - t0
    LOG.info("retention pass complete in %.1fs: %s", elapsed,
             {k: v for k, v in stats.items() if k not in ("archive_files", "db_geometry")})
    if elapsed > DEFAULT_INTERVAL_SEC:
        LOG.warning(
            "retention fell behind: pass took %.0fs > interval %ds — "
            "passes will overlap, sustained writer contention likely",
            elapsed, DEFAULT_INTERVAL_SEC,
        )

    if own_conn:
        conn.close()

    # Record stats for bake gate
    try:
        from .bake_gate import record_retention_stats
        record_retention_stats(stats)
    except Exception:
        pass

    return stats


def run_retention_once_with_sched(scheduler, conn=None) -> dict:
    """Pressure-aware retention pass.

    Drives the same chunk helpers as ``run_retention_once`` but routes
    every chunk through ``scheduler.before_chunk`` / ``after_chunk``.
    Aborts cleanly on ``AbortRetentionPass`` — the chunks already
    committed remain committed, the rest of the pass is skipped, and the
    scheduler records the abort reason on its pass record.

    Returns the stats dict (same shape as legacy) plus optional
    ``aborted=True`` and ``abort_reason`` keys when the pass did not
    complete fully.
    """
    from .db import get_conn
    from .retention_scheduler import AbortRetentionPass

    if not scheduler.begin_pass():
        return {"skipped": True, "skip_reason": "pre_pass_pressure"}

    own_conn = conn is None
    if own_conn:
        conn = get_conn()
        conn.execute(f"PRAGMA busy_timeout={RETENTION_BUSY_TIMEOUT_MS}")

    t0 = time.monotonic()
    stats: dict = {}
    aborted = False
    abort_reason: str | None = None

    def _run_op(label: str, fn) -> None:
        nonlocal aborted, abort_reason
        if aborted:
            stats[f"{label}_skipped"] = True
            return
        try:
            stats[label] = fn()
        except AbortRetentionPass as e:
            aborted = True
            abort_reason = e.reason
            stats[f"{label}_partial"] = True
            LOG.warning("retention aborted during %s: %s", label, e.reason)
        except _LockPressure as lp:
            # Soft abort: busy_timeout elapsed during a chunk. Rows
            # already committed by prior chunks remain in the DB.
            # Record the honest partial count + lock_pressure flag;
            # -1 is reserved for actually-unexpected exceptions.
            stats[label] = lp.partial_total
            stats[f"{label}_lock_pressure"] = True
            LOG.warning(
                "retention %s lock_pressure: %d rows committed before busy_timeout",
                label, lp.partial_total,
            )
        except Exception:
            LOG.exception("retention op %s failed", label)
            stats[label] = -1

    # claim_history archive runs FIRST (2026-05-08). It used to run last and
    # was systematically starved by raw_strip / events_prune budget under
    # pressure (skipped for ~2 weeks straight). The cold-path migration treats
    # claim_history capture as the load-bearing step: Parquet partitions are
    # the durable cold store the rest of the system increasingly depends on.
    # Heavier ops follow.
    if not aborted:
        try:
            arch = _archive_and_prune_claim_history(conn, scheduler=scheduler)
            stats["claims_archived"] = arch["archived"]
            stats["claims_pruned"] = arch["deleted"]
            stats["archive_files"] = arch["files"]
        except AbortRetentionPass as e:
            aborted = True
            abort_reason = e.reason
            stats["claims_partial"] = True
            LOG.warning("retention aborted during archive_claims: %s", e.reason)
        except sqlite3.OperationalError as e:
            if _is_lock_pressure(e):
                # Partial parquet/gzip artifacts on disk are durable and
                # idempotent-skipped on the next pass; deleted rows from
                # any completed day remain deleted. Record the lock_pressure
                # flag without claiming row counts we can't see from outside
                # the archive call.
                stats.setdefault("claims_archived", 0)
                stats.setdefault("claims_pruned", 0)
                stats["claims_lock_pressure"] = True
                LOG.warning(
                    "claim_history archive lock_pressure (partial artifacts on disk)"
                )
            else:
                LOG.exception("claim_history archive/prune failed")
                stats["claims_archived"] = -1
                stats["claims_pruned"] = -1
        except Exception:
            LOG.exception("claim_history archive/prune failed")
            stats["claims_archived"] = -1
            stats["claims_pruned"] = -1

    _run_op("raw_stripped",
            lambda: _strip_old_raw(conn, scheduler=scheduler))
    _run_op("events_pruned",
            lambda: _prune_table(conn, "events", "ctime",
                                  EVENTS_RETENTION_SEC, scheduler=scheduler))
    _run_op("event_versions_pruned",
            lambda: _prune_table(conn, "event_versions", "version_ts",
                                  EVENTS_RETENTION_SEC, scheduler=scheduler))
    _run_op("edges_pruned",
            lambda: _prune_table(conn, "edges", "ctime",
                                  EDGES_RETENTION_SEC, scheduler=scheduler))

    # Incremental vacuum — single SQL call, no chunk loop. Skip if aborted.
    if not aborted:
        try:
            from .maintenance import incremental_vacuum_chunk
            stats["incremental_vacuum"] = incremental_vacuum_chunk(conn, pages=1000)
            conn.commit()
        except Exception:
            LOG.exception("incremental_vacuum failed")
            stats["incremental_vacuum"] = {"status": "failed"}

    # DB geometry + retention lag + disk sample for scheduler health.
    try:
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0]
        db_size_b = page_count * page_size
        db_size_mb = round(db_size_b / (1024 * 1024), 1)
        freelist_mb = round(freelist_count * page_size / (1024 * 1024), 1)
        from .db import DATA_DIR
        wal_path = DATA_DIR / "labeler.sqlite-wal"
        wal_mb = (
            round(wal_path.stat().st_size / (1024 * 1024), 1)
            if wal_path.exists() else 0.0
        )
        stats["db_geometry"] = {
            "db_size_mb": db_size_mb,
            "freelist_mb": freelist_mb,
            "freelist_pct": round(100 * freelist_count / max(page_count, 1), 1),
            "wal_mb": wal_mb,
        }
        try:
            from .maintenance import check_disk_pressure
            dp = check_disk_pressure()
            free_b = int(dp.get("free_gb", 0) * (1024 ** 3))
            scheduler.record_disk_sample(db_size_b, free_b)
        except Exception:
            pass
        # Retention lag: oldest *believable* events.ctime vs the retention
        # window. ctime comes from the record's own createdAt and is fully
        # caller-controlled: production held a 2002-04-01 row that drove
        # retention_lag_s to ~11.8 years. Rows below the plausibility floor are
        # excluded from the metric and counted separately so the garbage stays
        # visible instead of silently distorting the gauge. Both queries use
        # idx_events_ctime.
        try:
            from .maintenance import CTIME_PLAUSIBLE_FLOOR, compute_retention_lag
            row = conn.execute(
                "SELECT MIN(ctime) FROM events WHERE ctime >= ?",
                (CTIME_PLAUSIBLE_FLOOR,),
            ).fetchone()
            lag_s = compute_retention_lag(
                row[0] if row else None, EVENTS_RETENTION_SEC
            )
            if lag_s is not None:
                scheduler.record_retention_lag(lag_s)
                stats["retention_lag_s"] = round(lag_s, 1)
            suspect = conn.execute(
                "SELECT COUNT(*) FROM events WHERE ctime < ?",
                (CTIME_PLAUSIBLE_FLOOR,),
            ).fetchone()
            if suspect and suspect[0]:
                stats["ctime_suspect_rows"] = suspect[0]
                LOG.warning(
                    "%d events rows have ctime before %s — excluded from "
                    "retention lag", suspect[0], CTIME_PLAUSIBLE_FLOOR,
                )
        except Exception:
            LOG.debug("retention lag read failed", exc_info=True)
    except Exception:
        LOG.exception("db geometry / disk sample failed")

    elapsed = time.monotonic() - t0
    LOG.info(
        "retention pass %s in %.1fs: %s",
        "aborted" if aborted else "complete", elapsed,
        {k: v for k, v in stats.items()
         if k not in ("archive_files", "db_geometry")},
    )

    scheduler.end_pass(stats, completed=not aborted, abort_reason=abort_reason)

    if aborted:
        stats["aborted"] = True
        stats["abort_reason"] = abort_reason

    if own_conn:
        conn.close()

    try:
        from .bake_gate import record_retention_stats
        record_retention_stats(stats)
    except Exception:
        pass

    return stats


async def run_periodic(consumer=None, scheduler=None):
    """Async retention loop.

    With ``consumer`` set, builds a pressure-aware ``RetentionScheduler``
    and uses the legacy sync path *with scheduling on top* — retention
    keeps its own write connection (does NOT route through the writer
    thread), but every chunk consults pressure signals and the
    rollback_lost tripwire. See ``retention_scheduler.py``.

    Pass an explicit ``scheduler`` to override (e.g. main.py constructs
    the scheduler so it can also be exposed on /health/extended).

    Without ``consumer``, falls back to the legacy unscheduled path with
    a no-op scheduler. Suitable for CLI cold-pass / tests.
    """
    from .retention_scheduler import RetentionScheduler, NullScheduler

    interval = DEFAULT_INTERVAL_SEC
    if scheduler is None:
        if consumer is not None:
            scheduler = RetentionScheduler(
                consumer=consumer,
                sleep_between_s=BATCH_SLEEP_SEC,
            )
        else:
            scheduler = NullScheduler(sleep_between_s=BATCH_SLEEP_SEC)
    mode = type(scheduler).__name__

    LOG.info(
        "retention loop started, interval=%ds, mode=%s "
        "(raw_strip=%dh, events=%dd, edges=%dd, claims=%dd)",
        interval, mode, RAW_STRIP_AGE_SEC // 3600,
        EVENTS_RETENTION_SEC // 86400, EDGES_RETENTION_SEC // 86400,
        CLAIM_RETENTION_SEC // 86400,
    )

    loop = asyncio.get_event_loop()
    while True:
        try:
            stats = await loop.run_in_executor(
                None, run_retention_once_with_sched, scheduler,
            )
            total = sum(
                v for v in stats.values() if isinstance(v, int) and v > 0
            )
            if total > 0 or stats.get("aborted") or stats.get("skipped"):
                LOG.info(
                    "retention pass: ops=%d aborted=%s skipped=%s",
                    total, stats.get("aborted", False),
                    stats.get("skipped", False),
                )
        except Exception:
            LOG.exception("retention pass failed")
        await asyncio.sleep(interval)
