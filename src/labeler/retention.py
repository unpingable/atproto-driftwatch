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
"""

import asyncio
import gzip
import json
import logging
import os
import pathlib
import time

LOG = logging.getLogger("labeler.retention")

DEFAULT_INTERVAL_SEC = int(os.getenv("RETENTION_INTERVAL_SEC", str(3600)))  # 1h

# Retention windows (seconds)
RAW_STRIP_AGE_SEC = int(os.getenv("RAW_STRIP_AGE_SEC", str(24 * 3600)))      # 24h
EVENTS_RETENTION_SEC = int(os.getenv("EVENTS_RETENTION_SEC", str(7 * 86400)))  # 7d
EDGES_RETENTION_SEC = int(os.getenv("EDGES_RETENTION_SEC", str(14 * 86400)))   # 14d
CLAIM_RETENTION_SEC = int(os.getenv("CLAIM_RETENTION_SEC", str(14 * 86400)))   # 14d

# Batch sizes for UPDATE/DELETE to avoid holding locks too long
STRIP_BATCH = int(os.getenv("RETENTION_STRIP_BATCH", "5000"))
DELETE_BATCH = int(os.getenv("RETENTION_DELETE_BATCH", "5000"))
ARCHIVE_BATCH = 50_000

# Sleep between batches to yield the DB lock to other writers (consumer, longitudinal)
BATCH_SLEEP_SEC = float(os.getenv("RETENTION_BATCH_SLEEP_SEC", "1.0"))

# Archive directory
ARCHIVE_DIR = pathlib.Path(os.getenv(
    "RETENTION_ARCHIVE_DIR",
    str(pathlib.Path(__file__).resolve().parents[1] / "data" / "archive"),
))

# Claim history columns to archive
_CLAIM_COLS = (
    "authorDid", "claim_fingerprint", "createdAt", "confidence",
    "provenance", "evidence_hash", "post_uri", "post_cid",
    "fingerprint_version", "evidence_class", "fp_kind", "observed_at",
)


def _strip_old_raw(conn):
    """NULL out events.raw for events older than RAW_STRIP_AGE_SEC.

    Batched to avoid long-held write locks. Returns total rows stripped.
    """
    cutoff = time.time() - RAW_STRIP_AGE_SEC
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))
    total = 0

    while True:
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
        total += n
        conn.commit()
        if n < STRIP_BATCH:
            break
        # Yield the DB lock so other writers can get through
        time.sleep(BATCH_SLEEP_SEC)

    return total


def _prune_table(conn, table, time_col, retention_sec):
    """Delete rows older than retention window. Returns rows deleted."""
    cutoff = time.time() - retention_sec
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime(cutoff))
    total = 0

    while True:
        c = conn.execute(
            f"DELETE FROM {table} "
            f"WHERE rowid IN ("
            f"  SELECT rowid FROM {table} "
            f"  WHERE {time_col} < ? "
            f"  LIMIT ?"
            f")",
            (cutoff_iso, DELETE_BATCH),
        )
        n = c.rowcount
        total += n
        conn.commit()
        if n < DELETE_BATCH:
            break
        time.sleep(BATCH_SLEEP_SEC)

    return total


def _archive_claim_history(conn) -> dict:
    """Archive old claim_history rows to gzipped JSONL, then delete.

    Uses COALESCE(observed_at, createdAt) as the retention timestamp.
    Archives one day at a time for partitioned files.
    Returns {"archived": N, "deleted": N, "files": [...]}
    """
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

        # Verify: written count should match day_count (or exceed if appending)
        if written < day_count:
            LOG.warning("archive count mismatch for %s: wrote %d, expected %d — skipping delete",
                        day_str, written, day_count)
            continue

        # Delete archived rows
        deleted_day = 0
        while True:
            c = conn.execute(
                f"DELETE FROM claim_history "
                f"WHERE rowid IN ("
                f"  SELECT rowid FROM claim_history "
                f"  WHERE {retention_col} >= ? AND {retention_col} <= ? "
                f"  LIMIT ?"
                f")",
                (day_start, day_end, DELETE_BATCH),
            )
            n = c.rowcount
            deleted_day += n
            conn.commit()
            if n < DELETE_BATCH:
                break
            time.sleep(BATCH_SLEEP_SEC)

        total_archived += written
        total_deleted += deleted_day
        files.append(str(archive_path))
        LOG.info("archived %s: %d rows → %s, deleted %d",
                 day_str, written, archive_path.name, deleted_day)

    return {"archived": total_archived, "deleted": total_deleted, "files": files}


def run_retention_once(conn=None):
    """Run all retention tasks once. Returns stats dict."""
    from .db import get_conn

    own_conn = conn is None
    if own_conn:
        conn = get_conn()
        conn.execute("PRAGMA busy_timeout=30000")

    t0 = time.monotonic()
    stats = {}

    # 1. Strip raw JSON from old events (24h default)
    try:
        t = time.monotonic()
        n = _strip_old_raw(conn)
        stats["raw_stripped"] = n
        if n > 0:
            LOG.info("stripped raw from %d events in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("raw strip failed")
        stats["raw_stripped"] = -1

    # 2. Prune old events (7d default)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "events", "ctime", EVENTS_RETENTION_SEC)
        stats["events_pruned"] = n
        if n > 0:
            LOG.info("pruned %d events in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("events prune failed")
        stats["events_pruned"] = -1

    # 3. Prune old event_versions (same window as events)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "event_versions", "version_ts", EVENTS_RETENTION_SEC)
        stats["event_versions_pruned"] = n
        if n > 0:
            LOG.info("pruned %d event_versions in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("event_versions prune failed")
        stats["event_versions_pruned"] = -1

    # 4. Prune old edges (14d default)
    try:
        t = time.monotonic()
        n = _prune_table(conn, "edges", "ctime", EDGES_RETENTION_SEC)
        stats["edges_pruned"] = n
        if n > 0:
            LOG.info("pruned %d edges in %.1fs", n, time.monotonic() - t)
    except Exception:
        LOG.exception("edges prune failed")
        stats["edges_pruned"] = -1

    # 5. Archive + prune old claim_history (14d default)
    try:
        t = time.monotonic()
        archive_stats = _archive_claim_history(conn)
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

    # 6. WAL checkpoint after bulk operations
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        LOG.exception("WAL checkpoint failed")

    elapsed = time.monotonic() - t0
    LOG.info("retention pass complete in %.1fs: %s", elapsed,
             {k: v for k, v in stats.items() if k != "archive_files"})

    if own_conn:
        conn.close()

    return stats


async def run_periodic():
    """Async retention loop."""
    interval = DEFAULT_INTERVAL_SEC
    LOG.info("retention loop started, interval=%ds (raw_strip=%dh, events=%dd, edges=%dd, claims=%dd)",
             interval, RAW_STRIP_AGE_SEC // 3600,
             EVENTS_RETENTION_SEC // 86400, EDGES_RETENTION_SEC // 86400,
             CLAIM_RETENTION_SEC // 86400)

    loop = asyncio.get_event_loop()
    while True:
        try:
            stats = await loop.run_in_executor(None, run_retention_once)
            total = sum(v for v in stats.values() if isinstance(v, int) and v > 0)
            if total > 0:
                LOG.info("retention: %d total operations", total)
        except Exception:
            LOG.exception("retention pass failed")
        await asyncio.sleep(interval)
