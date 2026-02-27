"""Export driftwatch facts to a SQLite sidecar for labelwatch consumption.

Output: /app/data/facts.sqlite (container path, host path depends on deploy config)

Strategy: update working DB in place, periodically snapshot via VACUUM INTO for
atomic reads by labelwatch.

Layout:
  facts_work.sqlite — in-place working DB (updated every cycle, WAL mode)
  facts.sqlite      — read-only snapshot (VACUUM INTO, DELETE journal, atomic replace)
"""

import asyncio
import logging
import os
import shutil
import sqlite3
import time

LOG = logging.getLogger("labeler.facts_export")

RETENTION_DAYS = 30
OVERLAP_HOURS = 72      # full overlap for first run / restart
HOURLY_OVERLAP_HOURS = 6  # steady-state hourly recompute window
BATCH_LIMIT = 500_000
DEFAULT_INTERVAL_SEC = 30 * 60  # 30 minutes
DEFAULT_SNAPSHOT_INTERVAL_SEC = 60 * 60  # 1 hour


def _default_facts_path():
    from .db import DATA_DIR
    return str(DATA_DIR / "facts.sqlite")


def _default_work_path():
    from .db import DATA_DIR
    return str(DATA_DIR / "facts_work.sqlite")


def _ensure_tables(sidecar):
    sidecar.executescript("""
        CREATE TABLE IF NOT EXISTS uri_fingerprint (
            post_uri       TEXT PRIMARY KEY,
            fingerprint    TEXT NOT NULL,
            created_epoch  INTEGER NOT NULL,
            rowid_src      INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_uri_fp ON uri_fingerprint(fingerprint);

        CREATE TABLE IF NOT EXISTS fingerprint_hourly (
            fingerprint    TEXT    NOT NULL,
            hour_epoch     INTEGER NOT NULL,
            event_count    INTEGER NOT NULL,
            unique_authors INTEGER NOT NULL,
            PRIMARY KEY (fingerprint, hour_epoch)
        );

        CREATE TABLE IF NOT EXISTS fingerprint_bounds (
            fingerprint      TEXT PRIMARY KEY,
            first_seen_epoch INTEGER NOT NULL,
            last_seen_epoch  INTEGER NOT NULL,
            total_claims     INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
    """)


def _get_meta_int(sidecar, key, default=0):
    row = sidecar.execute(
        "SELECT value FROM meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return default
    try:
        return int(row[0])
    except (ValueError, TypeError):
        return default


def _set_meta(sidecar, key, value):
    sidecar.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, str(value)),
    )


def _upsert_uri_fingerprints(source_conn, sidecar, last_rowid, batch_max_rowid):
    """Dedup by post_uri: highest rowid wins via MAX(rowid) subquery."""
    rows = source_conn.execute("""
        SELECT ch.post_uri, ch.claim_fingerprint,
               CAST(strftime('%s', ch.createdAt) AS INTEGER),
               ch.rowid
        FROM claim_history ch
        JOIN (
            SELECT post_uri, MAX(rowid) AS max_rowid
            FROM claim_history
            WHERE rowid > ? AND rowid <= ? AND post_uri IS NOT NULL
            GROUP BY post_uri
        ) m ON ch.post_uri = m.post_uri AND ch.rowid = m.max_rowid
    """, (last_rowid, batch_max_rowid)).fetchall()

    sidecar.executemany(
        "INSERT OR REPLACE INTO uri_fingerprint (post_uri, fingerprint, created_epoch, rowid_src) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )


def _recompute_hourly(source_conn, sidecar, overlap_start, now):
    overlap_start_hour = (overlap_start // 3600) * 3600

    sidecar.execute(
        "DELETE FROM fingerprint_hourly WHERE hour_epoch >= ?",
        (overlap_start_hour,),
    )

    source_rows = source_conn.execute("""
        SELECT claim_fingerprint,
               (CAST(strftime('%s', createdAt) AS INTEGER) / 3600) * 3600,
               COUNT(*),
               COUNT(DISTINCT authorDid)
        FROM claim_history
        WHERE createdAt >= datetime(?, 'unixepoch')
          AND createdAt <  datetime(?, 'unixepoch')
        GROUP BY 1, 2
    """, (overlap_start, now)).fetchall()

    sidecar.executemany(
        "INSERT OR REPLACE INTO fingerprint_hourly VALUES (?, ?, ?, ?)",
        source_rows,
    )


def _recompute_bounds(sidecar):
    """Full recompute from sidecar (small table, bounded by retention)."""
    sidecar.execute("DELETE FROM fingerprint_bounds")
    sidecar.execute("""
        INSERT INTO fingerprint_bounds (fingerprint, first_seen_epoch, last_seen_epoch, total_claims)
        SELECT fingerprint, MIN(created_epoch), MAX(created_epoch), COUNT(*)
        FROM uri_fingerprint
        GROUP BY fingerprint
    """)


def _prune(sidecar, retention_start):
    sidecar.execute(
        "DELETE FROM uri_fingerprint WHERE created_epoch < ?",
        (retention_start,),
    )
    sidecar.execute(
        "DELETE FROM fingerprint_hourly WHERE hour_epoch < ?",
        (retention_start,),
    )


def _snapshot(work_conn, snapshot_path):
    """Create a compacted snapshot via VACUUM INTO + atomic replace."""
    tmp_snap = snapshot_path + ".tmp"
    # Remove stale tmp if exists (e.g. from previous crash)
    if os.path.exists(tmp_snap):
        os.remove(tmp_snap)
    work_conn.execute(f"VACUUM INTO '{tmp_snap}'")
    # VACUUM INTO produces DELETE journal mode by default, which is what we want
    os.replace(tmp_snap, snapshot_path)


def export_once(source_conn, facts_path=None, work_path=None, force_snapshot=False):
    """Run one export cycle: update working DB in place, optionally snapshot.

    Args:
        source_conn: Connection to the main labeler.sqlite (claim_history).
        facts_path: Path for the read-only snapshot (labelwatch reads this).
        work_path: Path for the in-place working DB.
        force_snapshot: If True, always create a snapshot this cycle.
    """
    if facts_path is None:
        facts_path = _default_facts_path()
    if work_path is None:
        work_path = _default_work_path()

    t0 = time.monotonic()
    now = int(time.time())
    retention_start = now - (RETENTION_DAYS * 86400)
    overlap_start = now - (OVERLAP_HOURS * 3600)
    snap_interval = int(os.environ.get("FACTS_SNAPSHOT_INTERVAL", DEFAULT_SNAPSHOT_INTERVAL_SEC))

    # Seed working DB from existing snapshot on first run
    if not os.path.exists(work_path) and os.path.exists(facts_path):
        shutil.copyfile(facts_path, work_path)
        LOG.info("seeded working DB from existing snapshot")

    # Open working DB directly (no copy-forward!)
    sidecar = sqlite3.connect(work_path)
    sidecar.execute("PRAGMA journal_mode=WAL")
    sidecar.execute("PRAGMA busy_timeout=30000")
    _ensure_tables(sidecar)

    # Read checkpoint rowid and last export time
    last_rowid = _get_meta_int(sidecar, "last_checkpoint_rowid", 0)
    last_export = _get_meta_int(sidecar, "last_export_epoch", 0)
    rows_upserted = 0

    # Loop batches until drained
    t_batch = time.monotonic()
    while True:
        rows = source_conn.execute("""
            SELECT rowid, post_uri, claim_fingerprint, createdAt, authorDid
            FROM claim_history
            WHERE rowid > ? AND post_uri IS NOT NULL
            ORDER BY rowid LIMIT ?
        """, (last_rowid, BATCH_LIMIT)).fetchall()

        if not rows:
            break

        batch_max_rowid = rows[-1][0]
        rows_upserted += len(rows)

        _upsert_uri_fingerprints(source_conn, sidecar, last_rowid, batch_max_rowid)

        last_rowid = batch_max_rowid
        _set_meta(sidecar, "last_checkpoint_rowid", last_rowid)
        sidecar.commit()

        if len(rows) < BATCH_LIMIT:
            break
    elapsed_batch = time.monotonic() - t_batch

    # Hourly recompute window:
    # First run (or after long gap): full OVERLAP_HOURS (72h)
    # Subsequent runs: HOURLY_OVERLAP_HOURS (6h) — covers normal clock skew
    # and late ingestion while keeping the query fast. Outliers with very old
    # createdAt still get correct uri_fingerprint mappings (checkpoint-based);
    # only their hourly bin counts may be stale until the next snapshot.
    if last_export > 0 and (now - last_export) < OVERLAP_HOURS * 3600:
        hourly_start = now - (HOURLY_OVERLAP_HOURS * 3600)
    else:
        hourly_start = overlap_start

    t_hourly = time.monotonic()
    _recompute_hourly(source_conn, sidecar, hourly_start, now)
    elapsed_hourly = time.monotonic() - t_hourly

    # Prune all tables to retention
    t_prune = time.monotonic()
    _prune(sidecar, retention_start)
    elapsed_prune = time.monotonic() - t_prune

    # Update meta
    _set_meta(sidecar, "last_export_epoch", now)
    sidecar.commit()

    elapsed_update = time.monotonic() - t0

    # Snapshot if interval elapsed or forced
    # Recompute bounds only before snapshot (expensive, only needed for reads)
    last_snap = _get_meta_int(sidecar, "last_snapshot_epoch", 0)
    did_snapshot = False
    elapsed_bounds = 0.0
    if force_snapshot or (now - last_snap >= snap_interval):
        t_bounds = time.monotonic()
        _recompute_bounds(sidecar)
        sidecar.commit()
        elapsed_bounds = time.monotonic() - t_bounds

        t_snap = time.monotonic()
        _snapshot(sidecar, facts_path)
        _set_meta(sidecar, "last_snapshot_epoch", now)
        sidecar.commit()
        snap_elapsed = time.monotonic() - t_snap
        did_snapshot = True
        snap_mb = os.path.getsize(facts_path) / (1024 * 1024)
        LOG.info("snapshot created: %.0fMB in %.1fs (bounds=%.1fs)", snap_mb, snap_elapsed, elapsed_bounds)

    sidecar.close()

    elapsed = time.monotonic() - t0
    work_mb = os.path.getsize(work_path) / (1024 * 1024)
    hourly_window_h = (now - hourly_start) / 3600
    LOG.info("facts export complete: %.1fs (batch=%.1fs, hourly=%.1fs/%.0fh, prune=%.1fs, bounds=%.1fs, %d new rows, work=%.0fMB%s)",
             elapsed, elapsed_batch, elapsed_hourly, hourly_window_h,
             elapsed_prune, elapsed_bounds, rows_upserted, work_mb,
             ", +snapshot" if did_snapshot else "")


async def run_periodic(facts_path=None, work_path=None):
    """Run export on a periodic loop (async-friendly)."""
    interval = int(os.environ.get("FACTS_EXPORT_INTERVAL", DEFAULT_INTERVAL_SEC))
    if facts_path is None:
        facts_path = _default_facts_path()
    if work_path is None:
        work_path = _default_work_path()

    LOG.info("facts export periodic started, interval=%ds, path=%s", interval, facts_path)

    loop = asyncio.get_event_loop()
    first_run = True
    while True:
        try:
            _fp = facts_path
            _wp = work_path
            _force = first_run  # always snapshot on first run
            def _run():
                from .db import get_conn
                source_conn = get_conn()
                try:
                    export_once(source_conn, _fp, _wp, force_snapshot=_force)
                finally:
                    source_conn.close()
            await loop.run_in_executor(None, _run)
            first_run = False
        except Exception:
            LOG.exception("facts export failed")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # One-shot export for manual/cron runs
    from .db import get_conn
    facts_path = sys.argv[1] if len(sys.argv) > 1 else _default_facts_path()
    work_path = _default_work_path()
    source_conn = get_conn()
    export_once(source_conn, facts_path, work_path, force_snapshot=True)
    source_conn.close()
    LOG.info("one-shot export done")
