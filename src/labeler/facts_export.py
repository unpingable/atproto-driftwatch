"""Export driftwatch facts to a SQLite sidecar for labelwatch consumption.

Output: /app/data/facts.sqlite (container) = /opt/driftwatch/deploy/data/facts.sqlite (host)

Strategy: copy-forward existing sidecar → tmp, update incrementally, prune, atomic replace.
"""

import asyncio
import logging
import os
import shutil
import sqlite3
import time

LOG = logging.getLogger("labeler.facts_export")

RETENTION_DAYS = 30
OVERLAP_HOURS = 72
BATCH_LIMIT = 500_000
DEFAULT_INTERVAL_SEC = 30 * 60  # 30 minutes


def _default_facts_path():
    from .db import DATA_DIR
    return str(DATA_DIR / "facts.sqlite")


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


def export_once(source_conn, facts_path=None):
    """Run one export cycle: copy-forward → update → prune → atomic replace."""
    if facts_path is None:
        facts_path = _default_facts_path()

    tmp_path = facts_path + ".tmp"
    now = int(time.time())
    retention_start = now - (RETENTION_DAYS * 86400)
    overlap_start = now - (OVERLAP_HOURS * 3600)

    # 1. Copy-forward (preserves accumulated 30d data)
    if os.path.exists(facts_path):
        shutil.copyfile(facts_path, tmp_path)

    # 2. Open tmp (or create fresh)
    sidecar = sqlite3.connect(tmp_path)
    sidecar.execute("PRAGMA journal_mode=DELETE")
    _ensure_tables(sidecar)

    # 3. Read checkpoint rowid
    last_rowid = _get_meta_int(sidecar, "last_checkpoint_rowid", 0)

    # 4. Loop batches until drained
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

        # 5. Upsert uri_fingerprint (dedup by post_uri, highest rowid wins)
        _upsert_uri_fingerprints(source_conn, sidecar, last_rowid, batch_max_rowid)

        last_rowid = batch_max_rowid
        _set_meta(sidecar, "last_checkpoint_rowid", last_rowid)

        if len(rows) < BATCH_LIMIT:
            break

    # 6. Recompute fingerprint_hourly for 72h overlap (delete/replace from source)
    _recompute_hourly(source_conn, sidecar, overlap_start, now)

    # 7. Prune all tables to retention
    _prune(sidecar, retention_start)

    # 8. Recompute fingerprint_bounds from sidecar (AFTER prune)
    _recompute_bounds(sidecar)

    # 9. Update meta
    _set_meta(sidecar, "last_export_epoch", now)

    sidecar.commit()
    sidecar.close()

    # 10. Atomic replace
    os.replace(tmp_path, facts_path)
    LOG.info("facts export complete: %s", facts_path)


async def run_periodic(facts_path=None):
    """Run export on a periodic loop (async-friendly)."""
    interval = int(os.environ.get("FACTS_EXPORT_INTERVAL", DEFAULT_INTERVAL_SEC))
    if facts_path is None:
        facts_path = _default_facts_path()

    LOG.info("facts export periodic started, interval=%ds, path=%s", interval, facts_path)

    while True:
        try:
            from .db import get_conn
            source_conn = get_conn()
            export_once(source_conn, facts_path)
            source_conn.close()
        except Exception:
            LOG.exception("facts export failed")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # One-shot export for manual/cron runs
    from .db import get_conn
    facts_path = sys.argv[1] if len(sys.argv) > 1 else _default_facts_path()
    source_conn = get_conn()
    export_once(source_conn, facts_path)
    source_conn.close()
    LOG.info("one-shot export done")
