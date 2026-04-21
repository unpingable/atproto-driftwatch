"""Backfill DID vintage from plc.directory /export.

Primary path for enriching actor_identity_current.did_created_at. Paginated
through /export?after=<seq>&count=1000, picks out the first op per DID we
already know about, writes via set_vintage_many. The global PLC log contains
all DIDs (millions); we enrich only the ones in our local actor_identity_current
where vintage is still NULL.

The directory serves ops in sequence order. The first op we see for each DID
in sequence order is its genesis op (that's how the log is constructed).

Write-once is enforced by set_vintage: rows with non-null did_created_at are
skipped. Rerunning the script is safe but wasted work; use --limit during
pilot validation.

Usage:
    python3 scripts/plc_export_backfill.py \
        --db /opt/driftwatch/deploy/data/labeler.sqlite \
        --after 0 \
        --max-ops 1000000 \
        --note "pilot"

    # Resume an interrupted run:
    python3 scripts/plc_export_backfill.py \
        --db /opt/driftwatch/deploy/data/labeler.sqlite \
        --resume-run <run_id>
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

# Support running as a script: add src to path
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "src"))

from labeler.vintage import (  # noqa: E402
    set_vintage_many,
    start_export_run,
    finish_export_run,
)

LOG = logging.getLogger("plc_export_backfill")

PLC_EXPORT_URL = "https://plc.directory/export"
USER_AGENT = "driftwatch-vintage/0.1 (+https://github.com/unpingable/atproto-driftwatch)"
DEFAULT_COUNT = 1000
DEFAULT_BATCH_FLUSH = 2000  # flush set_vintage_many every N recorded ops
DEFAULT_SLEEP_S = 1.0       # polite pacing between pages
HTTP_TIMEOUT_S = 30
MAX_HTTP_RETRIES = 5


def fetch_page(after: str, count: int) -> list[dict]:
    """GET /export?after=<after>&count=<count>. Returns parsed JSONL rows.

    Empty list means no more ops after `after`.
    Retries on transient network / 5xx errors with exponential backoff.
    """
    params = urllib.parse.urlencode({"after": after, "count": count})
    url = f"{PLC_EXPORT_URL}?{params}"
    attempt = 0
    while True:
        attempt += 1
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/jsonl",
                "User-Agent": USER_AGENT,
            })
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
                body = resp.read()
            rows = []
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    import json
                    rows.append(json.loads(line))
                except Exception:
                    LOG.warning("skipping malformed JSONL line")
            return rows
        except urllib.error.HTTPError as e:
            if e.code == 429 or 500 <= e.code < 600:
                backoff = min(60, 2 ** attempt)
                LOG.warning("http_%d after=%s attempt=%d backoff=%ds", e.code, after, attempt, backoff)
                if attempt >= MAX_HTTP_RETRIES:
                    raise
                time.sleep(backoff)
                continue
            raise
        except Exception as e:
            backoff = min(60, 2 ** attempt)
            LOG.warning("network error %s after=%s attempt=%d backoff=%ds",
                        type(e).__name__, after, attempt, backoff)
            if attempt >= MAX_HTTP_RETRIES:
                raise
            time.sleep(backoff)
            continue


def load_target_dids(conn: sqlite3.Connection) -> set[str]:
    """Load DIDs that need vintage enrichment (did_created_at IS NULL).

    Scopes the backfill to DIDs we already know about, so we don't have to
    buffer the entire PLC directory. Unsupported methods (did:web) are
    filtered — set_vintage_many would reject them anyway.
    """
    rows = conn.execute(
        "SELECT did FROM actor_identity_current "
        "WHERE did_created_at IS NULL AND did LIKE 'did:plc:%'"
    ).fetchall()
    return {r[0] for r in rows}


def run_backfill(
    db_path: str,
    after: str = "",
    max_ops: int = 0,
    count: int = DEFAULT_COUNT,
    sleep_s: float = DEFAULT_SLEEP_S,
    batch_flush: int = DEFAULT_BATCH_FLUSH,
    note: str | None = None,
    run_id: str | None = None,
) -> dict:
    """Run the backfill. Returns summary stats dict."""
    conn = sqlite3.connect(db_path)
    # 5 min busy_timeout: the main driftwatch container can hold the write
    # lock for tens of seconds during heavy ingest bursts. Default 60s wasn't
    # enough; one lock miss crashed a 16h run.
    conn.execute("PRAGMA busy_timeout=300000")
    conn.execute("PRAGMA journal_mode=WAL")

    run_id = run_id or f"plc_export_{time.strftime('%Y%m%d_%H%M%SZ', time.gmtime())}_{uuid.uuid4().hex[:8]}"

    # Check if this is a resume
    existing = conn.execute(
        "SELECT last_cursor, ops_processed, dids_seen, genesis_ops_written "
        "FROM plc_export_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if existing:
        if existing[0] is not None:
            after = existing[0]
        LOG.info("resuming run_id=%s from after=%s", run_id, after)
        ops_processed = existing[1] or 0
        dids_seen_count = existing[2] or 0
        genesis_written = existing[3] or 0
    else:
        start_export_run(conn, run_id, note=note)
        conn.commit()
        ops_processed = 0
        dids_seen_count = 0
        genesis_written = 0

    target_dids = load_target_dids(conn)
    LOG.info("run_id=%s target_dids=%d after=%s", run_id, len(target_dids), after or "0")

    first_cursor = None
    last_cursor = None
    pending: list[tuple[str, str]] = []  # (did, createdAt) not yet flushed
    pending_dids: set[str] = set()  # DIDs already queued in `pending` (dedupe)
    seen_in_run: set[str] = set()  # DIDs we've already picked up (genesis captured)

    def flush():
        nonlocal genesis_written, pending, pending_dids
        if not pending:
            return
        # Retry loop for SQLite lock contention against the main driftwatch
        # container. Each attempt waits up to 5 min via busy_timeout; if that
        # times out, back off and retry before giving up on the whole run.
        max_attempts = 6
        for attempt in range(1, max_attempts + 1):
            try:
                stats = set_vintage_many(conn, pending, source="plc_export", run_id=run_id)
                conn.execute(
                    "UPDATE plc_export_runs SET "
                    "last_cursor = ?, ops_processed = ?, dids_seen = ?, "
                    "genesis_ops_written = genesis_ops_written + ? "
                    "WHERE run_id = ?",
                    (last_cursor, ops_processed, dids_seen_count, stats["written"], run_id),
                )
                conn.commit()
                genesis_written += stats["written"]
                LOG.info("flushed batch=%d written=%d skipped_existing=%d missing_row=%d",
                         len(pending), stats["written"], stats["skipped_existing"], stats["skipped_missing_row"])
                break
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or attempt == max_attempts:
                    raise
                backoff = min(120, 10 * attempt)
                LOG.warning("flush: db locked (attempt %d/%d), backing off %ds: %s",
                            attempt, max_attempts, backoff, e)
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(backoff)
        pending = []
        pending_dids = set()

    try:
        while True:
            rows = fetch_page(after, count)
            if not rows:
                LOG.info("empty page — reached end of export")
                break

            for row in rows:
                ops_processed += 1
                did = row.get("did")
                created_at = row.get("createdAt")
                # plc.directory /export paginates via ?after=<createdAt>; the
                # last createdAt in each page is our next cursor.
                if created_at:
                    last_cursor = created_at
                if first_cursor is None and created_at:
                    first_cursor = created_at

                if not did or not created_at:
                    continue
                if did in seen_in_run:
                    continue  # already captured genesis for this DID in this run
                if did not in target_dids:
                    continue  # not a DID we care about

                # This is the first op for a DID we want — it IS the genesis
                # (PLC log is sequence-ordered; first op per DID is genesis by construction).
                seen_in_run.add(did)
                dids_seen_count += 1
                if did not in pending_dids:
                    pending.append((did, created_at))
                    pending_dids.add(did)

                if len(pending) >= batch_flush:
                    flush()

                if max_ops and ops_processed >= max_ops:
                    break

            # Advance cursor: use the last createdAt of this page as the next `after`.
            if rows:
                next_after = rows[-1].get("createdAt")
                if next_after and next_after != after:
                    after = next_after
                else:
                    LOG.warning("cursor did not advance (after=%s); stopping to avoid loop", after)
                    break

            if max_ops and ops_processed >= max_ops:
                LOG.info("reached max_ops=%d", max_ops)
                break

            time.sleep(sleep_s)

        flush()
        finish_export_run(
            conn, run_id,
            first_cursor=first_cursor, last_cursor=last_cursor,
            ops_processed=ops_processed,
            dids_seen=dids_seen_count,
            genesis_ops_written=genesis_written,
            status="completed",
        )
        conn.commit()
    except KeyboardInterrupt:
        LOG.warning("interrupted — flushing and marking run as interrupted")
        flush()
        finish_export_run(
            conn, run_id,
            first_cursor=first_cursor, last_cursor=last_cursor,
            ops_processed=ops_processed,
            dids_seen=dids_seen_count,
            genesis_ops_written=genesis_written,
            status="interrupted",
            note="user interrupt",
        )
        conn.commit()
        raise
    finally:
        conn.close()

    return {
        "run_id": run_id,
        "ops_processed": ops_processed,
        "dids_seen": dids_seen_count,
        "genesis_ops_written": genesis_written,
        "last_cursor": last_cursor,
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="Path to labeler.sqlite")
    parser.add_argument("--after", default="", help="PLC /export cursor (createdAt) to start after")
    parser.add_argument("--max-ops", type=int, default=0, help="Stop after processing N ops (0=unlimited)")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help="Ops per /export page")
    parser.add_argument("--sleep-s", type=float, default=DEFAULT_SLEEP_S, help="Sleep between pages")
    parser.add_argument("--batch-flush", type=int, default=DEFAULT_BATCH_FLUSH, help="Flush to DB every N records")
    parser.add_argument("--note", default=None, help="Note for the run record")
    parser.add_argument("--resume-run", default=None, help="Resume an existing run_id")
    args = parser.parse_args()

    summary = run_backfill(
        db_path=args.db,
        after=args.after,
        max_ops=args.max_ops,
        count=args.count,
        sleep_s=args.sleep_s,
        batch_flush=args.batch_flush,
        note=args.note,
        run_id=args.resume_run,
    )
    import json
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
