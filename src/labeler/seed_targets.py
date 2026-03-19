"""Seed resolver queue from labelwatch labeled-target DIDs.

Bridges the population gap: driftwatch sees posters, labelwatch sees
labeled targets. Without seeding, the resolver only resolves accounts
that happen to post while driftwatch is running.

Usage (on VM):
    PYTHONPATH=src python3 -m labeler.seed_targets \
        --labelwatch-db /var/lib/labelwatch/labelwatch.db \
        --driftwatch-db /opt/driftwatch/deploy/data/labeler.sqlite \
        --limit 10000 --days 7

Inserts rows with identity_source='labelwatch_seed' and resolver_status=NULL
so the existing resolver picks them up automatically.
"""

import argparse
import logging
import sqlite3
import sys
import time

LOG = logging.getLogger("labeler.seed_targets")

DEFAULT_LIMIT = 10000
DEFAULT_DAYS = 7
BATCH_INSERT_SIZE = 1000


def fetch_top_target_dids(
    labelwatch_db: str,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
) -> list[tuple[str, int]]:
    """Get top labeled-target DIDs from labelwatch, ordered by label count.

    Returns list of (did, label_count) tuples.
    """
    conn = sqlite3.connect(f"file:{labelwatch_db}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        rows = conn.execute("""
            SELECT target_did, COUNT(*) as label_count
            FROM label_events
            WHERE target_did IS NOT NULL
              AND ts >= datetime('now', ?)
            GROUP BY target_did
            ORDER BY label_count DESC
            LIMIT ?
        """, (f"-{days} days", limit)).fetchall()
        return rows
    finally:
        conn.close()


def seed_resolver_queue(
    driftwatch_db: str,
    target_dids: list[tuple[str, int]],
) -> dict:
    """Insert seed DIDs into actor_identity_current for resolver pickup.

    Only inserts DIDs not already present. Updates existing live-observed
    DIDs to identity_source='both'.

    Returns stats dict.
    """
    conn = sqlite3.connect(driftwatch_db)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    stats = {"total": len(target_dids), "inserted": 0, "upgraded": 0, "skipped": 0}

    for i in range(0, len(target_dids), BATCH_INSERT_SIZE):
        batch = target_dids[i:i + BATCH_INSERT_SIZE]
        for did, label_count in batch:
            # Check if already exists
            row = conn.execute(
                "SELECT identity_source, resolver_status FROM actor_identity_current WHERE did = ?",
                (did,),
            ).fetchone()

            if row is None:
                # New DID — insert as seed, resolver_status NULL for pickup
                conn.execute(
                    "INSERT INTO actor_identity_current "
                    "(did, first_seen_at, last_seen_at, last_event_kind, "
                    " identity_source, reducer_version) "
                    "VALUES (?, ?, ?, 'seed', 'labelwatch_seed', 1)",
                    (did, now, now),
                )
                stats["inserted"] += 1
            else:
                source = row[0]
                if source == "live" or source is None:
                    # Upgrade to 'both'
                    conn.execute(
                        "UPDATE actor_identity_current SET identity_source = 'both' WHERE did = ?",
                        (did,),
                    )
                    stats["upgraded"] += 1
                else:
                    stats["skipped"] += 1

        conn.commit()
        LOG.info("batch %d-%d: inserted=%d upgraded=%d skipped=%d",
                 i, i + len(batch), stats["inserted"], stats["upgraded"], stats["skipped"])

    conn.close()
    return stats


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Seed driftwatch resolver from labelwatch labeled-target DIDs"
    )
    parser.add_argument("--labelwatch-db", required=True, help="Path to labelwatch.db")
    parser.add_argument("--driftwatch-db", required=True, help="Path to labeler.sqlite")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Lookback window")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max DIDs to seed")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be seeded")
    args = parser.parse_args()

    LOG.info("Fetching top %d labeled-target DIDs from last %d days...", args.limit, args.days)
    targets = fetch_top_target_dids(args.labelwatch_db, days=args.days, limit=args.limit)
    LOG.info("Found %d target DIDs", len(targets))

    if not targets:
        LOG.info("No targets to seed")
        return

    # Show top 10
    LOG.info("Top 10 by label count:")
    for did, count in targets[:10]:
        LOG.info("  %s: %d labels", did, count)

    if args.dry_run:
        LOG.info("Dry run — would seed %d DIDs", len(targets))
        return

    stats = seed_resolver_queue(args.driftwatch_db, targets)
    LOG.info("Seed complete: %s", stats)


if __name__ == "__main__":
    main()
