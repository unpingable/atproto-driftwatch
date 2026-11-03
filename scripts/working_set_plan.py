#!/usr/bin/env python3
"""Emit a read-only SQLite working-set plan and explicit removal choices."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from labeler.working_set import WorkingSetLimits, evaluate, sample


def utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--as-of must include a timezone")
    return parsed.astimezone(timezone.utc)


def removal_choices(as_of: datetime) -> list[dict]:
    """Describe choices only; no SQL is executed and no deletion is implied."""
    iso = lambda value: value.isoformat()
    return [
        {
            "choice": "A_preserve_all_online",
            "semantic_change": False,
            "removal": [],
            "effect": "working-set guard stops intake at the configured cap",
        },
        {
            "choice": "B_apply_existing_retention_contract",
            "semantic_change": False,
            "requires_separate_approval": True,
            "removal": [
                {"table": "events", "predicate": "ctime < ?", "cutoff": iso(as_of - timedelta(days=7))},
                {"table": "event_versions", "predicate": "version_ts < ?", "cutoff": iso(as_of - timedelta(days=7))},
                {"table": "edges", "predicate": "ctime < ?", "cutoff": iso(as_of - timedelta(days=14))},
                {
                    "table": "claim_history",
                    "predicate": "COALESCE(observed_at, createdAt) < ?",
                    "cutoff": iso(as_of - timedelta(days=14)),
                    "precondition": "each removed day has a verified Parquet custody receipt",
                },
            ],
            "effect": "reuses the already-documented 7/14-day online horizons",
        },
        {
            "choice": "C_shorten_online_horizons",
            "semantic_change": True,
            "requires_separate_approval": True,
            "removal": [],
            "effect": "not specified; requires a research/query consumer review first",
        },
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--filesystem-path", type=Path)
    parser.add_argument("--max-live-bytes", required=True, type=int)
    parser.add_argument("--filesystem-reserve-bytes", required=True, type=int)
    parser.add_argument("--shutdown-growth-bytes", required=True, type=int)
    parser.add_argument("--wal-allowance-bytes", required=True, type=int)
    parser.add_argument("--as-of", required=True, type=utc)
    args = parser.parse_args()
    limits = WorkingSetLimits(
        max_live_bytes=args.max_live_bytes,
        filesystem_reserve_bytes=args.filesystem_reserve_bytes,
        shutdown_growth_bytes=args.shutdown_growth_bytes,
        wal_allowance_bytes=args.wal_allowance_bytes,
    )
    result = evaluate(sample(args.database, args.filesystem_path), limits)
    result["as_of"] = args.as_of.isoformat()
    result["retention_choices"] = removal_choices(args.as_of)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()

