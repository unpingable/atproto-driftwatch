"""Vintage bucket boundaries for Phase D admissibility rerun.

Frozen constants per the pre-registered rule in docs/VINTAGE-ADMISSIBILITY.md.

Chosen:       2026-04-21
Chosen from:  out/admissibility/2026-04-21_vintage_buckets.json
Backfill:     plc_export_runs.run_id = plc_export_20260420_152312Z_db628ce9
Vintage rows: 668,700 eligible (did_created_at + resolver_status=ok + plc method)

Why quarterly fallback: the regime-change detector identified only one
distinct regime event — both jellybaby and stropharia came online
simultaneously between 2026-02-16 and 2026-03-16 (0% -> ~49% over 28 days
for each, deduping to a single boundary within the 14-day merge window).
The pre-registered rule requires >=2 regime changes to avoid fallback, so
equal-width quarterly buckets were used.

Do not modify these constants after Phase D analysis begins without
creating a new dated revision in VINTAGE-ADMISSIBILITY.md.
"""

from __future__ import annotations

# ISO-date boundaries. Bucket v0 covers (start, BOUNDARIES[0]), v1 covers
# [BOUNDARIES[0], BOUNDARIES[1]), ..., v_N covers [BOUNDARIES[-1], present).
BOUNDARIES: list[str] = [
    "2023-04-24",
    "2023-07-24",
    "2023-10-23",
    "2024-01-22",
    "2024-04-22",
    "2024-07-22",
    "2024-10-21",
    "2025-01-20",
    "2025-04-21",
    "2025-07-21",
    "2025-10-20",
    "2026-01-19",
    "2026-04-20",
]

# The single detected regime event, retained as metadata even though the
# primary bucketing is quarterly. A secondary analysis could collapse to
# "pre-regime" vs "post-regime" but that is not the pre-registered primary.
REGIME_EVENTS: list[dict] = [
    {
        "shards": ["jellybaby.us-east.host.bsky.network",
                   "stropharia.us-west.host.bsky.network"],
        "from_week": "2026-02-16",
        "to_week": "2026-03-16",
        "from_pct": 0.0,
        "to_pct": 48.91,
        "span_days": 28,
    },
]

PROVENANCE = {
    "chosen_at": "2026-04-21",
    "backfill_run_id": "plc_export_20260420_152312Z_db628ce9",
    "vintage_rows": 668700,
    "method": "quarterly_fallback",
    "fallback_reason": "single_joint_regime_event",
    "source_artifact": "out/admissibility/2026-04-21_vintage_buckets.json",
}


def bucket_label(created_at_iso: str) -> str:
    """Return the vintage bucket label for a DID created at the given ISO date.

    Returns e.g. 'v0_pre_2023-04-24', 'v1_2023-04-24_to_2023-07-24', ...,
    'v13_post_2026-04-20'. Treats NULL/empty as 'v_unknown'.
    """
    if not created_at_iso:
        return "v_unknown"
    # Compare by date prefix (yyyy-mm-dd) since BOUNDARIES are dates
    d = created_at_iso[:10]
    if d < BOUNDARIES[0]:
        return f"v0_pre_{BOUNDARIES[0]}"
    for i in range(len(BOUNDARIES) - 1):
        if BOUNDARIES[i] <= d < BOUNDARIES[i + 1]:
            return f"v{i+1}_{BOUNDARIES[i]}_to_{BOUNDARIES[i+1]}"
    return f"v{len(BOUNDARIES)}_post_{BOUNDARIES[-1]}"


def bucket_index(created_at_iso: str) -> int:
    """Return the integer index of the bucket. -1 for unknown."""
    if not created_at_iso:
        return -1
    d = created_at_iso[:10]
    if d < BOUNDARIES[0]:
        return 0
    for i in range(len(BOUNDARIES) - 1):
        if BOUNDARIES[i] <= d < BOUNDARIES[i + 1]:
            return i + 1
    return len(BOUNDARIES)
