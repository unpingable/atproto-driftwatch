"""Empirical vintage bucket boundary discovery.

Per docs/VINTAGE-ADMISSIBILITY.md pre-registered rule:

  - Sort DIDs by did_created_at.
  - Identify points where the running fraction of accounts assigned to
    the current dominant shard (jellybaby / stropharia) shifts by >=20pp
    within a <=30-day rolling window.
  - Bucket boundaries are placed at those regime-change points.
  - If fewer than two regime changes are detected, fall back to equal-width
    quarterly buckets.

Operates on actor_identity_current. Produces:
  - weekly timeseries JSON (creation week x shard distribution)
  - boundary candidate list with rationale
  - final chosen bucket boundaries

No Phase D joins happen here. Only vintage x current-shard distribution.

Usage:
    python3 scripts/vintage_bucket_discovery.py \
        --db /opt/driftwatch/deploy/data/labeler.sqlite \
        --out out/admissibility/
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

LOG = logging.getLogger("vintage_buckets")

# Shards we track for regime-change detection. "dominant" at any given time
# is the one with the highest cohort share.
# Note: stropharia is on us-west, not us-east (verified by direct query on
# 2026-04-21; the 2026-04-19 admissibility snapshot mislabeled it because
# its pds_host was only inspected inside the us-east.host.bsky.network
# suffix). The data pattern was the same; the region tag was the error.
TRACKED_SHARDS = [
    "jellybaby.us-east.host.bsky.network",
    "stropharia.us-west.host.bsky.network",
]

# Regime-change thresholds
REGIME_SHIFT_PP = 20.0         # percentage-point shift required
REGIME_WINDOW_DAYS = 30        # window across which the shift must happen
MIN_BIN_COUNT = 200            # bins with fewer accounts are treated as noise
MIN_BUCKET_SPAN_DAYS = 60      # adjacent regime points within this get merged


def parse_iso(ts: str) -> datetime:
    """Parse ISO timestamp with trailing Z or +00:00."""
    s = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(s)


def week_start(dt: datetime) -> str:
    """ISO date of Monday of the week containing dt."""
    d = dt.date() - timedelta(days=dt.weekday())
    return d.isoformat()


def query_cohort_rows(db_path: str) -> list[tuple[str, str]]:
    """Return (did_created_at, pds_host) for all vintage+host eligible rows.

    Filters per doctrine:
      - did:plc:* only (implied by vintage_source in plc_export/plc_audit_log)
      - did_created_at IS NOT NULL
      - resolver_status = 'ok'
      - pds_host IS NOT NULL
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        rows = conn.execute("""
            SELECT did_created_at, pds_host
            FROM actor_identity_current
            WHERE did_created_at IS NOT NULL
              AND vintage_source IN ('plc_export', 'plc_audit_log')
              AND resolver_status = 'ok'
              AND pds_host IS NOT NULL
        """).fetchall()
    finally:
        conn.close()
    return rows


def bin_by_week(rows: list[tuple[str, str]]) -> dict:
    """Bin rows by ISO week of creation. Returns dict keyed by week_start_iso.

    Each value is dict: {
        total: N,
        shards: {pds_host: count, ...},
    }
    """
    bins: dict[str, dict] = defaultdict(lambda: {"total": 0, "shards": defaultdict(int)})
    skipped = 0
    for created_at, pds_host in rows:
        try:
            dt = parse_iso(created_at)
        except Exception:
            skipped += 1
            continue
        wk = week_start(dt)
        bins[wk]["total"] += 1
        bins[wk]["shards"][pds_host] += 1
    LOG.info("binned %d rows into %d weeks (%d parse-skipped)",
             sum(b["total"] for b in bins.values()), len(bins), skipped)
    return bins


def shard_pct_series(bins: dict, shard: str) -> list[tuple[str, int, float]]:
    """Time series of (week, n_total, pct_on_shard) sorted by week."""
    out = []
    for wk in sorted(bins):
        tot = bins[wk]["total"]
        if tot < MIN_BIN_COUNT:
            continue
        n = bins[wk]["shards"].get(shard, 0)
        pct = 100.0 * n / tot if tot else 0
        out.append((wk, tot, round(pct, 2)))
    return out


def find_regime_changes(series: list[tuple[str, int, float]]) -> list[dict]:
    """Find weeks where shard pct shifts by >=REGIME_SHIFT_PP within
    REGIME_WINDOW_DAYS.

    For each week w, check if pct at w differs by >=20pp from pct at any
    earlier week within <=30 days. If so, mark w as a boundary candidate.
    """
    candidates = []
    for i, (wk_b, n_b, pct_b) in enumerate(series):
        dt_b = datetime.fromisoformat(wk_b)
        # Look back up to 30 days
        for j in range(i - 1, -1, -1):
            wk_a, n_a, pct_a = series[j]
            dt_a = datetime.fromisoformat(wk_a)
            span_d = (dt_b - dt_a).days
            if span_d > REGIME_WINDOW_DAYS:
                break
            if abs(pct_b - pct_a) >= REGIME_SHIFT_PP:
                candidates.append({
                    "from_week": wk_a,
                    "to_week": wk_b,
                    "span_days": span_d,
                    "from_pct": pct_a,
                    "to_pct": pct_b,
                    "delta_pp": round(pct_b - pct_a, 2),
                    "n_from": n_a,
                    "n_to": n_b,
                })
                break  # one anchor per target week is enough
    return candidates


def collapse_regimes(candidates: list[dict]) -> list[dict]:
    """Merge regime-change candidates that are closer than MIN_BUCKET_SPAN_DAYS
    into a single transition.

    Returns one transition per distinct regime, with the aggregate span.
    """
    if not candidates:
        return []
    # Sort by end of transition
    sorted_c = sorted(candidates, key=lambda c: c["to_week"])
    collapsed = [sorted_c[0].copy()]
    for c in sorted_c[1:]:
        last = collapsed[-1]
        last_to = datetime.fromisoformat(last["to_week"])
        c_to = datetime.fromisoformat(c["to_week"])
        gap = (c_to - last_to).days
        if gap <= MIN_BUCKET_SPAN_DAYS:
            # Extend the transition: take the earliest from_week and the
            # latest to_week, with delta calculated from those extremes.
            last_from = datetime.fromisoformat(last["from_week"])
            c_from = datetime.fromisoformat(c["from_week"])
            if c_from < last_from:
                last["from_week"] = c["from_week"]
                last["from_pct"] = c["from_pct"]
                last["n_from"] = c["n_from"]
            last["to_week"] = c["to_week"]
            last["to_pct"] = c["to_pct"]
            last["n_to"] = c["n_to"]
            last["delta_pp"] = round(last["to_pct"] - last["from_pct"], 2)
            last["span_days"] = (
                datetime.fromisoformat(last["to_week"]) -
                datetime.fromisoformat(last["from_week"])
            ).days
            last["merged"] = last.get("merged", 1) + 1
        else:
            collapsed.append(c.copy())
    return collapsed


def transition_midpoints(collapsed: list[dict]) -> list[str]:
    """Boundary = midpoint of each collapsed transition."""
    out = []
    for c in collapsed:
        a = datetime.fromisoformat(c["from_week"])
        b = datetime.fromisoformat(c["to_week"])
        mid = a + (b - a) / 2
        out.append(mid.date().isoformat())
    return out


def build_report(db_path: str) -> dict:
    rows = query_cohort_rows(db_path)
    LOG.info("loaded %d cohort rows", len(rows))
    bins = bin_by_week(rows)

    shard_series = {}
    all_regimes = {}
    for shard in TRACKED_SHARDS:
        series = shard_pct_series(bins, shard)
        shard_series[shard] = series
        regimes_raw = find_regime_changes(series)
        collapsed = collapse_regimes(regimes_raw)
        all_regimes[shard] = {
            "raw_candidates": regimes_raw,
            "collapsed": collapsed,
            "midpoints": transition_midpoints(collapsed),
        }

    # Combine midpoints across tracked shards, dedupe within 14 days
    all_midpoints: list[str] = []
    for shard_info in all_regimes.values():
        all_midpoints.extend(shard_info["midpoints"])
    all_midpoints.sort()

    merged_boundaries: list[str] = []
    for bdy in all_midpoints:
        if merged_boundaries:
            last = datetime.fromisoformat(merged_boundaries[-1])
            cur = datetime.fromisoformat(bdy)
            if (cur - last).days <= 14:
                continue
        merged_boundaries.append(bdy)

    # Fallback if fewer than 2 boundaries detected
    fallback_used = False
    if len(merged_boundaries) < 2:
        LOG.warning("fewer than 2 regime changes detected; falling back to quarterly")
        min_dt = datetime.fromisoformat(min(bins)) if bins else None
        max_dt = datetime.fromisoformat(max(bins)) if bins else None
        merged_boundaries = []
        if min_dt and max_dt:
            d = min_dt
            while d < max_dt:
                d = d + timedelta(days=91)
                merged_boundaries.append(d.date().isoformat())
        fallback_used = True

    # Final bucket list: [-inf, b1), [b1, b2), ..., [bN, +inf)
    bucket_list = []
    prev = None
    for i, b in enumerate(merged_boundaries):
        bucket_list.append({
            "index": i,
            "start": prev,
            "end": b,
            "label": f"v{i}_pre_{b}" if prev is None else f"v{i}_{prev}_to_{b}",
        })
        prev = b
    bucket_list.append({
        "index": len(merged_boundaries),
        "start": prev,
        "end": None,
        "label": f"v{len(merged_boundaries)}_post_{prev}",
    })

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": {
            "regime_shift_pp": REGIME_SHIFT_PP,
            "regime_window_days": REGIME_WINDOW_DAYS,
            "min_bin_count": MIN_BIN_COUNT,
            "min_bucket_span_days": MIN_BUCKET_SPAN_DAYS,
            "tracked_shards": TRACKED_SHARDS,
            "fallback_used": fallback_used,
        },
        "cohort_rows": len(rows),
        "weeks": len(bins),
        "shard_series": shard_series,
        "regimes_by_shard": all_regimes,
        "merged_boundaries": merged_boundaries,
        "buckets": bucket_list,
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", default="out/admissibility/")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    report = build_report(args.db)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%MZ")
    json_path = out_dir / f"{stamp}_vintage_buckets.json"
    json_path.write_text(json.dumps(report, indent=2))

    # Print summary to stdout
    print(f"\n=== Vintage bucket discovery ===")
    print(f"cohort rows: {report['cohort_rows']:,}")
    print(f"weeks observed: {report['weeks']}")
    print(f"boundaries chosen: {len(report['merged_boundaries'])}")
    print(f"  {report['merged_boundaries']}")
    if report["method"]["fallback_used"]:
        print("  (!) fallback: used equal-width quarterly boundaries, <2 regimes detected")
    print("\nTransitions per shard:")
    for shard, info in report["regimes_by_shard"].items():
        print(f"  {shard.split('.')[0]}:")
        for c in info["collapsed"]:
            print(f"    {c['from_week']} -> {c['to_week']} "
                  f"({c['from_pct']}% -> {c['to_pct']}%, +{c['delta_pp']}pp over {c['span_days']}d)")
    print(f"\nBuckets:")
    for b in report["buckets"]:
        start = b["start"] or "(beginning)"
        end = b["end"] or "(present)"
        print(f"  v{b['index']}: {start} .. {end}")
    print(f"\nwrote {json_path}")


if __name__ == "__main__":
    main()
