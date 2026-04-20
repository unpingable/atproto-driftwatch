"""Hosting-locus admissibility check — freshness × source × host-class strat.

Tests whether the seed vs live pds_host distribution gap is driven by stale-field
noise or real historical-infrastructure signal. Step 1 of the admissibility plan:
cheap freshness stratification against actor_identity_current, snapshotted to a
dated artifact.

The column is mutable (resolver overwrites in place). Snapshot now, interpret
later.

Usage (on VM, read-only):
    python3 scripts/admissibility_check.py \
        --db /opt/driftwatch/deploy/data/labeler.sqlite \
        --out out/admissibility/

Go/no-go rule for interpreting output:
  - persistent gap in <24h / 24-48h buckets → H1 (real signal) gains
  - collapsing gap as freshness improves → H2 (staleness) likely
  - mixed → formalization needed (seed-time vs live-time host distinction)
"""

import argparse
import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


# Freshness buckets (seconds since resolver_last_success_at)
BUCKETS = [
    ("<24h", 0, 24 * 3600),
    ("24-48h", 24 * 3600, 48 * 3600),
    ("2-7d", 48 * 3600, 7 * 24 * 3600),
    (">7d", 7 * 24 * 3600, 365 * 24 * 3600),
    ("never", None, None),
]

SOURCES = ["labelwatch_seed", "live", "both"]


def extract_host_family(pds_host):
    """Copied from labelwatch.hosting — registerable domain heuristic."""
    if not pds_host:
        return None
    host = pds_host.split(":")[0].rstrip(".")
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    candidate = ".".join(parts[-2:])
    if candidate in {"bsky.network", "bsky.social"}:
        if len(parts) >= 3:
            return ".".join(parts[-3:])
        return candidate
    return candidate


def is_major(pds_host):
    if not pds_host:
        return False
    return pds_host.endswith(".bsky.network") or pds_host == "bsky.social"


def age_bucket(resolver_last_success_at, now_ts):
    if not resolver_last_success_at:
        return "never"
    try:
        dt = datetime.fromisoformat(resolver_last_success_at.replace("Z", "+00:00"))
        age_s = now_ts - dt.timestamp()
    except Exception:
        return "never"
    for name, lo, hi in BUCKETS:
        if lo is None:
            continue
        if lo <= age_s < hi:
            return name
    return ">7d"


def run_query(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        rows = conn.execute("""
            SELECT identity_source, resolver_status, pds_host, resolver_last_success_at
            FROM actor_identity_current
            WHERE identity_source IN ('labelwatch_seed', 'live', 'both')
        """).fetchall()
    finally:
        conn.close()
    return rows


def stratify(rows):
    now_ts = time.time()

    # bucket[(source, age, host_class)] -> {count, family_counts}
    bucket = defaultdict(lambda: {"count": 0, "families": defaultdict(int)})

    for source, status, pds_host, last_success in rows:
        age = age_bucket(last_success, now_ts)

        if status != "ok" or not pds_host:
            host_class = "unresolved"
            family = None
        elif is_major(pds_host):
            host_class = "major"
            family = extract_host_family(pds_host)
        else:
            host_class = "non_major"
            family = extract_host_family(pds_host)

        key = (source, age, host_class)
        bucket[key]["count"] += 1
        if family:
            bucket[key]["families"][family] += 1

    return bucket


def compute_deltas(bucket):
    """Within each age bucket, seed vs live non-major share delta."""
    deltas = {}
    for _, lo, _ in BUCKETS:
        pass
    ages = [b[0] for b in BUCKETS]
    for age in ages:
        per_source = {}
        for src in SOURCES:
            resolved = sum(
                bucket[(src, age, cls)]["count"]
                for cls in ("major", "non_major")
            )
            non_major = bucket[(src, age, "non_major")]["count"]
            total = resolved + bucket[(src, age, "unresolved")]["count"]
            per_source[src] = {
                "total": total,
                "resolved": resolved,
                "non_major": non_major,
                "non_major_pct_of_resolved": round(
                    100.0 * non_major / resolved, 2
                ) if resolved else None,
            }
        seed_pct = per_source["labelwatch_seed"]["non_major_pct_of_resolved"]
        live_pct = per_source["live"]["non_major_pct_of_resolved"]
        gap = None
        if seed_pct is not None and live_pct is not None:
            gap = round(seed_pct - live_pct, 2)
        deltas[age] = {
            "per_source": per_source,
            "seed_minus_live_non_major_pct": gap,
        }
    return deltas


def top_families(bucket, source, age, host_class, n=10):
    fams = bucket.get((source, age, host_class), {}).get("families", {})
    items = sorted(fams.items(), key=lambda kv: -kv[1])[:n]
    return [{"family": f, "count": c} for f, c in items]


def build_report(bucket):
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    totals = defaultdict(int)
    for (src, age, cls), v in bucket.items():
        totals[f"{src}_{cls}"] += v["count"]
        totals[f"{src}_total"] += v["count"]
        totals["grand_total"] += v["count"]

    deltas = compute_deltas(bucket)

    # Top families per age bucket (resolved only) — seed vs live side by side
    family_tables = {}
    for age_name, _, _ in BUCKETS:
        family_tables[age_name] = {
            "seed_non_major": top_families(bucket, "labelwatch_seed", age_name, "non_major"),
            "seed_major": top_families(bucket, "labelwatch_seed", age_name, "major"),
            "live_non_major": top_families(bucket, "live", age_name, "non_major"),
            "live_major": top_families(bucket, "live", age_name, "major"),
        }

    return {
        "generated_at": now_iso,
        "totals": dict(totals),
        "deltas_by_age": deltas,
        "families_by_age": family_tables,
        "method": {
            "buckets": [b[0] for b in BUCKETS],
            "sources": SOURCES,
            "host_classes": ["major", "non_major", "unresolved"],
            "major_rule": "pds_host ends with .bsky.network OR equals bsky.social",
            "family_rule": "last two labels, or three for *.bsky.network/*.bsky.social",
            "note": "actor_identity_current.pds_host is mutable. resolver_last_success_at is freshness of the current value, NOT preservation of any prior value.",
        },
    }


def render_text(report):
    lines = []
    lines.append(f"# Hosting-locus admissibility check")
    lines.append(f"generated_at: {report['generated_at']}")
    lines.append("")
    lines.append("## Totals")
    t = report["totals"]
    for src in SOURCES:
        tot = t.get(f"{src}_total", 0)
        lines.append(f"  {src}: total={tot} "
                     f"major={t.get(f'{src}_major', 0)} "
                     f"non_major={t.get(f'{src}_non_major', 0)} "
                     f"unresolved={t.get(f'{src}_unresolved', 0)}")
    lines.append("")
    lines.append("## Seed vs live non-major % of resolved, by freshness bucket")
    lines.append(f"  {'bucket':<10} {'seed n':>8} {'seed %nm':>10} {'live n':>8} {'live %nm':>10} {'gap':>8}")
    for age in [b[0] for b in BUCKETS]:
        d = report["deltas_by_age"][age]
        ps = d["per_source"]
        seed = ps["labelwatch_seed"]
        live = ps["live"]
        gap = d["seed_minus_live_non_major_pct"]
        lines.append(
            f"  {age:<10} "
            f"{seed['resolved']:>8} "
            f"{(str(seed['non_major_pct_of_resolved']) + '%' if seed['non_major_pct_of_resolved'] is not None else '--'):>10} "
            f"{live['resolved']:>8} "
            f"{(str(live['non_major_pct_of_resolved']) + '%' if live['non_major_pct_of_resolved'] is not None else '--'):>10} "
            f"{(('+' if gap and gap > 0 else '') + str(gap) if gap is not None else '--'):>8}"
        )
    lines.append("")
    lines.append("## Top non-major families by freshness bucket")
    for age in [b[0] for b in BUCKETS]:
        fams = report["families_by_age"][age]
        lines.append(f"### {age}")
        lines.append("  seed_non_major:  " + ", ".join(
            f"{x['family']}={x['count']}" for x in fams["seed_non_major"][:5]
        ))
        lines.append("  live_non_major:  " + ", ".join(
            f"{x['family']}={x['count']}" for x in fams["live_non_major"][:5]
        ))
    lines.append("")
    lines.append("## Interpretation guide")
    lines.append("  - Gap in <24h / 24-48h buckets persists → real signal (H1)")
    lines.append("  - Gap shrinks as freshness improves → staleness (H2)")
    lines.append("  - Mixed → formalize seed-time vs live-time host distinction")
    lines.append("")
    lines.append("Caveat: resolver_last_success_at == freshness of current pds_host.")
    lines.append("It does NOT preserve the value at seed/label time. Column is mutable.")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", default="out/admissibility/")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = run_query(args.db)
    print(f"read {len(rows):,} rows", file=sys.stderr)

    bucket = stratify(rows)
    report = build_report(bucket)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%MZ")
    json_path = out_dir / f"{stamp}_freshness_strat.json"
    txt_path = out_dir / f"{stamp}_freshness_strat.txt"

    json_path.write_text(json.dumps(report, indent=2))
    txt_path.write_text(render_text(report))

    print(f"wrote {json_path}", file=sys.stderr)
    print(f"wrote {txt_path}", file=sys.stderr)
    print(render_text(report))


if __name__ == "__main__":
    main()
