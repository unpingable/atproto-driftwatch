"""Phase D: vintage-stratified admissibility rerun.

Applies pre-registered doctrine from docs/VINTAGE-ADMISSIBILITY.md to the
actor_identity_current table after the 2026-04-21 backfill completed.

Asks: after controlling for DID creation-era vintage, is there still
seed-vs-live non-major host skew among independent PDSes?

Outputs:
  - <stamp>_vintage_admissibility.json     : full numeric report
  - <stamp>_vintage_admissibility.txt      : human-readable summary
  - <stamp>_vintage_admissibility.md       : findings w/ provenance footer

No fields on the result are optional or ad-hoc. Every filter, floor, and
verdict is pre-registered.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "src"))

from labeler.vintage_buckets import (  # noqa: E402
    BOUNDARIES, bucket_label, bucket_index, PROVENANCE,
)

LOG = logging.getLogger("vintage_admissibility")


# --- Pre-registered thresholds (do not adjust here without amending doctrine) ---

SAMPLE_FLOOR = 100          # min DIDs per (bucket x source x family) cell
EFFECT_FLOOR_PP = 5.0       # min absolute pp delta in non-major share
EFFECT_FLOOR_RATIO = 1.5    # min ratio-of-ratios (or <=1/1.5=0.667 inverse)
MIN_BUCKETS_WITH_EFFECT = 2 # effect must show in >=2 vintage buckets
FRESHNESS_DAYS = 7          # resolver_last_success_at must be <= this many days
ROBUSTNESS_FRESH_HOURS = 48 # stricter freshness for sensitivity check
ROBUSTNESS_SHIFTS_DAYS = [-14, -7, 0, 7, 14]
ROBUSTNESS_SURVIVAL_MIN = 3 # of 5 perturbations


# --- Host classification (small, self-contained; matches labelwatch rule) ---

def is_major(pds_host: str | None) -> bool:
    if not pds_host:
        return False
    return pds_host.endswith(".bsky.network") or pds_host == "bsky.social"


def extract_host_family(pds_host: str | None) -> str | None:
    """Registerable-domain heuristic (copy of labelwatch.hosting logic)."""
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


# --- Row loading with doctrine filters ---

@dataclass
class Row:
    did: str
    identity_source: str
    pds_host: str
    did_created_at: str
    resolver_last_success_at: str | None
    resolver_status: str


def load_rows(db_path: str) -> tuple[list[Row], dict]:
    """Load rows passing doctrine filters 1-4 (method, vintage, resolver_ok).
    Returns (rows, exclusion_counts) where exclusion_counts keys:
      - excluded_non_plc
      - excluded_no_vintage
      - excluded_unsupported_method
      - excluded_resolver_not_ok
      - excluded_source (!= labelwatch_seed/live/both)
      - total_kept
      - kept_both_source (reported separately)
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        totals = conn.execute("""
            SELECT
              SUM(CASE WHEN did NOT LIKE 'did:plc:%' THEN 1 ELSE 0 END) AS non_plc,
              SUM(CASE WHEN did LIKE 'did:plc:%' AND did_created_at IS NULL THEN 1 ELSE 0 END) AS no_vintage,
              SUM(CASE WHEN vintage_source = 'unsupported_method' THEN 1 ELSE 0 END) AS unsupported,
              SUM(CASE WHEN did LIKE 'did:plc:%' AND did_created_at IS NOT NULL
                       AND (resolver_status IS NULL OR resolver_status != 'ok') THEN 1 ELSE 0 END) AS resolver_not_ok,
              SUM(CASE WHEN identity_source NOT IN ('labelwatch_seed', 'live', 'both')
                         OR identity_source IS NULL THEN 1 ELSE 0 END) AS bad_source,
              COUNT(*) AS total
            FROM actor_identity_current
        """).fetchone()

        rows_raw = conn.execute("""
            SELECT did, identity_source, pds_host, did_created_at,
                   resolver_last_success_at, resolver_status
            FROM actor_identity_current
            WHERE did LIKE 'did:plc:%'
              AND did_created_at IS NOT NULL
              AND vintage_source IN ('plc_export', 'plc_audit_log')
              AND resolver_status = 'ok'
              AND pds_host IS NOT NULL
              AND identity_source IN ('labelwatch_seed', 'live', 'both')
        """).fetchall()
    finally:
        conn.close()

    rows = [Row(*r) for r in rows_raw]
    exclusions = {
        "total_pre_filter": totals[5],
        "excluded_non_plc": totals[0] or 0,
        "excluded_no_vintage": totals[1] or 0,
        "excluded_unsupported_method": totals[2] or 0,
        "excluded_resolver_not_ok": totals[3] or 0,
        "excluded_bad_source": totals[4] or 0,
        "kept_total": len(rows),
        "kept_both_source": sum(1 for r in rows if r.identity_source == "both"),
    }
    return rows, exclusions


def filter_fresh(rows: list[Row], max_days: int, max_hours: int | None = None) -> list[Row]:
    """Keep rows whose resolver_last_success_at is within the freshness window."""
    now = datetime.now(timezone.utc)
    if max_hours is not None:
        cutoff = now - timedelta(hours=max_hours)
    else:
        cutoff = now - timedelta(days=max_days)
    out = []
    for r in rows:
        if not r.resolver_last_success_at:
            continue
        try:
            dt = datetime.fromisoformat(r.resolver_last_success_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt >= cutoff:
            out.append(r)
    return out


# --- Core aggregation ---

def aggregate(rows: list[Row], boundaries: list[str]) -> dict:
    """Build the (bucket, source, host_family) x count table."""
    agg: dict[tuple, int] = defaultdict(int)
    for r in rows:
        b = _bucket_for(r.did_created_at, boundaries)
        cls = "major" if is_major(r.pds_host) else "non_major"
        fam = extract_host_family(r.pds_host) or r.pds_host
        key = (b, r.identity_source, cls, fam)
        agg[key] += 1
    return dict(agg)


def _bucket_for(created_at_iso: str, boundaries: list[str]) -> int:
    if not created_at_iso:
        return -1
    d = created_at_iso[:10]
    if d < boundaries[0]:
        return 0
    for i in range(len(boundaries) - 1):
        if boundaries[i] <= d < boundaries[i + 1]:
            return i + 1
    return len(boundaries)


def bucket_label_for(i: int, boundaries: list[str]) -> str:
    if i == 0:
        return f"v0_pre_{boundaries[0]}"
    if i == len(boundaries):
        return f"v{i}_post_{boundaries[-1]}"
    return f"v{i}_{boundaries[i-1]}_to_{boundaries[i]}"


# --- Effect-size calculation ---

def family_effects(agg: dict, boundaries: list[str]) -> list[dict]:
    """For each (bucket, family), compute seed vs live non-major share delta."""
    # Extract distinct buckets and families
    buckets = sorted({k[0] for k in agg})
    families = sorted({k[3] for k in agg if k[2] == "non_major"})

    # Per-bucket totals per source (major+non_major+unresolved already excluded by filter)
    # Wait — we only agg'd major/non_major. Total resolved per (bucket, source) is
    # sum over all families + major counts.
    bucket_source_totals = defaultdict(lambda: {"major": 0, "non_major_total": 0})
    for (b, src, cls, fam), n in agg.items():
        if cls == "major":
            bucket_source_totals[(b, src)]["major"] += n
        else:
            bucket_source_totals[(b, src)]["non_major_total"] += n

    rows_out = []
    for b in buckets:
        for fam in families:
            seed_n = agg.get((b, "labelwatch_seed", "non_major", fam), 0)
            live_n = agg.get((b, "live", "non_major", fam), 0)

            seed_tot = (bucket_source_totals[(b, "labelwatch_seed")]["major"] +
                        bucket_source_totals[(b, "labelwatch_seed")]["non_major_total"])
            live_tot = (bucket_source_totals[(b, "live")]["major"] +
                        bucket_source_totals[(b, "live")]["non_major_total"])

            if seed_n < SAMPLE_FLOOR and live_n < SAMPLE_FLOOR:
                continue

            seed_pct = (100.0 * seed_n / seed_tot) if seed_tot else 0.0
            live_pct = (100.0 * live_n / live_tot) if live_tot else 0.0
            delta_pp = seed_pct - live_pct

            # Ratio-of-ratios: (seed_pct / live_pct)
            if live_pct > 0 and seed_pct > 0:
                ratio = seed_pct / live_pct
            elif seed_pct > 0 and live_pct == 0:
                ratio = float("inf")
            else:
                ratio = 0.0

            sample_floor_seed = seed_n >= SAMPLE_FLOOR
            sample_floor_live = live_n >= SAMPLE_FLOOR
            both_floor = sample_floor_seed and sample_floor_live

            clears_pp = abs(delta_pp) >= EFFECT_FLOOR_PP
            clears_ratio = ratio >= EFFECT_FLOOR_RATIO or (
                ratio > 0 and ratio <= 1 / EFFECT_FLOOR_RATIO
            )

            rows_out.append({
                "bucket_index": b,
                "bucket_label": bucket_label_for(b, boundaries),
                "family": fam,
                "seed_n": seed_n, "live_n": live_n,
                "seed_total": seed_tot, "live_total": live_tot,
                "seed_pct": round(seed_pct, 3), "live_pct": round(live_pct, 3),
                "delta_pp": round(delta_pp, 3),
                "ratio": round(ratio, 3) if ratio != float("inf") else None,
                "sample_floor_seed": sample_floor_seed,
                "sample_floor_live": sample_floor_live,
                "both_floor": both_floor,
                "clears_pp": clears_pp,
                "clears_ratio": clears_ratio,
                "clears_all": both_floor and clears_pp and clears_ratio,
            })
    return rows_out


def families_clearing(effects: list[dict]) -> list[dict]:
    """Families that clear sample + effect floors in >= MIN_BUCKETS_WITH_EFFECT buckets."""
    by_fam: dict[str, list[dict]] = defaultdict(list)
    for e in effects:
        if e["clears_all"]:
            by_fam[e["family"]].append(e)
    out = []
    for fam, rows in by_fam.items():
        if len(rows) >= MIN_BUCKETS_WITH_EFFECT:
            out.append({
                "family": fam,
                "clearing_buckets": sorted(e["bucket_label"] for e in rows),
                "per_bucket": rows,
            })
    return out


# --- Robustness checks ---

def robustness_bucket_shifts(rows: list[Row], clearing_families: list[dict]) -> dict:
    """For each shift delta, rebucket and check which families still clear."""
    results = {}
    for delta_days in ROBUSTNESS_SHIFTS_DAYS:
        shifted = shift_boundaries(BOUNDARIES, delta_days)
        agg2 = aggregate(rows, shifted)
        effects2 = family_effects(agg2, shifted)
        clearers2 = {f["family"] for f in families_clearing(effects2)}
        results[f"shift_{delta_days:+d}d"] = sorted(clearers2)
    # Compute survival
    original_clearers = {f["family"] for f in clearing_families}
    survival = {}
    for fam in original_clearers:
        hits = sum(1 for shift_res in results.values() if fam in shift_res)
        survival[fam] = {
            "hits": hits,
            "total": len(ROBUSTNESS_SHIFTS_DAYS),
            "survives": hits >= ROBUSTNESS_SURVIVAL_MIN,
        }
    return {"per_shift": results, "survival": survival}


def shift_boundaries(boundaries: list[str], delta_days: int) -> list[str]:
    out = []
    for b in boundaries:
        d = datetime.fromisoformat(b) + timedelta(days=delta_days)
        out.append(d.date().isoformat())
    return out


def robustness_freshness(rows: list[Row]) -> dict:
    """Restrict to <48h freshness and re-run. Families surviving here are
    cohort-vintage, not freshness-tied."""
    fresh_rows = filter_fresh(rows, max_days=FRESHNESS_DAYS, max_hours=ROBUSTNESS_FRESH_HOURS)
    agg = aggregate(fresh_rows, BOUNDARIES)
    effects = family_effects(agg, BOUNDARIES)
    clearers = {f["family"] for f in families_clearing(effects)}
    return {"rows": len(fresh_rows), "families_clearing": sorted(clearers)}


def robustness_include_both(all_rows: list[Row], clearing_families: list[dict]) -> dict:
    """Rerun with identity_source='both' merged into... which source?
    Doctrine: include in analysis but keep separate reporting. For robustness,
    treat 'both' as 'labelwatch_seed' (seed-origin DIDs that also posted)
    and see if effect direction is preserved."""
    merged = []
    for r in all_rows:
        if r.identity_source == "both":
            merged.append(Row(
                did=r.did, identity_source="labelwatch_seed",
                pds_host=r.pds_host, did_created_at=r.did_created_at,
                resolver_last_success_at=r.resolver_last_success_at,
                resolver_status=r.resolver_status,
            ))
        else:
            merged.append(r)
    agg = aggregate(merged, BOUNDARIES)
    effects = family_effects(agg, BOUNDARIES)
    clearers = families_clearing(effects)
    original = {f["family"]: f for f in clearing_families}
    preserved = {}
    for clearer in clearers:
        if clearer["family"] in original:
            orig_delta_signs = {e["bucket_label"]: (1 if e["delta_pp"] > 0 else -1)
                                for e in original[clearer["family"]]["per_bucket"]}
            new_delta_signs = {e["bucket_label"]: (1 if e["delta_pp"] > 0 else -1)
                               for e in clearer["per_bucket"]}
            same = all(orig_delta_signs.get(k) == v for k, v in new_delta_signs.items()
                       if k in orig_delta_signs)
            preserved[clearer["family"]] = same
    return {"families_still_clearing": sorted(f["family"] for f in clearers),
            "direction_preserved": preserved}


# --- Report building ---

def build_report(db_path: str) -> dict:
    all_rows, exclusions = load_rows(db_path)
    LOG.info("loaded %d eligible rows (pre-freshness)", len(all_rows))
    fresh_rows = filter_fresh(all_rows, max_days=FRESHNESS_DAYS)
    LOG.info("after freshness filter (<=%dd): %d rows", FRESHNESS_DAYS, len(fresh_rows))

    # Exclude 'both' from primary comparison (doctrine filter 6)
    primary = [r for r in fresh_rows if r.identity_source != "both"]
    both_rows = [r for r in fresh_rows if r.identity_source == "both"]

    agg = aggregate(primary, BOUNDARIES)
    effects = family_effects(agg, BOUNDARIES)
    clearers = families_clearing(effects)

    # Robustness checks (only if any clearers)
    robustness = {}
    if clearers:
        robustness["bucket_shift"] = robustness_bucket_shifts(primary, clearers)
        robustness["freshness_strict"] = robustness_freshness(primary)
        robustness["include_both"] = robustness_include_both(fresh_rows, clearers)

    # Per-bucket summary (for transparency)
    per_bucket: dict[int, dict] = defaultdict(lambda: {
        "labelwatch_seed": 0, "live": 0,
        "labelwatch_seed_non_major": 0, "live_non_major": 0,
    })
    for (b, src, cls, fam), n in agg.items():
        per_bucket[b][src] += n
        if cls == "non_major":
            per_bucket[b][f"{src}_non_major"] += n

    per_bucket_list = []
    for b in sorted(per_bucket):
        v = per_bucket[b]
        seed_tot = v["labelwatch_seed"]
        live_tot = v["live"]
        seed_nm = v["labelwatch_seed_non_major"]
        live_nm = v["live_non_major"]
        per_bucket_list.append({
            "bucket_index": b,
            "bucket_label": bucket_label_for(b, BOUNDARIES),
            "seed_total": seed_tot,
            "live_total": live_tot,
            "seed_non_major": seed_nm,
            "live_non_major": live_nm,
            "seed_non_major_pct": round(100 * seed_nm / seed_tot, 3) if seed_tot else None,
            "live_non_major_pct": round(100 * live_nm / live_tot, 3) if live_tot else None,
        })

    # Final verdict
    verdict = build_verdict(clearers, robustness)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "provenance": PROVENANCE,
        "boundaries": BOUNDARIES,
        "thresholds": {
            "sample_floor": SAMPLE_FLOOR,
            "effect_floor_pp": EFFECT_FLOOR_PP,
            "effect_floor_ratio": EFFECT_FLOOR_RATIO,
            "min_buckets_with_effect": MIN_BUCKETS_WITH_EFFECT,
            "freshness_days": FRESHNESS_DAYS,
            "robustness_shifts_days": ROBUSTNESS_SHIFTS_DAYS,
            "robustness_survival_min": ROBUSTNESS_SURVIVAL_MIN,
        },
        "exclusions": exclusions,
        "primary_cohort": {
            "rows_after_freshness": len(fresh_rows),
            "rows_primary": len(primary),
            "rows_both_separate": len(both_rows),
        },
        "per_bucket": per_bucket_list,
        "family_effects_all": effects,
        "families_clearing_primary": clearers,
        "robustness": robustness,
        "verdict": verdict,
    }


def build_verdict(clearers: list[dict], robustness: dict) -> dict:
    if not clearers:
        return {
            "outcome": "no_non_major_signal_after_vintage_control",
            "headline": "Vintage control collapses the non-major host skew. "
                        "The 04-19 shard-level observation was explained by cohort vintage.",
            "publishable_families": [],
        }
    if not robustness:
        return {
            "outcome": "clearers_without_robustness",
            "headline": "Families cleared the effect floor but robustness was not computed.",
            "publishable_families": [],
        }
    survival = robustness.get("bucket_shift", {}).get("survival", {})
    fresh_clearers = set(robustness.get("freshness_strict", {}).get("families_clearing", []))
    direction_preserved = robustness.get("include_both", {}).get("direction_preserved", {})
    publishable = []
    for c in clearers:
        fam = c["family"]
        s = survival.get(fam, {})
        passes = (
            s.get("survives", False)
            and fam in fresh_clearers
            and direction_preserved.get(fam, False)
        )
        if passes:
            publishable.append({
                "family": fam,
                "clearing_buckets": c["clearing_buckets"],
                "bucket_shift_hits": s.get("hits"),
                "freshness_strict_pass": fam in fresh_clearers,
                "direction_preserved": direction_preserved.get(fam),
            })
    if publishable:
        return {
            "outcome": "non_major_signal_survives_vintage_control",
            "headline": "Non-major host families with vintage-controlled seed vs live skew "
                        f"clearing all pre-registered floors: {len(publishable)}.",
            "publishable_families": publishable,
        }
    return {
        "outcome": "clearers_fail_robustness",
        "headline": "Families cleared effect floor in primary analysis but failed "
                    "one or more robustness checks.",
        "publishable_families": [],
        "partial_clearers": [c["family"] for c in clearers],
    }


def render_text(report: dict) -> str:
    lines = []
    lines.append("# Vintage-stratified admissibility — Phase D")
    lines.append(f"generated_at: {report['generated_at']}")
    lines.append(f"backfill_run_id: {report['provenance']['backfill_run_id']}")
    lines.append("")
    lines.append("## Verdict")
    v = report["verdict"]
    lines.append(f"  outcome: {v['outcome']}")
    lines.append(f"  {v['headline']}")
    if v.get("publishable_families"):
        lines.append("  publishable families:")
        for f in v["publishable_families"]:
            lines.append(f"    - {f['family']} in {f['clearing_buckets']}")
    lines.append("")
    lines.append("## Exclusions")
    e = report["exclusions"]
    lines.append(f"  total pre-filter: {e['total_pre_filter']:,}")
    lines.append(f"  excluded_non_plc: {e['excluded_non_plc']:,}")
    lines.append(f"  excluded_no_vintage: {e['excluded_no_vintage']:,}")
    lines.append(f"  excluded_unsupported_method: {e['excluded_unsupported_method']:,}")
    lines.append(f"  excluded_resolver_not_ok: {e['excluded_resolver_not_ok']:,}")
    lines.append(f"  kept_total: {e['kept_total']:,}")
    lines.append(f"  kept_both_source (reported separately): {e['kept_both_source']:,}")
    lines.append(f"  after freshness (<={FRESHNESS_DAYS}d): {report['primary_cohort']['rows_after_freshness']:,}")
    lines.append(f"  primary (excludes 'both'): {report['primary_cohort']['rows_primary']:,}")
    lines.append("")
    lines.append("## Per-bucket summary")
    lines.append(f"  {'bucket':<35} {'seed':>8} {'seed %nm':>10} {'live':>8} {'live %nm':>10}")
    for b in report["per_bucket"]:
        lines.append(
            f"  {b['bucket_label']:<35} "
            f"{b['seed_total']:>8} "
            f"{(str(b['seed_non_major_pct']) + '%' if b['seed_non_major_pct'] is not None else '--'):>10} "
            f"{b['live_total']:>8} "
            f"{(str(b['live_non_major_pct']) + '%' if b['live_non_major_pct'] is not None else '--'):>10}"
        )
    lines.append("")
    if report["families_clearing_primary"]:
        lines.append("## Families clearing all primary floors")
        for c in report["families_clearing_primary"]:
            lines.append(f"  - {c['family']} (buckets: {c['clearing_buckets']})")
            for e in c["per_bucket"]:
                lines.append(
                    f"      {e['bucket_label']}: "
                    f"seed {e['seed_n']}/{e['seed_total']} ({e['seed_pct']}%), "
                    f"live {e['live_n']}/{e['live_total']} ({e['live_pct']}%), "
                    f"delta={e['delta_pp']:+.2f}pp ratio={e['ratio']}"
                )
    else:
        lines.append("## Families clearing primary floors")
        lines.append("  (none)")
    lines.append("")
    if report.get("robustness"):
        lines.append("## Robustness")
        bs = report["robustness"].get("bucket_shift", {}).get("survival", {})
        for fam, s in bs.items():
            lines.append(f"  bucket shift survival: {fam} {s['hits']}/{s['total']} "
                         f"({'PASS' if s['survives'] else 'FAIL'})")
        fs = report["robustness"].get("freshness_strict", {})
        lines.append(f"  freshness <48h clearers: {fs.get('families_clearing', [])}")
        ib = report["robustness"].get("include_both", {}).get("direction_preserved", {})
        for fam, ok in ib.items():
            lines.append(f"  include_both direction preserved: {fam} {'PASS' if ok else 'FAIL'}")
    lines.append("")
    return "\n".join(lines)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", default="out/admissibility/")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    report = build_report(args.db)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%MZ")
    json_path = out_dir / f"{stamp}_vintage_admissibility.json"
    txt_path = out_dir / f"{stamp}_vintage_admissibility.txt"
    json_path.write_text(json.dumps(report, indent=2))
    txt_path.write_text(render_text(report))
    LOG.info("wrote %s", json_path)
    LOG.info("wrote %s", txt_path)
    print(render_text(report))


if __name__ == "__main__":
    main()
