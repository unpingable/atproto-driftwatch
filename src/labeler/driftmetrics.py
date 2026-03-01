"""Driftwatch longitudinal metrics layer.

Computes temporal dynamics on claim clusters (fingerprints):
- Fingerprint-level time series (posts, authors, confidence per time bin)
- Half-life (peak to 50% decay)
- Burst / synchrony scores
- Regime shift detection (label distribution divergence)

All queries run against existing tables (claim_history, label_decisions, edges).
No new tables. No raw text. Aggregate-first.
"""

import math
import datetime
from collections import Counter, defaultdict
from typing import Optional, List, Dict, Any

from .db import get_conn
from . import timeutil
from .detection import (
    SubjectRef,
    DetectionEnvelope,
    build_envelope,
    envelope_to_dict,
    sort_detections,
    compute_window_fingerprint,
    make_hashset_root,
    make_note,
    receipt_hash,
    ENVELOPE_SCHEMA_VERSION,
)


def _bin_timestamp(ts_str: str, bin_hours: int = 1) -> str:
    """Truncate an ISO timestamp to the nearest bin boundary."""
    try:
        dt = timeutil.to_utc_datetime(ts_str)
    except Exception:
        dt = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    hour = (dt.hour // bin_hours) * bin_hours
    return dt.replace(hour=hour, minute=0, second=0, microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Step 1: Fingerprint-level time series
# ---------------------------------------------------------------------------

def fingerprint_timeseries(
    conn=None,
    fingerprint: Optional[str] = None,
    bin_hours: int = 1,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Time-binned aggregation for claim fingerprints.

    Groups claim_history by (claim_fingerprint, time_bin) and computes:
      posts            — row count
      authors          — distinct authorDid count
      avg_confidence   — mean confidence (None if no values)
      evidence_variants — distinct evidence_hash count (variant spread)
    """
    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    query = "SELECT claim_fingerprint, createdAt, authorDid, confidence, COALESCE(evidence_class, 'none') FROM claim_history"
    params: list = []
    conditions: list = []

    if fingerprint:
        conditions.append("claim_fingerprint = ?")
        params.append(fingerprint)
    if since:
        conditions.append("createdAt >= ?")
        params.append(since)
    if until:
        conditions.append("createdAt <= ?")
        params.append(until)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY createdAt ASC"

    rows = conn.execute(query, params).fetchall()
    if own_conn:
        conn.close()

    buckets: Dict[tuple, dict] = defaultdict(lambda: {
        "posts": 0,
        "authors": set(),
        "confidences": [],
        "evidence_classes": Counter(),
    })

    for fp, created_at, author, confidence, evidence_class in rows:
        bin_key = _bin_timestamp(created_at, bin_hours)
        b = buckets[(fp, bin_key)]
        b["posts"] += 1
        b["authors"].add(author)
        if confidence is not None:
            b["confidences"].append(confidence)
        b["evidence_classes"][evidence_class or "none"] += 1

    result = []
    for (fp, bin_key), b in sorted(buckets.items()):
        avg_conf = (
            sum(b["confidences"]) / len(b["confidences"])
            if b["confidences"] else None
        )
        result.append({
            "fingerprint": fp,
            "bin": bin_key,
            "posts": b["posts"],
            "authors": len(b["authors"]),
            "avg_confidence": round(avg_conf, 4) if avg_conf is not None else None,
            "evidence_classes": dict(b["evidence_classes"]),
        })

    return result


# ---------------------------------------------------------------------------
# Step 2: Half-life
# ---------------------------------------------------------------------------

def compute_half_life(timeseries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Half-life from peak for a single fingerprint's time series.

    Expects entries sorted by bin, all sharing the same fingerprint.
    Returns dict with peak info and half_life_bins (None if not yet decayed).
    """
    if not timeseries:
        return None

    peak_idx = max(range(len(timeseries)), key=lambda i: timeseries[i]["posts"])
    peak = timeseries[peak_idx]
    threshold = peak["posts"] * 0.5

    if threshold < 1:
        return None

    for i in range(peak_idx + 1, len(timeseries)):
        if timeseries[i]["posts"] <= threshold:
            return {
                "fingerprint": peak["fingerprint"],
                "peak_bin": peak["bin"],
                "peak_posts": peak["posts"],
                "half_life_bins": i - peak_idx,
                "decayed_to_bin": timeseries[i]["bin"],
            }

    return {
        "fingerprint": peak["fingerprint"],
        "peak_bin": peak["bin"],
        "peak_posts": peak["posts"],
        "half_life_bins": None,
        "decayed_to_bin": None,
    }


# ---------------------------------------------------------------------------
# Step 3: Burst / synchrony
# ---------------------------------------------------------------------------

def _entropy(counts: List[int]) -> float:
    """Shannon entropy of a distribution given as counts."""
    total = sum(counts)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts if c > 0]
    return -sum(p * math.log2(p) for p in probs)


def compute_burst_scores(
    timeseries: List[Dict[str, Any]],
    window_bins: int = 24,
) -> List[Dict[str, Any]]:
    """Burst z-scores per fingerprint.

    For each fingerprint computes:
      burst_score     — z-score of latest bin vs rolling baseline
      synchrony       — authors/posts in latest bin (high = many authors)
      variant_entropy — Shannon entropy of per-bin evidence_variant counts
      author_growth   — latest authors vs prior window mean, as fraction
    """
    by_fp: Dict[str, list] = defaultdict(list)
    for entry in timeseries:
        by_fp[entry["fingerprint"]].append(entry)

    results = []
    for fp, series in by_fp.items():
        series = sorted(series, key=lambda x: x["bin"])
        posts_seq = [s["posts"] for s in series]

        if len(posts_seq) < 2:
            continue

        baseline = posts_seq[max(0, len(posts_seq) - window_bins - 1):-1]
        if not baseline:
            baseline = posts_seq[:-1]

        mean = sum(baseline) / len(baseline)
        variance = sum((x - mean) ** 2 for x in baseline) / len(baseline)
        std = math.sqrt(variance) if variance > 0 else 1.0

        latest = posts_seq[-1]
        burst_score = (latest - mean) / std

        latest_entry = series[-1]
        synchrony = (
            latest_entry["authors"] / latest_entry["posts"]
            if latest_entry["posts"] > 0 else 0.0
        )

        # Evidence class entropy: how diverse is the evidence type across bins?
        # Aggregate evidence class counts across all bins for this fingerprint
        agg_classes: Counter = Counter()
        for s in series:
            for cls, cnt in s.get("evidence_classes", {}).items():
                agg_classes[cls] += cnt
        variant_entropy = _entropy(list(agg_classes.values()))

        author_seq = [s["authors"] for s in series]
        prior_authors = author_seq[max(0, len(author_seq) - window_bins - 1):-1]
        author_mean = sum(prior_authors) / len(prior_authors) if prior_authors else 0
        author_growth = (
            (author_seq[-1] - author_mean) / author_mean
            if author_mean > 0 else 0.0
        )

        results.append({
            "fingerprint": fp,
            "burst_score": round(burst_score, 3),
            "synchrony": round(synchrony, 3),
            "variant_entropy": round(variant_entropy, 3),
            "author_growth": round(author_growth, 3),
            "latest_posts": latest,
            "latest_authors": latest_entry["authors"],
            "total_posts": sum(posts_seq),
            "total_bins": len(series),
        })

    results.sort(key=lambda x: x["burst_score"], reverse=True)
    return results


# ---------------------------------------------------------------------------
# Step 4: Regime shifts
# ---------------------------------------------------------------------------

def label_rate_vectors(
    conn=None,
    bin_hours: int = 1,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Label distribution vectors per time bin from label_decisions."""
    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    query = "SELECT created_at, label FROM label_decisions WHERE status = 'committed'"
    params: list = []
    if since:
        query += " AND created_at >= ?"
        params.append(since)
    if until:
        query += " AND created_at <= ?"
        params.append(until)
    query += " ORDER BY created_at ASC"

    rows = conn.execute(query, params).fetchall()
    if own_conn:
        conn.close()

    buckets: Dict[str, Counter] = defaultdict(Counter)
    for created_at, label in rows:
        bin_key = _bin_timestamp(created_at, bin_hours)
        buckets[bin_key][label] += 1

    result = []
    for bin_key in sorted(buckets.keys()):
        counts = dict(buckets[bin_key])
        result.append({
            "bin": bin_key,
            "label_counts": counts,
            "total": sum(counts.values()),
        })
    return result


def _jsd(p: Dict[str, float], q: Dict[str, float]) -> float:
    """Jensen-Shannon divergence between two label distributions."""
    all_labels = set(p.keys()) | set(q.keys())
    if not all_labels:
        return 0.0

    p_total = sum(p.values()) or 1
    q_total = sum(q.values()) or 1

    p_dist = {k: p.get(k, 0) / p_total for k in all_labels}
    q_dist = {k: q.get(k, 0) / q_total for k in all_labels}

    m_dist = {k: (p_dist[k] + q_dist[k]) / 2 for k in all_labels}

    def _kl(a, m):
        total = 0.0
        for k in all_labels:
            if a[k] > 0 and m[k] > 0:
                total += a[k] * math.log2(a[k] / m[k])
        return total

    return 0.5 * _kl(p_dist, m_dist) + 0.5 * _kl(q_dist, m_dist)


def detect_regime_shifts(
    vectors: List[Dict[str, Any]],
    baseline_bins: int = 168,
    threshold: float = 0.3,
) -> List[Dict[str, Any]]:
    """Compare each bin's label distribution to rolling baseline using JSD.

    Returns list with divergence scores; is_shift=True when >= threshold.
    """
    if len(vectors) < 2:
        return []

    results = []
    for i, vec in enumerate(vectors):
        start = max(0, i - baseline_bins)
        baseline_counts: Counter = Counter()
        baseline_total = 0
        for j in range(start, i):
            for label, count in vectors[j]["label_counts"].items():
                baseline_counts[label] += count
            baseline_total += vectors[j]["total"]

        if baseline_total == 0:
            continue

        divergence = _jsd(dict(baseline_counts), vec["label_counts"])
        results.append({
            "bin": vec["bin"],
            "divergence": round(divergence, 4),
            "is_shift": divergence >= threshold,
            "baseline_total": baseline_total,
            "bin_total": vec["total"],
        })

    return results


# ---------------------------------------------------------------------------
# Cluster kind helpers
# ---------------------------------------------------------------------------

def _query_cluster_kinds(conn, since: str) -> Dict[str, Dict[str, int]]:
    """Return {kind: {fingerprints: N, posts: N}} for the report window."""
    rows = conn.execute(
        "SELECT COALESCE(fp_kind, 'unknown'), COUNT(DISTINCT claim_fingerprint), COUNT(*) "
        "FROM claim_history WHERE createdAt >= ? GROUP BY fp_kind",
        (since,),
    ).fetchall()
    return {
        row[0]: {"fingerprints": row[1], "posts": row[2]}
        for row in rows
    }


def _query_fp_kind_map(conn, since: str) -> Dict[str, str]:
    """Return {claim_fingerprint: dominant_fp_kind} for fingerprints in the window.

    Uses the most common fp_kind per fingerprint (MODE).
    """
    rows = conn.execute(
        "SELECT claim_fingerprint, fp_kind, COUNT(*) as cnt "
        "FROM claim_history WHERE createdAt >= ? AND fp_kind IS NOT NULL "
        "GROUP BY claim_fingerprint, fp_kind ORDER BY claim_fingerprint, cnt DESC",
        (since,),
    ).fetchall()
    fp_map: Dict[str, str] = {}
    for fp, kind, _cnt in rows:
        if fp not in fp_map:
            fp_map[fp] = kind or "unknown"
    return fp_map


# ---------------------------------------------------------------------------
# Cluster report — the first Driftwatch artifact
# ---------------------------------------------------------------------------

def cluster_report(
    conn=None,
    top_n: int = 20,
    hours: int = 24,
    bin_hours: int = 1,
) -> Dict[str, Any]:
    """Top N fingerprints by burst score with half-life, author growth,
    variant entropy, plus overall label distribution drift.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    since = (timeutil.now_utc() - datetime.timedelta(hours=hours)).isoformat()

    ts = fingerprint_timeseries(conn=conn, bin_hours=bin_hours, since=since)

    # --- Window totals ---
    totals_row = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT claim_fingerprint) "
        "FROM claim_history WHERE createdAt >= ?",
        (since,),
    ).fetchone()
    total_claims = totals_row[0] if totals_row else 0
    distinct_fps = totals_row[1] if totals_row else 0

    # --- Cluster kinds summary ---
    cluster_kinds = _query_cluster_kinds(conn, since)

    # --- Per-fingerprint kind lookup ---
    fp_kind_map = _query_fp_kind_map(conn, since)

    bursts = compute_burst_scores(ts)[:top_n]

    # Separate single-author-heavy clusters (likely automation/spam)
    SINGLE_AUTHOR_THRESHOLD = 10  # posts threshold for single-author flagging
    clusters = []
    automation = []
    for b in bursts:
        fp = b["fingerprint"]
        fp_ts = [t for t in ts if t["fingerprint"] == fp]
        hl = compute_half_life(fp_ts)
        entry = {**b, "half_life": hl, "fp_kind": fp_kind_map.get(fp, "unknown")}
        if b["latest_authors"] <= 1 and b["total_posts"] >= SINGLE_AUTHOR_THRESHOLD:
            entry["single_author_heavy"] = True
            automation.append(entry)
        else:
            clusters.append(entry)

    baseline_since = (
        timeutil.now_utc() - datetime.timedelta(hours=hours * 7)
    ).isoformat()
    vectors = label_rate_vectors(conn=conn, bin_hours=bin_hours, since=baseline_since)
    shifts = detect_regime_shifts(vectors)
    recent_shifts = [s for s in shifts if s["bin"] >= since]

    if own_conn:
        conn.close()

    # Platform health context
    try:
        from . import platform_health
        health_snap = platform_health.get_health_snapshot()
        ph_state = health_snap.get("health_state", "unknown")
        ph_coverage = health_snap.get("coverage_pct", 0)
        ph_lag = health_snap.get("stream_lag_s", 0)
        ph_reasons = health_snap.get("gate_reasons", [])
    except Exception:
        health_snap = {}
        ph_state = "unavailable"
        ph_coverage = 0
        ph_lag = 0
        ph_reasons = []

    # --- Emit detection envelopes (M1+M2) ---
    now_iso = timeutil.now_utc().isoformat()
    window_str = f"{hours}h"

    # Config hash for burst detector (thresholds are implicit in code version)
    from .claims import fingerprint_config_hash
    cfg_hash = fingerprint_config_hash()

    # Window fingerprint (M2)
    wfp = compute_window_fingerprint(
        ts_start=since, ts_end=now_iso, window=window_str,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        config_hash=cfg_hash,
        watermark_snapshot=health_snap if health_snap else {},
    )

    detections: List[DetectionEnvelope] = []

    # Burst score envelopes
    for entry in clusters + automation:
        fp = entry["fingerprint"]
        bs = entry["burst_score"]
        if bs < 1.0:
            continue  # only envelope notable bursts

        severity = "info"
        if bs >= 5.0:
            severity = "high"
        elif bs >= 3.0:
            severity = "med"
        elif bs >= 2.0:
            severity = "low"

        hl = entry.get("half_life")
        explain = {
            "burst_score": bs,
            "synchrony": entry.get("synchrony", 0),
            "author_growth": entry.get("author_growth", 0),
            "latest_posts": entry.get("latest_posts", 0),
            "latest_authors": entry.get("latest_authors", 0),
            "total_posts": entry.get("total_posts", 0),
            "fp_kind": entry.get("fp_kind", "unknown"),
            "baseline_kind": "rolling_zscore",
            "baseline_window": window_str,
        }
        if hl and hl.get("half_life_bins") is not None:
            explain["half_life_bins"] = hl["half_life_bins"]
        if entry.get("single_author_heavy"):
            explain["single_author_heavy"] = True

        # Evidence: hashset_root for the top contributors
        evidence = (
            make_hashset_root(
                subjects=[fp],
                total_count=entry.get("total_posts", 0),
            ),
        )

        detections.append(build_envelope(
            detector_id="burst_score",
            detector_version="v1",
            ts_start=since,
            ts_end=now_iso,
            window=window_str,
            subject=SubjectRef("fingerprint", fp),
            detection_type="burst",
            score=bs,
            severity=severity,
            explain=explain,
            evidence=evidence,
            window_fingerprint=wfp,
            config_hash=cfg_hash,
        ))

    # Regime shift envelopes
    for shift in recent_shifts:
        if not shift.get("is_shift"):
            continue
        div = shift["divergence"]
        severity = "info"
        if div >= 0.7:
            severity = "high"
        elif div >= 0.5:
            severity = "med"
        elif div >= 0.3:
            severity = "low"

        explain = {
            "bin": shift["bin"],
            "divergence": div,
            "baseline_total": shift.get("baseline_total", 0),
            "bin_total": shift.get("bin_total", 0),
            "baseline_kind": "rolling_jsd",
            "baseline_window": f"{hours * 7}h",
        }

        detections.append(build_envelope(
            detector_id="regime_shift",
            detector_version="v1",
            ts_start=since,
            ts_end=now_iso,
            window=window_str,
            subject=SubjectRef("global", ""),
            detection_type="distribution_divergence",
            score=div,
            severity=severity,
            explain=explain,
            evidence=(make_note(f"jsd={div:.4f} bin={shift['bin']}"),),
            window_fingerprint=wfp,
            config_hash=cfg_hash,
        ))

    # Run M3 sensors if available
    try:
        from .sensors import run_sensors, SensorContext
        sensor_ctx = SensorContext(
            conn=conn if not own_conn else None,
            ts_start=since,
            ts_end=now_iso,
            window=window_str,
            watermark=health_snap if health_snap else {},
            window_fingerprint=wfp,
            config_hash=cfg_hash,
            timeseries=ts,
            total_claims=total_claims,
        )
        sensor_detections = run_sensors(sensor_ctx)
        detections.extend(sensor_detections)
    except ImportError:
        pass  # sensors package not yet available

    # Sort and cap detections
    detections = sort_detections(detections)
    MAX_TOTAL_DETECTIONS = 100
    if len(detections) > MAX_TOTAL_DETECTIONS:
        detections = detections[:MAX_TOTAL_DETECTIONS]

    return {
        "generated_at": now_iso,
        "window_hours": hours,
        "bin_hours": bin_hours,
        "total_claims_in_window": total_claims,
        "distinct_fps_in_window": distinct_fps,
        "cluster_kinds": cluster_kinds,
        "platform_health": ph_state,
        "coverage_pct": ph_coverage,
        "stream_lag_s": ph_lag,
        "gate_reasons": ph_reasons,
        "clusters": clusters[:top_n],
        "likely_automation": automation,
        "regime_shifts": recent_shifts,
        "detections": [envelope_to_dict(d) for d in detections],
    }
