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

    bursts = compute_burst_scores(ts)[:top_n]

    # Separate single-author-heavy clusters (likely automation/spam)
    SINGLE_AUTHOR_THRESHOLD = 10  # posts threshold for single-author flagging
    clusters = []
    automation = []
    for b in bursts:
        fp = b["fingerprint"]
        fp_ts = [t for t in ts if t["fingerprint"] == fp]
        hl = compute_half_life(fp_ts)
        entry = {**b, "half_life": hl}
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

    return {
        "generated_at": timeutil.now_utc().isoformat(),
        "window_hours": hours,
        "bin_hours": bin_hours,
        "clusters": clusters[:top_n],
        "likely_automation": automation,
        "regime_shifts": recent_shifts,
    }
