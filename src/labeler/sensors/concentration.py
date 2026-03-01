"""Concentration shift sensor (H1) — Herfindahl index on fingerprint distribution.

Detects when the fingerprint distribution becomes unusually concentrated,
signaling coordinated campaigns or single-source amplification.

Herfindahl index: H = sum((count_i / total)^2)
- Ranges from 1/N (uniform) to 1.0 (monopoly)
- Threshold: H increases by >50% relative to baseline, or absolute H > 0.3

Uses aggregate query only (GROUP BY fingerprint, COUNT). No per-event scans.
No COUNT(DISTINCT author) — EFF-002 compliant.
"""

import logging
from typing import List

from ..detection import (
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    make_query_commitment,
    receipt_hash,
    MAX_EXPLAIN_TOP_K,
)
from .base import SensorContext

LOG = logging.getLogger("labeler.sensors.concentration")

# --- Sensor config ---
CONCENTRATION_THRESHOLD_RELATIVE = 0.5   # H increases >50% vs baseline
CONCENTRATION_THRESHOLD_ABSOLUTE = 0.3   # absolute H > 0.3
BASELINE_EWMA_ALPHA = 2.0 / (24 + 1)    # ~24 window EWMA

# EWMA state (module-level, resets on restart — same pattern as platform_health)
_baseline_h: float = 0.0
_windows_seen: int = 0


def _herfindahl(counts: List[int]) -> float:
    """Herfindahl index from a list of counts."""
    total = sum(counts)
    if total == 0:
        return 0.0
    return sum((c / total) ** 2 for c in counts)


def _reset():
    """Reset EWMA state (for testing)."""
    global _baseline_h, _windows_seen
    _baseline_h = 0.0
    _windows_seen = 0


class ConcentrationSensor:
    sensor_id = "concentration_shift"
    sensor_version = "v1"
    min_volume = 50
    warmup_windows = 5

    def run(self, ctx: SensorContext) -> List[DetectionEnvelope]:
        global _baseline_h, _windows_seen

        # Aggregate fingerprint distribution from pre-computed timeseries
        fp_counts: dict = {}
        for entry in ctx.timeseries:
            fp = entry["fingerprint"]
            fp_counts[fp] = fp_counts.get(fp, 0) + entry["posts"]

        if not fp_counts:
            return []

        counts = list(fp_counts.values())
        current_h = _herfindahl(counts)
        total_posts = sum(counts)

        # Update EWMA baseline
        _windows_seen += 1
        if _baseline_h == 0.0:
            _baseline_h = current_h
        else:
            _baseline_h = (
                BASELINE_EWMA_ALPHA * current_h
                + (1 - BASELINE_EWMA_ALPHA) * _baseline_h
            )

        # Check thresholds
        relative_increase = (
            (current_h - _baseline_h) / _baseline_h
            if _baseline_h > 0 else 0.0
        )
        is_anomalous = (
            relative_increase > CONCENTRATION_THRESHOLD_RELATIVE
            or current_h > CONCENTRATION_THRESHOLD_ABSOLUTE
        )

        if not is_anomalous:
            return []

        # Build explain with top-k fingerprints by share
        sorted_fps = sorted(fp_counts.items(), key=lambda x: (-x[1], x[0]))
        top_fps = sorted_fps[:MAX_EXPLAIN_TOP_K]
        top_k_list = [
            {"fingerprint": fp, "posts": cnt, "share": round(cnt / total_posts, 4)}
            for fp, cnt in top_fps
        ]

        severity = "info"
        if current_h > 0.6:
            severity = "high"
        elif current_h > 0.4:
            severity = "med"
        elif current_h > 0.3:
            severity = "low"

        explain = {
            "herfindahl": round(current_h, 6),
            "baseline_herfindahl": round(_baseline_h, 6),
            "relative_increase": round(relative_increase, 4),
            "total_posts": total_posts,
            "distinct_fps": len(fp_counts),
            "top_fps": top_k_list,
            "baseline_kind": "ewma",
            "baseline_window": "24h",
        }
        if len(sorted_fps) > MAX_EXPLAIN_TOP_K:
            explain["truncated"] = True

        # Evidence: hashset root of top contributors
        from ..detection import make_hashset_root
        evidence = (
            make_hashset_root(
                subjects=[fp for fp, _ in top_fps],
                total_count=len(fp_counts),
            ),
        )

        return [build_envelope(
            detector_id=self.sensor_id,
            detector_version=self.sensor_version,
            ts_start=ctx.ts_start,
            ts_end=ctx.ts_end,
            window=ctx.window,
            subject=SubjectRef("global", ""),
            detection_type="concentration_spike",
            score=round(current_h, 6),
            severity=severity,
            explain=explain,
            evidence=evidence,
            window_fingerprint=ctx.window_fingerprint,
            config_hash=ctx.config_hash,
        )]
