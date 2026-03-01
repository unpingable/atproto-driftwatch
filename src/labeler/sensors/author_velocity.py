"""Author velocity anomaly sensor (H2) — author/post ratio dynamics.

Detects fingerprints where the author-to-post ratio diverges from baseline:
- "Spread" detection: ratio jumps (many new authors — going viral)
- "Amplification" detection: ratio drops (few authors, high volume — being botted)

Uses pre-computed fingerprint_timeseries output which already tracks unique
authors per bin. Does NOT run COUNT(DISTINCT author) — EFF-002 compliant.

Runs on top-N fingerprints by volume only (N=20). Not all fingerprints.
"""

import logging
from typing import List
from collections import defaultdict

from ..detection import (
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    make_hashset_root,
    MAX_EXPLAIN_TOP_K,
)
from .base import SensorContext

LOG = logging.getLogger("labeler.sensors.author_velocity")

# --- Sensor config ---
TOP_N_FINGERPRINTS = 20
RATIO_SPREAD_THRESHOLD = 0.5     # ratio jumps >50% vs baseline
RATIO_AMPLIFICATION_THRESHOLD = -0.4  # ratio drops >40% vs baseline
BASELINE_EWMA_ALPHA = 2.0 / (24 + 1)  # ~24 window EWMA

# Per-fingerprint EWMA baselines (resets on restart)
_baseline_ratios: dict = {}  # fp -> EWMA of author_ratio
_fp_windows_seen: dict = {}  # fp -> count


def _reset():
    """Reset state (for testing)."""
    _baseline_ratios.clear()
    _fp_windows_seen.clear()


class AuthorVelocitySensor:
    sensor_id = "author_velocity"
    sensor_version = "v1"
    min_volume = 50
    warmup_windows = 5

    def run(self, ctx: SensorContext) -> List[DetectionEnvelope]:
        # Aggregate timeseries by fingerprint
        fp_agg: dict = defaultdict(lambda: {"posts": 0, "authors": 0})
        for entry in ctx.timeseries:
            fp = entry["fingerprint"]
            fp_agg[fp]["posts"] += entry["posts"]
            fp_agg[fp]["authors"] += entry["authors"]

        if not fp_agg:
            return []

        # Top-N by volume
        sorted_fps = sorted(fp_agg.items(), key=lambda x: -x[1]["posts"])
        top_fps = sorted_fps[:TOP_N_FINGERPRINTS]

        detections: List[DetectionEnvelope] = []

        for fp, agg in top_fps:
            posts = agg["posts"]
            authors = agg["authors"]
            if posts == 0:
                continue

            current_ratio = authors / posts

            # Update EWMA baseline for this fingerprint
            _fp_windows_seen.setdefault(fp, 0)
            _fp_windows_seen[fp] += 1

            if fp not in _baseline_ratios:
                _baseline_ratios[fp] = current_ratio
                continue  # first window — no baseline to compare

            baseline = _baseline_ratios[fp]
            _baseline_ratios[fp] = (
                BASELINE_EWMA_ALPHA * current_ratio
                + (1 - BASELINE_EWMA_ALPHA) * baseline
            )

            # Compare current ratio to baseline
            if baseline == 0:
                continue
            delta = (current_ratio - baseline) / baseline

            detection_type = None
            severity = "info"

            if delta > RATIO_SPREAD_THRESHOLD:
                detection_type = "author_spread"
                severity = "low"
                if delta > 1.0:
                    severity = "med"
                if delta > 2.0:
                    severity = "high"

            elif delta < RATIO_AMPLIFICATION_THRESHOLD:
                detection_type = "author_amplification"
                severity = "low"
                if delta < -0.6:
                    severity = "med"
                if delta < -0.8:
                    severity = "high"

            if detection_type is None:
                continue

            explain = {
                "current_ratio": round(current_ratio, 4),
                "baseline_ratio": round(baseline, 4),
                "delta_pct": round(delta * 100, 1),
                "posts": posts,
                "authors": authors,
                "baseline_kind": "ewma",
                "baseline_window": "24h",
            }

            evidence = (
                make_hashset_root(subjects=[fp], total_count=posts),
            )

            detections.append(build_envelope(
                detector_id=self.sensor_id,
                detector_version=self.sensor_version,
                ts_start=ctx.ts_start,
                ts_end=ctx.ts_end,
                window=ctx.window,
                subject=SubjectRef("fingerprint", fp),
                detection_type=detection_type,
                score=round(abs(delta), 6),
                severity=severity,
                explain=explain,
                evidence=evidence,
                window_fingerprint=ctx.window_fingerprint,
                config_hash=ctx.config_hash,
            ))

        return detections
