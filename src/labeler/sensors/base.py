"""Sensor protocol, context, and budget enforcement for the Driftwatch sensor array.

Sensors are lightweight detectors that run within cluster_report() and emit
DetectionEnvelope objects. They use aggregate queries only, respect runtime
budgets, and gate on platform health.

Invariants:
- EFF-001: Max SENSOR_MAX_DETECTIONS per sensor per window, 100 total per window.
- EFF-002: No COUNT(DISTINCT author) in sensor SQL. Reuse pre-aggregated data.
- EFF-003: Only hot-zone lookback (24h default).
- EFF-004: Evidence stubs use top-k caps, not raw lists.
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import List, Optional, Protocol, runtime_checkable

from ..detection import (
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    make_note,
    sort_detections,
)

LOG = logging.getLogger("labeler.sensors")

# --- Constants ---

SENSOR_MAX_DETECTIONS = 20       # per sensor per window
SENSOR_RUNTIME_BUDGET_MS = 500   # skip + emit sensor_skipped if exceeded
MAX_TOTAL_DETECTIONS = 100       # across all sensors per window


@dataclass
class SensorContext:
    """Shared context passed to all sensors in a window."""
    conn: object                 # sqlite3.Connection or None (read-only)
    ts_start: str                # ISO timestamp of window start
    ts_end: str                  # ISO timestamp of window end
    window: str                  # e.g. "24h"
    watermark: dict              # from platform_health.get_health_snapshot()
    window_fingerprint: str      # from M2
    config_hash: str             # from M2
    timeseries: list             # pre-computed fingerprint_timeseries output
    total_claims: int            # total claims in window


@runtime_checkable
class Sensor(Protocol):
    """Protocol for Driftwatch sensors."""
    sensor_id: str
    sensor_version: str
    min_volume: int              # skip if total_posts < this
    warmup_windows: int          # skip first N windows

    def run(self, ctx: SensorContext) -> List[DetectionEnvelope]: ...


def _make_skip_envelope(
    reason: str,
    sensor_id: str,
    ctx: SensorContext,
    explain_extra: Optional[dict] = None,
) -> DetectionEnvelope:
    """Create a sensor_skipped envelope for debugging silence."""
    explain = {"reason": reason}
    if explain_extra:
        explain.update(explain_extra)
    return build_envelope(
        detector_id=sensor_id,
        detector_version="v1",
        ts_start=ctx.ts_start,
        ts_end=ctx.ts_end,
        window=ctx.window,
        subject=SubjectRef("global", ""),
        detection_type=f"sensor_skipped_{reason}",
        score=0.0,
        severity="info",
        explain=explain,
        evidence=(make_note(f"sensor skipped: {reason}"),),
        window_fingerprint=ctx.window_fingerprint,
        config_hash=ctx.config_hash,
    )


# Track warmup windows per sensor
_warmup_counters: dict = {}


def _get_disabled_sensor_ids() -> set:
    """Parse SENSOR_DISABLED_IDS env var (comma-separated)."""
    raw = os.getenv("SENSOR_DISABLED_IDS", "")
    if not raw.strip():
        return set()
    return {s.strip() for s in raw.split(",") if s.strip()}


def run_sensor_with_budget(
    sensor: Sensor,
    ctx: SensorContext,
) -> List[DetectionEnvelope]:
    """Run a single sensor with volume gate, warmup, and runtime budget.

    Returns list of DetectionEnvelope (may include skip envelopes).
    """
    sid = sensor.sensor_id

    # Per-sensor kill switch
    if sid in _get_disabled_sensor_ids():
        return []

    # Volume gate: skip silently (normal for low-traffic)
    if ctx.total_claims < sensor.min_volume:
        return []

    # Warmup suppression
    _warmup_counters.setdefault(sid, 0)
    _warmup_counters[sid] += 1
    if _warmup_counters[sid] <= sensor.warmup_windows:
        return []

    # Runtime budget
    t0 = time.monotonic()
    try:
        results = sensor.run(ctx)
    except Exception:
        LOG.exception("sensor %s raised", sid)
        return [_make_skip_envelope("error", sid, ctx)]

    elapsed_ms = (time.monotonic() - t0) * 1000
    if elapsed_ms > SENSOR_RUNTIME_BUDGET_MS:
        LOG.warning("sensor %s exceeded budget: %.0fms > %dms",
                    sid, elapsed_ms, SENSOR_RUNTIME_BUDGET_MS)
        return [_make_skip_envelope("budget", sid, ctx, {
            "elapsed_ms": round(elapsed_ms, 1),
            "budget_ms": SENSOR_RUNTIME_BUDGET_MS,
        })]

    # Cap per-sensor detections
    if len(results) > SENSOR_MAX_DETECTIONS:
        LOG.warning("sensor %s emitted %d detections, capping to %d",
                    sid, len(results), SENSOR_MAX_DETECTIONS)
        results = sort_detections(results)[:SENSOR_MAX_DETECTIONS]

    return results


def reset_warmup():
    """Reset warmup counters (for testing)."""
    _warmup_counters.clear()
