"""Driftwatch sensor array — M3 cheap heads pack.

Provides run_sensors() which executes all registered sensors with budget
enforcement, health gating, and detection cap.
"""

import logging
import os
from typing import List

from ..detection import (
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    make_note,
    sort_detections,
)
from .base import (
    SensorContext,
    run_sensor_with_budget,
    _get_disabled_sensor_ids,
    MAX_TOTAL_DETECTIONS,
    SENSOR_MAX_DETECTIONS,
    SENSOR_RUNTIME_BUDGET_MS,
)
from .concentration import ConcentrationSensor
from .author_velocity import AuthorVelocitySensor

from ..detection import ENVELOPE_SCHEMA_VERSION, VALID_EVIDENCE_KINDS

LOG = logging.getLogger("labeler.sensors")

# Re-export SensorContext for driftmetrics.py
__all__ = ["run_sensors", "get_capabilities", "SensorContext"]

# Registered sensors (order matters for budget allocation)
SENSORS = [
    ConcentrationSensor(),
    AuthorVelocitySensor(),
]


def _sensor_array_enabled() -> bool:
    """Check SENSOR_ARRAY_ENABLED env var (default: true)."""
    return os.getenv("SENSOR_ARRAY_ENABLED", "true").lower() not in ("false", "0", "no")


def get_capabilities() -> dict:
    """Machine-readable self-description of this sensor array instance.

    Consumers (e.g. labelwatch) can query this to discover what detections
    to expect without assumptions.
    """
    disabled_ids = _get_disabled_sensor_ids()
    return {
        "envelope_schema_version": ENVELOPE_SCHEMA_VERSION,
        "sensors": [
            {
                "sensor_id": s.sensor_id,
                "sensor_version": s.sensor_version,
                "enabled": s.sensor_id not in disabled_ids,
            }
            for s in SENSORS
        ],
        "evidence_kinds": list(VALID_EVIDENCE_KINDS),
        "max_detections_per_sensor": SENSOR_MAX_DETECTIONS,
        "max_detections_per_window": MAX_TOTAL_DETECTIONS,
        "runtime_budget_ms": SENSOR_RUNTIME_BUDGET_MS,
        "sensor_array_enabled": _sensor_array_enabled(),
    }


def run_sensors(ctx: SensorContext) -> List[DetectionEnvelope]:
    """Run all registered sensors with health gating and budget enforcement.

    Returns sorted list of DetectionEnvelope objects, capped at MAX_TOTAL_DETECTIONS.
    """
    # Global kill switch — operator intent, not an anomaly (no skip envelope)
    if not _sensor_array_enabled():
        return []

    # Health gating: if platform is degraded, emit single skip envelope
    watermark_state = ctx.watermark.get("health_state", "unknown")
    if watermark_state == "degraded":
        LOG.info("sensors skipped: platform degraded")
        return [build_envelope(
            detector_id="sensor_array",
            detector_version="v1",
            ts_start=ctx.ts_start,
            ts_end=ctx.ts_end,
            window=ctx.window,
            subject=SubjectRef("global", ""),
            detection_type="sensor_skipped_platform_degraded",
            score=0.0,
            severity="info",
            explain={
                "reason": "platform_degraded",
                "health_state": watermark_state,
                "gate_reasons": ctx.watermark.get("gate_reasons", []),
            },
            evidence=(make_note("sensors gated: platform degraded"),),
            window_fingerprint=ctx.window_fingerprint,
            config_hash=ctx.config_hash,
        )]

    all_detections: List[DetectionEnvelope] = []

    for sensor in SENSORS:
        results = run_sensor_with_budget(sensor, ctx)
        all_detections.extend(results)

    # Sort and cap total detections
    all_detections = sort_detections(all_detections)
    if len(all_detections) > MAX_TOTAL_DETECTIONS:
        LOG.warning("total sensor detections %d exceeds cap %d",
                    len(all_detections), MAX_TOTAL_DETECTIONS)
        all_detections = all_detections[:MAX_TOTAL_DETECTIONS]

    return all_detections
