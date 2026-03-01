"""Driftwatch sensor array — M3 cheap heads pack.

Provides run_sensors() which executes all registered sensors with budget
enforcement, health gating, and detection cap.
"""

import logging
from typing import List

from ..detection import (
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    sort_detections,
)
from .base import (
    SensorContext,
    run_sensor_with_budget,
    MAX_TOTAL_DETECTIONS,
)
from .concentration import ConcentrationSensor
from .author_velocity import AuthorVelocitySensor

LOG = logging.getLogger("labeler.sensors")

# Re-export SensorContext for driftmetrics.py
__all__ = ["run_sensors", "SensorContext"]

# Registered sensors (order matters for budget allocation)
SENSORS = [
    ConcentrationSensor(),
    AuthorVelocitySensor(),
]


def run_sensors(ctx: SensorContext) -> List[DetectionEnvelope]:
    """Run all registered sensors with health gating and budget enforcement.

    Returns sorted list of DetectionEnvelope objects, capped at MAX_TOTAL_DETECTIONS.
    """
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
            evidence=(),
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
