"""Preflight checks — verdict-shaped health gate for the sensor array.

Returns a structured checklist (PASS/WARN/FAIL per check) that operators
and automation can use to decide whether this instance is ready to produce
meaningful detections.
"""

import datetime
import logging

from . import timeutil

LOG = logging.getLogger("labeler.preflight")

HOT_ZONE_HOURS = 24


def _get_health_snapshot() -> dict:
    """Get health snapshot, isolating the import for testability."""
    from . import platform_health
    return platform_health.get_health_snapshot()


def _check_watermark() -> dict:
    """Platform health state is OK (not DEGRADED, not WARMING_UP with bad lag)."""
    try:
        snap = _get_health_snapshot()
        state = snap.get("health_state", "unknown")
        if state == "ok":
            return {"name": "watermark_ok", "status": "PASS", "detail": f"health_state={state}"}
        if state == "warming_up":
            lag = snap.get("stream_lag_s", 0)
            if lag > 120:
                return {"name": "watermark_ok", "status": "FAIL",
                        "detail": f"health_state={state}, lag={lag}s (>120s during warmup)"}
            return {"name": "watermark_ok", "status": "WARN",
                    "detail": f"health_state={state}, still calibrating"}
        if state == "degraded":
            reasons = snap.get("gate_reasons", [])
            return {"name": "watermark_ok", "status": "FAIL",
                    "detail": f"health_state={state}, reasons={reasons}"}
        return {"name": "watermark_ok", "status": "WARN",
                "detail": f"health_state={state}"}
    except Exception as e:
        return {"name": "watermark_ok", "status": "WARN",
                "detail": f"platform_health unavailable: {e}"}


def _check_indexes(conn) -> dict:
    """Required indexes exist for sensor queries."""
    required = ["idx_claim_history_fp", "idx_claim_history_created"]
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        existing = {r[0] for r in rows}
        missing = [idx for idx in required if idx not in existing]
        if missing:
            return {"name": "indexes_ok", "status": "FAIL",
                    "detail": f"missing indexes: {missing}"}
        return {"name": "indexes_ok", "status": "PASS",
                "detail": f"all {len(required)} required indexes present"}
    except Exception as e:
        return {"name": "indexes_ok", "status": "WARN",
                "detail": f"could not check indexes: {e}"}


def _check_hot_zone(conn) -> dict:
    """claim_history has data within the last 24h."""
    try:
        cutoff = (timeutil.now_utc() - datetime.timedelta(hours=HOT_ZONE_HOURS)).isoformat()
        row = conn.execute(
            "SELECT COUNT(*) FROM claim_history WHERE createdAt >= ?",
            (cutoff,),
        ).fetchone()
        count = row[0] if row else 0
        if count == 0:
            return {"name": "hot_zone_ok", "status": "FAIL",
                    "detail": f"no claims in last {HOT_ZONE_HOURS}h"}
        if count < 100:
            return {"name": "hot_zone_ok", "status": "WARN",
                    "detail": f"only {count} claims in last {HOT_ZONE_HOURS}h (thin data)"}
        return {"name": "hot_zone_ok", "status": "PASS",
                "detail": f"{count} claims in last {HOT_ZONE_HOURS}h"}
    except Exception as e:
        return {"name": "hot_zone_ok", "status": "WARN",
                "detail": f"could not check hot zone: {e}"}


def _check_sensors_enabled() -> dict:
    """SENSOR_ARRAY_ENABLED env var is not false."""
    try:
        from .sensors import _sensor_array_enabled
        if _sensor_array_enabled():
            return {"name": "sensors_enabled", "status": "PASS",
                    "detail": "SENSOR_ARRAY_ENABLED=true"}
        return {"name": "sensors_enabled", "status": "WARN",
                "detail": "SENSOR_ARRAY_ENABLED=false (operator disabled)"}
    except Exception as e:
        return {"name": "sensors_enabled", "status": "WARN",
                "detail": f"could not check sensor state: {e}"}


def preflight(conn=None) -> dict:
    """Run all preflight checks and return structured verdict.

    Returns:
        {
            "verdict": "PASS" | "WARN" | "FAIL",
            "checks": [{"name": ..., "status": ..., "detail": ...}, ...]
        }
    """
    own_conn = conn is None
    if own_conn:
        from .db import get_conn
        conn = get_conn()

    checks = [
        _check_watermark(),
        _check_indexes(conn),
        _check_hot_zone(conn),
        _check_sensors_enabled(),
    ]

    if own_conn:
        conn.close()

    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        verdict = "FAIL"
    elif "WARN" in statuses:
        verdict = "WARN"
    else:
        verdict = "PASS"

    return {"verdict": verdict, "checks": checks}
