"""Bake gate — machine-readable "are baselines trustworthy?" verdict.

Combines platform health, retention stats, and DB geometry into a single
BAKE_STATUS that tells operators (and automation) whether the instance has
stabilized enough for baselines to be meaningful.

Not a liveness check (that's /health). Not a preflight (that's /health/preflight).
This answers: "has the system been running well long enough that EWMA baselines,
regime detectors, and drift metrics are learning reality, not noise?"
"""

import logging
import os
import time

LOG = logging.getLogger("labeler.bake_gate")

# Thresholds (all overridable via env)
BAKE_MIN_WINDOWS = int(os.getenv("BAKE_MIN_WINDOWS", "60"))  # ~1 hour of STATS windows
BAKE_MAX_LAG_S = float(os.getenv("BAKE_MAX_LAG_S", "30"))
BAKE_MAX_DROP_FRAC = float(os.getenv("BAKE_MAX_DROP_FRAC", "0.001"))
BAKE_MAX_RECONNECTS_PER_HOUR = int(os.getenv("BAKE_MAX_RECONNECTS_HR", "2"))
BAKE_MAX_DB_GROWTH_MB_PER_HOUR = float(os.getenv("BAKE_MAX_DB_GROWTH_MB_HR", "500"))

# State: last retention stats (set by retention loop after each pass)
_last_retention_stats: dict = {}
_last_retention_ts: float = 0.0

# State: DB size tracking for growth rate
_db_size_history: list = []  # [(monotonic_ts, size_mb), ...]
_DB_HISTORY_MAX = 24  # keep ~24 hours of hourly samples


def record_retention_stats(stats: dict):
    """Called by retention loop after each pass."""
    global _last_retention_stats, _last_retention_ts
    _last_retention_stats = dict(stats)
    _last_retention_ts = time.monotonic()

    # Track DB size for growth rate
    geom = stats.get("db_geometry", {})
    if geom.get("db_size_mb"):
        _db_size_history.append((time.monotonic(), geom["db_size_mb"]))
        # Trim to last N entries
        if len(_db_size_history) > _DB_HISTORY_MAX:
            _db_size_history[:] = _db_size_history[-_DB_HISTORY_MAX:]


def _check_consumer_health() -> dict:
    """WS connection stable, no sustained drops."""
    try:
        from . import platform_health
        snap = platform_health.get_health_snapshot()
    except Exception as e:
        return {"name": "consumer_health", "status": "WARN",
                "detail": f"platform_health unavailable: {e}"}

    reasons = []

    state = snap.get("health_state", "unknown")
    if state == "warming_up":
        windows = snap.get("windows_seen", 0)
        return {"name": "consumer_health", "status": "WARN",
                "detail": f"warming_up ({windows}/{BAKE_MIN_WINDOWS} windows)"}
    if state == "degraded":
        return {"name": "consumer_health", "status": "FAIL",
                "detail": f"degraded, reasons={snap.get('gate_reasons', [])}"}

    # Check lag
    lag = snap.get("stream_lag_s", 0)
    if lag > BAKE_MAX_LAG_S:
        reasons.append(f"lag={lag:.0f}s (>{BAKE_MAX_LAG_S}s)")

    # Check drop fraction
    drop_frac = snap.get("drop_frac", 0)
    if drop_frac > BAKE_MAX_DROP_FRAC:
        reasons.append(f"drop_frac={drop_frac:.4f} (>{BAKE_MAX_DROP_FRAC})")

    # Check reconnects (rough: total count / uptime hours)
    windows_seen = snap.get("windows_seen", 0)
    reconnects = snap.get("reconnect_count", 0)
    uptime_hours = max(windows_seen / 60, 0.1)  # ~1 window/min
    reconnect_rate = reconnects / uptime_hours
    if reconnect_rate > BAKE_MAX_RECONNECTS_PER_HOUR:
        reasons.append(f"reconnects={reconnect_rate:.1f}/hr (>{BAKE_MAX_RECONNECTS_PER_HOUR}/hr)")

    # Check windows seen (minimum bake time)
    if windows_seen < BAKE_MIN_WINDOWS:
        reasons.append(f"windows={windows_seen}/{BAKE_MIN_WINDOWS} (too young)")

    if reasons:
        return {"name": "consumer_health", "status": "FAIL",
                "detail": "; ".join(reasons)}
    return {"name": "consumer_health", "status": "PASS",
            "detail": f"ok (lag={lag:.0f}s, drop_frac={drop_frac:.4f}, "
                       f"reconnects={reconnect_rate:.1f}/hr, windows={windows_seen})"}


def _check_retention_health() -> dict:
    """Retention loop completing in bounded time, no errors."""
    if not _last_retention_stats:
        return {"name": "retention_health", "status": "WARN",
                "detail": "no retention pass recorded yet"}

    reasons = []
    stats = _last_retention_stats

    # Check for failures (-1 means exception)
    for key in ("raw_stripped", "events_pruned", "edges_pruned", "claims_archived"):
        if stats.get(key, 0) == -1:
            reasons.append(f"{key} failed")

    # Check staleness (retention should run hourly)
    age_s = time.monotonic() - _last_retention_ts
    if age_s > 7200:  # 2 hours — stale
        reasons.append(f"last pass {age_s/3600:.1f}h ago (stale)")

    # Check WAL checkpoint — only flag if most pages are blocked,
    # not just a few from the continuous write stream.
    ckpt = stats.get("wal_checkpoint", {})
    ckpt_log = ckpt.get("log", 0)
    ckpt_busy = ckpt.get("busy", 0)
    if ckpt_log > 0 and ckpt_busy / ckpt_log > 0.9:
        reasons.append(f"checkpoint mostly blocked (busy={ckpt_busy}/{ckpt_log})")

    if reasons:
        return {"name": "retention_health", "status": "FAIL",
                "detail": "; ".join(reasons)}
    return {"name": "retention_health", "status": "PASS",
            "detail": f"ok (age={age_s/60:.0f}m, "
                       f"stripped={stats.get('raw_stripped', 0)}, "
                       f"pruned={stats.get('events_pruned', 0)})"}


def _check_db_growth() -> dict:
    """DB file size trend is bounded (not runaway growth)."""
    if len(_db_size_history) < 2:
        return {"name": "db_growth", "status": "WARN",
                "detail": "insufficient data (need 2+ retention passes)"}

    oldest_ts, oldest_mb = _db_size_history[0]
    newest_ts, newest_mb = _db_size_history[-1]
    elapsed_hours = max((newest_ts - oldest_ts) / 3600, 0.01)
    growth_mb_hr = (newest_mb - oldest_mb) / elapsed_hours

    geom = _last_retention_stats.get("db_geometry", {})
    freelist_pct = geom.get("freelist_pct", 0)

    detail = (f"growth={growth_mb_hr:+.0f}MB/hr over {elapsed_hours:.1f}h, "
              f"size={newest_mb:.0f}MB, freelist={freelist_pct:.1f}%")

    if growth_mb_hr > BAKE_MAX_DB_GROWTH_MB_PER_HOUR:
        return {"name": "db_growth", "status": "FAIL", "detail": detail}
    if growth_mb_hr > BAKE_MAX_DB_GROWTH_MB_PER_HOUR / 2:
        return {"name": "db_growth", "status": "WARN", "detail": detail}
    return {"name": "db_growth", "status": "PASS", "detail": detail}


def _check_baseline_quality() -> dict:
    """EWMA baselines have enough data and aren't oscillating."""
    try:
        from . import platform_health
        snap = platform_health.get_health_snapshot()
    except Exception as e:
        return {"name": "baseline_quality", "status": "WARN",
                "detail": f"unavailable: {e}"}

    windows = snap.get("windows_seen", 0)
    baseline_eps = snap.get("baseline_eps", 0)
    current_eps = snap.get("current_eps", 0)
    coverage = snap.get("coverage_pct", 0)

    reasons = []

    if windows < BAKE_MIN_WINDOWS:
        reasons.append(f"too few windows ({windows}/{BAKE_MIN_WINDOWS})")

    if baseline_eps < 1.0:
        reasons.append(f"baseline_eps={baseline_eps:.1f} (suspiciously low)")

    # Coverage should be stable and near 1.0
    if 0 < coverage < 0.7:
        reasons.append(f"coverage={coverage:.1%} (baselines learning from partial data)")

    recal = snap.get("recalibration_remaining", 0)
    if recal > 0:
        reasons.append(f"recalibrating ({recal} windows remaining)")

    if reasons:
        status = "FAIL" if any("too few" in r or "suspiciously" in r for r in reasons) else "WARN"
        return {"name": "baseline_quality", "status": status,
                "detail": "; ".join(reasons)}
    return {"name": "baseline_quality", "status": "PASS",
            "detail": f"ok (windows={windows}, baseline_eps={baseline_eps:.1f}, "
                       f"coverage={coverage:.1%})"}


def bake_check() -> dict:
    """Run all bake gate checks. Returns structured verdict.

    Returns:
        {
            "verdict": "OK" | "DEGRADED" | "BAD",
            "checks": [{"name": ..., "status": "PASS"|"WARN"|"FAIL", "detail": ...}, ...]
        }
    """
    checks = [
        _check_consumer_health(),
        _check_retention_health(),
        _check_db_growth(),
        _check_baseline_quality(),
    ]

    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        verdict = "BAD"
    elif "WARN" in statuses:
        verdict = "DEGRADED"
    else:
        verdict = "OK"

    return {"verdict": verdict, "checks": checks}
