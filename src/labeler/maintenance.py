"""Periodic maintenance loop — label expiry, disk monitoring, growth stats.

Runs on a configurable interval (default: 6 hours). Gated by ENABLE_MAINTENANCE env var.
"""

import asyncio
import logging
import os
import shutil
import time

from . import timeutil
from .db import get_conn, DATA_DIR

LOG = logging.getLogger("labeler.maintenance")

DEFAULT_INTERVAL_SEC = int(os.getenv("MAINTENANCE_INTERVAL_SEC", str(6 * 3600)))  # 6h

# Disk pressure thresholds (fraction of total disk used)
DISK_WARN_THRESHOLD = float(os.getenv("DISK_WARN_THRESHOLD", "0.85"))
DISK_CRITICAL_THRESHOLD = float(os.getenv("DISK_CRITICAL_THRESHOLD", "0.92"))

# Emergency brake: file touched when disk is critical → consumer checks this
DISK_PRESSURE_FLAG = DATA_DIR / ".disk_pressure"


def check_disk_pressure() -> dict:
    """Check disk usage for the data directory partition.

    Returns dict with usage stats and pressure level: ok/warn/critical.
    """
    try:
        usage = shutil.disk_usage(DATA_DIR)
        used_frac = (usage.total - usage.free) / usage.total if usage.total else 0
        free_gb = usage.free / (1024 ** 3)

        if used_frac >= DISK_CRITICAL_THRESHOLD:
            level = "critical"
        elif used_frac >= DISK_WARN_THRESHOLD:
            level = "warn"
        else:
            level = "ok"

        return {
            "level": level,
            "used_pct": round(used_frac * 100, 1),
            "free_gb": round(free_gb, 1),
            "total_gb": round(usage.total / (1024 ** 3), 1),
        }
    except Exception as e:
        LOG.warning("disk check failed: %s", e)
        return {"level": "unknown", "used_pct": 0, "free_gb": 0, "total_gb": 0}


def _set_disk_pressure_flag(active: bool):
    """Touch or remove the disk pressure flag file."""
    if active:
        DISK_PRESSURE_FLAG.touch()
        LOG.warning("DISK PRESSURE: emergency brake activated at %s", DISK_PRESSURE_FLAG)
    else:
        if DISK_PRESSURE_FLAG.exists():
            DISK_PRESSURE_FLAG.unlink()
            LOG.info("disk pressure cleared, emergency brake released")


def is_disk_pressure() -> bool:
    """Check if the emergency brake is engaged. Called by consumer."""
    return DISK_PRESSURE_FLAG.exists()


def claim_history_stats(conn=None) -> dict:
    """Measure claim_history table: row count, date range, estimated size.

    Also checks for garbage timestamps (year < 2020 or year > now+1).
    """
    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    try:
        row = conn.execute("SELECT COUNT(*) FROM claim_history").fetchone()
        total_rows = row[0] if row else 0

        range_row = conn.execute(
            "SELECT MIN(createdAt), MAX(createdAt) FROM claim_history"
        ).fetchone()
        min_ts = range_row[0] if range_row else None
        max_ts = range_row[1] if range_row else None

        # Check for garbage timestamps
        now_year = timeutil.now_utc().year
        garbage_row = conn.execute(
            "SELECT COUNT(*) FROM claim_history "
            "WHERE createdAt < '2020-01-01' OR createdAt > ?",
            (f"{now_year + 1}-01-01",),
        ).fetchone()
        garbage_count = garbage_row[0] if garbage_row else 0

        # Recent 24h count for growth rate estimation
        cutoff_24h = (timeutil.now_utc() - __import__('datetime').timedelta(hours=24)).isoformat()
        recent_row = conn.execute(
            "SELECT COUNT(*) FROM claim_history WHERE createdAt >= ?",
            (cutoff_24h,),
        ).fetchone()
        rows_24h = recent_row[0] if recent_row else 0

    finally:
        if own_conn:
            conn.close()

    # DB file size
    db_path = DATA_DIR / "labeler.sqlite"
    db_size_mb = db_path.stat().st_size / (1024 * 1024) if db_path.exists() else 0

    stats = {
        "total_rows": total_rows,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "garbage_timestamps": garbage_count,
        "rows_24h": rows_24h,
        "db_size_mb": round(db_size_mb, 1),
    }

    if garbage_count > 0:
        LOG.warning("claim_history has %d garbage timestamps (outside 2020..%d)",
                    garbage_count, now_year)

    return stats


def run_maintenance_once(conn=None) -> dict:
    """Run all maintenance tasks once. Returns combined stats."""
    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    results = {}

    # 1. Label expiry
    try:
        from .expiry import expire_labels_by_ttl
        results["label_expiry"] = expire_labels_by_ttl(conn=conn)
    except Exception:
        LOG.exception("label expiry failed")
        results["label_expiry"] = {"error": True}

    # 2. Disk pressure check
    disk = check_disk_pressure()
    results["disk"] = disk
    _set_disk_pressure_flag(disk["level"] == "critical")
    if disk["level"] == "warn":
        LOG.warning("DISK WARN: %.1f%% used (%.1f GB free of %.1f GB)",
                    disk["used_pct"], disk["free_gb"], disk["total_gb"])
    elif disk["level"] == "critical":
        LOG.error("DISK CRITICAL: %.1f%% used (%.1f GB free) — ingest paused via emergency brake",
                  disk["used_pct"], disk["free_gb"])

    # 3. claim_history growth stats
    try:
        results["claim_history"] = claim_history_stats(conn=conn)
        ch = results["claim_history"]
        LOG.info("claim_history: %d rows (24h=%d), db=%.1fMB, range=[%s..%s], garbage=%d",
                 ch["total_rows"], ch["rows_24h"], ch["db_size_mb"],
                 ch.get("min_ts", "?"), ch.get("max_ts", "?"), ch["garbage_timestamps"])
    except Exception:
        LOG.exception("claim_history stats failed")
        results["claim_history"] = {"error": True}

    if own_conn:
        conn.close()

    return results


async def run_periodic():
    """Async maintenance loop."""
    interval = DEFAULT_INTERVAL_SEC
    LOG.info("maintenance loop started, interval=%ds", interval)

    loop = asyncio.get_event_loop()
    while True:
        try:
            results = await loop.run_in_executor(None, run_maintenance_once)
            LOG.info("maintenance pass complete: disk=%s label_expiry=%d",
                     results.get("disk", {}).get("level", "?"),
                     results.get("label_expiry", {}).get("expired_this_run", 0))
        except Exception:
            LOG.exception("maintenance pass failed")
        await asyncio.sleep(interval)
