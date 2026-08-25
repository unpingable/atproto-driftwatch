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
# Hysteresis: once engaged, the brake does not release until usage falls back
# below this. Without a separate release threshold a volume hovering on the
# critical line flaps the brake on/off every sample.
DISK_RELEASE_THRESHOLD = float(os.getenv("DISK_RELEASE_THRESHOLD", "0.85"))
# Absolute floor, independent of percentage. On a 196G volume 8% is ~15G, which
# is far more runway than a nearly-full small volume gets at the same percent.
# Whichever of (percent, absolute) trips first wins.
DISK_MIN_FREE_BYTES = int(os.getenv("DISK_MIN_FREE_BYTES", str(2 * 1024 ** 3)))

# How long a disk sample is reused. ``is_disk_pressure()`` is called from the
# consumer's ingest path, so it must not issue a statvfs per event.
DISK_SAMPLE_TTL_SEC = float(os.getenv("DISK_SAMPLE_TTL_SEC", "5.0"))

# ---------------------------------------------------------------------------
# Emergency brake state.
#
# 2026-08-25 incident repair. The brake used to be a file (``.disk_pressure``)
# touched by ``run_maintenance_once``. That had two independent defects, both
# of which were live during the 2026-08-12 volume exhaustion:
#
#   1. ``run_maintenance_once`` only runs when ENABLE_MAINTENANCE is truthy.
#      Production ran with ENABLE_MAINTENANCE=false, so the only code path that
#      could arm the brake never executed. ``check_disk_pressure()`` is called
#      inline by /health/extended and reported ``level: critical`` correctly the
#      whole time — reporting worked, acting did not.
#   2. The flag lived in DATA_DIR — the very volume whose exhaustion it exists
#      to signal. Arming the emergency brake required a successful write to the
#      full filesystem.
#
# The brake is now in-process state derived from the same sample that feeds
# reporting, so it cannot drift from what health reports and cannot be disabled
# by a maintenance-loop toggle. Failure semantics: state is per-process and
# resets to "not engaged" on restart; the first sample after start re-derives it
# from actual disk usage (at most DISK_SAMPLE_TTL_SEC later), so a restart
# cannot strand the brake in either position.
# ---------------------------------------------------------------------------
_BRAKE = {"engaged": False, "since": None, "reason": None}
_LAST_SAMPLE = {"at": 0.0, "result": None}


def classify_disk_pressure(used_frac: float, free_bytes: int) -> str:
    """Pure: map (used fraction, absolute free bytes) to ok/warn/critical.

    Absolute and percentage limits are both honoured; the more severe wins.
    """
    if free_bytes <= 0:
        return "critical"
    if used_frac >= DISK_CRITICAL_THRESHOLD or free_bytes < DISK_MIN_FREE_BYTES:
        return "critical"
    if used_frac >= DISK_WARN_THRESHOLD:
        return "warn"
    return "ok"


def evaluate_brake(level: str, used_frac: float, engaged: bool) -> bool:
    """Pure: given current level and prior brake state, return new brake state.

    Engages on critical. Releases only once usage has fallen back below
    DISK_RELEASE_THRESHOLD — not merely below critical. ``unknown`` (the disk
    check itself failed) is deliberately non-committal: it neither engages nor
    releases, so a transient statvfs error cannot drop an active brake.
    """
    if level == "unknown":
        return engaged
    if level == "critical":
        return True
    if engaged:
        return used_frac >= DISK_RELEASE_THRESHOLD
    return False


def _sample_disk() -> dict:
    """Take one disk sample and fold it into brake state. Never raises."""
    try:
        usage = shutil.disk_usage(DATA_DIR)
        used_frac = (usage.total - usage.free) / usage.total if usage.total else 0
        level = classify_disk_pressure(used_frac, usage.free)
        result = {
            "level": level,
            "used_pct": round(used_frac * 100, 1),
            "free_gb": round(usage.free / (1024 ** 3), 1),
            "total_gb": round(usage.total / (1024 ** 3), 1),
            "free_bytes": usage.free,
        }
    except Exception as e:
        LOG.warning("disk check failed: %s", e)
        used_frac = 0.0
        level = "unknown"
        result = {"level": level, "used_pct": 0, "free_gb": 0, "total_gb": 0,
                  "free_bytes": 0}

    was = _BRAKE["engaged"]
    now_engaged = evaluate_brake(level, used_frac, was)
    if now_engaged and not was:
        _BRAKE.update(engaged=True, since=time.time(),
                      reason=f"disk_{level}:used_pct={result['used_pct']}")
        LOG.error("DISK PRESSURE: emergency brake ENGAGED (%.1f%% used, %.1f GB free)",
                  result["used_pct"], result["free_gb"])
    elif was and not now_engaged:
        _BRAKE.update(engaged=False, since=None, reason=None)
        LOG.warning("disk pressure cleared, emergency brake RELEASED (%.1f%% used)",
                    result["used_pct"])
    result["emergency_brake"] = _BRAKE["engaged"]
    result["brake_reason"] = _BRAKE["reason"]
    return result


def check_disk_pressure() -> dict:
    """Check disk usage for the data directory partition.

    Returns usage stats plus pressure level (ok/warn/critical/unknown) and the
    current brake state. Always takes a fresh sample; callers on a hot path
    should use ``is_disk_pressure()`` instead, which is TTL-cached.
    """
    result = _sample_disk()
    _LAST_SAMPLE.update(at=time.time(), result=result)
    return result


def is_disk_pressure() -> bool:
    """Is the emergency brake engaged? Called by the consumer's ingest path.

    Self-sampling with a short TTL, so the brake is live regardless of whether
    the optional maintenance loop is enabled.
    """
    now = time.time()
    if _LAST_SAMPLE["result"] is None or (now - _LAST_SAMPLE["at"]) > DISK_SAMPLE_TTL_SEC:
        _LAST_SAMPLE.update(at=now, result=_sample_disk())
    return bool(_BRAKE["engaged"])


def brake_state() -> dict:
    """Introspection for /health/extended."""
    return dict(_BRAKE)


def _reset_brake_for_test():
    """Test-only: clear in-process brake and sample cache."""
    _BRAKE.update(engaged=False, since=None, reason=None)
    _LAST_SAMPLE.update(at=0.0, result=None)


# ---------------------------------------------------------------------------
# SQLite space reclamation.
# ---------------------------------------------------------------------------

AUTO_VACUUM_NONE = 0
AUTO_VACUUM_FULL = 1
AUTO_VACUUM_INCREMENTAL = 2

# Fraction of live bytes a rebuild needs on the destination filesystem.
# VACUUM INTO writes a fresh copy of the live pages; the margin covers page
# overhead, the destination journal, and not landing at exactly 100% again.
REBUILD_MARGIN = float(os.getenv("REBUILD_MARGIN", "1.2"))

# Explicitly qualified recovery workspaces. Deliberately NOT defaulted to "/":
# free bytes somewhere is not qualified recovery workspace. An operator names
# real, attached, writable scratch here; anything else counts as zero.
RECOVERY_WORKSPACE_PATHS = [
    p for p in os.getenv("RECOVERY_WORKSPACE_PATHS", "").split(os.pathsep) if p
]


def db_geometry(conn) -> dict:
    """Page geometry of a SQLite connection, including derived live bytes."""
    ps = conn.execute("PRAGMA page_size").fetchone()[0]
    pc = conn.execute("PRAGMA page_count").fetchone()[0]
    fl = conn.execute("PRAGMA freelist_count").fetchone()[0]
    av = conn.execute("PRAGMA auto_vacuum").fetchone()[0]
    return {
        "page_size": ps,
        "page_count": pc,
        "freelist_count": fl,
        "auto_vacuum": av,
        "total_bytes": ps * pc,
        "freelist_bytes": ps * fl,
        "live_bytes": ps * (pc - fl),
        "freelist_pct": round(100.0 * fl / pc, 2) if pc else 0.0,
    }


def incremental_vacuum_chunk(conn, pages: int = 1000) -> dict:
    """Reclaim up to ``pages`` freelist pages, returning a typed status.

    Correct invocation matters, and is the whole reason this helper exists.
    ``conn.execute("PRAGMA incremental_vacuum(N)")`` reclaims exactly ONE page
    regardless of N: Python's sqlite3 runs the pragma inside its implicit
    transaction and steps the statement once. Adding ``.fetchall()`` does not
    help. Only ``executescript()`` (which commits first and runs the statement
    outside that management) reclaims the requested N.

    Measured 2026-08-25 on the deployed runtimes, freelist 15036, N=1000:
        conn.execute(...)              -> 1 page
        conn.execute(...).fetchall()   -> 1 page
        conn.executescript(...)        -> 1000 pages
        sqlite3 CLI                    -> 1000 pages
    Identical on SQLite 3.46.1 (container) and 3.37.2 (host).

    Status is one of:
      ``ok``                 — ran, ``reclaimed_pages`` says how many
      ``noop``               — mode 2 but the freelist was already empty
      ``mode_incompatible``  — auto_vacuum != INCREMENTAL, so nothing can ever
                               be reclaimed in place. This is a structural
                               fault, not an idle pass.
      ``failed``             — the pragma raised
    """
    try:
        geo_before = db_geometry(conn)
    except Exception as e:
        return {"status": "failed", "error": str(e)}

    mode = geo_before["auto_vacuum"]
    if mode != AUTO_VACUUM_INCREMENTAL:
        return {
            "status": "mode_incompatible",
            "auto_vacuum": mode,
            "freelist_count": geo_before["freelist_count"],
            "freelist_bytes": geo_before["freelist_bytes"],
            "detail": (
                "auto_vacuum=%d; in-place reclamation is impossible until the "
                "database is rebuilt as INCREMENTAL" % mode
            ),
        }

    if geo_before["freelist_count"] == 0:
        return {"status": "noop", "auto_vacuum": mode, "reclaimed_pages": 0}

    try:
        conn.executescript("PRAGMA incremental_vacuum(%d);" % int(pages))
    except Exception as e:
        LOG.exception("incremental_vacuum failed")
        return {"status": "failed", "auto_vacuum": mode, "error": str(e)}

    geo_after = db_geometry(conn)
    reclaimed = geo_before["freelist_count"] - geo_after["freelist_count"]
    if reclaimed > 0:
        LOG.info("incremental_vacuum: reclaimed %d pages (freelist %d -> %d)",
                 reclaimed, geo_before["freelist_count"], geo_after["freelist_count"])
    return {
        "status": "ok",
        "auto_vacuum": mode,
        "freelist_before": geo_before["freelist_count"],
        "freelist_after": geo_after["freelist_count"],
        "reclaimed_pages": reclaimed,
        "reclaimed_bytes": reclaimed * geo_before["page_size"],
    }


# Oldest timestamp we will believe from record-supplied data. events.ctime is
# derived from the record's own createdAt, which any account can set to any
# value; a single hostile row otherwise dominates MIN(ctime). Matches the floor
# already used by claim_history_stats.
CTIME_PLAUSIBLE_FLOOR = os.getenv("CTIME_PLAUSIBLE_FLOOR", "2020-01-01")


def compute_retention_lag(oldest_ctime, retention_sec: int, now=None):
    """How far past the retention cutoff the oldest *believable* row is.

    Returns None when there is nothing to measure. Positive means retention is
    behind by that many seconds; <= 0 means retention is at or inside its window.

    2026-08-25: production reported retention_lag_s = 371,522,939 (~11.8 years)
    because MIN(ctime) on the events table was a garbage 2002-04-01 timestamp.
    Callers must pass an already-floored value (see CTIME_PLAUSIBLE_FLOOR); this
    helper additionally refuses anything below the floor so a bad query cannot
    reintroduce epoch-scale nonsense.
    """
    if not oldest_ctime:
        return None
    from datetime import datetime, timezone
    try:
        oldest = datetime.fromisoformat(str(oldest_ctime).replace("Z", "+00:00"))
    except Exception:
        return None
    if oldest.tzinfo is None:
        oldest = oldest.replace(tzinfo=timezone.utc)
    try:
        floor = datetime.fromisoformat(CTIME_PLAUSIBLE_FLOOR).replace(tzinfo=timezone.utc)
    except Exception:
        floor = datetime(2020, 1, 1, tzinfo=timezone.utc)
    if oldest < floor:
        return None
    now_dt = now or datetime.now(timezone.utc)
    return (now_dt - oldest).total_seconds() - retention_sec


def rebuild_required_bytes(live_bytes: int, margin: float = None) -> int:
    """Bytes of scratch a VACUUM INTO rebuild of this DB needs."""
    m = REBUILD_MARGIN if margin is None else margin
    return int(live_bytes * m)


def qualified_recovery_workspace(paths=None, db_dir=None) -> dict:
    """Largest free space among *qualified* recovery workspaces.

    A path qualifies only if it exists, is a directory, is writable, and is on
    a different filesystem from the database. Space on the DB's own volume is
    not recovery workspace — you cannot rebuild a full volume onto itself.

    Unattached cloud volumes and unmounted NFS are, correctly, invisible here:
    they are not workspace until someone attaches them. That acquisition step
    has no representation in this codebase and is the documented missing seam.
    """
    candidates = RECOVERY_WORKSPACE_PATHS if paths is None else paths
    data_dir = DATA_DIR if db_dir is None else db_dir
    try:
        db_dev = os.stat(data_dir).st_dev
    except Exception:
        db_dev = None

    best = 0
    best_path = None
    considered = []
    for p in candidates:
        entry = {"path": p, "qualified": False, "free_bytes": 0, "reason": None}
        try:
            if not os.path.isdir(p):
                entry["reason"] = "missing_or_not_a_directory"
            elif not os.access(p, os.W_OK):
                entry["reason"] = "not_writable"
            elif db_dev is not None and os.stat(p).st_dev == db_dev:
                entry["reason"] = "same_filesystem_as_database"
            else:
                free = shutil.disk_usage(p).free
                entry.update(qualified=True, free_bytes=free)
                if free > best:
                    best, best_path = free, p
        except Exception as e:
            entry["reason"] = "error:%s" % e
        considered.append(entry)

    return {
        "workspace_bytes": best,
        "workspace_path": best_path,
        "candidates": considered,
        "configured": bool(candidates),
    }


def recovery_capacity_state(conn, paths=None) -> dict:
    """Is there enough qualified scratch to rebuild this database?

    This is a *level*, not a derivative: unlike disk-runway it does not go
    silent when the volume saturates. It goes red as soon as live data outgrows
    available workspace, which happens long before the volume fills.
    """
    geo = db_geometry(conn)
    required = rebuild_required_bytes(geo["live_bytes"])
    ws = qualified_recovery_workspace(paths=paths)
    have = ws["workspace_bytes"]
    ok = have >= required
    return {
        "ok": ok,
        "live_bytes": geo["live_bytes"],
        "required_bytes": required,
        "margin": REBUILD_MARGIN,
        "workspace_bytes": have,
        "workspace_path": ws["workspace_path"],
        "workspace_configured": ws["configured"],
        "deficit_bytes": max(0, required - have),
        "candidates": ws["candidates"],
        "reason": None if ok else (
            "no_qualified_recovery_workspace_configured"
            if not ws["configured"] else "insufficient_recovery_workspace"
        ),
    }


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

    # 2. Disk pressure check. The brake is folded in by check_disk_pressure()
    # itself — this loop is no longer the thing that arms it (see the brake
    # state comment above; ENABLE_MAINTENANCE=false must not disable the brake).
    disk = check_disk_pressure()
    results["disk"] = disk
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
