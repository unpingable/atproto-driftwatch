import logging
import os
import datetime
from .db import get_conn
from . import timeutil

LOG = logging.getLogger("labeler.expiry")

DEFAULT_TTL_DAYS = int(os.getenv("LABEL_TTL_DAYS", "30"))


def expire_labels_by_ttl(ttl_days: int = None, conn=None) -> dict:
    """Mark labels older than TTL as expired. Returns stats dict."""
    ttl_days = ttl_days or DEFAULT_TTL_DAYS
    cutoff = (timeutil.now_utc() - datetime.timedelta(days=ttl_days)).isoformat()
    now_iso = timeutil.now_utc().isoformat()

    own_conn = conn is None
    if own_conn:
        conn = get_conn()

    # Count before
    eligible = conn.execute(
        "SELECT COUNT(*) FROM labels WHERE ctime <= ? AND expired_at IS NULL",
        (cutoff,),
    ).fetchone()[0]

    already_expired = conn.execute(
        "SELECT COUNT(*) FROM labels WHERE expired_at IS NOT NULL",
    ).fetchone()[0]

    # Expire
    cur = conn.execute(
        "UPDATE labels SET expired_at = ? WHERE ctime <= ? AND expired_at IS NULL",
        (now_iso, cutoff),
    )
    try:
        expired_this_run = cur.rowcount
    except Exception:
        expired_this_run = eligible

    conn.commit()
    if own_conn:
        conn.close()

    stats = {
        "ttl_days": ttl_days,
        "cutoff": cutoff,
        "expired_this_run": expired_this_run,
        "already_expired": already_expired,
        "eligible": eligible,
    }

    if expired_this_run > 0:
        LOG.info("label expiry: expired=%d already_expired=%d ttl=%dd cutoff=%s",
                 expired_this_run, already_expired, ttl_days, cutoff)
    else:
        LOG.debug("label expiry: nothing to expire (already_expired=%d ttl=%dd)",
                  already_expired, ttl_days)

    return stats
