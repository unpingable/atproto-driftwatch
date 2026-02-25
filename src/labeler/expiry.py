import os
import datetime
from .db import get_conn
from . import timeutil

DEFAULT_TTL_DAYS = int(os.getenv("LABEL_TTL_DAYS", "30"))


def expire_labels_by_ttl(ttl_days: int = None):
    ttl_days = ttl_days or DEFAULT_TTL_DAYS
    cutoff = (timeutil.now_utc() - datetime.timedelta(days=ttl_days)).isoformat()
    conn = get_conn()
    # Mark labels older than cutoff and not yet expired
    conn.execute(
        "UPDATE labels SET expired_at = ? WHERE ctime <= ? AND expired_at IS NULL",
        (timeutil.now_utc().isoformat(), cutoff),
    )
    conn.commit()
    conn.close()
