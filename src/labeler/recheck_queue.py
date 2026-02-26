import os
import time
from typing import List
from . import timeutil
from . import metrics

REDIS_URL = os.getenv("REDIS_URL")


class LocalFallbackQueue:
    def __init__(self, conn):
        self.conn = conn

    def enqueue(self, claim_fingerprint: str):
        from . import queue_stats
        now = timeutil.now_utc().isoformat()
        # INSERT OR IGNORE — UNIQUE PK on claim_fingerprint provides debounce
        try:
            cur = self.conn.execute(
                "INSERT OR IGNORE INTO recheck_queue (claim_fingerprint, scheduled_at) VALUES (?, ?)",
                (claim_fingerprint, now),
            )
            changed = cur.rowcount if hasattr(cur, 'rowcount') else 1
        except Exception:
            existing = self.conn.execute(
                "SELECT 1 FROM recheck_queue WHERE claim_fingerprint = ?",
                (claim_fingerprint,),
            ).fetchall()
            if not existing:
                self.conn.execute(
                    "INSERT INTO recheck_queue (claim_fingerprint, scheduled_at) VALUES (?, ?)",
                    (claim_fingerprint, now),
                )
                changed = 1
            else:
                changed = 0
        if changed:
            queue_stats.inc("enqueue_inserted")
        else:
            queue_stats.inc("enqueue_ignored")
        self.conn.commit()
        try:
            rows = self.conn.execute("SELECT COUNT(*) FROM recheck_queue").fetchall()
            metrics.RECHECK_QUEUE_DEPTH.set(rows[0][0] if rows else 0)
        except Exception:
            pass

    def dequeue(self, limit: int = 100) -> List[str]:
        rows = self.conn.execute(
            "SELECT claim_fingerprint FROM recheck_queue ORDER BY scheduled_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        fps = [r[0] for r in rows]
        for fp in fps:
            self.conn.execute("DELETE FROM recheck_queue WHERE claim_fingerprint = ?", (fp,))
        self.conn.commit()
        try:
            rows = self.conn.execute("SELECT COUNT(*) FROM recheck_queue").fetchall()
            metrics.RECHECK_QUEUE_DEPTH.set(rows[0][0] if rows else 0)
        except Exception:
            pass
        return fps


class RedisQueue:
    def __init__(self):
        import redis
        self.r = redis.Redis.from_url(REDIS_URL)
        self.key = "recheck:queue"

    def enqueue(self, claim_fingerprint: str):
        # sorted set: duplicate fingerprints naturally deduplicate (zadd updates score)
        self.r.zadd(self.key, {claim_fingerprint: time.time()})
        try:
            metrics.RECHECK_QUEUE_DEPTH.set(self.r.zcard(self.key))
        except Exception:
            pass

    def dequeue(self, limit: int = 100) -> List[str]:
        try:
            items = self.r.zpopmin(self.key, limit)
            fps = [m.decode() if isinstance(m, bytes) else m for m, _ in items]
            try:
                metrics.RECHECK_QUEUE_DEPTH.set(self.r.zcard(self.key))
            except Exception:
                pass
            return fps
        except Exception:
            # fallback: range + remove
            items = self.r.zrange(self.key, 0, limit - 1)
            if not items:
                return []
            items = [it.decode() if isinstance(it, bytes) else it for it in items]
            self.r.zrem(self.key, *items)
            try:
                metrics.RECHECK_QUEUE_DEPTH.set(self.r.zcard(self.key))
            except Exception:
                pass
            return items


def get_queue(conn=None):
    if REDIS_URL:
        try:
            return RedisQueue()
        except Exception:
            pass
    # fallback to DB-backed queue
    if conn is None:
        from .db import get_conn
        conn = get_conn()
    return LocalFallbackQueue(conn)
