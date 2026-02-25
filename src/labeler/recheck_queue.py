import os
import time
from typing import List
from . import timeutil
from . import metrics

REDIS_URL = os.getenv("REDIS_URL")


class LocalFallbackQueue:
    def __init__(self, conn):
        self.conn = conn

    def enqueue(self, root_uri: str):
        now = timeutil.now_utc().isoformat()
        # upsert in DB table (portable across sqlite/duckdb)
        cur = self.conn.execute(
            "SELECT 1 FROM recheck_requests WHERE root_uri = ?",
            (root_uri,),
        ).fetchall()
        if cur:
            self.conn.execute(
                "UPDATE recheck_requests SET scheduled_at = ? WHERE root_uri = ?",
                (now, root_uri),
            )
        else:
            self.conn.execute(
                "INSERT INTO recheck_requests VALUES (?, ?)",
                (root_uri, now),
            )
        self.conn.commit()
        try:
            rows = self.conn.execute("SELECT COUNT(*) FROM recheck_requests").fetchall()
            metrics.RECHECK_QUEUE_DEPTH.set(rows[0][0] if rows else 0)
        except Exception:
            pass

    def dequeue(self, limit: int = 100) -> List[str]:
        rows = self.conn.execute("SELECT root_uri FROM recheck_requests ORDER BY scheduled_at ASC LIMIT ?", (limit,)).fetchall()
        roots = [r[0] for r in rows]
        for r in roots:
            self.conn.execute("DELETE FROM recheck_requests WHERE root_uri = ?", (r,))
        self.conn.commit()
        try:
            rows = self.conn.execute("SELECT COUNT(*) FROM recheck_requests").fetchall()
            metrics.RECHECK_QUEUE_DEPTH.set(rows[0][0] if rows else 0)
        except Exception:
            pass
        return roots


class RedisQueue:
    def __init__(self):
        import redis
        self.r = redis.Redis.from_url(REDIS_URL)
        self.key = "recheck:queue"

    def enqueue(self, root_uri: str):
        # use sorted set with timestamp score
        self.r.zadd(self.key, {root_uri: time.time()})
        try:
            metrics.RECHECK_QUEUE_DEPTH.set(self.r.zcard(self.key))
        except Exception:
            pass

    def dequeue(self, limit: int = 100) -> List[str]:
        # attempt to pop up to `limit` smallest-score members using ZPOPMIN if available
        try:
            items = self.r.zpopmin(self.key, limit)
            roots = [m.decode() if isinstance(m, bytes) else m for m, _ in items]
            try:
                metrics.RECHECK_QUEUE_DEPTH.set(self.r.zcard(self.key))
            except Exception:
                pass
            return roots
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
