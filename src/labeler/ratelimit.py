import time
import asyncio
from typing import Optional


class AsyncRateLimiter:
    """A simple token-bucket async rate limiter.

    rate: tokens per `per` seconds. e.g., rate=5, per=1 -> 5 tokens/sec.
    Acquire will wait until a token is available.
    This implementation is conservative and uses an asyncio.Lock to protect state.
    """

    def __init__(self, rate: float = 5.0, per: float = 1.0):
        assert rate > 0 and per > 0
        self._rate = float(rate)
        self._per = float(per)
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            # refill tokens according to elapsed time
            self._tokens = min(self._rate, self._tokens + elapsed * (self._rate / self._per))
            self._last = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            # need to wait until next token
            need = 1.0 - self._tokens
            wait = need * (self._per / self._rate)

        # sleep outside the lock
        await asyncio.sleep(wait)
        async with self._lock:
            # consume token
            self._tokens = max(0.0, self._tokens - 1.0)
            self._last = time.monotonic()


class AsyncConcurrencyLimiter:
    """Simple concurrency limiter backing onto asyncio.Semaphore.

    Exposes an async context manager interface.
    """

    def __init__(self, limit: int = 5):
        self._sem = asyncio.Semaphore(limit)

    async def __aenter__(self):
        await self._sem.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            self._sem.release()
        except Exception:
            pass


async def maybe_make_distributed(rate: float, per: float, redis_url: str, key: str):
    """Attempt to create a Redis-backed token bucket. Returns None on failure.

    This avoids hard dependency at import-time; caller should fall back to local limiter.
    """
    try:
        import redis.asyncio as redis
    except Exception:
        return None

    try:
        client = redis.from_url(redis_url)
        # Try a lightweight ping
        await client.ping()
        from .distributed_ratelimit import RedisTokenBucket
        return RedisTokenBucket(client, key, rate, per, capacity=rate)
    except Exception:
        return None
