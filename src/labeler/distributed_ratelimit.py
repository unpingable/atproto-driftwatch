import asyncio
from typing import Optional
import logging

from . import metrics

LOG = logging.getLogger("labeler.distributed_ratelimit")


class RedisTokenBucket:
    """Simple Redis-backed token bucket using a Lua script for atomic check-and-consume.

    The Lua script returns 1 when a token was consumed successfully, otherwise it
    returns an integer > 0 indicating milliseconds until next token is available.

    This class exposes async `acquire()` which will wait until a token is available.
    """

    LUA_SCRIPT = r"""
    -- KEYS[1] = key
    -- ARGV[1] = rate
    -- ARGV[2] = per
    -- ARGV[3] = capacity
    local key = KEYS[1]
    local rate = tonumber(ARGV[1])
    local per = tonumber(ARGV[2])
    local capacity = tonumber(ARGV[3])

    local now = redis.call('TIME')
    local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

    local data = redis.call('HMGET', key, 'tokens', 'last')
    local tokens = tonumber(data[1]) or capacity
    local last = tonumber(data[2]) or now_ms

    local refill = ((now_ms - last) / 1000.0) * rate
    tokens = math.min(capacity, tokens + refill)

    if tokens >= 1.0 then
        tokens = tokens - 1.0
        redis.call('HMSET', key, 'tokens', tostring(tokens), 'last', tostring(now_ms))
        redis.call('PEXPIRE', key, math.ceil(per*1000*2))
        return 1
    else
        local needed = 1.0 - tokens
        local secs = needed / rate
        local ms = math.floor(secs * 1000)
        return ms
    end
    """

    def __init__(self, redis_client, key: str, rate: float, per: float, capacity: Optional[float] = None):
        self.redis = redis_client
        self.key = key
        self.rate = float(rate)
        self.per = float(per)
        self.capacity = float(capacity if capacity is not None else rate)
        self._script = None

    async def _ensure_script(self):
        if self._script is None:
            # register the script and store the SHA
            try:
                self._script = await self.redis.script_load(self.LUA_SCRIPT)
            except Exception:
                # fallback: we will use EVAL with script text
                self._script = None

    async def _try_consume(self) -> (bool, Optional[int]):
        """Try to consume a token. Returns (True, None) if success; (False, ms_until_token) otherwise."""
        await self._ensure_script()
        try:
            if self._script:
                res = await self.redis.evalsha(self._script, 1, self.key, str(self.rate), str(self.per), str(self.capacity))
            else:
                res = await self.redis.eval(self.LUA_SCRIPT, 1, self.key, str(self.rate), str(self.per), str(self.capacity))
            # Lua returns 1 on success, or ms until token when not
            if isinstance(res, (int, float)):
                if int(res) == 1:
                    return True, None
                return False, int(res)
            # otherwise treat as failure
            return False, None
        except Exception as e:
            LOG.exception("redis eval failed: %s", e)
            raise

    async def acquire(self):
        # Try to consume; if not available, sleep for the suggested time (ms) then retry
        while True:
                ok, ms = await self._try_consume()
            if ok:
                return
            # record that distributed limiter returned a wait (no token available)
            if ms is not None:
                try:
                    metrics.LABEL_QUERY_DISTRIBUTED_RATE_LIMITED.inc()
                except Exception:
                    pass

            # on None ms we back off a small amount to avoid busy loop
            wait = (ms / 1000.0) if ms is not None else min(self.per, 0.5)
            await asyncio.sleep(wait)
