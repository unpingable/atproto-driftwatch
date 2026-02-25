import os
import logging
import asyncio
from typing import Optional

LOG = logging.getLogger("labeler.cooldown")

REDIS_URL = os.getenv("REDIS_URL")
COOLDOWN_PREFIX = os.getenv("COOLDOWN_PREFIX", "labeler:cooldown")
BACKOFF_PREFIX = os.getenv("COOLDOWN_BACKOFF_PREFIX", "labeler:backoff")
BACKOFF_MAX_MULTIPLIER = int(os.getenv("COOLDOWN_BACKOFF_MAX_MULTIPLIER", "6"))
BACKOFF_BASE_SECONDS = int(os.getenv("COOLDOWN_BACKOFF_BASE_SECONDS", "30"))  # base backoff
BACKOFF_RETENTION = int(os.getenv("COOLDOWN_BACKOFF_RETENTION", "86400"))  # retention of backoff counter


async def _get_redis():
    if not REDIS_URL:
        return None
    try:
        import redis.asyncio as redis
        return redis.from_url(REDIS_URL)
    except Exception as e:
        LOG.warning("redis not available for cooldowns: %s", e)
        return None


async def is_in_cooldown(key: str) -> Optional[int]:
    """Return TTL in seconds if in cooldown, otherwise None.

    Returns: ttl seconds remaining (int) or None.
    """
    r = await _get_redis()
    if r is None:
        return None
    try:
        full_key = f"{COOLDOWN_PREFIX}:{key}"
        ttl = await r.ttl(full_key)
        if ttl and ttl > 0:
            return ttl
    except Exception:
        LOG.exception("error checking cooldown")
    return None


async def set_cooldown(key: str, seconds: int):
    """Set a cooldown with exponential backoff tracked per `key`.

    - Increments a backoff counter for the key (stored at BACKOFF_PREFIX:key)
    - Computes multiplier as min(2**count, BACKOFF_MAX_MULTIPLIER), and sets cooldown = max(seconds, BACKOFF_BASE_SECONDS * multiplier)
    - Stores the cooldown key with TTL and sets backoff counter expiry to BACKOFF_RETENTION
    """
    r = await _get_redis()
    if r is None:
        return

    try:
        backoff_key = f"{BACKOFF_PREFIX}:{key}"
        full_key = f"{COOLDOWN_PREFIX}:{key}"
        count = await r.incr(backoff_key)
        await r.expire(backoff_key, BACKOFF_RETENTION)
        multiplier = min(2 ** (count - 1), BACKOFF_MAX_MULTIPLIER)
        cooldown = max(seconds, BACKOFF_BASE_SECONDS * multiplier)
        await r.set(full_key, "1", ex=int(cooldown))
        LOG.info("set cooldown for %s seconds (mult=%s, count=%s)", key, multiplier, count)
    except Exception:
        LOG.exception("error setting cooldown")
        return


def normalize_endpoint(endpoint: str) -> str:
    """Normalize an endpoint URL to a canonical host[:port] key.

    Examples:
      - https://labeler.example/xrpc -> labeler.example
      - https://labeler.example:8080/xrpc -> labeler.example:8080
      - labeler.example -> labeler.example
      - 127.0.0.1:8080/path -> 127.0.0.1:8080
    """
    if not endpoint:
        return endpoint
    try:
        from urllib.parse import urlparse
        p = urlparse(endpoint)
        host = p.netloc or p.path or endpoint
        # strip any trailing path segments
        host = host.split("/")[0]
        # strip leading scheme remnants if present
        return host
    except Exception:
        return endpoint


async def add_labeler_endpoint_mapping(endpoint: str, labeler_did: str):
    """Record that `labeler_did` has been seen via `endpoint` (best-effort).

    Stores labeler_did in Redis set `labeler:endpoint:{normalized_endpoint}`.
    """
    r = await _get_redis()
    if r is None:
        return
    try:
        norm = normalize_endpoint(endpoint)
        key = f"labeler:endpoint:{norm}"
        await r.sadd(key, labeler_did)
        # set a TTL so stale mappings eventually expire
        await r.expire(key, BACKOFF_RETENTION)
    except Exception:
        LOG.exception("error adding labeler->endpoint mapping")


async def list_labeler_dids_for_endpoint(endpoint: str):
    """Return list of labeler DIDs associated with an endpoint (normalized), or empty list."""
    r = await _get_redis()
    if r is None:
        return []
    try:
        norm = normalize_endpoint(endpoint)
        key = f"labeler:endpoint:{norm}"
        vals = await r.smembers(key)
        # redis returns bytes in some libs; convert to str
        return [v.decode() if isinstance(v, (bytes, bytearray)) else v for v in vals]
    except Exception:
        LOG.exception("error listing labeler dids for endpoint")
        return []


async def get_all_mappings():
    """Return a dict mapping normalized endpoints -> list of labeler DIDs."""
    r = await _get_redis()
    if r is None:
        return {}
    try:
        # use scan instead of keys for production safety
        out = {}
        cursor = 0
        pattern = "labeler:endpoint:*"
        try:
            # use scan_iter if available
            async for key in r.scan_iter(match=pattern):
                # key may be bytes
                if isinstance(key, (bytes, bytearray)):
                    key_str = key.decode()
                else:
                    key_str = key
                endpoint = key_str.split("labeler:endpoint:", 1)[-1]
                vals = await r.smembers(key)
                vals = [v.decode() if isinstance(v, (bytes, bytearray)) else v for v in vals]
                out[endpoint] = vals
            return out
        except AttributeError:
            # fallback to synchronous KEYS (less ideal)
            keys = await r.keys(pattern)
            for key in keys:
                key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
                endpoint = key_str.split("labeler:endpoint:", 1)[-1]
                vals = await r.smembers(key)
                vals = [v.decode() if isinstance(v, (bytes, bytearray)) else v for v in vals]
                out[endpoint] = vals
            return out
    except Exception:
        LOG.exception("error listing all mappings")
        return {}


async def get_all_active_cooldowns():
    """Return a dict mapping normalized endpoints (from cooldown keys) -> TTL seconds."""
    r = await _get_redis()
    if r is None:
        return {}
    try:
        out = {}
        pattern = f"{COOLDOWN_PREFIX}:*"
        try:
            async for key in r.scan_iter(match=pattern):
                key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
                endpoint = key_str.split(f"{COOLDOWN_PREFIX}:", 1)[-1]
                ttl = await r.ttl(key)
                out[endpoint] = int(ttl) if ttl and ttl > 0 else 0
            return out
        except AttributeError:
            keys = await r.keys(pattern)
            for key in keys:
                key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
                endpoint = key_str.split(f"{COOLDOWN_PREFIX}:", 1)[-1]
                ttl = await r.ttl(key)
                out[endpoint] = int(ttl) if ttl and ttl > 0 else 0
            return out
    except Exception:
        LOG.exception("error listing cooldowns")
        return {}
