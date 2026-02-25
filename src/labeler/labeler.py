import os
import httpx
import asyncio
import random
import time
from typing import List, Dict, Any

from .ratelimit import AsyncRateLimiter, AsyncConcurrencyLimiter
from . import metrics
from . import timeutil

LABELER_ENDPOINT = os.getenv("LABELER_ENDPOINT", "https://labeler.example/xrpc/com.atproto.label.queryLabels")
LABEL_QUERY_RATE = float(os.getenv("LABEL_QUERY_RATE", "5"))  # requests per second
LABEL_QUERY_CONCURRENCY = int(os.getenv("LABEL_QUERY_CONCURRENCY", "3"))
LABEL_QUERY_RETRIES = int(os.getenv("LABEL_QUERY_RETRIES", "3"))
LABEL_QUERY_BACKOFF_BASE = float(os.getenv("LABEL_QUERY_BACKOFF_BASE", "0.5"))
LABEL_QUERY_BACKOFF_MAX = float(os.getenv("LABEL_QUERY_BACKOFF_MAX", "5.0"))

# module-level limiter + concurrency semaphore
import os

REDIS_URL = os.getenv("REDIS_URL")
REDIS_TOKEN_KEY = os.getenv("REDIS_TOKEN_KEY", "labeler:tokenbucket:labels")

_rate_limiter = None

# Attempt to use distributed limiter if REDIS_URL is set
if REDIS_URL:
    try:
        # try to make the distributed limiter
        _dist = None
        import asyncio

        async def _init_dist():
            from .ratelimit import maybe_make_distributed
            return await maybe_make_distributed(LABEL_QUERY_RATE, 1.0, REDIS_URL, REDIS_TOKEN_KEY)

        _dist = asyncio.get_event_loop().run_until_complete(_init_dist())
        if _dist:
            _rate_limiter = _dist
    except Exception:
        # fallback silently to local limiter
        _rate_limiter = None

if _rate_limiter is None:
    _rate_limiter = AsyncRateLimiter(rate=LABEL_QUERY_RATE)

_concurrency = AsyncConcurrencyLimiter(limit=LABEL_QUERY_CONCURRENCY)


async def query_labels_for_subject(subject_uri: str) -> List[Dict[str, Any]]:
    """Query labels with rate limiting, concurrency control, and retries.

    Behavior:
    - Enforces a token-bucket rate limit (LABEL_QUERY_RATE RPS)
    - Limits concurrent outgoing requests to LABEL_QUERY_CONCURRENCY
    - Retries on network errors and 429/5xx responses using exponential backoff
    - Returns an empty list on failure
    """
    metrics.LABEL_QUERY_TOTAL.inc()
    # Adaptive cooldown: if Redis is configured and there's an active cooldown for this endpoint, skip the query
    try:
        from . import cooldown
        ttl = None
        try:
            ep_norm = cooldown.normalize_endpoint(LABELER_ENDPOINT)
            ttl = __import__("asyncio").run(cooldown.is_in_cooldown(ep_norm))
        except Exception:
            # ignore cooldown check failures
            ttl = None
        if ttl:
            # metric and early return
            try:
                metrics.LABEL_QUERY_COOLDOWN_SKIPPED.inc()
            except Exception:
                pass
            return []
    except Exception:
        # either module missing or redis not available
        pass

    await _rate_limiter.acquire()

    async with _concurrency:
        attempt = 0
        backoff = LABEL_QUERY_BACKOFF_BASE
        start = time.perf_counter()
        while attempt < LABEL_QUERY_RETRIES:
            attempt += 1
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.get(LABELER_ENDPOINT, params={"uri": subject_uri})
                    status = r.status_code
                    if status == 200:
                        metrics.LABEL_QUERY_SUCCESS.inc()
                        metrics.LABEL_QUERY_DURATION.observe(time.perf_counter() - start)
                        return r.json().get("labels", [])

                    # Handle 429 specially with Retry-After when provided
                    if status == 429:
                        metrics.LABEL_QUERY_429.inc()
                        metrics.LABEL_QUERY_RETRIES.inc()
                        retry_after = None
                        try:
                            retry_after = r.headers.get("Retry-After") if hasattr(r, "headers") else None
                        except Exception:
                            retry_after = None

                        if retry_after:
                            # Try parsing seconds integer
                            try:
                                sec = int(retry_after.strip())
                                delay = min(sec, LABEL_QUERY_BACKOFF_MAX)
                            except Exception:
                                # Try HTTP-date parse
                                try:
                                    from email.utils import parsedate_to_datetime

                                    dt = parsedate_to_datetime(retry_after)
                                    delay = max(0.0, (dt - timeutil.now_utc()).total_seconds())
                                    delay = min(delay, LABEL_QUERY_BACKOFF_MAX)
                                except Exception:
                                    delay = min(LABEL_QUERY_BACKOFF_MAX, backoff)
                        else:
                            delay = min(LABEL_QUERY_BACKOFF_MAX, backoff)

                        # Set adaptive cooldown in Redis (best-effort)
                        try:
                            from . import cooldown
                            # call set_cooldown asynchronously and don't block main loop; but ensure it's scheduled
                            try:
                                __import__("asyncio").get_event_loop().create_task(cooldown.set_cooldown(LABELER_ENDPOINT, int(delay)))
                                try:
                                    metrics.LABEL_QUERY_COOLDOWN_SET.inc()
                                except Exception:
                                    pass
                            except RuntimeError:
                                # no running loop, fallback to run
                                try:
                                    __import__("asyncio").run(cooldown.set_cooldown(LABELER_ENDPOINT, int(delay)))
                                    try:
                                        metrics.LABEL_QUERY_COOLDOWN_SET.inc()
                                    except Exception:
                                        pass
                                except Exception:
                                    pass

                            # Also attempt to fetch known labeler DIDs for this endpoint and set per-DID cooldowns
                            try:
                                async def _set_per_did():
                                    try:
                                        dids = await cooldown.list_labeler_dids_for_endpoint(LABELER_ENDPOINT)
                                        for d in dids or []:
                                            try:
                                                await cooldown.set_cooldown(d, int(delay))
                                                try:
                                                    metrics.LABEL_QUERY_COOLDOWN_SET.inc()
                                                except Exception:
                                                    pass
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass

                                # schedule the per-DID job
                                try:
                                    __import__("asyncio").get_event_loop().create_task(_set_per_did())
                                except RuntimeError:
                                    # no running loop; run synchronously (best-effort)
                                    try:
                                        __import__("asyncio").run(_set_per_did())
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                        except Exception:
                            pass

                        # Sleep according to Retry-After (or backoff fallback) then retry
                        await asyncio.sleep(delay)
                        backoff *= 2
                        continue

                    # Retry on server errors
                    if status in (500, 502, 503, 504):
                        metrics.LABEL_QUERY_RETRIES.inc()
                        # fall through to retry logic
                        pass
                    else:
                        # treat other statuses as non-retryable
                        metrics.LABEL_QUERY_FAILURE.inc()
                        metrics.LABEL_QUERY_DURATION.observe(time.perf_counter() - start)
                        return []
            except Exception:
                # network or parsing error — retry
                metrics.LABEL_QUERY_RETRIES.inc()
                pass

            # backoff before next attempt (jittered)
            delay = min(LABEL_QUERY_BACKOFF_MAX, backoff) * (0.5 + random.random() * 0.5)
            await asyncio.sleep(delay)
            backoff *= 2

        # exhausted retries
        metrics.LABEL_QUERY_FAILURE.inc()
        metrics.LABEL_QUERY_DURATION.observe(time.perf_counter() - start)
        return []
