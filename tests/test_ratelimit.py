import asyncio
import time
import pytest
from labeler.ratelimit import AsyncRateLimiter


def test_rate_limiter_basic():
    async def inner():
        # rate=2 tokens/sec -> 4 acquires should take at least ~1s
        limiter = AsyncRateLimiter(rate=2.0, per=1.0)
        t0 = time.monotonic()
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()
        t1 = time.monotonic()
        elapsed = t1 - t0
        assert elapsed >= 0.9

    asyncio.run(inner())


def test_concurrency_respected(monkeypatch):
    pytest.importorskip("httpx")

    async def inner():
        # test concurrency by using the labeler concurrency limiter indirectly
        from labeler.labeler import LABEL_QUERY_CONCURRENCY, query_labels_for_subject
        import labeler.labeler as lbl

        # create a fake httpx.AsyncClient.get that tracks concurrent invocations
        concurrent = {"count": 0, "max": 0}

        async def fake_get(self, url, params=None):
            concurrent["count"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["count"])
            # small sleep to simulate in-flight request
            await asyncio.sleep(0.05)
            concurrent["count"] -= 1

            class FakeResp:
                status_code = 200
                def json(self):
                    return {"labels": []}

            return FakeResp()

        monkeypatch.setattr("httpx.AsyncClient.get", fake_get)

        # launch many queries concurrently
        tasks = [asyncio.create_task(query_labels_for_subject(f"uri:{i}")) for i in range(10)]
        await asyncio.gather(*tasks)

        assert concurrent["max"] <= LABEL_QUERY_CONCURRENCY

    asyncio.run(inner())
