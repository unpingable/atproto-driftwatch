import asyncio
import pytest

pytest.importorskip("redis.asyncio")

from labeler.distributed_ratelimit import RedisTokenBucket
from labeler import metrics as metrics_module


class FakeRedis:
    def __init__(self, responses):
        # responses is a queue of eval return values
        self._responses = list(responses)
        self._called = []

    async def script_load(self, script):
        return "sha-mock"

    async def evalsha(self, sha, numkeys, key, rate, per, cap):
        if not self._responses:
            return 1000
        return self._responses.pop(0)

    async def eval(self, script, numkeys, key, rate, per, cap):
        # same behavior
        if not self._responses:
            return 1000
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_redis_token_bucket_basic(monkeypatch):
    # Simulate responses: first two calls succeed (1), then one request returns ms until next token (500), then success
    responses = [1, 1, 500, 1]
    fake = FakeRedis(responses.copy())
    bucket = RedisTokenBucket(fake, key="k1", rate=2.0, per=1.0, capacity=2.0)

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)
        # don't actually sleep
        return None

    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    # First two acquires should return immediately
    await bucket.acquire()
    await bucket.acquire()

    # Third acquire will see ms=500 and call sleep; then retry and succeed (based on queued responses)
    before = metrics_module.LABEL_QUERY_DISTRIBUTED_RATE_LIMITED._value.get()
    await bucket.acquire()

    assert any(d >= 0.4 for d in sleeps)
    assert metrics_module.LABEL_QUERY_DISTRIBUTED_RATE_LIMITED._value.get() - before >= 1
