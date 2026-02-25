import asyncio
import pytest

pytest.importorskip("redis.asyncio")

from labeler import metrics as metrics_module


@pytest.mark.asyncio
async def test_query_skips_due_to_cooldown(monkeypatch):
    # Simulate cooldown being active by monkeypatching cooldown.is_in_cooldown
    async def fake_is_in_cooldown(key):
        return 10

    async def fake_set_cooldown(key, seconds):
        # record call
        fake_set_cooldown.called = True

    monkeypatch.setattr("labeler.cooldown.is_in_cooldown", fake_is_in_cooldown)
    monkeypatch.setattr("labeler.cooldown.set_cooldown", fake_set_cooldown)

    # Reset metric
    before = metrics_module.LABEL_QUERY_COOLDOWN_SKIPPED._value.get()

    # Call query (should short-circuit and increment cooldown skipped metric)
    from labeler.labeler import query_labels_for_subject
    res = await query_labels_for_subject("uri:test-cooldown")

    assert res == []
    after = metrics_module.LABEL_QUERY_COOLDOWN_SKIPPED._value.get()
    assert after - before >= 1


@pytest.mark.asyncio
async def test_429_sets_cooldown(monkeypatch):
    # Simulate a 429 response with Retry-After seconds and ensure set_cooldown is invoked
    calls = {"count": 0}
    headers1 = {"Retry-After": "2"}

    async def fake_get(self, url, params=None):
        calls["count"] += 1
        class Fake429:
            status_code = 429
            headers = headers1
            def json(self):
                return {}
        class Fake200:
            status_code = 200
            headers = {}
            def json(self):
                return {"labels": [{"labeler": "did:lab:1", "val": "x"}]}
        if calls["count"] == 1:
            return Fake429()
        return Fake200()

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)

    called = {"set": False}

    async def fake_set_cooldown(key, seconds):
        called["set"] = True
        called["secs"] = seconds

    monkeypatch.setattr("labeler.cooldown.set_cooldown", fake_set_cooldown)

    # Patch asyncio.sleep to avoid waiting
    async def fake_sleep(delay):
        return None
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    from labeler.labeler import query_labels_for_subject
    res = await query_labels_for_subject("uri:test-429-set")

    assert isinstance(res, list)
    assert called["set"] is True
    assert called["secs"] >= 2
