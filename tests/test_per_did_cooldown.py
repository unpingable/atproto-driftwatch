import asyncio
import pytest

pytest.importorskip("prometheus_client")


@pytest.mark.asyncio
async def test_429_sets_per_did_cooldowns(monkeypatch):
    # Simulate 429 response and mock cooldown.list_labeler_dids_for_endpoint to return two DIDs
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

    # Mock list_labeler_dids_for_endpoint and set_cooldown to capture calls
    sets = {"dids": ["did:lab:1", "did:lab:2"], "set_calls": []}

    async def fake_list(endpoint):
        return sets["dids"]

    async def fake_set(key, seconds):
        sets["set_calls"].append((key, seconds))

    monkeypatch.setattr("labeler.cooldown.list_labeler_dids_for_endpoint", fake_list)
    monkeypatch.setattr("labeler.cooldown.set_cooldown", fake_set)

    # Patch asyncio.sleep to avoid delay
    async def fake_sleep(delay):
        return None
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    from labeler.labeler import query_labels_for_subject
    res = await query_labels_for_subject("uri:per-did-test")

    assert isinstance(res, list)
    # Ensure per-DID cooldowns were set
    assert len(sets["set_calls"]) >= len(sets["dids"]) 
    # and ensure the keys correspond to DIDs
    keys = [c[0] for c in sets["set_calls"]]
    for d in sets["dids"]:
        assert d in keys
