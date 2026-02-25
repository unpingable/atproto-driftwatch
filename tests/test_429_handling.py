import asyncio
import datetime
import email.utils
import pytest

pytest.importorskip("httpx")
pytest.importorskip("prometheus_client")

from labeler import metrics as metrics_module


def test_429_retry_after_seconds(monkeypatch):
    # Arrange: first response is 429 with Retry-After seconds, second is 200
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

    # Patch asyncio.sleep to avoid waiting and capture delay values
    sleeps = []
    async def fake_sleep(delay):
        sleeps.append(delay)
        return None
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    # Metrics before
    before_429 = metrics_module.LABEL_QUERY_429._value.get()
    before_success = metrics_module.LABEL_QUERY_SUCCESS._value.get()

    # Act
    from labeler.labeler import query_labels_for_subject
    res = asyncio.run(query_labels_for_subject("uri:test:429s"))

    # Assert
    assert isinstance(res, list)
    assert sleeps, "sleep was not called"
    # first sleep should be approximately 2 (we patched sleep to capture value)
    assert any(delay >= 2.0 for delay in sleeps)
    assert metrics_module.LABEL_QUERY_429._value.get() - before_429 >= 1
    assert metrics_module.LABEL_QUERY_SUCCESS._value.get() - before_success >= 1


def test_429_retry_after_date_header(monkeypatch):
    # Arrange: set Retry-After to HTTP-date a few seconds in future
    future_dt = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3)
    ra_value = email.utils.format_datetime(future_dt, usegmt=True)
    headers1 = {"Retry-After": ra_value}
    calls = {"count": 0}

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
                return {"labels": [{"labeler": "did:lab:1", "val": "y"}]}
        if calls["count"] == 1:
            return Fake429()
        return Fake200()

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)

    sleeps = []
    async def fake_sleep(delay):
        sleeps.append(delay)
        return None
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    before_429 = metrics_module.LABEL_QUERY_429._value.get()

    from labeler.labeler import query_labels_for_subject
    res = asyncio.run(query_labels_for_subject("uri:test:429date"))

    assert isinstance(res, list)
    assert sleeps and any(d >= 2.0 for d in sleeps)
    assert metrics_module.LABEL_QUERY_429._value.get() - before_429 >= 1
