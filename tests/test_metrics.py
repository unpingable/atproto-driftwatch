import pytest
import datetime

pytest.importorskip("prometheus_client")

from labeler import metrics as metrics_module
from labeler.db import init_db, insert_event, get_labels_for_subject


def test_ingest_metrics_increment(monkeypatch):
    # ensure DB is ready
    init_db()
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    now = now_dt.isoformat()
    insert_event("uri:metric:1", now_dt, "did:alice", {"uri": "uri:metric:1", "time": now, "author": "did:alice"})

    # start counters
    before = metrics_module.LABELS_INSERTED._value.get()

    # monkeypatch query to return one label
    async def fake_query(uri):
        return [{"labeler": "did:lab:1", "val": "misinfo", "time": datetime.datetime.now(datetime.timezone.utc).isoformat()}]

    monkeypatch.setattr("labeler.labeler.query_labels_for_subject", fake_query)

    # run ingestion
    from labeler.ingest import ingest_once
    inserted = __import__("asyncio").run(ingest_once(window_hours=48, limit=10))

    after = metrics_module.LABELS_INSERTED._value.get()
    assert after - before >= inserted


def test_query_metrics(monkeypatch):
    # skip if httpx missing
    pytest.importorskip("httpx")

    from labeler.labeler import query_labels_for_subject

    before_total = metrics_module.LABEL_QUERY_TOTAL._value.get()
    before_success = metrics_module.LABEL_QUERY_SUCCESS._value.get()

    # fake httpx response
    async def fake_get(self, url, params=None):
        class FakeResp:
            status_code = 200
            def json(self):
                return {"labels": [{"labeler": "did:lab:1", "val": "x"}]}

        return FakeResp()

    monkeypatch.setattr("httpx.AsyncClient.get", fake_get)

    result = __import__("asyncio").run(query_labels_for_subject("uri:test"))
    assert isinstance(result, list)

    after_total = metrics_module.LABEL_QUERY_TOTAL._value.get()
    after_success = metrics_module.LABEL_QUERY_SUCCESS._value.get()
    assert after_total - before_total >= 1
    assert after_success - before_success >= 1
