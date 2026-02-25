import asyncio
import datetime
import pytest
pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event, get_unlabeled_subjects, get_labels_for_subject


@pytest.mark.asyncio
async def test_ingest_once(monkeypatch):
    init_db()
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    now = now_dt.isoformat()

    # create an event that should be picked up
    insert_event("uri:ingest:1", now_dt, "did:alice", {"uri": "uri:ingest:1", "time": now, "author": "did:alice"})

    # monkeypatch the label query to return a fake label
    async def fake_query(uri):
        return [{"labeler": "did:lab:1", "val": "misinfo", "time": datetime.datetime.now(datetime.timezone.utc).isoformat()}]

    monkeypatch.setattr("labeler.labeler.query_labels_for_subject", fake_query)

    # run the ingestion once
    from labeler.ingest import ingest_once

    inserted = await ingest_once(window_hours=48, limit=10)

    assert inserted >= 1

    labels = get_labels_for_subject("uri:ingest:1")
    assert labels and labels[0]["label"]
