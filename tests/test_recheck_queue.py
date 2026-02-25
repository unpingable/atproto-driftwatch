import importlib

import pytest
pytest.importorskip("fakeredis")
pytest.importorskip("redis")
pytest.importorskip("prometheus_client")

from labeler.db import init_db, get_conn
from labeler import metrics as metrics_module


def test_local_fallback_queue_enqueue_dequeue():
    init_db()
    conn = get_conn()
    from labeler.recheck_queue import LocalFallbackQueue

    q = LocalFallbackQueue(conn)
    q.enqueue("root:1")
    q.enqueue("root:2")
    q.enqueue("root:1")
    assert metrics_module.RECHECK_QUEUE_DEPTH._value.get() >= 1

    got = q.dequeue(limit=2)
    assert set(got) == {"root:1", "root:2"}
    assert metrics_module.RECHECK_QUEUE_DEPTH._value.get() == 0

    remaining = conn.execute("SELECT root_uri FROM recheck_requests").fetchall()
    conn.close()
    assert not remaining


def test_redis_queue_enqueue_dequeue(monkeypatch):
    import fakeredis
    import redis

    fake = fakeredis.FakeRedis()
    monkeypatch.setenv("REDIS_URL", "redis://example")
    monkeypatch.setattr(redis.Redis, "from_url", lambda url: fake)

    import labeler.recheck_queue as rq
    importlib.reload(rq)

    q = rq.get_queue()
    q.enqueue("root:a")
    q.enqueue("root:b")
    assert metrics_module.RECHECK_QUEUE_DEPTH._value.get() >= 2

    got = q.dequeue(limit=1)
    assert len(got) == 1
    rest = q.dequeue(limit=10)
    assert len(rest) == 1
    assert metrics_module.RECHECK_QUEUE_DEPTH._value.get() == 0
