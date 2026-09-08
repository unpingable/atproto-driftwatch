"""Source accounting under saturation and replay, independent of live ingestion."""
import asyncio
import json

from labeler import consumer, db
import pytest


def message(t, key, text="first"):
    return json.dumps({"did": "did:plc:fixture", "time_us": t, "kind": "commit",
        "commit": {"operation": "create", "collection": "app.bsky.feed.post",
                   "rkey": key, "cid": text, "record": {"text": text,
                   "createdAt": "2026-09-08T00:00:00Z"}}})


def test_saturated_checkpoint_reconnect(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    db.init_db()
    c = consumer.ATProtoConsumer()
    c._event_queue = asyncio.Queue(maxsize=1)
    async def exercise():
        await c._handle_message(message(10_000_000, "a"))
        earlier = [c._event_queue.get_nowait()]
        await c._handle_message(message(11_000_000, "b"))
        pending = asyncio.create_task(c._handle_message(message(12_000_000, "c")))
        await asyncio.sleep(0.01)
        if hasattr(c, "_queue_waits_total"):
            assert c._queue_waits_total == 1
            assert not pending.done(), "full queue must preserve pending source input"
        c._process_batch(earlier)
        checkpoint = db.get_cursor(consumer.CONSUMER_NAME)
        assert checkpoint is not None, "successful batch must durably checkpoint completed work"
        assert int(checkpoint) < 11_000_000, "checkpoint passed unresolved events"
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        assert c._received_total - c._admitted_total - c._unadmitted_interrupted_total == 0
        assert c._admitted_total - c._source_committed_total == 1
        c._writer_conn.close()
        c._writer_conn = None
        replay = consumer.ATProtoConsumer()
        for raw in [message(10_000_000, "a"), message(11_000_000, "b"),
                    message(12_000_000, "c"), message(13_000_000, "a", "updated")]:
            await replay._handle_message(raw)
            replay._process_batch([replay._event_queue.get_nowait()])
        # The same replay must not revert the newer update or add versions.
        for raw in [message(10_000_000, "a"), message(13_000_000, "a", "updated")]:
            await replay._handle_message(raw)
            replay._process_batch([replay._event_queue.get_nowait()])
        conn = db.get_conn()
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 3
        assert conn.execute("SELECT count(*) FROM event_versions").fetchone()[0] == 1
        assert json.loads(conn.execute("SELECT raw FROM events WHERE event_uri LIKE '%/a'").fetchone()[0])["text"] == "updated"
        conn.close()
        replay._writer_conn.close()
        replay._writer_executor.shutdown()
    try:
        asyncio.run(exercise())
    finally:
        c._writer_executor.shutdown()


def test_identity_rollback_ties_and_expired_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    db.init_db()
    c = consumer.ATProtoConsumer()
    js = {"did": "did:plc:fixture", "time_us": 10_000_000, "kind": "identity",
          "identity": {"did": "did:plc:fixture", "handle": "fixture.test"}}
    batch = [{"_source": js}, {"_source": json.loads(message(10_000_000, "a"))}]
    original = consumer.insert_event_txn
    def fail(*args, **kwargs):
        raise RuntimeError("injected batch failure after identity processing")
    monkeypatch.setattr(consumer, "insert_event_txn", fail)
    assert c._process_batch(batch) == (0, 2)
    conn = db.get_conn()
    assert conn.execute("SELECT count(*) FROM identity_events").fetchone()[0] == 0
    assert conn.execute("SELECT count(*) FROM ingest_receipts").fetchone()[0] == 0
    assert db.get_cursor(consumer.CONSUMER_NAME) is None
    monkeypatch.setattr(consumer, "insert_event_txn", original)
    assert c._process_batch(batch) == (1, 0)
    assert c._process_batch(batch) == (0, 0)
    assert conn.execute("SELECT count(*) FROM identity_events").fetchone()[0] == 1
    assert c._process_batch([{"_source": json.loads(message(20_000_000, "a", "newer"))}]) == (1, 0)
    before = db.get_cursor(consumer.CONSUMER_NAME)
    assert c._process_batch(batch) == (0, 2)  # expired receipts never allow reapplication
    assert db.get_cursor(consumer.CONSUMER_NAME) == before
    assert conn.execute("SELECT count(*) FROM identity_events").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM event_versions").fetchone()[0] == 1
    conn.close()
    c._writer_conn.close()
    c._writer_executor.shutdown()


def test_account_filter_and_nondecreasing_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    db.init_db()
    c = consumer.ATProtoConsumer()
    account = {"did": "did:plc:fixture", "time_us": 12_000_000, "kind": "account",
               "account": {"did": "did:plc:fixture", "active": False}}
    assert c._process_batch([{"_source": account}]) == (0, 0)
    checkpoint = db.get_cursor(consumer.CONSUMER_NAME)
    assert c._process_batch([{"_source": json.loads(message(11_000_000, "a"))}]) == (1, 0)
    assert db.get_cursor(consumer.CONSUMER_NAME) == checkpoint
    conn = db.get_conn()
    assert conn.execute("SELECT is_active FROM actor_identity_current").fetchone()[0] == 0
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    conn.close()
    c._writer_conn.close()
    c._writer_executor.shutdown()


def test_receipt_capacity_holds_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(consumer, "REPLAY_RECEIPT_LIMIT", 1)
    db.init_db()
    c = consumer.ATProtoConsumer()
    assert c._process_batch([{"_source": json.loads(message(10_000_000, "a"))}]) == (1, 0)
    assert c._process_batch([{"_source": json.loads(message(10_000_000, "b"))}]) == (0, 1)
    assert db.get_cursor(consumer.CONSUMER_NAME) == "5000000"
    conn = db.get_conn()
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1
    conn.close()
    c._writer_conn.close()
    c._writer_executor.shutdown()


def test_baseline_reads_and_writes_candidate_state(tmp_path, monkeypatch):
    """The additive receipt tables do not require a database rewind on rollback."""
    import subprocess
    import sys
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    db.init_db()
    c = consumer.ATProtoConsumer()
    assert c._process_batch([{"_source": json.loads(message(10_000_000, "a"))}]) == (1, 0)
    c._writer_conn.close()
    c._writer_executor.shutdown()
    source = subprocess.check_output(["git", "show", "7a68b027:src/labeler/db.py"], text=True)
    import types
    baseline = types.ModuleType("labeler.baseline_db")
    baseline.__file__ = db.__file__
    baseline.__package__ = "labeler"
    exec(compile(source, "baseline@7a68b027/db.py", "exec"), baseline.__dict__)
    baseline.DATA_DIR = tmp_path
    baseline.init_db()
    assert baseline.get_cursor(consumer.CONSUMER_NAME) == "5000000"
    baseline.insert_event("at://did:plc:fixture/app.bsky.feed.post/b", "2026-09-08T00:00:00Z", "did:plc:fixture", {"text": "baseline"})
    conn = baseline.get_conn()
    assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 2
    assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    conn.close()


@pytest.mark.parametrize("source_ref", ["7a68b027", "3a81b95", "working"])
def test_actual_drain_completed_prefix(source_ref, tmp_path, monkeypatch):
    """Identical one-slot load exposes received-cursor saves in both old refs."""
    import subprocess
    import threading
    import types
    from labeler import maintenance
    monkeypatch.setattr(maintenance, "is_disk_pressure", lambda: False)
    module = consumer
    if source_ref != "working":
        module = types.ModuleType("labeler.old_consumer")
        module.__package__ = "labeler"
        code = subprocess.check_output(["git", "show", f"{source_ref}:src/labeler/consumer.py"], text=True)
        exec(compile(code, f"{source_ref}/consumer.py", "exec"), module.__dict__)
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(module, "BATCH_MAX_EVENTS", 1)
    monkeypatch.setattr(module, "BATCH_MAX_WAIT_S", 0)
    monkeypatch.setattr(module, "CURSOR_SAVE_INTERVAL", 1)
    db.init_db()
    c = module.ATProtoConsumer()
    c._event_queue = asyncio.Queue(maxsize=1)
    entered, release = threading.Event(), threading.Event()
    original = c._process_batch
    original_save = module.upsert_cursor
    def save_and_stop(*args):
        original_save(*args)
        c._stop = True
    monkeypatch.setattr(module, "upsert_cursor", save_and_stop)
    calls = 0
    def controlled(batch):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            assert release.wait(3)
            result = original(batch)
            if source_ref == "working":
                c._stop = True
            return result
        return (0, len(batch))  # later work deliberately remains unresolved
    c._process_batch = controlled
    c._maybe_wal_truncate = lambda conn: None
    async def exercise():
        asyncio.get_running_loop().set_default_executor(c._writer_executor)
        await c._handle_message(message(10_000_000, "a"))
        drain = asyncio.create_task(c._drain_queue())
        while not entered.is_set():
            await asyncio.sleep(.001)
        await c._handle_message(message(11_000_000, "b"))
        pending = asyncio.create_task(c._handle_message(message(12_000_000, "c")))
        await asyncio.sleep(.01)
        release.set()
        for _ in range(1000):
            if c._event_count and db.get_cursor(module.CONSUMER_NAME):
                break
            await asyncio.sleep(.001)
        checkpoint = int(db.get_cursor(module.CONSUMER_NAME))
        c._stop = True
        pending.cancel()
        drain.cancel()
        await asyncio.gather(pending, drain, return_exceptions=True)
        if source_ref == "working":
            assert checkpoint == 5_000_000
            assert c._events_dropped_total == 0
        else:
            assert checkpoint == 12_000_000  # durable checkpoint skips b and c
            assert c._events_dropped_total == 1
        conn = db.get_conn()
        assert conn.execute("SELECT count(*) FROM events").fetchone()[0] == 1
        conn.close()
    async def checked():
        try:
            await asyncio.wait_for(exercise(), timeout=5)
        finally:
            c._stop = True
            tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(checked())
    finally:
        release.set()
        c._writer_executor.shutdown()
        loop.close()
