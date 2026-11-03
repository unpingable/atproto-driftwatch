"""Optional DID enrichment must not hold the ingest prefix or its shutdown."""
import asyncio
import json
import threading
import time

from labeler import consumer, db, ops_runtime, platform_health, resolver


def test_pending_enrichment_cannot_hold_prefix_or_shutdown(tmp_path, monkeypatch):
    """A resolver taking external time cannot park the sole queue drainer."""
    monkeypatch.setattr(ops_runtime, "DATA_DIR", tmp_path)
    monkeypatch.setattr(platform_health, "CHECKPOINT_PATH", tmp_path / "baseline.json")
    platform_health._reset()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def pending_resolution():
        entered.set()
        try:
            if not release.wait(5):
                raise RuntimeError("fixture release deadline")
            return {"resolved": 0, "ok": 0, "not_found": 0, "error": 0}
        finally:
            finished.set()

    # Keep real consumer/SQLite/executors; control only external enrichment and
    # the consumer-local stats clock so its minute hook is reached promptly.
    monkeypatch.setattr(resolver, "resolve_batch", pending_resolution)
    monkeypatch.setattr(consumer, "BATCH_MAX_EVENTS", 1)

    async def exercise():
        loop = asyncio.get_running_loop()

        class StatsLoop:
            calls = 0

            def time(self):
                self.calls += 1
                return 0.0 if self.calls <= 2 else 60.0

            def run_in_executor(self, *args):
                return loop.run_in_executor(*args)

        stats_loop = StatsLoop()

        class LocalAsyncio:
            def get_event_loop(self):
                return stats_loop

            def __getattr__(self, name):
                return getattr(asyncio, name)

        monkeypatch.setattr(consumer, "asyncio", LocalAsyncio())
        c = consumer.ATProtoConsumer(ws_url="wss://fixture.invalid/subscribe")
        task = None
        observed = {}
        base = int(time.time() * 1_000_000)

        def message(n):
            return json.dumps({"did": "did:plc:fixture0001", "kind": "identity",
                               "time_us": base + n,
                               "identity": {"handle": "fixture.invalid", "seq": n}})

        async def until(predicate):
            deadline = loop.time() + 1
            while not predicate() and loop.time() < deadline:
                await asyncio.sleep(0.005)
            return predicate()

        try:
            await c._handle_message(message(1))
            task = asyncio.create_task(c._drain_queue())
            assert await until(lambda: c._source_committed_total == 1 and stats_loop.calls >= 3)
            await c._handle_message(message(2))
            await until(lambda: c._source_committed_total == 2)
            observed["completed"] = c._source_committed_total
            observed["unfinished"] = c._event_queue._unfinished_tasks
            c.stop()
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)
            observed["resolver_left_running"] = entered.is_set() and not finished.is_set()
            conn = db.get_conn()
            try:
                observed["identity_rows"] = conn.execute("SELECT count(*) FROM identity_events").fetchone()[0]
                observed["receipt_rows"] = conn.execute("SELECT count(*) FROM ingest_receipts").fetchone()[0]
                observed["cursor"] = db.get_cursor(consumer.CONSUMER_NAME)
            finally:
                conn.close()
        finally:
            release.set()
            c.stop()
            if task and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if entered.is_set():
                assert await until(finished.is_set)

            def close_writer():
                if c._writer_conn is not None:
                    c._writer_conn.close()
                    c._writer_conn = None

            await loop.run_in_executor(c._writer_executor, close_writer)
            c._writer_executor.shutdown(wait=True)

        assert observed["completed"] == 2, "enrichment held the second admitted event behind the durable prefix"
        assert observed["unfinished"] == 0
        assert not observed["resolver_left_running"], "drainer shutdown left external executor work running"
        assert observed["identity_rows"] == 2
        assert observed["receipt_rows"] == 2
        assert observed["cursor"] == str(base + 2 - 5_000_000)

    try:
        asyncio.run(exercise())
    finally:
        platform_health._reset()
