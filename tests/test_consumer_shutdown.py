"""Normal app shutdown must preserve the admitted SQLite prefix."""
import asyncio
import json
import logging
import threading
import time

import pytest
import websockets

from labeler import consumer, db, main, ops_runtime, platform_health, startup_policy


@pytest.fixture(autouse=True)
def consumer_only(tmp_path, monkeypatch):
    for name in startup_policy._CONTROLS:
        monkeypatch.setenv(name, "0")
    monkeypatch.setenv("DRIFTWATCH_OPERATOR_PAUSED", "0")
    monkeypatch.setenv("FIREHOSE_AUTO_START", "1")
    monkeypatch.setattr(ops_runtime, "DATA_DIR", tmp_path)
    monkeypatch.setattr(platform_health, "CHECKPOINT_PATH", tmp_path / "baseline.json")
    platform_health._reset()
    yield
    platform_health._reset()


def message(stamp):
    return json.dumps({"did": "did:plc:shutdownfixture", "time_us": stamp,
                       "kind": "identity", "identity": {"handle": "fixture.invalid"}})


async def until(predicate):
    deadline = asyncio.get_running_loop().time() + 3
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "fixture phase deadline"
        await asyncio.sleep(0.005)


def internal_tasks(c):
    return [task for task in asyncio.all_tasks() if not task.done()
            and task.get_coro().cr_frame is not None
            and task.get_coro().cr_frame.f_locals.get("self") is c
            and ("_drain_queue" in task.get_coro().__qualname__
                 or "monitor_event_loop" in task.get_coro().__qualname__)]


async def dispose_fixture(c):
    """Retire only this test's resources even when the old product leaks."""
    c.stop()
    tasks = internal_tasks(c)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if not c._writer_executor._shutdown:
        def close():
            if c._writer_conn is not None:
                c._writer_conn.close()
                c._writer_conn = None
        await asyncio.get_running_loop().run_in_executor(c._writer_executor, close)
        c._writer_executor.shutdown(wait=True)
    await main.shutdown_event()


def test_app_shutdown_during_real_reconnect_backoff(tmp_path, monkeypatch):
    async def exercise():
        backoff, close_peer = asyncio.Event(), asyncio.Event()

        class Notice(logging.Handler):
            def emit(self, record):
                if str(record.msg).startswith("Jetstream connection ended:"):
                    backoff.set()

        notice = Notice()
        logger = logging.getLogger("labeler.consumer")
        logger.addHandler(notice)
        stamp = int(time.time() * 1_000_000)
        db.upsert_cursor(consumer.CONSUMER_NAME, str(stamp - 10_000_000))
        generation = db.get_store_lineage()

        async def peer(ws, path):
            await ws.send(message(stamp))
            await close_peer.wait()
            await ws.close()

        c = None
        try:
            async with websockets.serve(peer, "127.0.0.1", 0, ping_interval=None) as server:
                monkeypatch.setattr(consumer, "JETSTREAM_WS", f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/subscribe")
                await main.startup_event()
                c = main._consumer_instance
                await until(lambda: c._source_committed_total == 1)
                close_peer.set()
                await asyncio.wait_for(backoff.wait(), 3)
                await asyncio.wait_for(main.shutdown_event(), 3)
                assert c._writer_conn is None, "backoff cancellation bypassed writer cleanup"
                assert c._writer_executor._shutdown
                assert not internal_tasks(c)
                assert c._event_queue._unfinished_tasks == 0
                assert db.get_store_lineage() == generation
                baseline = json.loads(platform_health.CHECKPOINT_PATH.read_text())
                assert baseline["store_generation"] == generation["generation_id"]
                assert db.get_cursor(consumer.CONSUMER_NAME) == str(stamp - 5_000_000)
        finally:
            close_peer.set()
            if c is not None:
                await dispose_fixture(c)
            logger.removeHandler(notice)

    asyncio.run(exercise())


@pytest.mark.parametrize("cancel_again", [False, True])
def test_app_shutdown_drains_queued_and_inflight_work(monkeypatch, cancel_again):
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(consumer, "BATCH_MAX_EVENTS", 1)
    actual_consumer = consumer.ATProtoConsumer

    def create():
        c = actual_consumer()
        c._event_queue = asyncio.Queue(maxsize=1)
        process = c._process_batch

        def held_batch(batch):
            entered.set()
            assert release.wait(3), "fixture writer release deadline"
            return process(batch)

        c._process_batch = held_batch
        return c

    monkeypatch.setattr(main, "ATProtoConsumer", create)

    async def exercise():
        stamp = int(time.time() * 1_000_000)
        db.upsert_cursor(consumer.CONSUMER_NAME, str(stamp - 10_000_000))
        generation = db.get_store_lineage()

        async def peer(ws, path):
            for n in range(4):
                await ws.send(message(stamp + n))
            await ws.wait_closed()

        c = None
        timer = None
        try:
            async with websockets.serve(peer, "127.0.0.1", 0, ping_interval=None) as server:
                monkeypatch.setattr(consumer, "JETSTREAM_WS", f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/subscribe")
                await main.startup_event()
                c, task = main._consumer_instance, main._consumer_task
                await until(lambda: entered.is_set() and c._admitted_total == 2 and c._admission_pending)
                shutdown = asyncio.create_task(main.shutdown_event())
                # Thread release also bounds the unchanged implementation's
                # synchronous executor shutdown when this test is RED.
                timer = threading.Timer(0.2, release.set)
                timer.start()
                await until(lambda: c._unadmitted_interrupted_total == 1)
                if cancel_again:
                    task.cancel()
                await asyncio.wait_for(shutdown, 3)
                assert c._source_committed_total == 2, "shutdown abandoned admitted work"
                assert (c._received_total, c._admitted_total, c._unadmitted_interrupted_total) == (3, 2, 1)
                assert c._event_queue._unfinished_tasks == 0
                assert c._writer_conn is None and c._writer_executor._shutdown
                assert not internal_tasks(c)
                assert db.get_cursor(consumer.CONSUMER_NAME) == str(stamp + 1 - 5_000_000)
                assert db.get_store_lineage() == generation
                assert json.loads(platform_health.CHECKPOINT_PATH.read_text())["store_generation"] == generation["generation_id"]
        finally:
            release.set()
            if timer is not None:
                timer.join()
            if c is not None:
                await dispose_fixture(c)

    asyncio.run(exercise())


def test_cancelled_coverage_floor_is_unadmitted(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    record = db.record_coverage_floor

    def held_floor(*args):
        entered.set()
        assert release.wait(3), "fixture floor release deadline"
        return record(*args)

    monkeypatch.setattr(db, "record_coverage_floor", held_floor)

    async def exercise():
        c = consumer.ATProtoConsumer()
        c._coverage_start_id = db.record_coverage_start(consumer.CONSUMER_NAME, db.COVERAGE_UNBOUNDED, None)
        c._coverage_floor_pending = True
        try:
            task = asyncio.create_task(c._handle_message(message(10_000_000)))
            await until(entered.is_set)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert (c._received_total, c._admitted_total, c._unadmitted_interrupted_total) == (1, 0, 1)
            assert db.get_cursor(consumer.CONSUMER_NAME) is None
        finally:
            release.set()
            c._writer_executor.shutdown(wait=True)

    asyncio.run(exercise())


def test_app_shutdown_exposes_unfinished_work_after_drainer_failure(monkeypatch):
    """A real source-custody refusal must not become a successful shutdown."""
    async def exercise():
        conn = db.get_conn()
        conn.execute("CREATE TABLE ingest_source (consumer TEXT PRIMARY KEY, source TEXT NOT NULL)")
        conn.execute("INSERT INTO ingest_source VALUES (?,?)",
                     (consumer.CONSUMER_NAME, "ws://previous.invalid/subscribe"))
        conn.commit()
        conn.close()

        async def peer(ws, path):
            await ws.send(message(10_000_000))
            await ws.wait_closed()

        c = None
        try:
            async with websockets.serve(peer, "127.0.0.1", 0, ping_interval=None) as server:
                monkeypatch.setattr(consumer, "JETSTREAM_WS", f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}/subscribe")
                await main.startup_event()
                c = main._consumer_instance
                await until(lambda: c._stop and c._admitted_total == 1)
                with pytest.raises(RuntimeError, match="admitted"):
                    await asyncio.wait_for(main.shutdown_event(), 3)
                assert c._source_committed_total == 0
                assert c._event_queue._unfinished_tasks == 1
                assert db.get_cursor(consumer.CONSUMER_NAME) is None
                assert c._writer_conn is None and c._writer_executor._shutdown
                assert not internal_tasks(c)
        finally:
            if c is not None:
                await dispose_fixture(c)

    asyncio.run(exercise())
