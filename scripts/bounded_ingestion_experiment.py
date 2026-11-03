#!/usr/bin/env python3
"""One-shot bounded Jetstream ingestion experiment.

Runs inside the qualified Driftwatch image with no application server or other
background jobs. Output is selected metadata only; raw events and identities
are never printed or retained outside the normal database path.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import resource
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

from labeler import ops_runtime, platform_health
from labeler.consumer import ATProtoConsumer, CONSUMER_NAME, _build_ws_url
from labeler.db import DATA_DIR, get_conn, get_cursor, init_db


EXPECTED_CURSOR = os.environ.get("EXPERIMENT_EXPECTED_CURSOR")
IMAGE_ID = os.environ.get("EXPERIMENT_IMAGE_ID")
RUN_ID = os.environ.get("EXPERIMENT_RUN_ID")
MAX_TOTAL_SECONDS = float(os.environ.get("EXPERIMENT_MAX_TOTAL_SECONDS", "300"))
INTAKE_SECONDS = float(os.environ.get("EXPERIMENT_INTAKE_SECONDS", "270"))
MAX_RECEIVED = int(os.environ.get("EXPERIMENT_MAX_RECEIVED", "250000"))
MAX_ALLOC_GROWTH = int(
    os.environ.get("EXPERIMENT_MAX_ALLOC_GROWTH", str(256 * 1024 * 1024))
)
INTAKE_ALLOC_STOP = int(
    os.environ.get("EXPERIMENT_INTAKE_ALLOC_STOP", str(192 * 1024 * 1024))
)
ROOT_FLOOR = int(os.environ.get("EXPERIMENT_ROOT_FLOOR", "16642998272"))
ZONE_FLOOR = int(os.environ.get("EXPERIMENT_ZONE_FLOOR", "49981787751"))
GAP_DIR = DATA_DIR / "coverage-gaps"
RECEIPT_DIR = DATA_DIR / "experiment-receipts"
RECEIPT_PATH = RECEIPT_DIR / f"{RUN_ID}.json" if RUN_ID else None
RECEIPT_INTERVAL_SECONDS = float(
    os.environ.get("EXPERIMENT_RECEIPT_INTERVAL_SECONDS", "1")
)
DISK_SAMPLE_INTERVAL_SECONDS = float(
    os.environ.get("EXPERIMENT_DISK_SAMPLE_INTERVAL_SECONDS", "0.25")
)
FIXTURE_EVENT_COUNT = int(os.environ.get("EXPERIMENT_FIXTURE_EVENT_COUNT", "0"))
FIXTURE_WRITER_DELAY_SECONDS = float(
    os.environ.get("EXPERIMENT_FIXTURE_WRITER_DELAY_SECONDS", "0")
)
FIXTURE_START_OFFSET_US = int(
    os.environ.get("EXPERIMENT_FIXTURE_START_OFFSET_US", "10000000")
)


class ForcedStreamTimeout(TimeoutError):
    """Deterministic off-host source failure after a bounded fixture stream."""


class FixtureStream:
    def __init__(self, count: int, first_time_us: int):
        self.count = count
        self.first_time_us = first_time_us
        self.sent = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def recv(self):
        if self.sent >= self.count:
            raise ForcedStreamTimeout("deterministic fixture disconnect")
        stamp = self.first_time_us + self.sent * 1_000
        rkey = f"bounded{self.sent:013d}"
        self.sent += 1
        return json.dumps(
            {
                "did": "did:plc:boundedexperimentfixture",
                "time_us": stamp,
                "kind": "commit",
                "commit": {
                    "operation": "create",
                    "collection": "app.bsky.feed.post",
                    "rkey": rkey,
                    "record": {
                        "$type": "app.bsky.feed.post",
                        "createdAt": iso_us(stamp),
                        "text": "bounded integration fixture",
                    },
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def iso_us(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()


def allocated(path: Path) -> int:
    try:
        return path.stat().st_blocks * 512
    except FileNotFoundError:
        return 0


def atomic_json(path: Path, value: dict) -> None:
    """Durably replace a mode-0600 selected-field receipt."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temp.unlink()


def update_terminal_failure(exc: BaseException) -> None:
    """Retain a failure class even when main cannot reach normal serialization."""
    if RECEIPT_PATH is None:
        return
    try:
        prior = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    except Exception:
        prior = {"schema": "driftwatch.bounded-ingestion-receipt.v2", "run_id": RUN_ID}
    prior.update(
        {
            "phase": "terminal_failure",
            "terminal": True,
            "qualified": False,
            "failure_class": type(exc).__name__,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    atomic_json(RECEIPT_PATH, prior)


def disk_state() -> dict[str, int]:
    db = DATA_DIR / "labeler.sqlite"
    paths = (db, Path(str(db) + "-wal"), Path(str(db) + "-shm"))
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1)
    try:
        page_count = conn.execute("PRAGMA page_count").fetchone()[0]
        page_size = conn.execute("PRAGMA page_size").fetchone()[0]
        freelist_count = conn.execute("PRAGMA freelist_count").fetchone()[0]
    finally:
        conn.close()
    root = os.statvfs("/")
    zone = os.statvfs(DATA_DIR)
    return {
        "db": allocated(paths[0]),
        "wal": allocated(paths[1]),
        "shm": allocated(paths[2]),
        "total": sum(allocated(path) for path in paths),
        "db_page_count": page_count,
        "db_freelist_count": freelist_count,
        "db_live_bytes": (page_count - freelist_count) * page_size,
        "db_freelist_bytes": freelist_count * page_size,
        "root_free": root.f_bavail * root.f_frsize,
        "zone_free": zone.f_bavail * zone.f_frsize,
    }


def record_gap(previous: str, resumed: int) -> str:
    gap_id = f"jetstream-gap-{previous}-{resumed}"
    now = datetime.now(timezone.utc).isoformat()
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observation_coverage_gaps (
            gap_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            previous_completed_cursor TEXT NOT NULL,
            resumed_stream_cursor TEXT NOT NULL,
            gap_start_us INTEGER NOT NULL,
            gap_end_us INTEGER NOT NULL,
            disposition TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO observation_coverage_gaps
          (gap_id, source, previous_completed_cursor, resumed_stream_cursor,
           gap_start_us, gap_end_us, disposition, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            gap_id,
            "wss://jetstream2.us-east.bsky.network/subscribe",
            previous,
            str(resumed),
            int(previous),
            resumed,
            "accepted_unrecovered_gap_outputs_continuity_blocked",
            now,
        ),
    )
    conn.commit()
    conn.close()

    GAP_DIR.mkdir(mode=0o700, exist_ok=True)
    marker = GAP_DIR / f"{gap_id}.json"
    value = {
        "schema": "driftwatch.observation-coverage-gap.v1",
        "gap_id": gap_id,
        "source": "wss://jetstream2.us-east.bsky.network/subscribe",
        "previous_completed_cursor": previous,
        "previous_completed_utc": iso_us(int(previous)),
        "resumed_stream_boundary": str(resumed),
        "resumed_stream_boundary_utc": iso_us(resumed),
        "disposition": "accepted_unrecovered_gap",
        "continuity": False,
        "outputs_continuity_blocked": True,
        "recorded_at": now,
    }
    temp = marker.with_name(f".{marker.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temp, 0o600)
    os.replace(temp, marker)
    return gap_id


async def close_writer(consumer: ATProtoConsumer) -> None:
    def close() -> None:
        if consumer._writer_conn is not None:
            consumer._writer_conn.close()
            consumer._writer_conn = None

    await asyncio.get_running_loop().run_in_executor(consumer._writer_executor, close)
    consumer._writer_executor.shutdown(wait=True, cancel_futures=False)


async def main() -> None:
    if not EXPECTED_CURSOR or not IMAGE_ID or not RUN_ID:
        raise SystemExit(
            "EXPERIMENT_EXPECTED_CURSOR, EXPERIMENT_IMAGE_ID, and "
            "EXPERIMENT_RUN_ID are required"
        )
    begun = time.monotonic()
    deadline = begun + MAX_TOTAL_SECONDS
    intake_deadline = begun + INTAKE_SECONDS
    atomic_json(
        RECEIPT_PATH,
        {
            "schema": "driftwatch.bounded-ingestion-receipt.v2",
            "run_id": RUN_ID,
            "phase": "initializing",
            "terminal": False,
            "qualified": False,
            "image_id": IMAGE_ID,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    init_db()
    previous = get_cursor(CONSUMER_NAME)
    if previous != EXPECTED_CURSOR:
        raise SystemExit(f"refused: cursor mismatch {previous!r}")

    baseline = disk_state()
    if baseline["root_free"] < ROOT_FLOOR or baseline["zone_free"] < ZONE_FLOOR + MAX_ALLOC_GROWTH:
        raise SystemExit("refused: fresh filesystem admission failed")

    consumer = ATProtoConsumer()
    consumer._resume_cursor = previous
    if FIXTURE_WRITER_DELAY_SECONDS:
        process_batch = consumer._process_batch

        def delayed_process_batch(batch):
            time.sleep(FIXTURE_WRITER_DELAY_SECONDS)
            return process_batch(batch)

        consumer._process_batch = delayed_process_batch
    drain = asyncio.create_task(consumer._drain_queue())
    url = _build_ws_url(consumer.ws_url, cursor=previous)
    first_time = None
    last_time = None
    gap_id = None
    stop_reason = "unknown"
    peak = {
        **baseline,
        "physical_growth": 0,
        "live_growth": 0,
        "accounted_growth": 0,
        "growth": 0,
        "queue": 0,
        "rss_kib": 0,
    }
    raw_received_bytes = 0
    drain_completed = False
    runtime_failure = None
    last_receipt = 0.0
    last_disk_sample = 0.0
    last_state = {
        **baseline,
        "physical_growth": 0,
        "live_growth": 0,
        "accounted_growth": 0,
        "growth": 0,
        "queue": 0,
        "rss_kib": 0,
    }

    def counters() -> dict[str, int | bool]:
        return {
            "source_received": consumer._received_total,
            "source_admitted": consumer._admitted_total,
            "source_durably_completed": consumer._source_committed_total,
            "events_materialized": consumer._event_count,
            "replay_duplicates": consumer._replayed_total,
            "queue_waits": consumer._queue_waits_total,
            "queue_depth": consumer._event_queue.qsize(),
            "queue_unfinished": consumer._event_queue._unfinished_tasks,
            "unadmitted_interrupted": consumer._unadmitted_interrupted_total,
            "batch_failures": consumer._batch_failures_total,
            "batch_retry_pending": consumer._batch_retry_pending,
            "events_dropped": consumer._events_dropped_total,
            "rollback_lost": consumer._rollback_lost_total,
        }

    def persist(phase: str, state: dict | None = None, *, force: bool = False, **extra) -> None:
        nonlocal last_receipt
        now = time.monotonic()
        if not force and now - last_receipt < RECEIPT_INTERVAL_SECONDS:
            return
        value = {
            "schema": "driftwatch.bounded-ingestion-receipt.v2",
            "run_id": RUN_ID,
            "phase": phase,
            "terminal": False,
            "qualified": False,
            "image_id": IMAGE_ID,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": now - begun,
            "previous_completed_cursor": previous,
            "resumed_stream_boundary": str(first_time) if first_time is not None else None,
            "last_received_cursor": str(last_time) if last_time is not None else None,
            "coverage_gap_id": gap_id,
            "counters": counters(),
            "allocation": state or disk_state(),
            "peak": peak,
        }
        value.update(extra)
        atomic_json(RECEIPT_PATH, value)
        last_receipt = now

    async def sample(phase: str = "intake", *, force: bool = False) -> dict[str, int]:
        nonlocal last_disk_sample, last_state
        now = time.monotonic()
        if not force and now - last_disk_sample < DISK_SAMPLE_INTERVAL_SECONDS:
            return last_state
        state = disk_state()
        state["physical_growth"] = max(0, state["total"] - baseline["total"])
        state["live_growth"] = max(
            0, state["db_live_bytes"] - baseline["db_live_bytes"]
        )
        state["accounted_growth"] = (
            state["live_growth"]
            + max(0, state["wal"] - baseline["wal"])
            + max(0, state["shm"] - baseline["shm"])
        )
        state["growth"] = max(state["physical_growth"], state["accounted_growth"])
        state["queue"] = consumer._event_queue.qsize()
        state["rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        for key in peak:
            if key in state:
                if key in ("root_free", "zone_free"):
                    peak[key] = min(peak[key], state[key])
                else:
                    peak[key] = max(peak[key], state[key])
        persist(phase, state, force=force)
        last_disk_sample = now
        last_state = state
        return state

    await sample("admitted", force=True)

    try:
        try:
            stream = (
                FixtureStream(FIXTURE_EVENT_COUNT, int(previous) + FIXTURE_START_OFFSET_US)
                if FIXTURE_EVENT_COUNT
                else websockets.connect(
                    url,
                    max_size=10 * 1024 * 1024,
                    # The experiment deliberately saturates the bounded
                    # application queue. A protocol keepalive would classify
                    # that intentional read backpressure as a dead peer.
                    ping_interval=None,
                    close_timeout=10,
                    open_timeout=15,
                )
            )
            async with stream as ws:
                platform_health.record_connection(True)
                while True:
                    now = time.monotonic()
                    state = await sample()
                    if now >= intake_deadline:
                        stop_reason = "five_minute_total_with_shutdown_reserve"
                        break
                    if consumer._received_total >= MAX_RECEIVED:
                        stop_reason = "received_event_cap"
                        break
                    if state["growth"] >= INTAKE_ALLOC_STOP:
                        stop_reason = "allocation_shutdown_reserve"
                        break
                    if state["root_free"] <= ROOT_FLOOR:
                        stop_reason = "root_reserve"
                        break
                    if state["zone_free"] <= ZONE_FLOOR:
                        stop_reason = "zonestorage_reserve"
                        break
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    raw_received_bytes += len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
                    message = json.loads(raw)
                    stamp = message.get("time_us")
                    if not isinstance(stamp, int) or stamp <= 0:
                        raise RuntimeError("source event missing positive time_us")
                    if first_time is None:
                        first_time = stamp
                        if stamp <= int(previous):
                            raise RuntimeError("resumed boundary did not expose accepted gap")
                        gap_id = record_gap(previous, stamp)
                        persist("intake", state, force=True)
                    if last_time is not None and stamp < last_time:
                        raise RuntimeError("source cursor became non-monotonic")
                    last_time = stamp
                    await consumer._handle_message(raw)
        except Exception as exc:
            # A source/runtime failure closes intake and enters the same bounded
            # drain path as a normal cap. Preserve only the exception class;
            # error text may contain a URL or other operator-only detail.
            runtime_failure = type(exc).__name__
            stop_reason = "source_or_runtime_failure"
            await sample(
                "intake_closed_on_failure",
                force=True,
            )

        # Intake is closed. Drain only within the total five-minute envelope,
        # sampling the same hard allocation/reserve limits during shutdown.
        while consumer._event_queue._unfinished_tasks:
            state = await sample("draining")
            if state["growth"] >= MAX_ALLOC_GROWTH:
                stop_reason += "+hard_allocation_cap_during_drain"
                break
            if state["root_free"] <= ROOT_FLOOR:
                stop_reason += "+root_reserve_during_drain"
                break
            if state["zone_free"] <= ZONE_FLOOR:
                stop_reason += "+zonestorage_reserve_during_drain"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                stop_reason += "+drain_deadline"
                break
            try:
                await asyncio.wait_for(
                    consumer._event_queue.join(), timeout=min(0.5, remaining)
                )
            except asyncio.TimeoutError:
                continue
        else:
            drain_completed = True
            await sample("drain_complete", force=True)
    finally:
        with contextlib.suppress(Exception):
            await sample("closing_writer", force=True)
        consumer.stop()
        drain.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain
        await close_writer(consumer)
        platform_health.record_connection(False)

    final = await sample("closed", force=True)
    if final["growth"] > MAX_ALLOC_GROWTH:
        raise RuntimeError("hard allocation cap exceeded")
    if final["root_free"] < ROOT_FLOOR or final["zone_free"] < ZONE_FLOOR:
        raise RuntimeError("filesystem reserve crossed")

    durable = get_cursor(CONSUMER_NAME)
    fact = ops_runtime.read_fact("consumer") or {}
    fact.update({
        "coverage_continuity": False,
        "coverage_gap_id": gap_id,
        "previous_completed_cursor": previous,
        "resumed_stream_boundary": str(first_time),
        "coverage_disposition": "accepted_unrecovered_gap",
        "outputs_continuity_blocked": True,
    })
    ops_runtime.write_fact("consumer", fact)

    result = {
        "schema": "driftwatch.bounded-ingestion-result.v1",
        "qualified_image": IMAGE_ID,
        "elapsed_seconds": time.monotonic() - begun,
        "stop_reason": stop_reason,
        "runtime_failure_class": runtime_failure,
        "qualified": runtime_failure is None and drain_completed,
        "drain_completed": drain_completed,
        "previous_completed_cursor": previous,
        "resumed_stream_boundary": str(first_time) if first_time is not None else None,
        "resumed_stream_boundary_utc": iso_us(first_time) if first_time is not None else None,
        "last_received_cursor": str(last_time) if last_time is not None else None,
        "durable_completed_cursor": durable,
        "coverage_gap_id": gap_id,
        "coverage_continuity": False,
        "source_received": consumer._received_total,
        "source_admitted": consumer._admitted_total,
        "source_durably_completed": consumer._source_committed_total,
        "events_materialized": consumer._event_count,
        "replay_duplicates": consumer._replayed_total,
        "queue_waits": consumer._queue_waits_total,
        "queue_peak": peak["queue"],
        "raw_received_bytes_upper_bound": raw_received_bytes,
        "unresolved_received": consumer._received_total - consumer._source_committed_total - consumer._unadmitted_interrupted_total,
        "unadmitted_interrupted": consumer._unadmitted_interrupted_total,
        "batch_failures": consumer._batch_failures_total,
        "batch_retry_pending": consumer._batch_retry_pending,
        "events_dropped": consumer._events_dropped_total,
        "rollback_lost": consumer._rollback_lost_total,
        "baseline_allocation": baseline,
        "final_allocation": final,
        "peak": peak,
        "max_rss_kib": peak["rss_kib"],
    }
    terminal_receipt = {
        **result,
        "schema": "driftwatch.bounded-ingestion-receipt.v2",
        "run_id": RUN_ID,
        "phase": "terminal",
        "terminal": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    atomic_json(RECEIPT_PATH, terminal_receipt)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except BaseException as exc:
        update_terminal_failure(exc)
        raise
