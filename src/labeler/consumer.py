"""Jetstream-based ATProto event consumer.

Connects to a Bluesky Jetstream endpoint (JSON over WebSocket) instead of the
raw firehose (CBOR/CAR). Filters to post and repost collections.

Jetstream docs: https://docs.bsky.app/blog/jetstream
"""

import os
import json
import time
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import websockets
from .db import insert_event, insert_edges, insert_event_txn, insert_edges_txn, init_db, upsert_cursor, get_cursor, get_conn
from .extractor import extract_edges_from_event
from . import timeutil

LOG = logging.getLogger("labeler.consumer")

JETSTREAM_WS = os.getenv(
    "FIREHOSE_WS_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe",
)
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "driftwatch_consumer")
WANTED_COLLECTIONS = os.getenv(
    "JETSTREAM_COLLECTIONS",
    "app.bsky.feed.post,app.bsky.feed.repost",
).split(",")

# Cursor persistence interval (every N events)
CURSOR_SAVE_INTERVAL = int(os.getenv("CURSOR_SAVE_INTERVAL", "500"))

# Batched-write knobs: drain up to N events per writer transaction, or wait at
# most M seconds for the batch to fill. One commit per batch.
BATCH_MAX_EVENTS = int(os.getenv("BATCH_MAX_EVENTS", "100"))
BATCH_MAX_WAIT_S = float(os.getenv("BATCH_MAX_WAIT_S", "0.25"))

# Writer-owned WAL truncate. The persistent writer thread calls
# wal_checkpoint(TRUNCATE) on its own connection right after a successful
# commit — that's the cleanest moment to attempt a WAL restart, since the
# writer just released its frame and is least likely to be racing against
# concurrent readers. Rate-limited so per-batch overhead is bounded.
WAL_TRUNCATE_INTERVAL_S = float(os.getenv("WAL_TRUNCATE_INTERVAL_S", "30"))


def _build_ws_url(base_url: str, cursor: Optional[str] = None) -> str:
    """Append wantedCollections and optional cursor to the Jetstream URL."""
    params = []
    for col in WANTED_COLLECTIONS:
        col = col.strip()
        if col:
            params.append(f"wantedCollections={col}")
    if cursor:
        params.append(f"cursor={cursor}")
    if params:
        sep = "&" if "?" in base_url else "?"
        return base_url + sep + "&".join(params)
    return base_url


def _jetstream_to_event(js: dict) -> Optional[dict]:
    """Transform a Jetstream commit event into the canonical event dict
    that the rest of the pipeline (insert_event, extract_edges, claims) expects.

    Returns None for events we don't care about (identity, account, deletes).
    """
    if js.get("kind") != "commit":
        return None

    commit = js.get("commit", {})
    if not isinstance(commit, dict):
        return None
    operation = commit.get("operation")

    # We only ingest creates and updates, not deletes
    if operation not in ("create", "update"):
        return None

    did = js.get("did", "")
    collection = commit.get("collection", "")
    rkey = commit.get("rkey", "")
    cid = commit.get("cid", "")
    record = commit.get("record", {})
    if not isinstance(record, dict):
        record = {}

    # Build AT URI: at://{did}/{collection}/{rkey}
    uri = f"at://{did}/{collection}/{rkey}"

    # Convert Jetstream time_us (microseconds) to ISO timestamp
    time_us = js.get("time_us")
    if time_us:
        ctime = timeutil.to_utc_iso(time_us / 1_000_000)
    else:
        ctime = timeutil.now_utc().isoformat()

    if collection == "app.bsky.feed.post":
        # Extract reply pointers
        reply = record.get("reply", {})
        reply_parent = reply.get("parent", {}) if isinstance(reply, dict) else {}
        reply_root = reply.get("root", {}) if isinstance(reply, dict) else {}
        if not isinstance(reply_parent, dict):
            reply_parent = {}
        if not isinstance(reply_root, dict):
            reply_root = {}

        # Extract external links from embeds (defensive: any field could be non-dict)
        external_links = []
        embed = record.get("embed", {})
        if isinstance(embed, dict):
            ext = embed.get("external", {})
            if isinstance(ext, dict) and ext.get("uri"):
                external_links.append(ext["uri"])
            # record-with-media embeds
            media = embed.get("media", {})
            if isinstance(media, dict):
                ext2 = media.get("external", {})
                if isinstance(ext2, dict) and ext2.get("uri"):
                    external_links.append(ext2["uri"])

        return {
            "uri": uri,
            "cid": cid,
            "text": record.get("text", ""),
            "authorDid": did,
            "createdAt": record.get("createdAt", ctime),
            "replyParentUri": reply_parent.get("uri"),
            "replyRootUri": reply_root.get("uri"),
            "facets": record.get("facets", []),
            "embeds": [embed] if embed else [],
            "externalLinks": external_links,
            # Keep the raw record for edge extraction
            "record": record,
            "_collection": collection,
            "_operation": operation,
        }

    elif collection == "app.bsky.feed.repost":
        subject = record.get("subject", {})
        if not isinstance(subject, dict):
            subject = {}
        return {
            "uri": uri,
            "cid": cid,
            "text": "",
            "authorDid": did,
            "createdAt": record.get("createdAt", ctime),
            "replyParentUri": None,
            "replyRootUri": None,
            "facets": [],
            "embeds": [],
            "externalLinks": [],
            "record": record,
            "type": "repost",
            "subject": subject,
            "_collection": collection,
            "_operation": operation,
        }

    return None


class ATProtoConsumer:
    def __init__(self, ws_url: Optional[str] = None):
        self.ws_url = ws_url or JETSTREAM_WS
        self._stop = False
        self._event_count = 0
        self._last_cursor: Optional[str] = None
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)
        self._events_dropped = 0
        # Single-thread writer: ensures the persistent SQLite conn is only
        # ever touched from one OS thread (sqlite3 default check_same_thread).
        self._writer_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="dw-writer"
        )
        self._writer_conn = None  # lazily opened inside the writer thread
        self._last_wal_truncate_mono = 0.0  # writer thread only
        # Counts events shed by the writer when a batch hits a write-lock
        # conflict (e.g. retention holding the lock) and rolls back. Tracked
        # alongside queue-overflow drops so the platform_health gate sees
        # lock-conflict shedding too — a green recovery flag must not hide it.
        self._events_lost_to_rollback = 0  # main thread only

    @staticmethod
    def _get_queue_depth() -> int:
        conn = get_conn()
        n = conn.execute("SELECT COUNT(*) FROM recheck_queue").fetchone()[0]
        conn.close()
        return n

    def _get_writer_conn(self):
        """Return the persistent writer connection, opening it on first call.

        Must only be called from the writer executor thread.
        """
        if self._writer_conn is None:
            self._writer_conn = get_conn()
        return self._writer_conn

    def _process_batch(self, batch):
        """Synchronous DB work for a batch of events. Runs in the writer thread.

        One transaction, one commit per batch. On any exception, rolls back
        the entire batch — we'd rather lose a batch than half-write it.

        Returns (written, lost_to_rollback). lost_to_rollback is non-zero
        only when the batch failed; it must count against intake health so
        the platform_health gate sees lock-conflict shedding.
        """
        if not batch:
            return (0, 0)
        conn = self._get_writer_conn()
        try:
            for ev in batch:
                event_uri = ev["uri"]
                author = ev["authorDid"]
                ctime = ev["createdAt"]
                insert_event_txn(conn, event_uri, ctime, author, ev)
                edges = extract_edges_from_event(ev)
                insert_edges_txn(conn, edges)
            conn.commit()
            self._maybe_wal_truncate(conn)
            return (len(batch), 0)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                LOG.exception("rollback failed after batch error")
            LOG.exception("batch failed; rolled back %d events", len(batch))
            return (0, len(batch))

    def _maybe_wal_truncate(self, conn):
        """Attempt PRAGMA wal_checkpoint(TRUNCATE) from the writer thread.

        Called only from inside _process_batch after a successful commit —
        the writer just released its frame, the least racy moment to attempt
        a WAL restart. Rate-limited so the cost is bounded.

        Logs only when the result is interesting (busy or non-trivial work
        done); silent on the typical no-op case.
        """
        now = time.monotonic()
        if now - self._last_wal_truncate_mono < WAL_TRUNCATE_INTERVAL_S:
            return
        self._last_wal_truncate_mono = now
        try:
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if not row:
                return
            busy, log, ckpt = row
            if busy or log >= 1000:
                LOG.info(
                    "wal_truncate: busy=%d log=%d checkpointed=%d",
                    busy, log, ckpt,
                )
        except Exception:
            LOG.debug("wal_truncate failed", exc_info=True)

    async def _drain_queue(self):
        """Background task that drains the event queue without blocking the WS read loop.

        Pulls events in batches (up to BATCH_MAX_EVENTS or BATCH_MAX_WAIT_S)
        and hands each batch to the dedicated writer thread, which runs the
        whole batch in a single SQLite transaction with one commit.
        """
        from . import queue_stats
        from . import platform_health
        loop = asyncio.get_event_loop()
        last_stats_ts = asyncio.get_event_loop().time()
        _disk_brake_logged = False
        events_since_cursor_save = 0

        while not self._stop:
            # Emergency brake: pause ingest when disk is critical
            try:
                from .maintenance import is_disk_pressure
                if is_disk_pressure():
                    if not _disk_brake_logged:
                        LOG.error("DISK PRESSURE: pausing event processing until brake released")
                        _disk_brake_logged = True
                    await asyncio.sleep(10)
                    continue
                elif _disk_brake_logged:
                    LOG.info("DISK PRESSURE: cleared, resuming event processing")
                    _disk_brake_logged = False
            except ImportError:
                pass

            # Build a batch: first event blocks (with a sane timeout); subsequent
            # events are pulled non-blockingly until cap or short-wait deadline.
            batch = []
            try:
                first = await asyncio.wait_for(self._event_queue.get(), timeout=10.0)
                batch.append(first)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

            if batch:
                deadline = loop.time() + BATCH_MAX_WAIT_S
                while len(batch) < BATCH_MAX_EVENTS:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        ev = self._event_queue.get_nowait()
                        batch.append(ev)
                    except asyncio.QueueEmpty:
                        # Wait briefly for the next event; bail when deadline hits.
                        try:
                            ev = await asyncio.wait_for(
                                self._event_queue.get(), timeout=remaining
                            )
                            batch.append(ev)
                        except asyncio.TimeoutError:
                            break
                        except asyncio.CancelledError:
                            self._stop = True
                            break

                try:
                    written, lost = await loop.run_in_executor(
                        self._writer_executor, self._process_batch, batch
                    )
                except Exception:
                    LOG.exception("failed to process batch")
                    written, lost = 0, 0

                if lost:
                    # Database-locked rollbacks are intake loss; platform_health
                    # must see them so the recovery gate cannot hide them.
                    self._events_lost_to_rollback += lost

                if written:
                    queue_stats.inc("events_in", written)
                    self._event_count += written
                    events_since_cursor_save += written

                    # Save cursor after a successful commit, every
                    # CURSOR_SAVE_INTERVAL events. last_cursor is the
                    # high-watermark from the WS read loop.
                    if events_since_cursor_save >= CURSOR_SAVE_INTERVAL and self._last_cursor:
                        await loop.run_in_executor(None, upsert_cursor, CONSUMER_NAME, self._last_cursor)
                        events_since_cursor_save = 0

            # Per-minute stats line (fires even when idle)
            now_mono = loop.time()
            if now_mono - last_stats_ts >= 60:
                last_stats_ts = now_mono
                snap = queue_stats.snapshot_and_reset()
                try:
                    depth = await loop.run_in_executor(None, self._get_queue_depth)
                except Exception:
                    depth = -1
                median_age = snap.get("median_dequeue_age_secs", -1)
                # Build kind distribution string (always include unknown as canary)
                kind_counts = {
                    k.split(":", 1)[1]: v
                    for k, v in snap.items()
                    if k.startswith("kind:") and v
                }
                total_kinds = sum(kind_counts.values()) or 1
                unknown_pct = round(100 * kind_counts.get("unknown", 0) / total_kinds)
                if unknown_pct > 5:
                    LOG.warning(
                        "CANARY fp_kind unknown=%d%% (%d/%d) — "
                        "check extractor health or schema migration",
                        unknown_pct,
                        kind_counts.get("unknown", 0),
                        total_kinds,
                    )
                kinds_str = ",".join(
                    f"{k[0].upper()}:{round(100*v/total_kinds)}%"
                    for k, v in sorted(kind_counts.items())
                ) or "n/a"
                # Snapshot and reset drop counters (before record_window uses them)
                dropped = self._events_dropped
                self._events_dropped = 0
                rollback_lost = self._events_lost_to_rollback
                self._events_lost_to_rollback = 0
                # Both queue-overflow and lock-conflict rollbacks count as
                # intake loss for health purposes.
                total_lost = dropped + rollback_lost

                # Platform health watermark
                backlog = self._event_queue.qsize()
                health_snap = platform_health.record_window(
                    snap["events_in"], snap["window_secs"], backlog,
                    dropped=total_lost,
                )
                health_state = health_snap["health_state"]
                coverage_str = (
                    "n/a" if health_state == "warming_up"
                    else f"{health_snap['coverage_pct'] * 100:.1f}%"
                )
                # Show primary gate reason in STATS (priority order)
                gate_reasons = health_snap.get("gate_reasons", [])
                if gate_reasons:
                    # Priority: consumer_backlog > lag_high > platform_low_eps
                    priority = ["consumer_backlog", "lag_high", "platform_low_eps"]
                    primary = next((r for r in priority if r in gate_reasons), gate_reasons[0])
                    health_display = f"degraded({primary})"
                else:
                    health_display = health_state
                # Disk pressure (cheap check, once per minute)
                try:
                    from .maintenance import check_disk_pressure
                    dp = check_disk_pressure()
                    disk_str = f"{dp['used_pct']}%({dp['free_gb']}GB)"
                except Exception:
                    disk_str = "n/a"

                # DB file size
                try:
                    from .db import DATA_DIR as _dd
                    _db = _dd / "labeler.sqlite"
                    db_mb = _db.stat().st_size / (1024 * 1024) if _db.exists() else 0
                    db_str = f"{db_mb:.0f}MB"
                except Exception:
                    db_str = "n/a"

                LOG.info(
                    "STATS window=%.0fs events_in=%d claims=%d "
                    "enq_attempt=%d enq_insert=%d enq_ignore=%d enq_gated=%d "
                    "dequeued=%d queue_depth=%d median_age=%.0fs backlog=%d "
                    "dropped=%d rollback_lost=%d "
                    "kinds=%s coverage=%s health=%s baseline_eps=%.1f lag=%.1fs "
                    "disk=%s db=%s",
                    snap["window_secs"],
                    snap["events_in"],
                    snap["claims_written"],
                    snap["enqueue_attempts"],
                    snap["enqueue_inserted"],
                    snap["enqueue_ignored"],
                    snap["enqueue_gated"],
                    snap["dequeued"],
                    depth,
                    median_age,
                    backlog,
                    dropped,
                    rollback_lost,
                    kinds_str,
                    coverage_str,
                    health_display,
                    health_snap["baseline_eps"],
                    health_snap["stream_lag_s"],
                    disk_str,
                    db_str,
                )

                # Checkpoint baseline periodically
                try:
                    platform_health.maybe_checkpoint()
                except Exception:
                    pass

                # Resolve unresolved DIDs (M2 PDS enrichment sidecar)
                try:
                    from .resolver import resolve_batch
                    rstats = await loop.run_in_executor(None, resolve_batch)
                    if rstats["resolved"] > 0:
                        LOG.info(
                            "RESOLVER resolved=%d ok=%d not_found=%d error=%d",
                            rstats["resolved"], rstats["ok"],
                            rstats["not_found"], rstats["error"],
                        )
                except Exception:
                    LOG.debug("resolver batch failed", exc_info=True)

    async def _handle_message(self, raw: str):
        try:
            js = json.loads(raw)
        except Exception:
            LOG.warning("failed to parse JSON message, skipping")
            return

        # Track cursor for resume and lag
        time_us = js.get("time_us")
        if time_us:
            self._last_cursor = str(time_us)
            try:
                from . import platform_health
                platform_health.record_event_time(time_us)
            except Exception:
                pass

        # Identity/account events: capture and reduce
        kind = js.get("kind")
        if kind in ("identity", "account"):
            try:
                from .identity import parse_identity_event, apply_identity_event
                delta = parse_identity_event(js)
                if delta is not None:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, apply_identity_event, delta)
            except Exception:
                LOG.debug("identity event processing failed", exc_info=True)
            return

        # Transform to canonical event
        ev = _jetstream_to_event(js)
        if ev is None:
            return

        # Non-blocking put — drop events when queue is full rather than
        # blocking the event loop (which kills WS pings → reconnect churn)
        try:
            self._event_queue.put_nowait(ev)
        except asyncio.QueueFull:
            self._events_dropped += 1

    async def run(self):
        """Connect to Jetstream and process messages with reconnect resilience."""
        init_db()

        # Restore baseline from checkpoint (avoid cold start on restart)
        try:
            from . import platform_health
            platform_health.restore_baseline()
        except Exception:
            LOG.debug("baseline restore skipped (no checkpoint or error)")

        saved_cursor = get_cursor(CONSUMER_NAME)
        ws_url = _build_ws_url(self.ws_url, cursor=saved_cursor)
        LOG.info("starting Jetstream consumer, url=%s", ws_url)

        # Start background drain task
        drain_task = asyncio.ensure_future(self._drain_queue())

        while not self._stop:
            try:
                url = _build_ws_url(self.ws_url, cursor=self._last_cursor or saved_cursor)
                async with websockets.connect(
                    url,
                    max_size=10 * 1024 * 1024,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10,
                ) as ws:
                    LOG.info("connected to Jetstream")
                    try:
                        from . import platform_health
                        platform_health.record_reconnect()
                    except Exception:
                        pass
                    async for msg in ws:
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                LOG.exception("Jetstream connection error, reconnecting in 5s")
                await asyncio.sleep(5)

        # Graceful shutdown: cancel drain, then close the writer thread.
        try:
            drain_task.cancel()
        except Exception:
            pass

        def _close_writer():
            if self._writer_conn is not None:
                try:
                    self._writer_conn.close()
                except Exception:
                    LOG.debug("writer conn close failed", exc_info=True)
                self._writer_conn = None

        try:
            await asyncio.get_event_loop().run_in_executor(
                self._writer_executor, _close_writer
            )
        except Exception:
            LOG.debug("writer close hop failed", exc_info=True)
        finally:
            self._writer_executor.shutdown(wait=True, cancel_futures=False)

        # Cleanup
        drain_task.cancel()
        if self._last_cursor:
            upsert_cursor(CONSUMER_NAME, self._last_cursor)
            LOG.info("saved cursor on shutdown: %s", self._last_cursor)
        try:
            from . import platform_health
            platform_health.force_checkpoint()
            LOG.info("baseline checkpoint saved on shutdown")
        except Exception:
            pass

    def stop(self):
        self._stop = True


def run_consumer_blocking():
    loop = asyncio.get_event_loop()
    consumer = ATProtoConsumer()
    try:
        loop.run_until_complete(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()
