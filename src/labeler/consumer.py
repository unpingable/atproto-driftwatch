"""Jetstream-based ATProto event consumer.

Connects to a Bluesky Jetstream endpoint (JSON over WebSocket) instead of the
raw firehose (CBOR/CAR). Filters to post and repost collections.

Jetstream docs: https://docs.bsky.app/blog/jetstream
"""

import os
import json
import asyncio
import logging
from typing import Optional
import websockets
from .db import insert_event, insert_edges, init_db, upsert_cursor, get_cursor, get_conn
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
    operation = commit.get("operation")

    # We only ingest creates and updates, not deletes
    if operation not in ("create", "update"):
        return None

    did = js.get("did", "")
    collection = commit.get("collection", "")
    rkey = commit.get("rkey", "")
    cid = commit.get("cid", "")
    record = commit.get("record", {})

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
        reply_parent = reply.get("parent", {}) if reply else {}
        reply_root = reply.get("root", {}) if reply else {}

        # Extract external links from embeds
        external_links = []
        embed = record.get("embed", {})
        if embed:
            ext = embed.get("external", {})
            if ext and ext.get("uri"):
                external_links.append(ext["uri"])
            # record-with-media embeds
            media = embed.get("media", {})
            if media:
                ext2 = media.get("external", {})
                if ext2 and ext2.get("uri"):
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

    @staticmethod
    def _get_queue_depth() -> int:
        conn = get_conn()
        n = conn.execute("SELECT COUNT(*) FROM recheck_queue").fetchone()[0]
        conn.close()
        return n

    def _process_event(self, ev: dict):
        """Synchronous DB work — called from background drain task."""
        event_uri = ev["uri"]
        author = ev["authorDid"]
        ctime = ev["createdAt"]
        insert_event(event_uri, ctime, author, ev)
        edges = extract_edges_from_event(ev)
        insert_edges(edges)

    async def _drain_queue(self):
        """Background task that drains the event queue without blocking the WS read loop."""
        from . import queue_stats
        from . import platform_health
        loop = asyncio.get_event_loop()
        last_stats_ts = asyncio.get_event_loop().time()
        _disk_brake_logged = False

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

            try:
                ev = await asyncio.wait_for(self._event_queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                ev = None
            except asyncio.CancelledError:
                break

            if ev is not None:
                try:
                    await loop.run_in_executor(None, self._process_event, ev)
                except Exception:
                    LOG.exception("failed to process event")

                queue_stats.inc("events_in")
                self._event_count += 1

                if self._event_count % CURSOR_SAVE_INTERVAL == 0:
                    if self._last_cursor:
                        await loop.run_in_executor(None, upsert_cursor, CONSUMER_NAME, self._last_cursor)

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
                # Snapshot and reset drop counter (before record_window uses it)
                dropped = self._events_dropped
                self._events_dropped = 0

                # Platform health watermark
                backlog = self._event_queue.qsize()
                health_snap = platform_health.record_window(
                    snap["events_in"], snap["window_secs"], backlog,
                    dropped=dropped,
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
                    "dropped=%d "
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
