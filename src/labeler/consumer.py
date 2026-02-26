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
        loop = asyncio.get_event_loop()
        last_stats_ts = asyncio.get_event_loop().time()

        while not self._stop:
            try:
                ev = await self._event_queue.get()
            except asyncio.CancelledError:
                break
            try:
                await loop.run_in_executor(None, self._process_event, ev)
            except Exception:
                LOG.exception("failed to process event")

            queue_stats.inc("events_in")
            self._event_count += 1

            if self._event_count % CURSOR_SAVE_INTERVAL == 0:
                if self._last_cursor:
                    await loop.run_in_executor(None, upsert_cursor, CONSUMER_NAME, self._last_cursor)

            # Per-minute stats line
            now_mono = loop.time()
            if now_mono - last_stats_ts >= 60:
                last_stats_ts = now_mono
                snap = queue_stats.snapshot_and_reset()
                try:
                    depth = await loop.run_in_executor(None, self._get_queue_depth)
                except Exception:
                    depth = -1
                median_age = snap.get("median_dequeue_age_secs", -1)
                # Build kind distribution string
                kind_counts = {
                    k.split(":", 1)[1]: v
                    for k, v in snap.items()
                    if k.startswith("kind:") and v
                }
                total_kinds = sum(kind_counts.values()) or 1
                kinds_str = ",".join(
                    f"{k[0].upper()}:{round(100*v/total_kinds)}%"
                    for k, v in sorted(kind_counts.items())
                ) or "n/a"
                LOG.info(
                    "STATS window=%.0fs events_in=%d claims=%d "
                    "enq_attempt=%d enq_insert=%d enq_ignore=%d enq_gated=%d "
                    "dequeued=%d queue_depth=%d median_age=%.0fs backlog=%d "
                    "kinds=%s",
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
                    self._event_queue.qsize(),
                    kinds_str,
                )

    async def _handle_message(self, raw: str):
        try:
            js = json.loads(raw)
        except Exception:
            LOG.warning("failed to parse JSON message, skipping")
            return

        # Track cursor for resume
        time_us = js.get("time_us")
        if time_us:
            self._last_cursor = str(time_us)

        # Transform to canonical event
        ev = _jetstream_to_event(js)
        if ev is None:
            return

        # Put on queue — non-blocking so WS read loop stays responsive for pings
        await self._event_queue.put(ev)

    async def run(self):
        """Connect to Jetstream and process messages with reconnect resilience."""
        init_db()
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
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=10,
                ) as ws:
                    LOG.info("connected to Jetstream")
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

    def stop(self):
        self._stop = True


def run_consumer_blocking():
    loop = asyncio.get_event_loop()
    consumer = ATProtoConsumer()
    try:
        loop.run_until_complete(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()
