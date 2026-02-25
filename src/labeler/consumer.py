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
from .db import insert_event, insert_edges, init_db, upsert_cursor, get_cursor
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

        event_uri = ev["uri"]
        author = ev["authorDid"]
        ctime = ev["createdAt"]

        insert_event(event_uri, ctime, author, ev)

        edges = extract_edges_from_event(ev)
        insert_edges(edges)

        self._event_count += 1

        # Persist cursor periodically
        if self._event_count % CURSOR_SAVE_INTERVAL == 0:
            if self._last_cursor:
                upsert_cursor(CONSUMER_NAME, self._last_cursor)
            if self._event_count % (CURSOR_SAVE_INTERVAL * 10) == 0:
                LOG.info("ingested %d events, cursor=%s", self._event_count, self._last_cursor)

    async def run(self):
        """Connect to Jetstream and process messages with reconnect resilience."""
        init_db()
        saved_cursor = get_cursor(CONSUMER_NAME)
        ws_url = _build_ws_url(self.ws_url, cursor=saved_cursor)
        LOG.info("starting Jetstream consumer, url=%s", ws_url)

        while not self._stop:
            try:
                url = _build_ws_url(self.ws_url, cursor=self._last_cursor or saved_cursor)
                async with websockets.connect(url, max_size=10 * 1024 * 1024) as ws:
                    LOG.info("connected to Jetstream")
                    async for msg in ws:
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                LOG.exception("Jetstream connection error, reconnecting in 5s")
                await asyncio.sleep(5)

        # Save cursor on shutdown
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
