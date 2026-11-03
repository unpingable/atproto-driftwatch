#!/usr/bin/env python3
"""Bounded, body-free Jetstream connection probe.

The probe never opens an application database and retains only counters,
source timestamps, timings, and sanitized close-frame metadata.
"""

import argparse
import asyncio
import json
import time
from urllib.parse import urlencode, urlsplit, urlunsplit

import websockets

from labeler.connection_diagnostics import connection_failure_record


def _url(base, cursor):
    parts = urlsplit(base)
    query = [
        ("wantedCollections", "app.bsky.feed.post"),
        ("wantedCollections", "app.bsky.feed.repost"),
    ]
    if cursor:
        query.append(("cursor", cursor))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


async def probe(args):
    started = time.monotonic()
    count = 0
    raw_bytes = 0
    first_cursor = None
    last_cursor = None
    max_receive_wait = 0.0
    max_processing = 0.0
    ws = None
    outcome = "completed_budget"
    failure = None
    try:
        async with websockets.connect(
            _url(args.endpoint, args.cursor),
            max_size=10 * 1024 * 1024,
            ping_interval=None,
            close_timeout=10,
        ) as ws:
            while count < args.max_events:
                remaining = args.max_seconds - (time.monotonic() - started)
                if remaining <= 0:
                    break
                before = time.monotonic()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 30.0))
                except asyncio.TimeoutError:
                    # A bounded wait is a probe outcome, not a transport claim.
                    outcome = "receive_wait_budget"
                    break
                max_receive_wait = max(max_receive_wait, time.monotonic() - before)
                processing_started = time.monotonic()
                obj = json.loads(raw)
                source_cursor = obj.get("time_us")
                if isinstance(source_cursor, int) and not isinstance(source_cursor, bool):
                    first_cursor = source_cursor if first_cursor is None else first_cursor
                    last_cursor = source_cursor
                count += 1
                raw_bytes += len(raw.encode("utf-8") if isinstance(raw, str) else raw)
                if args.processing_delay_ms:
                    await asyncio.sleep(args.processing_delay_ms / 1000)
                max_processing = max(max_processing, time.monotonic() - processing_started)
    except Exception as exc:
        outcome = "connection_failure"
        failure = connection_failure_record(exc, ws)
    return {
        "schema": "driftwatch.jetstream-connection-probe.v1",
        "outcome": outcome,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "events": count,
        "raw_bytes": raw_bytes,
        "first_source_cursor": first_cursor,
        "last_source_cursor": last_cursor,
        "max_receive_wait_seconds": round(max_receive_wait, 6),
        "max_processing_seconds": round(max_processing, 6),
        "processing_delay_ms": args.processing_delay_ms,
        "failure": failure,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="wss://jetstream2.us-east.bsky.network/subscribe")
    parser.add_argument("--cursor")
    parser.add_argument("--max-seconds", type=float, required=True)
    parser.add_argument("--max-events", type=int, required=True)
    parser.add_argument("--processing-delay-ms", type=float, default=0.0)
    args = parser.parse_args()
    if not (0 < args.max_seconds <= 300 and 0 < args.max_events <= 250_000):
        parser.error("bounds exceed the diagnostic ceiling")
    if not (0 <= args.processing_delay_ms <= 1000):
        parser.error("processing delay outside bounded range")
    print(json.dumps(asyncio.run(probe(args)), sort_keys=True))


if __name__ == "__main__":
    main()
