"""Jetstream-based ATProto event consumer.

Connects to a Bluesky Jetstream endpoint (JSON over WebSocket) instead of the
raw firehose (CBOR/CAR). Filters to post and repost collections.

Jetstream docs: https://docs.bsky.app/blog/jetstream
"""

import os
import json
import hashlib
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
REPLAY_REWIND_US = 5_000_000
REPLAY_RECEIPT_LIMIT = 100_000

# Writer-owned WAL truncate. Rate-limited so per-batch overhead is bounded.
WAL_TRUNCATE_INTERVAL_S = float(os.getenv("WAL_TRUNCATE_INTERVAL_S", "30"))

# When the writer is under intake pressure, attempting wal_checkpoint(TRUNCATE)
# blocks up to busy_timeout (60s) waiting for readers to release. That blocking
# IS the failure mode under multi-subsystem reader concurrency: the writer
# parks in _maybe_wal_truncate while events_dropped grows at the queue
# boundary. Skip the truncate when intake is backlogged. The writer
# prioritizes intake over filesystem tidiness.
WAL_TRUNCATE_PRESSURE_BACKLOG = int(os.getenv("WAL_TRUNCATE_PRESSURE_BACKLOG", "500"))

# In calm windows, prefer PASSIVE checkpoint (non-blocking, no busy wait) and
# only escalate to TRUNCATE if PASSIVE reports busy=0 (all readers caught up)
# AND log size justifies the work.
WAL_TRUNCATE_LOG_FRAMES_MIN = int(os.getenv("WAL_TRUNCATE_LOG_FRAMES_MIN", "5000"))


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
        self._events_lost_to_rollback = 0  # main thread only; reset per window
        # Cumulative cousins — never reset. The retention scheduler reads
        # these as a tripwire: any non-zero delta during a retention pass
        # means the scheduler is too aggressive and the pass must abort.
        self._rollback_lost_total = 0
        self._events_dropped_total = 0
        self._received_total = 0
        self._admitted_total = 0
        self._queue_waits_total = 0
        self._source_committed_total = 0
        self._replayed_total = 0
        self._resume_cursor = None
        self._batch_failures_total = 0
        self._batch_retry_pending = False
        self._admission_pending = False
        self._unadmitted_interrupted_total = 0

    def _write_ops_snapshot(self):
        """Publish current producer facts without classifying attention."""
        try:
            from . import ops_runtime, platform_health
            snap = platform_health.get_health_snapshot()
            snap.update({
                "intake_queue_depth": self._event_queue.qsize(),
                "intake_queue_capacity": self._event_queue.maxsize,
                "events_committed_total": self._event_count,
                "events_dropped_total": self._events_dropped_total,
                "rollback_lost_total": self._rollback_lost_total,
                "last_cursor": self._last_cursor,
                "durable_resume_cursor": self._resume_cursor,
                "source_received_total": self._received_total,
                "source_admitted_total": self._admitted_total,
                "source_completed_total": self._source_committed_total,
                "replay_duplicates_total": self._replayed_total,
                "queue_waits_total": self._queue_waits_total,
                "unresolved_admitted": self._admitted_total - self._source_committed_total,
                "batch_failures_total": self._batch_failures_total,
                "batch_retry_pending": self._batch_retry_pending,
                "admission_pending": self._admission_pending,
                "unadmitted_interrupted_total": self._unadmitted_interrupted_total,
                "unresolved_received": self._received_total - self._source_committed_total - self._unadmitted_interrupted_total,
                "unadmitted_pending": self._received_total - self._admitted_total - self._unadmitted_interrupted_total,
            })
            ops_runtime.write_fact("consumer", snap)
        except Exception:
            LOG.debug("consumer ops fact write failed", exc_info=True)

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
            self._writer_conn.execute("CREATE TABLE IF NOT EXISTS ingest_receipts (source TEXT NOT NULL, time_us INTEGER NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(source,time_us,digest))")
            self._writer_conn.execute("CREATE TABLE IF NOT EXISTS ingest_source (consumer TEXT PRIMARY KEY, source TEXT NOT NULL)")
            self._writer_conn.execute("INSERT OR IGNORE INTO ingest_source VALUES (?,?)", (CONSUMER_NAME, self.ws_url))
            source = self._writer_conn.execute("SELECT source FROM ingest_source WHERE consumer=?", (CONSUMER_NAME,)).fetchone()[0]
            if source != self.ws_url:
                self._writer_conn.rollback()
                self._writer_conn.close()
                self._writer_conn = None
                raise ValueError("source change requires explicit replay custody reconciliation")
            self._writer_conn.commit()
        return self._writer_conn

    def _process_batch(self, batch):
        """Synchronous DB work for a batch of events. Runs in the writer thread.

        One transaction covers source receipts, required processing and resume
        state. The drain retains failed batches and retries without advancing.

        Returns (written, unresolved_batch_size). Failure is replay debt,
        not evidence of irreversible loss.
        """
        if not batch:
            return (0, 0)
        conn = self._get_writer_conn()
        try:
            written = duplicates = completed = 0
            row = conn.execute("SELECT cursor FROM cursors WHERE consumer=?", (CONSUMER_NAME,)).fetchone()
            checkpoint = row[0] if row else None
            frontier = int(checkpoint) + REPLAY_REWIND_US if checkpoint else 0
            for item in batch:
                if "_source" in item:
                    js = item["_source"]
                    if js.get("kind") not in ("commit", "identity", "account"):
                        raise ValueError("unsupported source event kind")
                    timestamp = js["time_us"]
                    digest = hashlib.sha256(json.dumps(js, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                    # Receipt expiry is safe only because older input is refused,
                    # never silently reprocessed or counted as successful replay.
                    if checkpoint and timestamp < int(checkpoint):
                        raise ValueError("source event precedes durable replay boundary")
                    exists = conn.execute("SELECT 1 FROM ingest_receipts WHERE source=? AND time_us=? AND digest=?", (self.ws_url, timestamp, digest)).fetchone()
                    completed += 1
                    if exists:
                        duplicates += 1
                        continue
                    if js.get("kind") in ("identity", "account"):
                        from .identity import parse_identity_event, apply_identity_event
                        delta = parse_identity_event(js)
                        if delta is None:
                            raise ValueError("invalid identity/account event")
                        apply_identity_event(delta, connection=conn)
                        ev = None
                    else:
                        ev = _jetstream_to_event(js)
                    conn.execute("INSERT INTO ingest_receipts VALUES (?,?,?)", (self.ws_url, timestamp, digest))
                    frontier = max(frontier, timestamp)
                else:
                    ev = item  # Existing offline callers have no stream cursor.
                if ev is None:
                    continue
                event_uri = ev["uri"]
                author = ev["authorDid"]
                ctime = ev["createdAt"]
                inserted, updated = insert_event_txn(conn, event_uri, ctime, author, ev, strict=True)
                if inserted or updated:
                    edges = extract_edges_from_event(ev)
                    insert_edges_txn(conn, edges)
                written += 1
            if completed:
                resume = str(max(0, frontier - REPLAY_REWIND_US))
                conn.execute("INSERT INTO cursors VALUES (?,?,?) ON CONFLICT(consumer) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at", (CONSUMER_NAME, resume, timeutil.now_utc().isoformat()))
                conn.execute("DELETE FROM ingest_receipts WHERE source=? AND time_us<?", (self.ws_url, int(resume)))
                if conn.execute("SELECT count(*) FROM ingest_receipts WHERE source=?", (self.ws_url,)).fetchone()[0] > REPLAY_RECEIPT_LIMIT:
                    raise ValueError("replay receipt capacity exceeded; checkpoint held")
            conn.commit()
            self._batch_retry_pending = False
            if completed:
                self._resume_cursor = resume
                self._source_committed_total += completed
                self._replayed_total += duplicates
            self._maybe_wal_truncate(conn)
            return (written, 0)
        except Exception:
            self._batch_failures_total += 1
            self._batch_retry_pending = True
            try:
                conn.rollback()
            except Exception:
                LOG.exception("rollback failed after batch error")
            LOG.exception("batch failed; rolled back %d events", len(batch))
            return (0, len(batch))

    async def submit_mutation(self, fn, *args, **kwargs):
        """Submit a mutation job to run inside the writer thread.

        Single-writer invariant: any code that mutates the labeler DB
        outside of event-batch processing must route through this method.
        The writer executor serializes mutation jobs with event-batch jobs
        (one _process_batch or one mutation runs at a time, never both).

        fn signature: ``fn(writer_conn, *args, **kwargs) -> result``. Runs
        in self._writer_executor and returns the result via awaitable.

        The writer connection is shared and persistent. fn must:
          * call conn.commit() (or rollback) before returning
          * leave the connection in autocommit-ish state (no dangling txn)
          * not close the connection
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._writer_executor, self._run_mutation, fn, args, kwargs
        )

    def _run_mutation(self, fn, args, kwargs):
        """Run a mutation job inside the writer thread."""
        conn = self._get_writer_conn()
        return fn(conn, *args, **kwargs)

    def get_ingest_backlog(self) -> int:
        """Current ingest queue depth. Background mutation paths (e.g.
        retention) read this for cooperative scheduling — yield more
        aggressively when ingest is under pressure.
        """
        return self._event_queue.qsize()

    def get_pressure_snapshot(self) -> dict:
        """All pressure signals retention reads, sampled at one moment.

        ``rollback_lost_total`` and ``events_dropped_total`` are cumulative
        and never reset, so the retention scheduler can detect any delta
        during a pass — that's the tripwire that says "the scheduler is
        too aggressive, fall back."
        """
        backlog = self._event_queue.qsize()
        try:
            queue_max = self._event_queue.maxsize
        except Exception:
            queue_max = 0
        try:
            from . import queue_stats as _qs
            median_age = _qs._gauges.get("median_dequeue_age_secs", 0.0)
        except Exception:
            median_age = 0.0
        try:
            from . import platform_health as _ph
            health_snap = _ph.get_health_snapshot()
            stream_lag_s = health_snap.get("stream_lag_s", 0.0) or 0.0
        except Exception:
            stream_lag_s = 0.0
        return {
            "backlog": backlog,
            "queue_max": queue_max,
            "median_dequeue_age_s": float(median_age),
            "stream_lag_s": float(stream_lag_s),
            "rollback_lost_total": self._rollback_lost_total,
            "events_dropped_total": self._events_dropped_total,
        }

    def _maybe_wal_truncate(self, conn):
        """Maintain WAL size from the writer thread without blocking intake.

        Called from _process_batch after a successful commit, rate-limited.

        Behavior:
        * If the ingest queue is non-trivially backlogged, skip entirely.
          Writer prioritizes intake over filesystem tidiness; under reader
          concurrency, wal_checkpoint(TRUNCATE) blocks the writer up to
          busy_timeout, which IS the stall mode we are avoiding.
        * Otherwise issue PASSIVE checkpoint (non-blocking — never waits on
          readers, just advances the checkpoint frame as far as the live
          snapshot allows).
        * Only when PASSIVE reports busy=0 (no reader is pinning frames)
          AND log frames are large enough to justify the write, escalate
          to TRUNCATE. busy=0 is the proof that TRUNCATE will not block.
        """
        now = time.monotonic()
        if now - self._last_wal_truncate_mono < WAL_TRUNCATE_INTERVAL_S:
            return
        self._last_wal_truncate_mono = now

        backlog = self._event_queue.qsize()
        if backlog > WAL_TRUNCATE_PRESSURE_BACKLOG:
            return

        try:
            row = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        except Exception:
            LOG.debug("wal_passive failed", exc_info=True)
            return
        if not row:
            return
        busy, log, ckpt = row
        if busy or log >= 1000:
            LOG.info(
                "wal_passive: busy=%d log=%d checkpointed=%d backlog=%d",
                busy, log, ckpt, backlog,
            )

        if busy == 0 and log >= WAL_TRUNCATE_LOG_FRAMES_MIN:
            try:
                row2 = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if row2:
                    b2, l2, c2 = row2
                    if b2 or l2 >= 1000:
                        LOG.info(
                            "wal_truncate: busy=%d log=%d checkpointed=%d",
                            b2, l2, c2,
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
                    while not self._stop:
                        written, lost = await loop.run_in_executor(
                            self._writer_executor, self._process_batch, batch
                        )
                        if not lost:
                            for _ in batch:
                                self._event_queue.task_done()
                            break
                        # Retain this exact batch; no later work may checkpoint
                        # around a failed transaction. Backpressure reaches WS.
                        self._write_ops_snapshot()
                        await asyncio.sleep(1)
                    else:
                        break
                except Exception:
                    LOG.exception("failed to process batch")
                    self._stop = True
                    break  # unresolved work remains behind the durable cursor

                if lost:
                    # Database-locked rollbacks are intake loss; platform_health
                    # must see them so the recovery gate cannot hide them.
                    self._events_lost_to_rollback += lost
                    self._rollback_lost_total += lost

                if written:
                    queue_stats.inc("events_in", written)
                    self._event_count += written
                    events_since_cursor_save += written

                    # Save cursor after a successful commit, every
                    # CURSOR_SAVE_INTERVAL events. last_cursor is the
                    # high-watermark from the WS read loop.
                    events_since_cursor_save = 0  # atomic checkpoint per source batch

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
                self._write_ops_snapshot()
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
            try:
                from . import platform_health
                platform_health.record_parse_failure()
            except Exception:
                pass
            self._write_ops_snapshot()
            raise ValueError("unparseable source event prevents checkpoint progress")

        # Track cursor for resume and lag
        time_us = js.get("time_us")
        if not isinstance(time_us, int) or isinstance(time_us, bool) or time_us <= 0:
            raise ValueError("source event requires positive integer time_us")
        self._received_total += 1
        if time_us:
            self._last_cursor = str(time_us)
            try:
                from . import platform_health
                platform_health.record_event_time(time_us)
            except Exception:
                pass

        # All source kinds share one ordered writer. Awaiting capacity yields
        # the event loop, including ping tasks; cancellation leaves the durable
        # checkpoint behind the unadmitted message for reconnect replay.
        ev = {"_source": js}
        try:
            self._event_queue.put_nowait(ev)
        except asyncio.QueueFull:
            self._queue_waits_total += 1
            self._admission_pending = True
            try:
                while not self._stop:
                    try:
                        await asyncio.wait_for(self._event_queue.put(ev), timeout=1)
                        break
                    except asyncio.TimeoutError:
                        self._write_ops_snapshot()
                else:
                    raise asyncio.CancelledError
            except asyncio.CancelledError:
                self._unadmitted_interrupted_total += 1
                raise
            finally:
                self._admission_pending = False
        self._admitted_total += 1

    async def run(self):
        """Connect to Jetstream and process messages with reconnect resilience."""
        init_db()

        # Restore baseline from checkpoint (avoid cold start on restart)
        try:
            from . import platform_health
            platform_health.restore_baseline()
            platform_health.record_connection(False)
        except Exception:
            LOG.debug("baseline restore skipped (no checkpoint or error)")
        self._write_ops_snapshot()

        saved_cursor = get_cursor(CONSUMER_NAME)
        ws_url = _build_ws_url(self.ws_url, cursor=saved_cursor)
        LOG.info("starting Jetstream consumer, url=%s", ws_url)

        # Start background drain task
        drain_task = asyncio.ensure_future(self._drain_queue())

        while not self._stop:
            try:
                # Finish already-admitted work before choosing a reconnect
                # boundary; do not interleave old replay with newer queue work.
                while not self._stop:
                    try:
                        await asyncio.wait_for(self._event_queue.join(), timeout=1)
                        break
                    except asyncio.TimeoutError:
                        pass
                if self._stop:
                    break
                # Only committed work defines resume, including after a socket
                # failure while queued work or a batch remains outstanding.
                url = _build_ws_url(self.ws_url, cursor=get_cursor(CONSUMER_NAME))
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
                        platform_health.record_connection(True)
                    except Exception:
                        pass
                    self._write_ops_snapshot()
                    async for msg in ws:
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                LOG.exception("Jetstream connection error, reconnecting in 5s")
                await asyncio.sleep(5)
            finally:
                try:
                    from . import platform_health
                    platform_health.record_connection(False)
                except Exception:
                    pass
                self._write_ops_snapshot()

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
        LOG.info("shutdown retains atomic durable cursor: %s", get_cursor(CONSUMER_NAME))
        try:
            from . import platform_health
            platform_health.force_checkpoint()
            LOG.info("baseline checkpoint saved on shutdown")
        except Exception:
            pass
        self._write_ops_snapshot()

    def stop(self):
        self._stop = True


def run_consumer_blocking():
    loop = asyncio.get_event_loop()
    consumer = ATProtoConsumer()
    try:
        loop.run_until_complete(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()
