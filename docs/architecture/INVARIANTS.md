# Invariants

Load-bearing rules for driftwatch, discovered by runtime, not by armchair design.

## Why this file exists

Architecture overviews describe how the pieces fit. This file names the rules that, when broken, made the pieces stop working — including rules whose load-bearing nature wasn't obvious before contact with a real firehose. Each invariant here is the post-runtime version: written knowing what specifically went wrong without it.

If a future change wants to relax one of these, the right move is to read the *failure prevented* line first and decide whether that failure mode is now genuinely irrelevant. Most of the time it isn't.

## Non-goals

- **Not an architecture overview.** See `OVERVIEW.md` and `DATAFLOW.md` for what the system does.
- **Not a gap index.** See `specs/gaps/` for candidate work.
- **Not a tuning guide.** Env vars and chunk sizes belong in deploy config, not here.

## The five invariants

### 1. Single writer

**Rule.** One thread, one persistent connection writes to `labeler.sqlite`. All mutation paths — event ingest, retention, schema migration, WAL maintenance, CLI write commands — route through that thread.

**Why.** SQLite arbitrates concurrent writers via `busy_timeout` retry. That arbitration is correct but coarse: when two writers contend, one of them loses, sometimes silently, within the timeout window. Multiple writers in the same process turn the lock contest into an intermittent shedding source.

**Failure prevented.** 2026-04-30. The 2026-04-28 batched-writer fix made the consumer's writer fast enough to fully compete with retention. Retention's chunked DELETE/UPDATE batches started winning the lock contest about half the time; the consumer's batches hit `database is locked` and rolled back. ~444 events lost in 24h — and `platform_health` reported `recovered` because the rollback bucket wasn't instrumented.

**Where it lives.** `consumer.ATProtoConsumer.submit_mutation` (the gate); `retention.run_retention_once_async` (a routed mutation path); `specs/gaps/gap-spec-single-writer-invariant.md`; commit `5850d01`.

---

### 2. Batching is correctness infrastructure

**Rule.** A streaming consumer against a real firehose must use a persistent writer connection and one transaction per batch. Per-event `sqlite3.connect()` + per-event `commit()` is not a starting point that gets refined; it is a design that silently fails under load while reporting health.

**Why.** At ~100 events/sec, per-event commits + new-connection PRAGMA setup ceiling out around 20–60 eps. The internal queue (`asyncio.Queue`) becomes the loss boundary. Events drop silently via `QueueFull`. Persistent writer + batched transaction is the only shape that decouples per-event arrival cost from per-batch fsync.

**Failure prevented.** 2026-04-15..28. Driftwatch dropped 30–40% of jetstream events at the consumer's own queue. Process liveness was fine, retention ran, disk was fine, `/health` was green. The only honest signal was `platform_health.degraded(high_drop_rate)` plus archive day-files that quietly shrank from ~270 MB/day to 39–203 MB/day.

**Where it lives.** `consumer._process_batch`, `consumer._drain_queue`; commit `2879058`; `docs/JETSTREAM_INGEST_REALITIES.md`.

---

### 3. Maintenance shares the write contract

**Rule.** Maintenance is mutation. Retention, archive deletion, WAL truncation, schema migration, host-side cron writes — all subject to the single-writer invariant. "Background job" is not an immunity from the lock contest.

**Why.** Background jobs sound like they're outside the plant. They aren't — they hold the same write lock. A maintenance path that opens its own connection reintroduces the contention the invariant exists to prevent. The lock contest is structural, not categorical.

**Failure prevented.** 2026-04-30. Fixing the consumer's queue-overflow shedding (invariant 2) moved contention to retention, which was still opening its own write connection. The shedding bucket migrated from `QueueFull` to `OperationalError: database is locked` — same incident, different bucket. Routing retention through `submit_mutation` made the contention structurally impossible: only one writer exists.

**Where it lives.** `retention._strip_raw_chunk`, `retention._prune_table_chunk`, `retention._delete_archived_day_chunk` (the chunk functions that run inside the writer thread); commit `5850d01`. Host-side `deploy/maintenance.sh` is still a violator on record — it opens its own `sqlite3` CLI connection from cron — and is documented in the gap spec as a known boundary case (daily, brief, low contention probability) rather than fixed.

---

### 4. Coverage honesty: the bucket vocabulary

**Rule.** A green recovery signal is structurally inadmissible if any known shedding path has no instrumented loss bucket, OR has a bucket that is currently zero only because the path isn't being exercised. Health metrics are conditional admissions about *observed* buckets, not unconditional claims about reality.

**Why.** When a fix changes throughput characteristics, contention migrates. Loss often migrates with it. A health metric whose bucket vocabulary was designed for the old failure mode becomes a green light that the loss has moved, not stopped. Recovery in such a system is parole, not exoneration.

**Failure prevented.** 2026-04-29 false recovery stamp. `drop_frac=0.0` sustained 27h triggered `platform_recovered`. But the lock-conflict rollback path had no bucket — its loss surfaced only as `LOG.exception` lines that didn't aggregate. The system "remembered" recovery but not the condition under which recovery stopped being true.

**Where it lives.** `consumer._events_lost_to_rollback` (the new bucket added 2026-04-30); `consumer` STATS line `dropped=N rollback_lost=N` (separate fields, both summed into `platform_health.record_window`); Continuity doctrine `mem_1caf694af4454bd18f23c246aa7ad4c8`; commit `7398f7b`.

**Operational rule.** Before stamping recovery: enumerate every known shedding path and confirm each has an instrumented bucket. When recovery is reaffirmed, re-ask: what shedding paths exist now that weren't instrumented when this signal was designed?

---

### 5. Degraded is a semantic state

**Rule.** "Operationally up" and "epistemically degraded" are different states. Process liveness, intake coverage, and output truthfulness are three axes; a green light on one does not imply the others. Driftwatch's outputs may be conditioned by loss even while the service is healthy.

**Why.** A labeler can keep its websocket open, write events to disk, return 200 from `/health`, and silently produce loss-conditioned outputs. "Service healthy" and "observatory valid" are different facts. Treating them as one collapses the distinction the observatory exists to maintain.

**Failure prevented.** Same 04-15..28 window. Service was up; retention was running; disk was fine; `/health` was green. But ~30% of jetstream events were being shed at the queue, and every artifact derived from that window — archive day-files, claim_history rates, cluster reports — was conditioned by loss. Reading those artifacts as ground truth would have been a category error.

**Where it lives.** `platform_health.py` state machine (WARMING_UP / OK / DEGRADED with explicit `gate_reasons`); auto-memory `lesson_operationally_up_epistemically_degraded.md`; `docs/JETSTREAM_INGEST_REALITIES.md` operational doctrine.

**Operational rule.** Outputs from a degraded window are usable for *qualitative* observation only. Anything quantitative needs the loss factor folded in or excluded. Stamp degraded windows; don't let them blend into history.

---

## Related artifacts

- `specs/gaps/gap-spec-single-writer-invariant.md` — the architectural fix that ratified invariant 1 and 3.
- `docs/JETSTREAM_INGEST_REALITIES.md` — the running case study for invariants 2, 4, 5.
- Auto-memory `lesson_self_shedding_queue_boundary.md` — invariant 2 doctrine.
- Auto-memory `lesson_operationally_up_epistemically_degraded.md` — invariant 5 doctrine.
- Continuity `mem_1caf694af4454bd18f23c246aa7ad4c8` — invariant 4 doctrine (bucket migration).
- Commit `2879058` — batched writer (invariant 2).
- Commit `7398f7b` — rollback bucket + writer-owned WAL truncate (invariant 4).
- Commit `5850d01` — retention through writer thread (invariants 1, 3).

## Maintenance of this file

When a runtime incident teaches a sixth invariant, it goes here. Don't pre-emptively add invariants from imagined failures; the value of this file is that every entry has a real failure-prevented receipt. An invariant without a receipt is a preference.
