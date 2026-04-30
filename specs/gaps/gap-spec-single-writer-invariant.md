# Gap spec: single-writer invariant for the labeler SQLite DB

**Status:** candidate, non-binding. Filed 2026-04-30 from the WAL-bloat / retention-contention investigation.

## Premise

There is exactly one logical writer to `labeler.sqlite`: the persistent
writer thread owned by the `ATProtoConsumer` (`consumer.py`,
`self._writer_executor` / `self._writer_conn`). Every mutation to the main
DB — INSERT, UPDATE, DELETE, ALTER, plus WAL maintenance operations like
`PRAGMA wal_checkpoint(TRUNCATE)` — should converge through that thread.

This is currently violated. As of this spec's filing, multiple subsystems
issue direct mutations against their own connections:

- `retention.py` runs chunked `UPDATE`/`DELETE` against `events`, `edges`,
  `event_versions`, `claim_history` from its own connection. Per-pass it
  may issue hundreds of write transactions.
- `maintenance.py` and the host-side `maintenance.sh` cron call
  `wal_checkpoint(TRUNCATE)` against their own connections.
- The host-side cron and various CLI tools open transient connections
  that mutate the main DB.

SQLite arbitrates concurrent writers via the busy-timeout retry path. That
arbitration is correct but coarse: when two writers contend, one of them
loses, sometimes silently. The 2026-04-30 investigation traced lost events
to exactly this pattern: the consumer's batched writer failed `database is
locked` while retention held the lock through a multi-second chunk.

## Why this is gap-spec-shaped

This is an **architectural surface** in the YAGNI sense — a wire-format-
adjacent invariant whose retrofit cost rises with usage spread. Every new
subsystem that touches the DB inherits the question "where does my write
go?" If the answer accretes as scattered direct connections, eventually
you can't reason about contention or correctness without grepping the
whole codebase.

Filing this as a gap spec, not implementing it: the immediate WAL-bloat
fix (smaller retention batches, writer-owned TRUNCATE) is a lighter
patch. This record exists so the architectural debt has a name.

## Proposal

All write paths — including retention, maintenance, schema migrations,
CLI mutations, and WAL maintenance pragmas — route through a queue
serviced by the persistent writer thread.

Concretely:

- The writer thread accepts not just event-batch jobs but also generic
  "mutation jobs" with a small protocol: a callable that receives the
  writer connection, plus a result future.
- `retention.run_retention_once` no longer opens its own connection. It
  produces mutation jobs (one per chunk) and submits them to the writer
  queue. The writer interleaves them with event-batch jobs.
- `wal_checkpoint(TRUNCATE)` is exclusively a writer-thread operation
  (already done as the immediate patch).
- CLI tools and the maintenance cron either run while the consumer is
  stopped, or use a separate "offline mode" entry point that takes
  exclusive control.

This collapses the contention model: only one connection ever holds the
write lock; busy-timeout retries become unnecessary; lost events from
lock-conflict become structurally impossible.

## Why not now

- The immediate symptom (WAL bloat, lost events) is addressed by the
  smaller-batch + writer-owned-TRUNCATE patch. That buys runway.
- A queue-based mutation API touches every subsystem that writes. That's
  a real refactor with its own correctness surface (deadlock-free,
  fairness, error propagation, shutdown semantics).
- We need at least one more incident or one concrete cross-module
  consumer (e.g. a new sensor that wants to write back) before we know
  whether the queue API should be sync or async, and whether retention
  should be allowed to starve the consumer or vice versa.

## Tripwires that escalate this from candidate to required

- A second instance of lost events traced to write-lock contention
  *after* the immediate patch lands.
- A new subsystem (sensor, exporter, archive consumer) that needs to
  mutate the DB and would otherwise open its own write connection.
- Schema-migration tooling that needs to coordinate with the live
  consumer.

## Related

- `consumer.py` — current sole legitimate writer
- `retention.py` — current largest violator
- `maintenance.sh` (deploy/) — host-side TRUNCATE caller, also a violator
- `docs/JETSTREAM_INGEST_REALITIES.md` — incident records that surfaced this
- Auto-memory `lesson_self_shedding_queue_boundary.md` — the queue
  boundary lesson generalizes to "the place the system serializes is the
  place the system loses things"
