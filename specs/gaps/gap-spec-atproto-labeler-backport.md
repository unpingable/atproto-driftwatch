# Gap spec: backport plan for atproto-labeler reference implementation

**Status:** candidate, non-binding. Filed 2026-05-08 evening, after the driftwatch incident arc 2026-04-23..05-08 reached architectural stability via Option A retention-time Parquet capture.

**This is not authorization to start patching atproto-labeler.** It is the record of *what* should propagate, *in what order*, and *with what scars preserved*, when that work begins.

## Core thesis

> **The generic labeler gets the operating model, not just the patches.**

> **A permissionless labeler needs a hot/cold plant boundary before it pretends it can survive the firehose.**

The patches alone are easy to copy. The framework that produced them — and the failures that necessitated them — is the load-bearing thing. The reference impl should arrive with **scars visible**, not as a clean-room "best practices" doc that future maintainers will simplify back into the original traps.

## What NOT to backport

- The writer-thread retention attempt (commit `5850d01`). Failed acceptance; documented scar at `specs/gaps/gap-spec-single-writer-invariant.md` footer.
- Blocking writer-owned `wal_checkpoint(TRUNCATE)`. Was the right call when WAL bloat under absent reader concurrency was the failure mode; became part of the failure mode under multi-subsystem reader concurrency.
- `DISTINCT DATE()` scans over live `claim_history`. 28-minute WAL pin, drop_frac=12%, intake loss. See `mem_2b481a31`.
- JSONL gzip archive as the *primary forward path* for the cold store. Acceptable as fallback; not as the production write path.
- Retention pass ordering with archive *last*. Starves the cold-path producer behind raw_strip/prune budget. See `mem_58e57b88`.

These are scars worth carrying in commit messages or a HISTORY doc, not in code.

## Backport in layers, not as one blob

### Layer A — docs / operating doctrine *(does not change runtime)*

Bring the operating model first. Code without doctrine is rediscovered as "best practices" within a year.

```
docs/OPERATING_MODEL.md
docs/INGEST_INVARIANTS.md
docs/SQLITE_REALITIES.md
docs/RETENTION_AND_RECLAIM.md
docs/HEALTH_SEMANTICS.md
docs/HARDENING_SCARS.md   ← named incidents and what each fix actually fixed
```

The 8-point doctrine list lives in `mem_75858d6a` (continuity, scope=`reference_labeler`):
1. Ingest protection is sacred.
2. Every loss bucket must be named.
3. Background work is workload, not background.
4. Observability is not outside the plant.
5. Retention is not reclamation.
6. WAL health is not DB health.
7. Green is scoped.
8. Hot/cold split is not "enterprise architecture."

Top-of-doc keeper line:

> *This implementation assumes the protocol grants permission, not survivability. These invariants describe what keeps the service alive after permission is granted.*

**Acceptance for layer A:** docs land. Reference impl README points at them. No code changes.

### Layer B — loss accounting / health semantics *(observability before mitigation)*

Before backporting any mitigation, backport the language used to talk about failure. Otherwise the next maintainer will see "drop_frac=0" and call the system green while events_dropped grows by 1.5M/day.

Implement:
- `rollback_lost_total` (writer batch integrity)
- `events_dropped_total` (queue-boundary intake loss; replay-dependent on upstream cursor)
- `drop_frac` scoped explicitly to *queue-boundary intake*, with EWMA window documented
- `retention_state_reason` enumerating: `last_pass_aborted:*`, `pressure_returned:*`, `consecutive_pressure_skip`, `disk_runway_days<*`
- WAL size + checkpoint state in `/health/extended`
- `disk_runway_days` (with explicit caveat that the metric is jittery under facts/work file flips)

Workspace constraint to preserve: `mem_b219ffb7` — "Distinguish intake loss from rollback loss in writer-health language" (actionable).

**Acceptance for layer B:** /health/extended shape + named loss buckets land. *Without* backporting the mitigations these will show the existing system's loss for what it is — that's the point. Maintainer sees the real picture before they get the fixes.

### Layer C — writer WAL-truncate pressure logic

Backport `consumer.py:_maybe_wal_truncate` rewrite from this driftwatch tree. Behavior:

```
1. Skip TRUNCATE entirely when backlog > WAL_TRUNCATE_PRESSURE_BACKLOG (default 500).
2. Otherwise PASSIVE checkpoint first (non-blocking; never waits on readers).
3. Escalate to TRUNCATE only when PASSIVE returns busy=0 AND log >= WAL_TRUNCATE_LOG_FRAMES_MIN (default 5000 frames).
4. busy=0 from PASSIVE is the proof TRUNCATE will not block.
```

Rationale: a fix that protected yesterday's bottleneck (WAL bloat from absent truncate) became today's bottleneck (writer parked in TRUNCATE waiting for readers). See `mem_4e3ff763` (workspace lesson). Periodic audit of mitigations is required when load shape changes.

**Acceptance for layer C:** writer no longer parks in `_maybe_wal_truncate` under reader concurrency. py-spy verification on the reference impl under synthetic load.

### Layer D — pressure-gated, bounded, resumable retention scheduler

Backport `retention_scheduler.py` with:
- pre-pass gate (`begin_pass`)
- per-chunk gates (`before_chunk` / `after_chunk`)
- backlog threshold abort
- queue_depth threshold abort
- chunk overrun tolerance
- `rollback_lost` tripwire
- partial-progress receipts

**Do not backport a stream_lag threshold abort.** Driftwatch removed it 2026-06-02 (CLEANUP_DEBT.md #3): `stream_lag_s = now - latest_event_time` is jetstream catch-up, not writer pressure, and false-trips after every restart's cursor rewind. Keep `stream_lag_s` observable in platform_health / summary / /health/extended; just don't gate retention on it.

And the retention pass ordering: **archive op first**, raw_strip / events_prune / event_versions_prune / edges_prune *after*.

Driftwatch lesson `mem_58e57b88` (workspace, actionable): "Retention archive must run before destructive/expensive lifecycle work."

**Acceptance for layer D:** retention runs against synthetic backlog. Archive completes even when subsequent ops abort under pressure. No more silently skipped archive across many passes.

### Layer E — retention-time Parquet capture (Option A)

Backport `_archive_claim_history_to_parquet` from `retention.py`. Default to Parquet as forward path; JSONL as legacy fallback via `ENABLE_RETENTION_PARQUET_CAPTURE=0`.

Required design notes (from the three-iteration journey):
- **No DISTINCT DATE scan** over live claim_history. Iterate candidate dates over a fixed lookback window (`RETENTION_PARQUET_LOOKBACK_DAYS`, default 21). Skip dates with existing complete partitions. See `mem_2b481a31`.
- Per-day filter uses an indexed column. In driftwatch this is `createdAt`; for a clean reference impl this should be paired with a proper index on the retention column (or `observed_at`, with that column getting an index).
- Per-batch scheduler gating inside the read loop. A 1M-row day must not block the writer for minutes; the scheduler must be able to interrupt mid-day.
- Tmp file + atomic rename only after row-count verification.
- Per-partition receipt JSON with src_rows, dst_rows, duration, dst_bytes.
- Idempotent: skip rewrites if existing partition matches; do delete-only path if partition exists with sufficient row count.
- Known-leak documentation: rows with bogus user-supplied timestamps (years far outside reasonable range) won't be captured. See `mem_3e38eaf4`.

**Acceptance for layer E:** retention-time Parquet capture works against synthetic claim_history with multi-day backlog. Each pass produces 1+ partitions. Drop_frac stays 0 during normal cadence, may spike under heavy backfill (cap with `RETENTION_PARQUET_MAX_DAYS_PER_PASS`).

### Layer F — DuckDB facts_export *(do not backport until parity receipts exist)*

Backport `facts_export_duckdb_prototype.py` shape, but **do not switch the production facts_export until**:
- Reference impl Parquet has recent partitions (last 30+ days continuously).
- Parity check vs SQLite-backed facts_export passes for at least one full snapshot cycle.
- Row count receipts and sample hashes published per partition.
- DuckDB query coverage for all facts_export consumer queries demonstrated.
- No hot-DB fallback in the production path. Prototype mode (`source_conn_or_factory`) acceptable for tests only.

**Acceptance for layer F:** parity is boring. Cutover decision is documented and reversible. Old SQLite-backed code stays in tree as legacy with a clear deprecation horizon.

## Layer dependencies

```
A (docs)         → no deps
B (loss accounting) → no deps
C (WAL truncate) → C depends on B (need loss vocabulary to evaluate fix)
D (retention)    → D depends on C (writer must survive retention's reads)
E (Parquet)      → E depends on D (retention scheduler must work first)
F (DuckDB cutover) → F depends on E (Parquet must exist), and on parity receipts
```

A and B can land in any order, before everything else. C through F are sequential.

## What the reference impl does NOT need yet

- The bucket-migration scar (rowid-reset orphaning facts_export checkpoint) is driftwatch-specific. Reference impl just needs to choose a stable cold-path identifier (date partition, not rowid) and document why. See `mem_15ad4324`.
- The 25k bogus-createdAt rows are driftwatch's accumulated history. Reference impl can filter or quarantine bogus timestamps at insert time.
- The disk-runway oscillation around facts_work file flips is driftwatch-specific operational detail; reference impl can document the metric's jitter without reproducing the flip.

## Scars to preserve in commit messages / HISTORY doc

- "Writer-thread retention failed acceptance under firehose load (driftwatch 2026-05-01)."
- "Writer-owned WAL TRUNCATE was correct under WAL bloat; became failure mode under reader concurrency (driftwatch 2026-05-08)."
- "DISTINCT DATE discovery over live claim_history pinned WAL 28 min and induced 12% drop_frac (driftwatch 2026-05-08)."
- "Retention archive starved behind raw_strip/prune for ~2 weeks until reordered (driftwatch 2026-05-07..08)."
- "Phased thin-slice approach can become decision-deferral when evidence has crossed the architectural-promotion threshold (driftwatch 2026-05-08)."

## Anti-patterns documented

- Treating diagnostic reads as harmless. They are workloads.
- Treating `rollback_lost=0` as proof of clean intake. It is proof of *committed-write integrity*. Intake loss is a separate axis.
- Treating "the cron will run it" as a forward-progress guarantee. The cron only converts what the upstream producer emits. A starved upstream producer is invisible to the cron's logs.
- Reaching for VACUUM as a retention answer. Retention is lifecycle; reclamation is partition deletion.

## Tripwires that escalate this from candidate to required

- A second labeler implementation pulled from driftwatch starts experiencing the same incident pattern.
- Reference impl maintainer asks "why is this code this way?" and the answer requires telling the whole driftwatch story.
- A third party adopts the reference impl and reproduces a known-scar incident before noticing the doctrine.

## Related

- `gap-spec-cold-path-parquet-duckdb.md` — parent architectural spec
- `gap-spec-cold-path-update-2026-05-07.md` — tripwire status
- `gap-spec-cold-path-phase-3.5-forward-parquet-capture.md` — Phase 3.5 design space
- `docs/ISOLATION_TOGGLE_PROTOCOL.md` — operational protocol
- Continuity (queryable): `memory_query(scope="driftwatch", kind="lesson")` for the actionable scars; `memory_query_latest(scope="driftwatch", kind="project_state")` for current state; `memory_query(scope="reference_labeler", kind="note")` for the operating-model framing.

## Keepers

> **Generic labeler gets the operating model, not just the patches.**

> **A permissionless labeler needs a hot/cold plant boundary before it pretends it can survive the firehose.**

> **Preserve scars in docs so future maintainers know why the code is shaped this way.**

> **Do not let one bad row corrupt a day's partition.**
