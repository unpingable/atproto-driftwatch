# Gap spec: Phase 3.5 — forward Parquet capture for claim_history

**Status:** candidate, non-binding. Filed 2026-05-08 evening, after Phase 1 historical mirror landed and the Phase 3 (DuckDB-backed facts_export) prototype proved internal correctness against Parquet.

**This is not authorization to build.** It is the record of the design space for the recent-data path that Phase 3 prod cutover requires. Companion to:
- `gap-spec-cold-path-parquet-duckdb.md` (the spec this fits inside, between its Phase 1 and Phase 3)
- `gap-spec-cold-path-update-2026-05-07.md` (today's tripwire status)

## Architecture sentence

> Phase 1 caught the past tense. Phase 3 reads from the past tense. Phase 3.5 keeps the past tense current.

## Premise

The Phase 3 dry-run prototype (2026-05-08, mem_b2cfece9) shows DuckDB-backed `facts_export` reading from Parquet produces internally exact results — `uri_fingerprint`, `fingerprint_hourly`, `fingerprint_bounds`, busiest-fingerprint event count all match the Parquet source row-for-row.

But the Phase 1 Parquet mirror only covers `2026-04-21..24` because that is the extent of the existing `data/archive/claim_history/*.jsonl.gz` artifacts. Recent claim_history rows (last 7 days, where the JSONL hasn't yet been written) live only in `labeler.sqlite`. For Phase 3 prod cutover to be meaningful, the Parquet store must include those.

Constraints from chatty's framing on 2026-05-08:

> **Avoid SQLite fallback as the default, because fallback preserves the hot-DB dependency we are trying to remove.**

> **Do not reintroduce a hot-DB reader we are actively building a cold path to eliminate.**

So the recent-data path cannot be "open a SQLite read connection on the side." It needs to capture rows without re-opening the wound that Track 1+2 just closed.

## Design options

### A. Retention-time partition write *(recommended for production)*

When `retention.py` archives `claim_history` rows (currently to `data/archive/claim_history/{date}.jsonl.gz`), additionally write the same rows to `data/parquet/claim_history/date={date}/part-NN.parquet`.

Both writes happen during the retention pass, on retention's own connection (not the writer thread). The reads that produce the archived rows already happen — Phase 3.5 piggybacks on them, no new hot-DB reader.

```python
# In retention.py, alongside _archive_claim_history:
# if PARQUET_ARCHIVE_ENABLED:
#     _archive_claim_history_parquet(rows, partition_date)
```

**Latency:** equal to `CLAIM_RETENTION_SEC` (currently 7 days). For a 30-day Phase 3 facts_export window, 7-day latency on the most recent partition is fine — the prod facts_export already operates on data older than the firehose anyway.

**Pros:**
- No new hot-DB reader. Retention is the only subsystem that reads claim_history at scale; Phase 3.5 is a write-side consequence of that read.
- Partitions arrive in the same shape as Phase 1 (date-partitioned). Phase 1's converter becomes the fallback/repair tool, not the production path.
- One source of cold-path truth.

**Cons:**
- Retention is the busiest cold-path actor. Adding work to it must not slow it. The Parquet write is bounded (per-partition zstd) but adds I/O.
- Retention currently aborts under writer pressure (per the pressure-aware scheduler). If it aborts mid-partition, partial Parquet must be discarded or marked incomplete.

**Acceptance:**
- Retention archive and Parquet partition write happen as one logical operation (transactionally, or with explicit "Parquet committed" marker).
- Row counts match between JSONL.gz and Parquet for each partition.
- No measurable retention slowdown (chunk timings within tolerance).
- Retention abort cleanly discards partial Parquet for that pass.
- Existing Phase 1 converter remains usable for repair.

### B. Cron'd JSONL→Parquet conversion *(recommended as interim)*

Run the existing `/usr/local/bin/parquet_mirror_claim_history.py` on a cron, e.g., every hour. It already skips partitions that exist (`if out_path.exists(): skip`), so it's idempotent over the existing JSONL archive directory.

**Latency:** `CLAIM_RETENTION_SEC + cron_interval` (~7d + 1h).

**Pros:**
- Zero hot-DB read. Reads only existing archive files.
- Tool already exists, already produces receipts.
- Decouples Phase 3.5 from production retention logic — pure cold-path operation.
- Right answer if we don't yet trust adding work to retention.

**Cons:**
- Two-stage (JSONL.gz → Parquet) keeps the JSONL stage. Phase 6 ("cold retention becomes partition lifecycle") eventually wants only one cold format.
- Slightly more disk while both formats coexist (Parquet is currently 1.05× of JSONL.gz on this shape; not a big concern).

**Acceptance:**
- Cron runs hourly without overlapping itself (lockfile or `flock`).
- Receipts updated each run.
- Disk footprint of Parquet bounded by retention rules applied to the parquet/ dir matching the archive/ dir lifecycle.
- Hourly run completes well under one hour.

### C. Continuous streaming exporter

A long-running background task reads new `claim_history` rows from a checkpoint and appends to Parquet partitions in real time.

**Reject for now.** Two reasons:
1. Long-running readers against `labeler.sqlite` are exactly the failure mode we deployed Track 1+2 against. Even with the Track 2 truncate fix, a continuous reader holds WAL frames open across snapshots.
2. Parquet doesn't append cleanly. Each append-burst becomes a new `part-NN.parquet` file, and per Phase 0 anti-patterns "tiny-file hell" is something to avoid. Real-time streaming to Parquet wants batch sizes that conflict with low-latency goals.

**Reconsider only if** Phase 3 facts_export grows a real-time SLA that retention-cadence cannot meet. As of today, no such SLA exists.

### D. Dual-write at ingest

Writer thread writes to both SQLite and Parquet (or fanout to a Parquet appender).

**Reject.** The writer-protection lessons from this incident arc make this dangerous: the writer is the single most important thread, and adding any new I/O failure mode to it raises the blast radius of any unrelated bug. Phase 5 of the parent gap spec already addresses raw-payload offload as a separate effort with its own care.

### E. Buffer-then-batch

Writer enqueues events into an in-memory ring buffer; a separate task drains the buffer to Parquet on a tick.

**Hold for Phase 6+.** This becomes interesting once the partition-lifecycle work is on the table — at which point retention can become "drain the buffer, flush to Parquet, no SQLite involvement." Until Phase 6 is sketched seriously, this option is too entangled with the rebuild question to be evaluated standalone.

## Recommendation

> **Option B (cron'd conversion) for the next operational window.**
> **Option A (retention-time partition write) once Track 1+2 has stayed stable for at least a week and we have appetite to extend retention.py.**

B requires no driftwatch code change. It reuses the Phase 1 converter we already deployed. It puts a ceiling on the recent-data lag (~ retention_age + 1h) that is acceptable for facts_export.

A is the cleaner long-term shape but adds complexity to retention, the most-stressed cold-path actor.

In neither case do we open a new hot-DB reader.

## Phase 3 cutover sequence under this recommendation

1. Today: Phase 3 prototype produces correct results from existing Parquet (mem_b2cfece9). DONE.
2. Tomorrow-ish: deploy Option B cron — hourly run of `/usr/local/bin/parquet_mirror_claim_history.py`. Partitions for everything in `data/archive/claim_history/` start landing automatically.
3. Wait: until JSONL archives accumulate from the live system (next ~7 days as `claim_history` retention rolls forward).
4. Once Parquet has ~14+ days of recent partitions: run the Phase 3 prototype again with the wider window. Compare against live `facts.sqlite` (which by then has been re-enabled and will have its own data — see also the live-anomaly investigation, mem_15ad4324).
5. Cutover decision: only after parity is boring across enough days.
6. Post-cutover: Option A becomes the obvious next step (collapse the JSONL+Parquet duo into Parquet-only at retention).

## Identity facts (out of scope, surfaced for the record)

`actor_identity_current` is a current-state table, not archived. Phase 3.5 here is about `claim_history` only. The Phase 3 prototype already documents `actor_identity_current → actor_identity_facts` as a remaining hot-DB dependency that needs its own design — likely a thin Parquet snapshot or a separate sidecar fed from the resolver, not a cold-path archive.

## What this gap spec does NOT do

- Does not authorize building Option A's retention extension.
- Does not change any in-flight retention/maintenance logic.
- Does not delete source archives (Phase 1 source-archive preservation is still in force).
- Does not commit a date for Phase 3 cutover.

## Tripwires that escalate this from candidate to required

- Phase 3 cutover gets explicitly authorized by the user.
- A second labelwatch consumer surfaces requiring fresh `uri_fingerprint`/`fingerprint_hourly` data.
- Disk pressure forces a decision about the JSONL+Parquet dual cold store.

## Related

- `gap-spec-cold-path-parquet-duckdb.md` — parent spec
- `gap-spec-cold-path-update-2026-05-07.md` — current tripwire status
- `/usr/local/bin/parquet_mirror_claim_history.py` — Phase 1 converter (becomes Option B's worker)
- `/usr/local/bin/facts_export_duckdb_prototype.py` — Phase 3 dry-run
- Continuity memory: `mem_b2cfece9` (project state), `mem_15ad4324` (uri_fingerprint anomaly root cause), `mem_4e3ff763` (yesterday's-fix-becomes-today's-bottleneck lesson)

## Keepers

> The cold path keeps current by inheriting retention's reads, not by opening new ones.

> Tiny-file hell is a real-time-streaming anti-pattern. Date-partitioned batches are the boring default, and boring is the goal.
