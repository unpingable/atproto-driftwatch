# Gap spec: cold-path data architecture (Parquet + DuckDB)

**Status:** candidate, non-binding. Filed 2026-05-05 evening, post-retention-scheduler-deploy and post-workload-contention incident. **This is not authorization to build.** It is the named handle for the architectural direction that the night's debt list keeps pointing at.

## Architecture sentence

> SQLite keeps the present tense. Parquet gets the past tense. DuckDB reads the past without bothering the present.

## Premise

`labeler.sqlite` is being asked to be the operational store, the historical archive, the read source for analytics, the export feed, the longitudinal recheck source, and the health-probe substrate — at 93 GB, on shared block storage, with ~10 concurrent connections from one process. That overload is the source of the 2026-04-30 lock-conflict incident, the 2026-05-01 5850d01 writer-thread starvation failure, and tonight's (2026-05-05) workload-contention incident where the writer entered kernel D-state on a page-cache miss.

The fixes shipped so far — single-writer invariant, batched writer, writer-owned WAL truncate, pressure-aware retention scheduler — are necessary AND have stopped fully solving the problem. Each one moves the bottleneck rather than removing it. The next architectural axis is **storage layout**, not scheduling.

## The shape

```
SQLite (hot operational store):
  recent / current operational state
  small indexes
  queue metadata
  current labels / unresolved work

Parquet (cold archive store, partitioned by date):
  data/parquet/events/date=YYYY-MM-DD/*.parquet
  data/parquet/claims/date=YYYY-MM-DD/*.parquet
  data/parquet/labels/date=YYYY-MM-DD/*.parquet

DuckDB (read-only query engine over Parquet):
  longitudinal historical scans
  facts export prototypes / source
  analytics / rebuilds
  archive verification
```

**DuckDB does not stand in front of ingest.** Hot path stays:
`Jetstream → durable spool → SQLite hot writer`. DuckDB is the off-ramp, not the intake valve.

## Why this helps

Today's retention is archaeological surgery: `DELETE FROM events WHERE ctime < cutoff` against a 93 GB file, then maybe `VACUUM` (which needs ~93 GB free space we don't have). With a date-partitioned cold store, retention becomes:

```bash
rm -rf data/parquet/claim_history/date=2026-03-01
```

File-system lifecycle. No DELETE, no VACUUM, no freelist, no auto_vacuum mode debate, no haunted pantry.

It also moves read-heavy workloads off the hot DB:
- facts_export stops pinning the WAL frontier with long-lived source reads.
- Longitudinal historical scans run against immutable Parquet, not the live writer's house.
- /health and ad-hoc analytics stop opening fresh SQLite connections that compete for the page cache.

## Phased plan

The user's intent is explicit: **phased, with receipts at each phase.** No big bang.

### Phase 0 — stabilize (current trench, not future work)

Keep ingest alive, buy disk runway, don't make new wounds.

- Stale archive deletion (done 2026-05-05, +9.8 GB).
- Pressure-aware retention scheduler (deployed `f86287c`).
- `rollback_lost`, `drop_frac`, WAL, queue depth, disk runway visible at `/health/extended`.
- Do not run cold-path experiments inside the hot writer.

**Acceptance:** ingest_loss=0, rollback_lost=0, WAL bounded, disk runway not collapsing daily, retention either makes progress or visibly skips.

**Status:** in flight.

### Phase 1 — Parquet mirror for existing archives

Convert existing `data/archive/claim_history/*.jsonl.gz` files to date-partitioned Parquet. **One-way conversion, no app integration.**

```
data/parquet/claim_history/date=2026-04-25/part-000.parquet
data/parquet/claim_history/date=2026-04-26/part-000.parquet
...
```

**Acceptance:**
- Parquet `row_count == jsonl row_count` per day.
- Sample hashes match (e.g., 100 random rows per partition).
- DuckDB can query the Parquet directly.
- Old archives deletable after verification.
- Disk savings real (Parquet's columnar compression should give 3–5× over gzipped JSONL on this shape).
- Zero hot-path code changed.

**Why first:** archives are already cold, already isolated, already disk pressure. Lowest-risk landing for the format.

### Phase 2 — DuckDB read-only sidecar tooling

Add small standalone scripts that use DuckDB to query the Parquet cold store. **No app integration.**

```
scripts/query_claim_history.py
scripts/parquet_stats.py
scripts/verify_parquet_export.py
```

**Acceptance:**
- DuckDB answers historical queries without opening `labeler.sqlite`.
- Facts-export-style queries can run against Parquet manually.
- Zero WAL impact.
- Zero SQLite reader pinning during these queries.

**Keeper:** *DuckDB is allowed to read the cold room. It is not allowed to stand in the doorway of ingest.*

### Phase 3 — facts_export source moves to Parquet

Today: `facts_export` reads from `labeler.sqlite` source connection, pinning WAL. Target: `facts_export` reads from Parquet via DuckDB.

Cutover via parallel run:
1. Run facts export both ways simultaneously for a verification window.
2. Compare counts, key fields, samples, edge cases.
3. Cut over after parity is boring.
4. Disable SQLite-backed facts export.

**Acceptance:**
- Parquet-backed facts export matches SQLite-backed within declared tolerance.
- Runtime acceptable.
- No hot DB handles opened by facts export.
- Export runs while ingest is active without WAL/frontier impact.

**Why this matters:** the facts_export connection is one of the prime suspects for tonight's WAL pinning. Removing it from the hot DB is probably the first major operational payoff.

### Phase 4 — longitudinal source material moves to Parquet

Trickier than facts_export because longitudinal needs *current-ish* state, not just history. Split:

- Current unresolved work queue → SQLite hot DB (narrow reads).
- Historical context for rechecks → Parquet via DuckDB.
- Recheck results → small bounded writes back to SQLite.

**Acceptance:**
- Longitudinal queue drains without WAL pinning.
- SQLite read durations bounded.
- DuckDB handles historical scans.
- Ingest unaffected.

**Note:** this is the phase where "just export old rows" stops being enough. Needs actual design — likely a follow-up gap spec for the longitudinal split shape.

### Phase 5 — raw payload offload

Today: `events.raw` column stores 1–2 KB JSON per event in SQLite, ~36 GB worth.
Target: SQLite stores pointers, Parquet stores the raw payloads.

Schema sketch:
```
SQLite events row:
  event_id, did, timestamps, current label fields,
  cold_uri = parquet://events/date=2026-05-05/part-000.parquet#row_group=...
```

Or boring lookup:
```
event_id, date_partition, parquet_file, row_offset, payload_hash
```

**Acceptance:**
- Raw payloads safely landed in Parquet (verified).
- SQLite `raw` column stripped aggressively (now safe).
- Payload lookup works for sampled events.
- Retention can null/strip SQLite raw without losing forensic source.

**This is where SQLite file growth materially changes.**

### Phase 6 — cold retention becomes partition lifecycle

Replace SQLite-DELETE-then-VACUUM-and-cry with file-level lifecycle:

```bash
# safer than rm -rf
mv data/parquet/claim_history/date=2026-02-01 data/parquet/.pending-delete/
# verify not referenced, wait grace period
rm -rf data/parquet/.pending-delete/date=2026-02-01
```

SQLite retention remains, but only for bounded hot/current state.

**Acceptance:**
- Cold retention deletes files, not SQLite rows.
- SQLite DB stops growing with history volume.
- VACUUM becomes rare planned maintenance, not survival ritual.

**This is the architectural payoff.**

### Phase 7 — rebuild / shrink the hot SQLite DB

After cold offload, the hot DB is still 93 GB on disk because SQLite doesn't auto-shrink. Don't VACUUM the monster — build a new one:

1. Stop nonessential workers; freeze or spool ingest briefly.
2. Build `labeler_new.sqlite` from current operational subset.
3. Validate counts/invariants.
4. Swap.
5. Keep old DB as rollback for a short window.

**Acceptance:**
- Hot DB size drops drastically (target: low single-digit GB).
- Current operational state preserved.
- Ingest resumes cleanly.
- Cold history still queryable via DuckDB.

**Cleaner than carving a 93 GB file into health.**

### Phase 8 — reassess whether SQLite is still right for the hot path

If hot SQLite is now small, bounded, low-reader, low-retention, mostly current state — keep it. SQLite is fine when not used as a haunted warehouse.

If it still fights us, consider Postgres or another operational store. **Don't migrate before the split** — otherwise you just move bad workload boundaries into a more expensive room.

## What does NOT move first

- The hot writer path. Jetstream → spool → SQLite stays SQLite.
- The retention scheduler design from `gap-spec-single-writer-invariant.md`. Different surface; the cold-path migration changes the workload retention operates on, not the scheduler shape.
- Anything that requires changing the WS read loop or the writer thread executor.

## Anti-patterns to avoid

- **Tiny-file hell.** Don't overpartition by DID or labeler unless queries force it. Date-only partitioning is the boring default; add `hour=HH` only if daily files get too large to query.
- **Big-bang migration.** Each phase has receipts and parallel-run verification before cutover.
- **DuckDB as application substrate.** It's a read engine over Parquet, not a database we write into from app code.
- **Pretending Parquet is transactional.** It isn't. Anything that needs read-after-write consistency stays in SQLite.

## Tripwires that escalate this from candidate to required

- A second workload-contention incident (writer in sustained kernel I/O wait under non-burst load).
- Disk runway dropping below 30 days for two consecutive weeks despite retention working.
- A new subsystem proposed that would add another long-lived SQLite reader (analytics dashboard, public query endpoint, downstream label browser).
- A planned-obsolescence date for the current Linode plan that forces a migration anyway.

## Related

- `specs/gaps/gap-spec-single-writer-invariant.md` — the scheduling debt; this gap spec is the storage-layout dual.
- `specs/gaps/gap-spec-storage-layout-labelwatch-driftwatch.md` — earlier filing on storage layout (re-read before drafting any phase).
- `docs/CLEANUP_DEBT.md` — the workload-contention entry points here as the architectural answer.
- `docs/RUNBOOK.md` — manual VACUUM INTO playbook (becomes obsolete after Phase 7).
- Continuity: `mem_cf1afe52` (single-writer ≠ scheduling ≠ reclamation), `mem_bf77f2e2` (rollback_lost is one axis), tonight's notes.

## Keepers

> SQLite got asked to be the entire city. Duck/Parquet is how you stop making the sewage treatment plant host the public library.

> DuckDB is not the new plant. It is the cold-room reader.

> Don't migrate before the split — otherwise you just move bad workload boundaries into a more expensive room.
