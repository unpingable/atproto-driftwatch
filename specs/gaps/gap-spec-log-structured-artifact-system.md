# Gap spec: Boring Log-Structured Artifact System

**Status:** planning sketch / candidate, non-binding. Filed 2026-05-13.

**This is not authorization to build.** It is the integrating doctrine document for the cold-path direction the previous cold-path gap-specs have been concrete slices of. Each phase here may produce its own gap-spec or build slice as the work is justified.

Companion to (and integrating):
- `gap-spec-cold-path-parquet-duckdb.md` (2026-05-05) — the original "SQLite present / Parquet past / DuckDB questions" handle. This document is its successor in scope.
- `gap-spec-cold-path-update-2026-05-07.md` (2026-05-07) — tripwire evidence as of mid-week
- `gap-spec-cold-path-phase-3.5-forward-parquet-capture.md` (2026-05-08) — Phase 3.5 forward Parquet capture (Option A, validated live)
- `gap-spec-facts-export-duckdb-productionization.md` (2026-05-11) — facts cutover gap
- `gap-spec-storage-layout-labelwatch-driftwatch.md` — storage policy
- `gap-spec-single-writer-invariant.md` — current writer-side invariant
- `gap-spec-atproto-labeler-backport.md` — reference-labeler backport layers

**Scope:** driftwatch / labelwatch / reference atproto-labeler cold-path storage.

**Goal:** stop asking one hot SQLite DB to behave like an ingest buffer, history store, facts warehouse, archive, retention engine and analytics backend.

---

## Summary

Do not write a general-purpose database.

Write a narrow artifact system for the shape this workload keeps trying to become:

```text
SQLite = present tense
Parquet = past tense
Manifest = custody / inventory
Receipts = evidence that writes, rewrites and drops happened correctly
DuckDB = question engine over cold artifacts
```

The system is log-structured in the boring sense:

* ingest and claim history are written into append-like partitions
* partitions are immutable once sealed
* rewrites create new artifacts and receipts
* retention deletes or tombstones whole artifacts where possible
* queries read artifacts through DuckDB or direct file scans
* hot SQLite holds only current operational state, queues, cursors and small indexes

Keeper:

> Do not write a database. Write the admissibility-preserving file lifecycle.

## Problem

The current labeler/driftwatch shape has repeatedly shown the same failure family:

* historical data accumulates inside one large SQLite file
* background analytical jobs compete with ingest
* retention deletes rows but does not reliably return disk
* diagnostic reads can pin WAL and become operational load
* facts export and longitudinal scans behave like analytical workloads
* checkpoint/WAL behavior becomes part of application health
* green metrics over one bucket hide loss elsewhere

The fix is not "make SQLite heroic forever."

The fix is to split the workload by tense:

```text
present/current/small/transactional  → SQLite
past/historical/append/query-heavy   → Parquet + manifests + DuckDB
```

## Non-Goals

This system must not become:

* a general database
* a query planner
* a transaction engine
* a replacement for SQLite on the hot path
* a service daemon by default
* a cluster
* a new authority layer
* a place where all artifacts become mutable because it is convenient
* a hidden dependency of ingest availability

The first version should be file-based, local, boring and inspectable.

## Core Invariants

### 1. Hot path stays hot

The ingest path must not wait on cold-path writes, analytical queries, compaction, retention, facts export or diagnostics.

Allowed hot-path writes:

* current event metadata
* current queue/cursor state
* minimal current labeler state
* small receipts/counters

Forbidden hot-path dependencies:

* DuckDB query execution
* Parquet compaction
* full-history scans
* retention deletes
* facts export rollups
* diagnostic geometry scans

Keeper:

> Nothing non-ingest gets to make ingest lossy.

### 2. Cold artifacts are written atomically

Every artifact write follows:

```text
write temp → fsync if warranted → validate → atomic rename → receipt
```

A partition is not visible as complete until its manifest and receipt say so.

### 3. Partition completion is explicit

A date/hour partition can be:

```text
open
partial
sealed
superseded
dropped
quarantined
```

No consumer should infer completeness from file existence alone.

### 4. Rewrites do not mutate history silently

If an artifact is corrected, compacted or regenerated, the new artifact gets a new receipt and the manifest records lineage.

### 5. Retention is artifact lifecycle

Cold retention should delete or tombstone partitions, not perform row archaeology.

SQLite row deletion remains only for bounded hot/current state.

### 6. Receipts are not authority

Receipts prove an artifact operation occurred under a declared rule. They do not authorize downstream claims, publication or action.

### 7. Observability is workload

Any scanner, verifier or report generator that reads large artifacts is a workload. It must be scheduled, bounded or run off the hot path.

## Directory Shape

Initial local layout:

```text
data/
  hot/
    labeler.sqlite
    queues.sqlite                  # optional split later

  artifacts/
    claim_history/
      date=2026-05-13/
        part-000.parquet
        part-001.parquet
        _manifest.json
        _receipt.json
      date=2026-05-14/
        ...

    label_events/
      date=2026-05-13/
        part-000.parquet
        _manifest.json
        _receipt.json

    raw_events/
      date=2026-05-13/hour=00/
        part-000.parquet
        _manifest.json
        _receipt.json

  manifests/
    claim_history.manifest.jsonl
    label_events.manifest.jsonl
    raw_events.manifest.jsonl

  receipts/
    claim_history/
      2026-05-13T00-archive.json
      2026-05-13T01-rewrite.json
      2026-05-13T02-drop.json

  quarantine/
    bogus-createdAt/
      ...
```

The per-partition manifest is convenient for local reads. The global manifest JSONL is convenient for inventory and lineage.

## Artifact Types

### claim_history

Historical claim rows used by facts export, labelwatch reporting, audits and longitudinal context.

Partition:

```text
date=YYYY-MM-DD
```

Candidate schema:

```text
claim_id
uri
cid
actor_did
labeler_did
claim_type
claim_value
created_at
observed_at
source
payload_hash
raw_ref
schema_version
```

### label_events

Observed labels from labeler endpoints or streams.

Partition:

```text
date=YYYY-MM-DD
```

Candidate schema:

```text
labeler_did
subject_uri
subject_cid
label_value
created_at
observed_at
signature_status
source_endpoint
payload_hash
schema_version
```

### raw_events

Raw or semi-raw incoming event payloads. This is optional at first because it can grow quickly.

Partition:

```text
date=YYYY-MM-DD/hour=HH
```

Candidate schema:

```text
event_id
source
received_at
actor_did
collection
operation
payload_hash
raw_json_or_ref
schema_version
```

### facts_snapshot

Derived facts export outputs. These may be written as SQLite for compatibility or Parquet/DuckDB-native artifacts later.

Partition:

```text
snapshot_date=YYYY-MM-DD/snapshot_hour=HH
```

Candidate outputs:

```text
uri_fingerprint
fingerprint_hourly
labeler_activity_window
reference_health_window
```

## Manifest Format

Manifest entries should be append-only JSONL at first.

Example:

```json
{
  "artifact_type": "claim_history",
  "partition": "date=2026-05-13",
  "state": "sealed",
  "path": "data/artifacts/claim_history/date=2026-05-13/part-000.parquet",
  "schema_version": 1,
  "row_count": 1431120,
  "min_created_at": "2026-05-13T00:00:00Z",
  "max_created_at": "2026-05-13T23:59:59Z",
  "content_hash": "sha256:...",
  "receipt_id": "receipt:claim_history:2026-05-13:archive:001",
  "created_at": "2026-05-14T00:15:00Z",
  "created_by": "retention-parquet-capture",
  "supersedes": null
}
```

The manifest is an inventory and custody record. It is not a query index pretending to be a database.

## Receipt Format

Receipts should answer:

* what operation ran
* what inputs were used
* what output was produced
* how it was validated
* whether the artifact is complete, partial or quarantined
* what was deliberately excluded

Example:

```json
{
  "receipt_id": "receipt:claim_history:2026-05-13:archive:001",
  "operation": "archive_partition",
  "artifact_type": "claim_history",
  "partition": "date=2026-05-13",
  "input": {
    "source": "labeler.sqlite.claim_history",
    "where": "createdAt >= 2026-05-13 and createdAt < 2026-05-14",
    "schema_version": 1
  },
  "output": {
    "path": "data/artifacts/claim_history/date=2026-05-13/part-000.parquet",
    "row_count": 1431120,
    "content_hash": "sha256:..."
  },
  "validation": {
    "row_count_checked": true,
    "sample_hashes_checked": true,
    "duckdb_query_checked": true
  },
  "known_exclusions": [
    "rows with invalid createdAt outside supported partition range"
  ],
  "state": "sealed",
  "created_at": "2026-05-14T00:15:00Z"
}
```

## Write Paths

### Path A: Retention-Time / Scheduled Cold Capture

Current forward path, with an important refinement: cold capture uses the retention loop today, but it should not be conceptually dependent on destructive retention completing.

```text
cold-capture pass starts
  → identify oldest eligible candidate date by bounded indexed iteration
  → write one date partition to temp Parquet
  → validate
  → atomic rename
  → receipt
  → then raw_strip / prune as budget allows, if this pass is also carrying destructive lifecycle work
```

Reason for order:

> Cold capture must not sit behind destructive or budget-consuming maintenance.

If destructive retention is parked or disabled, cold capture may still be scheduled independently as a pressure-gated cold-path producer. The carrier is implementation detail; the invariant is that archive/cold capture must be able to make progress without waiting for raw stripping or pruning to finish.

### Path B: Cron Mirror / Legacy Compatibility

Converts existing JSONL archives to Parquet.

Use for:

* historical backfill
* legacy compatibility
* operator-run replay

Do not use as the primary forward path if JSONL production is downstream of retention work that may starve.

### Path C: Future Segment Writer

Longer-term, a dedicated segment writer may produce Parquet directly from a durable spool or batch buffer.

Do not build until hot/cold boundaries are stable.

## Read Paths

### DuckDB Facts Export

Reads Parquet partitions and produces facts artifacts.

Initial mode:

```text
DuckDB over Parquet → compatibility facts.sqlite snapshot
```

Later mode:

```text
Labelwatch reads DuckDB/Parquet-derived artifacts directly
```

Acceptance requires consumer inventory first.

### Reports / Audits

Reports should read cold artifacts, not hot SQLite.

### Longitudinal Rechecks

Longitudinal should eventually read historical context from cold artifacts and only touch hot SQLite for current unresolved state.

## Retention Model

Cold retention:

```text
find partitions older than policy
verify no live dependency
write drop receipt
move to pending-delete/
after grace period, delete
append manifest state=dropped
```

Hot SQLite retention:

```text
strip raw payloads after cold capture receipt exists
prune hot rows only after partition sealed
keep current operational subset bounded
```

Important distinction:

```text
retention = logical lifecycle
VACUUM/rebuild = physical SQLite reclaim
partition drop = physical cold reclaim
```

## Quarantine Model

Malformed rows should not poison partition logic.

Examples:

* bogus createdAt years such as 1997 or 2123
* missing timestamps
* invalid DID/URI fields
* malformed payloads

Quarantine path:

```text
artifacts/quarantine/<reason>/date_observed=YYYY-MM-DD/part-000.parquet
```

Receipt records:

* reason
* count
* sample IDs/hashes
* whether excluded from facts export

Keeper:

> Bad timestamps do not get to define time.

## Phased Plan

### Phase -1: Anticipatory Research Spikes

Do not wait for every known artifact-store failure mode to happen locally before learning from prior art.

This phase is not implementation. It is a set of bounded research spikes that produce short write-ups and either revise the plan, resolve an open question, add a guardrail or explicitly defer a concern.

Keeper:

> Anticipation is not overbuild when the failure class is already known.

Each spike must have:

```text
question
bounded scope
prior art checked
local experiment if cheap
output artifact
resulting disposition
```

Candidate spikes:

#### Atomic rename and filesystem semantics

Question:

> What guarantees does `/mnt/zonestorage` provide for temp-write → atomic rename → receipt?

Scope:

* identify filesystem and mount options
* test same-directory rename behavior
* test crash-ish partial-write recovery with disposable files
* document fsync requirements if needed

Output:

```text
research/atomic-rename-semantics.md
```

#### Artifact/receipt crash consistency

Question:

> What happens if artifact rename succeeds but receipt write fails, or receipt exists for a missing artifact?

Prior art:

* Delta transaction log
* Iceberg manifests / manifest lists
* Hudi commit timeline

Output:

* chosen recovery discipline
* orphan artifact policy
* phantom receipt policy
* artifact doctor requirements

#### Parquet schema evolution

Question:

> What schema changes are allowed without breaking readers?

Scope:

* column addition
* nullable fields
* type widening
* renamed fields
* schema_version discipline

Output:

```text
research/parquet-schema-evolution.md
```

#### DuckDB over local Parquet at expected scale

Question:

> Where does DuckDB-over-Parquet stop being a script and start becoming a service-shaped dependency?

Scope:

* 30-day synthetic or real claim_history partition set
* facts-export-style queries
* memory use
* file descriptors
* latency
* spill behavior

Output:

```text
research/duckdb-parquet-scale.md
```

#### Tiny-file and compaction threshold

Question:

> At what file/partition count does scan overhead make compaction non-optional?

Prior art:

* Iceberg compaction guidance
* Delta small-file management
* DuckDB Parquet scan behavior

Output:

* row-group/file-size target range
* compaction trigger candidate
* explicit deferral if not yet needed

#### Manifest scaling

Question:

> When does append-only JSONL manifest scanning become its own problem?

Prior art:

* Iceberg manifest lists
* Delta transaction log checkpoints
* Hudi timeline

Output:

* threshold for sidecar index
* checkpoint/snapshot policy if needed
* explicit non-adoption list

#### Out-of-order arrivals and watermarks

Question:

> When is a date partition complete if `created_at` and `observed_at` disagree?

Scope:

* late-arrival policy
* correction/rewrite policy
* sealed vs reopen semantics
* quarantine path for bad timestamps

Output:

```text
research/partition-watermarks.md
```

#### Bad timestamp catalog

Question:

> How many rows have bogus, NULL, impossible or future timestamps?

Scope:

* bounded indexed queries only
* no live full scans unless scheduled
* quantify quarantine load

Output:

* quarantine sizing
* partition policy update

#### Non-adoption notes

Question:

> What do Iceberg, Delta and Hudi solve that we deliberately are not adopting yet?

Output:

```text
research/non-adoption-iceberg-delta-hudi.md
```

This is the anti-reactive layer. It prevents the plan from becoming merely a museum of driftwatch injuries.

### Phase 0: Stabilize Current Plant

Partially complete, not closed.

Current driftwatch proved several stabilization moves:

* ingest protection works better than before
* writer WAL truncation no longer blocks the writer under pressure
* loss vocabulary distinguishes rollback loss from queue-boundary intake loss
* cold capture producer has been validated
* facts_export and longitudinal reductions are intentionally parked behind workload-contention work

But Phase 0 is not closed while incident-era reductions remain in the prod override.

Open stabilization debt:

* facts_export disabled
* longitudinal / claim_recheck disabled
* queue health still needs worker-enabled context
* hot SQLite still large enough that rebuild/reclaim remains unresolved
* cold path is forward-producing but not yet the only analytical source

Exit criteria:

* drop_frac stable near 0
* rollback_lost 0
* WAL bounded
* disk runway known
* cold capture producer validated
* incident-era reductions either re-enabled safely, replaced by cold-path readers, or explicitly retired
* health distinguishes disabled-by-design from broken/stuck

### Phase 1: Claim-History Parquet Forward Path

Current Option A.

* retention-time Parquet capture
* archive first
* one candidate day per pass
* receipts per partition
* bypass JSONL middleman

Exit criteria:

* recent partitions drain to current retention window
* DuckDB can query cross-partition
* no hot-path impact
* invalid timestamps quarantined or documented

### Phase 2: Facts Export Consumer Inventory

Before production DuckDB facts export, inventory what Labelwatch actually consumes.

Questions:

* which `facts.sqlite` tables are read?
* which columns?
* which joins?
* what freshness contract?
* what happens if facts are missing/stale?
* which outputs are public/user-facing?

Exit criteria:

* parity target defined by consumers, not by old table names alone
* stale legacy facts behavior understood

### Phase 2.5: Facts Export Migration Shape Decision

This is a distinct gate, not an implementation detail.

Decision:

```text
DuckDB/Parquet → compatibility facts.sqlite snapshot
```

or:

```text
Labelwatch learns to read DuckDB/Parquet-derived artifacts directly
```

The answer determines Phase 3's real shape.

Compatibility snapshot is safer for consumers but risks preserving stale assumptions. Direct DuckDB/Parquet consumption is cleaner architecturally but changes Labelwatch's read path and failure modes.

Exit criteria:

* chosen compatibility strategy
* rollback strategy
* freshness contract
* consumer parity target
* explicit expiration review if compatibility `facts.sqlite` snapshots are retained

### Phase 3: DuckDB-Backed Facts Export

First production-like read path.

* DuckDB reads Parquet claim_history
* produces compatibility artifact if needed
* no hot SQLite read
* no rowid checkpoint
* date partitions define coverage

Exit criteria:

* one full snapshot cycle passes
* output parity acceptable
* no live DB handles
* rollback path documented
* legacy facts work files removed or quarantined

### Phase 4: Longitudinal Cold Read Split

Longitudinal reads historical context from cold artifacts.

* current unresolved queue in SQLite
* history from DuckDB/Parquet
* bounded writes back to SQLite

Exit criteria:

* recheck queue semantics clear
* disabled consumer disables producer
* stale queued work either expired or drained intentionally
* no WAL pinning from longitudinal history reads

### Phase 5: Raw Payload Offload

Move old raw payloads out of SQLite.

* Parquet raw payload partitions or external raw blobs with references
* SQLite keeps pointer/hash/current metadata
* strip raw from hot rows after cold receipt

Exit criteria:

* hot SQLite growth materially reduced
* raw lookup works for sampled records
* malformed payloads quarantined

### Phase 6: Hot SQLite Rebuild

Once cold path is current, rebuild hot SQLite from the operational subset.

Prefer rebuild over trying to rescue the giant file indefinitely.

Plan:

* freeze or spool ingest
* create new SQLite DB
* copy current operational subset only
* validate counts/invariants
* swap
* retain old DB briefly as rollback

Exit criteria:

* hot DB drastically smaller
* cold history queryable
* ingest resumes cleanly
* VACUUM stops being routine survival work

### Phase 7: Evaluate Service-Grade OLAP Only If Needed

Only consider ClickHouse/Pinot/Druid/etc. if DuckDB/Parquet becomes a service wearing a script hat.

Trigger:

* multiple concurrent consumers
* latency-sensitive historical API
* continuous rollups beyond cron tolerance
* DuckDB jobs become hard to schedule safely
* file manifest management becomes a bottleneck

Keeper:

> Do not replace SQLite heroics with platform heroics.

## MVP Slice

The minimum useful-now slice is not a new database.

It is:

1. `claim_history` Parquet partitions written by retention-time capture.
2. Per-partition receipts.
3. Append-only manifest JSONL.
4. DuckDB query script proving cross-partition reads.
5. A compatibility facts-export prototype reading only Parquet.
6. Docs stating that Parquet is the cold forward path and JSONL is legacy fallback.

This is already close to current reality.

## Implementation Tasks

### Short-Term

* add global manifest writer for claim_history partitions
* ensure per-partition receipt schema is stable
* quarantine bogus createdAt rows rather than silently leaking them
* add `artifact doctor` command:

  * list partitions
  * show gaps
  * verify receipts
  * DuckDB count by date
* inventory facts consumers
* define facts export parity target

### Medium-Term

* productionize DuckDB-backed facts export
* move longitudinal history reads off hot SQLite
* add raw payload offload
* add hot DB rebuild plan

### Long-Term

* define segment writer if needed
* evaluate service-grade OLAP only if DuckDB becomes operationally awkward
* backport operating model into reference atproto-labeler as doctrine first, code second

## Artifact Doctor

A small CLI should exist before the system grows more machinery.

Example commands:

```text
artifact doctor claim_history
artifact list claim_history --from 2026-04-01 --to 2026-05-13
artifact verify claim_history/date=2026-05-13
artifact gaps claim_history
artifact query claim_history 'select date, count(*) group by date'
```

This tool should use manifests and receipts first, not scan the world by default.

## Health Signals

Cold-path health should be separate from platform health.

Suggested fields:

```text
cold_path.enabled
cold_path.latest_partition
cold_path.partition_lag_days
cold_path.missing_partitions
cold_path.last_receipt_state
cold_path.quarantine_count
cold_path.duckdb_query_ok
cold_path.manifest_ok
```

Do not collapse cold-path lag into ingest health.

## Backport Policy

Backport to the reference atproto-labeler as layers:

1. Doctrine/docs
2. Loss accounting and health semantics
3. WAL pressure behavior
4. Pressure-gated retention
5. Parquet cold capture
6. DuckDB facts export only after consumer parity

Do not backport failed implementations as cargo-cult fixes:

* failed writer-thread retention pattern
* blocking writer-owned TRUNCATE under pressure
* DISTINCT date scans over live SQLite
* JSONL mirror as the primary forward path
* Lean/Z3/formal claim machinery before claim discipline exists

But topology mismatch is **not** a veto on anticipatory design. It is a veto on blind transplant.

If driftwatch exposed a failure class that the reference labeler is structurally likely to encounter under scale, then the correct response is not "wait for the reference labeler to fail." The correct response is a bounded anticipation pass:

1. Name the failure class.
2. Identify the topology that produces it.
3. Check whether the reference implementation has that topology now, will likely grow it, or can cheaply avoid it.
4. Look for prior art or known patterns.
5. Choose one of:

   * port now
   * design now, leave disabled
   * add guardrail/test now
   * document only
   * explicitly reject as non-applicable

This prevents two opposite errors:

* cargo-culting driftwatch's local scars into the reference implementation
* using "topology mismatch" as a polite excuse to ignore predictable failure

Keeper:

> Topology mismatch blocks blind transplant, not anticipatory design.

> Port scars as doctrine first; port code when the topology matches or when prior art makes the failure cheaply preventable.

## Open Questions

1. Should manifests live only as JSONL, or also in hot SQLite for transactional lookup?
2. What is the exact schema for claim_history v1?
3. How should invalid createdAt rows be quarantined and surfaced?
4. How much freshness does Labelwatch actually need from facts export?
5. What is the minimal durable spool required before hot DB rebuild?
6. Should raw payloads live in Parquet rows, external blob files, or both?
7. What are the backup/restore semantics for manifests + partitions + receipts?
8. How do we detect manifest/partition drift without scanning every artifact?

## Risks

### Writing a database by accident

If the artifact system grows indexes, query planning, concurrent mutation and bespoke recovery logic, it has failed.

Mitigation:

Keep artifacts immutable-ish, manifests simple and DuckDB responsible for questions.

### Conservatism laundering

"Do not blindly port local scars" can degrade into "do not act until the next system fails the same way." That is not discipline; it is deferred learning.

Mitigation:

For each major scar, run a small anticipation pass: topology check, prior-art check, cheap guardrail candidates and explicit disposition. The output can be "do nothing," but only after the predictable failure has been named and rejected on purpose.

### Manifest drift

Files may exist without manifest entries, or manifest entries may point to missing files.

Mitigation:

Artifact doctor and receipt verification.

### Tiny-file hell

Overpartitioning by DID, labeler or subject creates too many small files.

Mitigation:

Partition first by date/hour. Add clustering only when queries force it.

### Stale cold path

Cold artifacts may lag behind hot state.

Mitigation:

Expose partition lag explicitly. Do not let cold-path lag masquerade as ingest failure.

### Compatibility trap

Producing `facts.sqlite` snapshots forever may preserve old assumptions.

Mitigation:

Treat compatibility snapshots as migration artifacts with an expiration review.

## Keeper Lines

> Do not write a database. Write the admissibility-preserving file lifecycle.

> SQLite keeps the present tense. Parquet keeps the past tense. DuckDB asks questions of the past.

> Cold capture must not sit behind destructive maintenance.

> Retention is logical lifecycle. Rebuild is physical reclaim.

> Observability is not outside the plant.

> Files are not complete because they exist. They are complete because custody says so.

> Topology mismatch blocks blind transplant, not anticipatory design.

> Port scars as doctrine first; port code when the topology matches or when prior art makes the failure cheaply preventable.

> Anticipation is not overbuild when the failure class is already known.

> Nothing non-ingest gets to make ingest lossy.

> Bad timestamps do not get to define time.

> Do not replace SQLite heroics with platform heroics.
