# Driftwatch facts export DuckDB snapshot 001 — Parquet-backed compatibility facts.sqlite

**Status:** ratified slice spec. Filed 2026-06-10 immediately after the
Phase 2.5 decision in `gap-spec-facts-export-consumer-inventory.md`.

**Inherits:**
- Ratification verbatim from
  `gap-spec-facts-export-consumer-inventory.md § Ratification — 2026-06-10`.
- Consumer contract from
  `gap-spec-facts-export-consumer-inventory.md § What labelwatch actually reads from facts.sqlite`.

**Companion:**
- `gap-spec-cold-path-parquet-duckdb.md` (umbrella doctrine)
- `gap-spec-log-structured-artifact-system.md` (this is Phase 3 of 7)
- `gap-spec-facts-export-duckdb-productionization.md` (older productionization
  handle; superseded for the snapshot-001 scope, kept for the cutover-shape
  decisions that still apply when Phase 3.5 lands)

## Architecture sentence

> Parquet is authoritative past. DuckDB is the question engine.
> `facts.sqlite` is a compatibility projection/cache, not the source of custody.

Cite this in the writer module's header. The line is not decoration — it
is the difference between Path A as a doctrinally-impure-adapter (correct)
and Path A as "see, SQLite forever" (incorrect re-read by a future maintainer).

## Premise

Phase 2.5 ratified `A now → B later`. Phase 3 is the implementation of A.

Produce a fresh `facts.sqlite` at retention time by materializing the V0
consumer facts projection. `uri_fingerprint` is derived from existing
`claim_history` Parquet through DuckDB. `actor_identity_facts` is derived
from Driftwatch SQLite `actor_identity_current` for V0, matching today's
projection. DuckDB/Parquet are introduced here as snapshot/materialization
machinery; this phase does not require pre-existing identity Parquet.
Labelwatch consumes the output unchanged via the existing ATTACH path. No
labelwatch changes are in scope.

## Scope (what the slice produces)

1. **Reader:** open the existing cold-path Parquet partitions where they
   already exist. For V0 this means `claim_history` partitions written by
   Phase 1 / Phase 3.5 (see
   `gap-spec-cold-path-phase-3.5-forward-parquet-capture.md`). Identity
   facts use the SQLite source split called out above.
2. **Writer:** materialize a fresh `facts.sqlite.tmp` with the schemas
   labelwatch consumes (per the inventory).
3. **Populate at least:**
   - `actor_identity_facts` — 8 columns, schema byte-identical to today's
     `facts_export.py` producer
   - `uri_fingerprint` — 4 columns, schema byte-identical to today's
     `facts_export.py` producer
4. **Preserve only if cheap:**
   - `fingerprint_hourly`
   - `fingerprint_bounds`
   - `meta`
   These are not currently read by labelwatch. Skip if they require
   nontrivial DuckDB plumbing; their absence is a documented driftwatch-side
   decision, not a consumer break.
5. **Atomic rename:** `facts.sqlite.tmp` → `facts.sqlite` only after the
   manifest is written and the table row counts have been verified against
   their post-filter expectations. No partial-state half-flushes.
6. **Manifest/receipt:** emit alongside the artifact (see § Manifest below).

## Manifest

The doc that scoped Path A noted that A "inherits the implicit mtime
contract." Add the manifest anyway. Cheap custody is cheap until you don't
do it and later need archaeology.

Fields (minimum):

```
generated_at                                  -- writer start, ISO-8601 UTC
producer_git_sha                              -- if available, else null
input_parquet_paths                           -- list, or globs resolved
input_partition_window                        -- {min, max} inclusive
output_path                                   -- /mnt/.../facts.sqlite
row_counts:
  actor_identity_facts                        -- written
  uri_fingerprint                             -- written
  fingerprint_hourly                          -- written or null if skipped
  fingerprint_bounds                          -- written or null if skipped
  meta                                        -- written or null if skipped
uri_fingerprint_rows_quarantined_bogus_created_epoch
uri_fingerprint_min_created_epoch_written
uri_fingerprint_max_created_epoch_written
duration_seconds
writer_version                                -- semver of this writer module
```

Location: alongside the snapshot, e.g.
`/mnt/zonestorage/driftwatch/data/facts.sqlite.manifest.json` (atomic write
+ rename, like the snapshot).

## Bogus timestamp quarantine — in scope, not deferred

Per the ratification, Phase 3 quarantines `uri_fingerprint` rows with
`created_epoch` outside policy bounds. No silent pass-through.

Policy:

```
valid_created_epoch:
  >= 2020-01-01      # ATProto pre-history; nothing real before this
  <= generated_at + 1 day
```

Quarantined rows are:
- excluded from `uri_fingerprint` output
- counted in `uri_fingerprint_rows_quarantined_bogus_created_epoch`
- not written to a side ledger (yet). If forensic recovery is later
  needed, the source Parquet still has them; the quarantine is at the
  projection boundary, not at the canonical store.

Acceptance test 5 below asserts this.

## Doctrine compliance check

| Doctrine | Phase 3 posture |
|---|---|
| Hot/cold separation | `uri_fingerprint` writes from Parquet; V0 identity facts use the current SQLite projection until an identity cold-path stream exists. |
| Single-writer invariant on hot DB | Unaffected; no hot DB writes here. |
| Custody | Parquet is canonical for `claim_history`; the V0 manifest documents the mixed-source projection. |
| Detect-only structural constraint | No labels emitted; this is a derived artifact. |
| Aggregate-first | `uri_fingerprint` is per-URI but that's the existing contract, not new surface. |

## Acceptance tests (minimum, non-ceremonial)

1. Snapshot writer creates `facts.sqlite.tmp` then atomically renames to
   `facts.sqlite`. The rename does not occur if any prior step failed.
2. `actor_identity_facts` schema matches current consumer contract
   byte-for-byte (8 columns, types, primary key).
3. `uri_fingerprint` schema matches current consumer contract byte-for-byte
   (4 columns, types, primary key + `idx_uri_fp` index).
4. Row counts match fixtures: identity rows come from the SQLite
   `actor_identity_current` fixture without filtering; URI rows come from
   Parquet after bogus-epoch quarantine filters.
5. Bogus `created_epoch` rows (year < 2020 OR > generated_at + 1 day) are
   quarantined and counted in the manifest. Fixture includes at least one
   row at year=1997 and one at year=2199.
6. Existing labelwatch ATTACH smoke works unchanged against the produced
   `facts.sqlite`. (Run `scan.py` derive pass against the snapshot in a
   test fixture; assert it does not raise and `lag_sec_claimed` falls
   within sensible bounds.)
7. Missing or empty Parquet input produces a controlled identity-only
   snapshot: identity row counts come from SQLite, URI row count is 0,
   quarantine count is 0, input paths are empty, and partition window is
   null — not partial junk. Document this in the writer header.
8. Manifest records input Parquet paths, output path, per-table row
   counts, and quarantine counts. JSON parses; required fields present.

## Out of scope for Phase 3

- Labelwatch DuckDB dependency.
- `scan.py` rewrite.
- Direct Parquet consumption by any consumer.
- New consumer contract changes.
- Dropping unread tables (`fingerprint_hourly`, `fingerprint_bounds`,
  `meta`) unless explicitly ratified separately.
- Schema-drift handling for Parquet (deferred to Phase 4+ or a separate
  spec when the first schema rev happens).
- Retention cadence for the snapshot itself (defer; default: each writer
  run overwrites; last-good-on-disk).

## Rollback path

If the snapshot writer misbehaves in production:

1. Disable the writer cron/timer.
2. The last successfully-renamed `facts.sqlite` stays in place (atomic
   rename guarantee). Labelwatch continues consuming it; mtime ages but
   that is the existing failure mode (`snapshot=0` + caveat, no 5xx).
3. The pre-Phase-3 URI producer path (the SQLite-backed `facts_export.py`)
   is **not** the rollback target — it depends on the hot claim-history scan
   path that the cold-path doctrine retired. V0 identity still deliberately
   uses the current SQLite identity projection. Rollback to "last good
   snapshot until diagnosis" is the only supported state.
4. Diagnosis happens against the manifest + the writer log, not against
   the consumer.

## Phase 3.5 handle (preserved, not started)

After Phase 3 has run snapshot cycles successfully:

> Phase 3.5: labelwatch direct DuckDB projection spike.
>
> Target the hard site first: `tmp_candidate_uris JOIN drift.uri_fingerprint`
> (`scan.py:792–814`). If DuckDB cross-source JOIN works cleanly, identity
> facts is paperwork. If it doesn't, we learned at low cost without
> touching labelwatch's hot path.

Phase 3.5 is filed here, not staffed. Forcing-case gate applies before
implementation: at least one of (a) a third consumer requesting the
artifact, (b) snapshot writer cost outgrowing its budget, or (c) explicit
ratification that the snapshot is becoming the kind of lie the doctrine
was meant to prevent.

## Acceptance for this spec

- [x] Scope concrete (5 numbered scope items).
- [x] Manifest fields enumerated.
- [x] Bogus-timestamp quarantine policy specified.
- [x] Acceptance tests (8) named.
- [x] Out-of-scope items enumerated.
- [x] Rollback path documented.
- [x] Phase 3.5 handle preserved without authorizing implementation.
