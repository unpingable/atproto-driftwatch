# Gap Spec: Storage Layout for Labelwatch / Driftwatch

## Status

Open gap. Immediate host-level migration exists as of 2026-04-17, but
long-term storage layout policy is not yet formally specified.

Off-host backup is treated as a separate gap because "where live state
lives" and "how we survive losing it" are related but distinct design
questions. See `gap-spec-off-host-backup-labelwatch-driftwatch.md`.

## Problem

Labelwatch / Driftwatch currently produce large, mutable SQLite state
that grows, churns, and periodically requires compaction. Root-disk
pressure exposed that the system had drifted into an implicit storage
policy: bulky observatory state lived wherever it happened to fit.

That is no longer acceptable.

An attached volume is now mounted at `/mnt/zonestorage`, and the
Labelwatch and Driftwatch live databases have been migrated there. This
solves the immediate host-pressure problem, but it does **not** yet
constitute a complete storage-layout policy. The system still lacks an
explicit statement of:

- which state belongs on root vs attached storage
- which SQLite files are durable working state vs regenerable byproduct
- how cutover / rollback / future migrations should work
- when large observatory state should be treated as volume-resident by
  default

## Scope

This spec covers **live storage layout** for Labelwatch / Driftwatch on
the host.

It does **not** settle:

- off-host backup / replication / archival policy (separate gap)
- retention horizons by evidence class
- long-term historical export formats
- cross-region disaster recovery

## Current Reality

### Host layout

- Root disk is limited and has shown pressure repeatedly under
  observatory growth.
- Attached block storage is available and mounted at `/mnt/zonestorage`.
- As of 2026-04-17, Labelwatch and Driftwatch live databases are
  volume-resident via symlink at their legacy paths.

### Current databases

- `labeler.sqlite` is large, high-churn, and not safely housed on root
  long-term. **Volume-resident** since 2026-04-17.
- `facts_work.sqlite` is also large and mutable. **Volume-resident**
  since 2026-04-17.
- `facts.sqlite` is rotational / rebuildable from `facts_work.sqlite`
  via the facts-export rotation. **Volume-resident** since 2026-04-17.
- `labelwatch.db` is comparatively small and currently not exhibiting
  allocator pathologies. **Volume-resident** since 2026-04-17 as a
  de-facto placement from the migration pass; this spec ratifies or
  revisits that decision explicitly (see Open Questions).

### Observed behavior

- SQLite freelist growth in `labeler.sqlite` is material and ongoing
  (~3 GB/day at current ingest and retention cadence).
- This is operationally useful signal, not alert-noise to suppress.
- Migration to attached storage solved root pressure, but did not solve
  allocator waste or long-term layout policy.

## Operational Notes (from the 2026-04-17 migration)

- Hardened systemd services using `ProtectSystem=strict` with explicit
  `ReadWritePaths` / `ReadOnlyPaths` must include the volume path in
  their allowlists. A path migration that works for unsandboxed
  services may fail under hardening with
  `sqlite3.OperationalError: unable to open database file`. Future
  observatory services should either allowlist the volume mount from
  the start or preserve a legacy path via bind mount.
- Docker bind mounts with a symlinked source path resolve the symlink
  at mount time (kernel behavior), so `./data:/app/data` in
  `docker-compose.yml` continued working after `data` became a symlink
  to the volume. No compose edit was needed.
- Plain `cp` was 16x faster than `VACUUM INTO` for migrating a 54 GB
  DB, because `VACUUM INTO` does random-order page reads that saturate
  the source disk. For pure-relocation migrations (no compaction),
  plain copy is the default. `VACUUM INTO` is for compaction windows
  that explicitly want the rebuild.

## Goals

1. Keep bulky mutable observatory state off root.
2. Preserve a clear split between:
   - OS / runtime / app code
   - large persistent observatory state
3. Make future growth legible and routine rather than incident-driven.
4. Keep rollback / cutover mechanics boring.
5. Avoid path ambiguity that turns storage placement into folklore.

## Non-Goals

1. Replacing SQLite.
2. Designing archival storage.
3. Solving backup / ransomware / regional outage recovery here.
4. Prematurely moving all project state to attached storage.
5. Generalizing this into a host-wide doctrine beyond observatory
   workloads.

## Working Classification

### Storage class A: volume-resident durable working state

Large mutable databases whose growth, churn, or compaction behavior
can pressure root or destabilize host operation.

Current members:

- `labeler.sqlite` (driftwatch)
- `facts_work.sqlite` (driftwatch)
- `labelwatch.db` (labelwatch) — currently placed here; see Open
  Questions

### Storage class B: regenerable or rotational byproduct

Files that may be large but are either rebuilt, rotated, or
operationally disposable relative to class A.

Current members:

- `facts.sqlite` (driftwatch, rebuilt from `facts_work.sqlite` via
  rotation)

### Storage class C: root-resident local operational state

Smaller databases, code, units, caches, and logs that do not justify
volume placement.

Current members:

- app code
- systemd units
- nq and related observability tooling (unless growth proves
  otherwise)
- small caches and per-service runtime scratch

## Policy Direction

### Primary rule

**Large mutable observatory SQLite state for Labelwatch / Driftwatch
should live on attached storage, not root.**

### Default placement

The default home for class A state is the attached volume, currently:

`/mnt/zonestorage/{service}/data/...`

Example canonical paths:

- `/mnt/zonestorage/driftwatch/data/labeler.sqlite`
- `/mnt/zonestorage/driftwatch/data/facts_work.sqlite`
- `/mnt/zonestorage/driftwatch/data/facts.sqlite`
- `/mnt/zonestorage/labelwatch/labelwatch.db`

### Root usage

Root remains the home for:

- OS
- service definitions and systemd units
- code and deploy assets
- small local state
- logs and caches within normal bounds

Root is **not** the default long-term home for bulky observatory
SQLite files.

## Path / Cutover Guidance

Preferred long-term shape:

- canonical live DB paths reside on the volume
- services point directly to those paths where practical

Acceptable transition shape:

- volume-backed canonical files with symlink or bind-mount
  compatibility at legacy paths

Design preference:

- boring and explicit beats elegant and surprising

That means a symlink or bind mount is acceptable if it reduces
migration risk, but long-term path ownership should remain legible.

## Rollback Expectations

For any future storage cutover:

1. new live DB copy is created on target volume
2. integrity and basic application behavior are verified on target
3. service cutover happens only after verification
4. prior copy is retained temporarily as rollback
5. old copy is removed only after stability window passes (default:
   at least 1-2 hours of clean operation, longer if off-host backup
   not yet in place)

Rollback copies are operational scaffolding, not archival policy.

## Open Questions

1. Should `labelwatch.db` remain volume-resident as part of the
   observatory-owned SQLite rule, or is there a principled reason to
   move it back to root? (Current placement is volume. Default
   assumption of this spec is to keep it there for consistency.)
2. Should `facts.sqlite` remain class B, or does its operational role
   justify colocating it with class A despite rebuildability? (It is
   currently volume-resident because it lives alongside
   `facts_work.sqlite` in the driftwatch data dir.)
3. Do we want a more permanent mountpoint than `/mnt/zonestorage`
   once this stops being "new" and starts being infrastructure?
4. Should service configs reference volume paths directly, or should
   we preserve stable legacy paths via symlink / bind mount?
5. What host-level threshold should trigger review of "root-resident
   by default" for observatory components not yet on the volume?

## Decision Triggers

This gap can be considered resolved when the following are explicit
and implemented:

- storage classes are named and documented
- canonical live paths are defined
- service configs or compatibility indirection are stabilized
- rollback pattern is documented
- root-vs-volume rule is explicit enough that future growth does not
  require re-litigating placement

## Exit Criteria

This gap closes when:

1. Labelwatch / Driftwatch class A databases are stably
   volume-resident.
2. The canonical path policy is documented in deploy / runbook
   material.
3. Root is no longer treated as an acceptable default for bulky
   mutable observatory SQLite state.
4. A related gap spec exists for off-host backup / restore posture.
5. Future contributors can answer "where should new observatory state
   live?" without reconstructing this incident history from chat logs.

## Notes

This gap exists because the system already chose a storage policy in
practice; it simply did so implicitly, under pressure, and with root
disk as the absorber of ambiguity.

That phase is over.
