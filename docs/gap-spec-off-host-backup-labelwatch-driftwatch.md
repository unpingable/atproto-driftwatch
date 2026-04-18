# Gap Spec: Off-Host Backup for Labelwatch / Driftwatch

## Status

Open gap. No off-host backup exists today.

This is a sibling to
`gap-spec-storage-layout-labelwatch-driftwatch.md`. That spec settles
*where live state lives on the host.* This one settles *how we survive
losing it.*

## Problem

As of 2026-04-17 migration, Labelwatch and Driftwatch live state is
housed on a single Linode block-storage volume mounted at
`/mnt/zonestorage`. There is no second copy anywhere. If that volume
is lost, detached incorrectly, corrupted, or the host is compromised,
the entire operational history of both observatories is gone with no
restore path.

The only backups that currently exist are the `.pre-migration` copies
left on root during the same migration — still on the same host, and
in any case stale within hours under the append-heavy ingest workload.

Observatory state has moved beyond "small enough to lose" but the
backup posture has not caught up.

## Scope

This spec covers **how off-host copies of Labelwatch / Driftwatch
state are produced, stored, verified, and restored.**

It does **not** settle:

- which files belong on which volume (see storage-layout gap spec)
- retention horizons by evidence class
- historical export formats for downstream analysis
- archival / cold-storage tiering

## Failure Modes to Cover

1. **Volume loss / corruption.** Block storage detach or filesystem
   failure. Primary scenario.
2. **Host loss.** VM destroyed, locked out, or provider incident.
3. **Regional outage.** Linode region unavailable.
4. **Rogue operator action.** `rm -rf`, misdirected VACUUM, script
   with a wrong path. Includes Claude instances, including this one.
5. **Creeping data corruption.** Subtle logical damage discovered
   weeks later; need a pre-corruption restore point.
6. **Compromise / ransomware.** Adversary with host access encrypts
   or deletes the volume.

Point-in-time rollback during normal operations is **out of scope
here** — that's what the storage-layout spec's "keep rollback copies
briefly" clause covers. Off-host is about *surviving loss of the
entire host footprint*.

## Current Reality

- No off-host backup exists.
- `.pre-migration` rollback copies are on-host and go stale fast.
- The user's desktop has ~143 GB free and could fit the current
  ~107 GB footprint, but that's a workstation, not a backup target.
- No backup-related monitoring, alerting, or verification exists.

## Goals

1. At least one off-host copy of durable observatory state exists at
   all times.
2. The copy's age is bounded and monitored — stale backups are
   alerted, not silently accepted.
3. Restore has been rehearsed at least once before it is needed.
4. Backup failure is visible, not silent.
5. The backup path does not itself become a new disk-pressure vector
   on host or target.

## Non-Goals

1. Continuous replication. The observatory workload does not need
   synchronous or near-synchronous durability. Cadence-scaled
   copies are sufficient.
2. Cold-archive tiering. This is live-recovery posture, not
   long-term archival.
3. Backing up regenerable byproducts at full fidelity when the
   source-of-truth can rebuild them. (e.g., `facts.sqlite` rebuilds
   from `facts_work.sqlite`.)
4. Pretending the user's desktop is a durable backup target on its
   own.

## Design Axes to Settle

### What to back up

- **Must**: `labeler.sqlite`, `labelwatch.db`
- **Should**: `facts_work.sqlite`, `archive/` (driftwatch's long-term
  claim_history gzipped JSONL)
- **Maybe**: `facts.sqlite` (rebuildable from `facts_work` — skipping
  costs one rotation to reconstruct)
- **Don't**: WAL / SHM files (they are snapshot-time ephemera), empty
  service caches

### Cadence

Options, from cheapest to most operationally heavy:

- **Daily full snapshot** of consistent DB copies (via SQLite
  `.backup` API or `VACUUM INTO`). Simple, up to 24h loss on restore.
- **Incremental deltas on top of weekly baselines** (rsync delta or
  similar). Cheaper bandwidth, more state to track.
- **Continuous WAL shipping**. Lowest RPO, highest complexity. Likely
  overkill for this workload.

### Target storage

- **Linode Object Storage (S3-compatible).** Cheapest, same provider
  (not true cross-region). ~$0.02/GB/month.
- **AWS S3 / S3 Glacier / Wasabi / Backblaze B2.** True
  cross-provider. Pricier but real durability insurance.
- **User's desktop.** Cheap, zero-cost storage, but not a durable
  target (workstation, can crash, not always on).
- **Second Linode in another region.** Proper cross-region but doubles
  the infra cost.

Reasonable starting posture: **Linode Object Storage for cadence-level
durability, with at least one monthly snapshot pulled to desktop for
provider-independent insurance.**

### Consistency

- SQLite backups must be consistent snapshots, not "copy a live file
  and hope." Use the SQLite backup API, `VACUUM INTO`, or a brief
  quiesce + `cp` window.
- Each backup must verify `PRAGMA integrity_check` (or at least
  `quick_check`) on the target before it counts as a successful
  backup.
- WAL shipping, if chosen later, has its own consistency story.

### Restore rehearsal

- Restore-from-backup must be rehearsed at some cadence (quarterly?)
  on a throwaway host.
- A backup that has never been restored is speculative. Untested
  backups are worse than no backup because they encourage false
  confidence.

### Monitoring & alerting

- Failed backups generate an nq finding.
- Backup age exceeding threshold (e.g., 2× expected cadence) generates
  an nq finding.
- Restore-rehearsal-overdue can be a lower-priority finding.

## Current Reality vs Minimum Acceptable

| Capability | Now | Minimum acceptable |
|---|---|---|
| Off-host copy exists | no | yes |
| Backup age monitored | no | yes, with alerting |
| Consistency verified | no | integrity_check on target |
| Restore rehearsed | never | at least once |
| Documented restore flow | no | yes, in runbook |

## Open Questions

1. Is Linode Object Storage sufficient for primary durability, or does
   the cross-provider case (AWS/Wasabi/B2) need to be in place from
   the start?
2. Daily cadence or more aggressive?
3. Should `archive/` (day-partitioned gzipped JSONL) be treated
   specially — it's already compressed and append-mostly — or folded
   into the same backup stream?
4. How long does a full restore take? (Must be measured, not guessed.)
5. How do we protect against adversarial deletion of backups
   themselves? (Versioning on the bucket? Immutable retention?)
6. Who/what has credentials to the backup target, and how are those
   credentials rotated?
7. Does this spec need to accommodate multi-host observatories later,
   or is single-host-with-off-host-copy the target shape?

## Exit Criteria

This gap closes when:

1. At least one off-host backup of class A DBs exists and is less than
   one cadence-interval old.
2. Backup success/failure and age are reflected in nq as findings.
3. A documented restore procedure exists and has been executed at
   least once end-to-end on non-production state.
4. Credential rotation for the backup target is defined.
5. An operator can answer "if the volume disappears right now, what
   do we lose and how long does it take to come back" without guessing.

## Notes

Backups are the kind of thing observatories either have before they
matter or wish they had after they matter. Writing this spec does not
yet produce a backup; it produces the scaffolding that makes not
having one obvious.
