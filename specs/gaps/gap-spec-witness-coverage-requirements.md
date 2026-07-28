# GAP: Witness Coverage Requirements — Driftwatch

> Status: candidate / requirements only. Filed 2026-07-28, after the
> 2026-07-23..27 silent outage (receipt:
> `/tank/nfs/driftwatch-recovery/2026-07-27/RECEIPT.md`).
> Scope: what "full" external witness coverage of driftwatch means. This doc
> constrains any witness implementation (NQ-ng or otherwise) and names the
> signals this repo is obliged to export. It does not design the witness.
> Subsumes the witness-facing half of `gap-spec-self-health-contract.md`
> (whose exporter-side obligations remain in force there).

## Why this exists

Driftwatch was fully blind — zero ingest, cursor frozen, volume at 0 bytes
free — for 3d20h while `/health` returned `{"status":"ok"}` and Docker
reported the container healthy. The then-current witness (NQ) had no
driftwatch service subject and no `/mnt/zonestorage` disk subject; its one
host collector reported the root filesystem only. Nothing fired because
nothing was looking.

Two prior incidents had the same shape at lower amplitude: 2026-07-13 (WAL
pinned by an orphaned reader; 19–26% of the stream shed for 97 minutes,
surfaced only by manual archaeology) and 2026-07-18..20 (retention
self-locked; ~4,500 events/min dropped for days).

The recurring failure is not "a metric was wrong." It is **liveness standing
in for observation**. A witness that cannot distinguish *process up* from
*observatory observing* will be lied to by every future incident of this
class.

## Layering rule

Requirements are split into two families that MUST remain separately
attributable end to end:

- **H-x (host witnesses)** — facts any host witness could collect with no
  knowledge of driftwatch: mounts, processes, units, resources.
- **A-x (application / APM witnesses)** — facts meaningful only against
  driftwatch's own claims about what it does: cursor advancement, commit
  progress, retention progress, export freshness.

Composite findings (C-x) may join the two families, but a witness MUST be
able to report which individual signals contributed. A finding that cannot
name its inputs is not admissible testimony.

Rationale: the 07-23 incident *was* the composite `host says disk full ∧ app
says healthy`. Collapsing the layers is how that contradiction stayed
invisible.

## H — Host-level requirements

| ID | Requirement | Provenance |
|----|-------------|------------|
| H-1 | **Every mounted filesystem driftwatch writes to** is a disk subject: used %, free **bytes** (not just %), inode %, and growth rate. Today that is `/mnt/zonestorage` AND `/` (docker overlay, logs, cold-offload). A witness watching only one is out of compliance. | 07-23: the watched fs was fine; the unwatched one was at 0 bytes. |
| H-2 | Free-space semantics MUST use the effective writer's uid. ext4 root reserve (5%) means non-root `Avail` hits 0 while root can still write; the production writer runs as root. Witness both numbers or the one matching the writer. | Discovered during recovery: `df` Avail read 0 for non-root at 6.7 GB actual free. |
| H-3 | Container/service state as a subject (`driftwatch` container: running, restart count, OOM kills), distinct from any app signal. | Baseline liveness; necessary, never sufficient. |
| H-4 | The host cron jobs that mutate driftwatch state (`maintenance.sh`, `parquet_mirror`, `resolver_pending_sampler`) are witnessed for *execution and success*, not assumed. A cron that silently stops or errors is a coverage loss. | parquet_mirror's source dir was deleted 2026-07-27; whether it noops or errors was unknown at close. |
| H-5 | Disk runway as a first-class derived signal: `free_bytes / observed_growth_rate` in days, per H-1 filesystem. Alert on runway, not on a fixed % threshold. | 196G filled in ~10 weeks; a % threshold fires either too early or too late. |

## A — Application-level (APM) requirements

| ID | Requirement | Provenance |
|----|-------------|------------|
| A-1 | **Cursor advancement vs wall clock** is the primary epistemic liveness signal: `now - last_cursor.updated_at` beyond N minutes is a finding regardless of every other green light. This is the signal that was frozen for 3d20h behind an "ok" health check. | 07-23..27. |
| A-2 | **Commit progress distinct from ingest liveness**: events are being *persisted* (write path advancing), not merely received. The 07-13 incident was exactly live-but-not-committing. | 07-13; gap-spec-self-health-contract finding shape. |
| A-3 | `drop_frac` **sustained against its own baseline**, not instantaneous. Includes all shedding buckets: queue overflow, `rollback_lost`, lock-conflict. A shedding bucket without a witness reading is a silent-loss channel (2026-04-30: 444 events lost while platform_health said `recovered`). | 04-28..05-05 incident family. |
| A-4 | `queue_depth` read against baseline semantics: a constant non-zero depth (e.g. 10735 with the recheck consumer disabled by design) is BASELINE, not backlog. The witness config MUST carry per-signal baseline notes; raw-counter alerting on this signal is out of compliance. | Health-check disposition, ratified 2026-06-15. |
| A-5 | **WAL telemetry**: size, growth rate, and checkpoint blockage (`checkpoint_busy` climbing while `checkpoint_done` flat). WAL > threshold with no forward checkpoint progress is a finding (orphaned-reader signature). | 07-13 (WAL 22MB→6.8GB); labelwatch WAL-bloat scars. |
| A-6 | **Retention progress**: passes completing, `retention_lag_s`, and rows-pruned trending nonzero when data is age-eligible. Retention silently gated/looping without progress was the 07-18 incident; ENABLE_RETENTION=0 config drift was the 07-23 precondition. Config state (enabled/disabled) is itself a witnessed fact. | 07-18..20; 07-23. |
| A-7 | Capacity model: live pages, freelist pages, page reuse rate → projected DB growth. This is the app-level half of H-5; `df` alone cannot see freelist reuse (a dense 186 GiB file with freelist 0 and a 186 GiB file with 50% freelist read identically from the host). | Recovery analysis 07-27. |
| A-8 | Resolver: pending count, drain rate, and **aged-tail** (oldest pending, share older than 24h) per the existing sampler's fields. Throughput-green with a starved tail is the known failure mode. | resolver-pending-aged-tail watch item, 2026-06-24. |
| A-9 | Export freshness where an export is *enabled*: facts_export age vs its cadence. Where an export is disabled by config, the witness carries that as declared state — staleness of a disabled export is not a finding (but silent transition enabled→disabled is). | facts_export stale=parked disposition; config drift class from A-6. |
| A-10 | Jetstream reconnect churn rate vs baseline. | 236 reconnects preceded 07-23 diagnosis; cheap leading indicator. |
| A-11 | The witness MUST NOT source any A-x signal solely from `/health` (liveness-only) or the Docker healthcheck. `/health/extended` fields and log/DB-derived readings are the admissible sources until the self-health contract lands typed signals. | `/health` lied for four days. |

## C — Composite findings (join H and A)

| ID | Finding shape | Incident it names |
|----|---------------|-------------------|
| C-1 | *Evidence loss*: service up ∧ ingest nominal-or-degraded ∧ commit progress absent-or-shedding ∧ (optionally) storage pressure rising. | 07-13, 07-18, 07-23. |
| C-2 | *Health contradiction*: app health surface reports ok ∧ any of A-1/A-2/H-1 in finding state. The contradiction itself is reportable — it means the health surface is lying and should not be trusted for the duration. | 07-23..27. |
| C-3 | *Runway closure*: H-5 or A-7 runway below the time it would take an operator to respond (days, not hours). | 196G filled with no alert at any point on the curve. |

## X — Custody and anti-requirements

- X-1 **Witness reads; never writes.** No witness component mutates driftwatch
  state, config, or data. (Self-subject reconciliation requires externality —
  a component cannot resolve a finding whose subject is itself.)
- X-2 **Testimony, not standing.** Witness findings are evidence for an
  operator; they mint no authority, trigger no automated remediation, and
  must not be consumed by driftwatch itself as an input to its own claims.
- X-3 **Declaration-aware.** Every signal above MUST be coverable by a
  maintenance declaration (declare-before-disturb), and a declaration
  annotates — it never suppresses. Disturbance past the declared window is a
  new signal (overrun).
- X-4 **Baselines are part of the config.** Any signal whose healthy state is
  nonzero (A-4) or environment-dependent (A-3, A-10) carries its baseline
  definition in witness config, versioned, so false-reassurance is auditable.
- X-5 Coverage here is **testimony-typed (graded)** — a missing signal
  degrades visibility, it does not falsify the others. But the acceptance
  test below is conjunctive over incidents: a witness that cannot catch every
  *known* incident shape is not "full" coverage, whatever else it sees.

## Acceptance test

For each incident in the table, the proposed witness configuration must
identify at least one signal that would have produced a finding within 30
minutes of onset:

| Incident | Must be caught by |
|----------|-------------------|
| 2026-04-30 rollback loss (444 events, "recovered") | A-3 |
| 2026-05-01 queue overflow (drop_frac 67%) | A-3, A-4-baseline-aware |
| 2026-07-13 WAL pinned / 19–26% shed, 97 min | A-2, A-5, C-1 |
| 2026-07-18..20 retention self-lock (~4,500/min dropped) | A-3, A-6, C-1 |
| 2026-07-23..27 disk full, cursor frozen, health "ok" | H-1, A-1, C-2, C-3 |

A witness passing all five rows against *replayed or synthesized* incident
state (lab substrate is fine; label it lab-backed) meets the bar. Live
authority about the real estate accrues only from real deployment.

## Non-goals

- Not a design for NQ-ng, its schema, or its transport.
- Not the exporter implementation (repo-side typed signals remain
  `gap-spec-self-health-contract.md`).
- Not alert-routing/severity policy — operator's domain.
- No hot-path coupling: witness outages must be invisible to driftwatch.
