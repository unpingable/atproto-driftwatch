# Driftwatch Prod Status

**As of 2026-05-04 22:00 UTC. Build `5850d01` running with `ENABLE_RETENTION=0`.**

This file is overwritten on each status check. Treat the date stamp as authoritative.

## Headline

Contained but **time-boxed**. Disk runway is roughly 4 days at current burn.

## State

| Signal | Value | Reading |
|---|---|---|
| Container | Up 2d, healthy | OK |
| `platform_health` | mostly `ok`, transient `degraded(lag_high\|high_drop_rate)` | Upstream variance, not our path |
| Ingest eps | ~80–95 vs baseline ~80 | Tracking |
| `rollback_lost` | 0 sustained since 2026-05-01 deploy | L2 fix holds |
| `drop_frac` | 0 most windows; transient bursts when ingest spikes >10k/min | Queue overflow, not lock conflict |
| `queue_depth` | pinned 10024–10997 hourly | Buffer near saturation; writer barely keeps up |
| WAL | 22.7 MB | Bounded — writer-owned TRUNCATE working |
| DB size | 82.4 GB | Growing |
| **DB growth** | **~11.8 GB/day post-retention-disable** | (Not 6 GB/day; that average was diluted by retention-on days) |
| Disk free | 60.1 GB of 195.8 GB total | 69% used |
| **Brake at 92%** | **~44 GB headroom / ~11 GB/day = ~4 days** | **Estimated brake date: 2026-05-08** |
| `resolver.pending` | ~301k | Equilibrium (~19/min resolved, ~18/min new) — neither draining nor accumulating |
| `facts_export` | 5.2 GB work + 2.85 GB snapshot | By design |

## Why retention is off

Build `5850d01` (2026-05-01) routed retention's chunked DELETE/UPDATE through the persistent writer thread to eliminate lock-conflict rollbacks. The single-writer invariant is correct; the implementation **failed acceptance under firehose load** — retention chunks of 5000 rows blocked the writer thread for 30–65 s each, the ingest queue overflowed, `drop_frac` hit 67%. Loss migrated from `rollback_lost` to plain `QueueFull`. Disabled via `ENABLE_RETENTION=0` in the compose override.

## What's required before re-enable

A retention scheduler with:

- Wall-clock-bounded chunks (per-chunk occupancy << time-to-fill ingest queue at peak rate).
- Ingest priority over maintenance.
- Pre-chunk gate on `queue_depth` / `median_age` — yield when ingest is under pressure.
- Resumable progress so a yielded pass picks up next cycle.
- `/health` surfaces retention lag and disk runway.

See `specs/gaps/gap-spec-single-writer-invariant.md` (footer) and `docs/architecture/INVARIANTS.md` §7.

## Fallback before scheduler ships

Manual prune/VACUUM playbook lives in `docs/RUNBOOK.md` (section: *Manual retention pressure relief*). Use only if scheduler work is not landing inside the runway.

## Do not

- Do not upgrade the disk as the first move. That trades cost for time without changing the bucket vocabulary.
- Do not let `ENABLE_RETENTION=0` become the new steady state. The override is parole, not exoneration.

## Related

- Incident record: `docs/JETSTREAM_INGEST_REALITIES.md` (ends at 7398f7b containment; 5850d01 reopen lives in the gap-spec footer for now).
- Reference labeler backport: 415a80c (docs) + 510b983 (L2 code) on `unpingable/atproto-labeler` main, 2026-05-04.
