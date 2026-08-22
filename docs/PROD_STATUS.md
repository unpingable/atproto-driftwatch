# Driftwatch Prod Status

**As of 2026-08-21 23:00 UTC. Build `70109c8` running, ingest resumed after the 2026-08-12…08-21 blind period.**

This file is overwritten on each status check. Treat the date stamp as authoritative.

> The previous revision of this file was stamped 2026-06-02 and went **2.5 months**
> without update, through three incidents and a full database rebuild. It was
> contradicted on nearly every line by the time it was read. If you are reading
> this and the stamp is more than a few weeks old, distrust it and re-measure.

## Headline

Recovered and observing. Ingest resumed 2026-08-21 22:47 UTC after a 24-hour
retention drain. `page_count` has not grown through the entire operation, which is
the signal that matters: writes reuse reclaimed pages rather than extending the
file. The binding constraint is **not** database size — it is the 3.03 GiB of
filesystem headroom, and the fact that the file cannot shrink.

Full account: `docs/INCIDENT-2026-08-12-volume-exhaustion.md`.

## State

| Signal | Value | Reading |
|---|---|---|
| Container | Up, healthy | OK (note: healthcheck is liveness only — it reported healthy throughout the outage) |
| `platform_health` | `degraded` | **Artifact.** EWMA baseline was learned during 25× catch-up (`baseline_eps=405`) vs real ~95. Self-correcting. Gates are empty. |
| `gate_reasons` | `[]` | OK |
| Ingest eps | 94.9 vs pre-incident baseline 82.8 | Normal |
| `drop_frac` | 0.0 | OK |
| `stream_lag_s` | 0.0 | OK |
| Cursor gap | 0.5 s | OK — the primary epistemic liveness signal |
| `rollback_lost_total` | 0 | OK (was 57,199,140 at incident peak) |
| WAL | 57 MB | OK |
| DB file (`page_count`) | 50,518,271 pages / 192.71 GiB | **High-water mark. Never exceeded. Cannot shrink — `auto_vacuum=none`.** |
| Freelist | ~166 GiB | Internal slack. Refills as ingest runs. |
| Live data | ~26 GiB | Monotonic baseline measured at drain floor: **17.97 GiB** |
| Volume free | **3.03 GiB** (root view) / 0 (non-root) | **The binding constraint.** `avail` reads 0 permanently — ext4 reserve. |
| Retention | Passes completing, ~15 min interval | OK |
| Resolver | ~3.7 M total | Steady |

## Active production override

`/opt/driftwatch/deploy/docker-compose.override.yml` — now mirrored in-repo at
`deploy/docker-compose.override.prod.yml`. Key values:

```
ENABLE_RETENTION=1
FIREHOSE_AUTO_START=1
RAW_STRIP_AGE_SEC=21600
EVENTS_RETENTION_SEC=259200      # 3d
EDGES_RETENTION_SEC=604800       # 7d
CLAIM_RETENTION_SEC=2592000      # 30d — deliberately NOT lowered, see below
RETENTION_BATCH_SLEEP_SEC=2.0
RETENTION_INTERVAL_SEC=900
ENABLE_FACTS_EXPORT=false
ENABLE_LONGITUDINAL_RECHECK=0
ENABLE_CLAIM_RECHECK=0
```

The 30d growth-curve experiment (all windows 30d, set 2026-07-27) is **reverted
and falsified** — it filled the volume in 16 days at ~12.1 GB/day against its own
stated ~6 GB/day tripwire.

## The envelope is viable — this is settled

`maintenance.log` shows the database flat at **120 G for 65 consecutive days**
(2026-05-10 → 07-13) under events 3d / edges 7d / claims 7d, and at 54 G for
weeks before that. Growth only resumed when retention itself started failing
(07-13 WAL pin → 07-18…20 self-lock → 07-23 full).

**Do not conclude the retention windows are too generous.** They equilibrate on
this hardware. What failed was the ability to enforce and observe that envelope.

## Monitoring

- `/mnt/zonestorage` is now visible to node_exporter and NQ. It was **structurally
  invisible from 2026-04-17 to 2026-08-20** because the Ubuntu package patches the
  filesystem collector's exclude regex to include `/mnt`.
- Two saved NQ checks watch the envelope — source in `deploy/nq-checks/`. The
  aggregator evaluates them every generation (~60s); no cron needed.
- **Still missing: cursor-age (A-1).** Disk is only one way to go blind.

## Warning horizon if retention stops

| Stage | Budget |
|---|---|
| equilibrium → file high-water mark | ~4.3 days |
| high-water → hard wedge | ~6 hours |
| **total** | **~4.5 days** |

Disk-free alone only sees the last ~6 hours; that is why the freelist check
exists.

## Do not

- Do not trust `/health` or the Docker healthcheck as evidence the observatory is
  observing. Both reported OK for the entire nine-day outage.
- Do not run unbounded `COUNT(*)` against `labeler.sqlite`. A stray one pinned the
  WAL 63 MB → 1.1 GB during recovery. Use `SELECT NOT EXISTS(SELECT 1 FROM t)` and
  `LIMIT`-bounded counts.
- Do not run full `VACUUM` — it needs a second copy of the file. `VACUUM INTO` was
  briefly feasible at the drain floor (17.97 GiB live vs ~21 GiB free on root);
  that window has closed as the DB refills.
- Do not lower `CLAIM_RETENTION_SEC` to 7d yet. Claim pruning archives to Parquet
  on this same volume before deleting (~250 MB/day-partition); ~2.2 GiB would not
  fit in ~3.0 GiB of headroom.
- Do not set `ENABLE_RETENTION_PARQUET_CAPTURE=0` on this build as a workaround —
  it activates the legacy JSONL writer that deletes rows it never archived.
- Do not let `df` reassure you. It reads flat through the slow 4.3 days of a
  retention failure.

## Related

- Incident record: `docs/INCIDENT-2026-08-12-volume-exhaustion.md`
- Witness requirements: `specs/gaps/gap-spec-witness-coverage-requirements.md`
- Archive semantics: `docs/ARCHIVE_PROTOCOL.md`
- Cold-path plan: `specs/gaps/gap-spec-cold-path-parquet-duckdb.md`
- Off-host backup (still open, no backup exists): `specs/gaps/gap-spec-off-host-backup-labelwatch-driftwatch.md`
- NQ checks: `deploy/nq-checks/README.md`
