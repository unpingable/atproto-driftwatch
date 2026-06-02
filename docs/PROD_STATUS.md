# Driftwatch Prod Status

**As of 2026-06-02 15:05 UTC. Build `54a1711-recheckgate` running with pressure-aware retention scheduler.**

This file is overwritten on each status check. Treat the date stamp as authoritative.

## Headline

Steady state. Retention is on, parole-mode tuning still in effect, ingest is healthy. Disk runway is the dominant operational signal — 23 GB free against a 120 GB DB on shared block storage. Cold-path Parquet/DuckDB (`specs/gaps/gap-spec-cold-path-parquet-duckdb.md`) is the architectural answer. Next code slice is `CLEANUP_DEBT.md` #3.

## State

| Signal | Value | Reading |
|---|---|---|
| Container | Up 3 weeks healthy | OK |
| `platform_health` | `ok` | OK |
| Ingest eps | 279.8 vs baseline 90.0 | Above baseline (healthy, no shedding) |
| `drop_frac` | 0.0 | OK |
| `stream_lag_s` | 0.0 | OK |
| `gate_reasons` | `[]` | OK |
| `rollback_lost_total` | clean across passes (per CLEANUP_DEBT verdict) | OK |
| `queue_depth` (recheck) | 10,735 | Producer-gated since 2026-05-12; rows sit indefinitely (no consumer, no enqueue). Not backlog. |
| WAL | 1.2 GB | Larger than ideal; not pinned (`wal_truncate_busy=0`). |
| DB size | 120 GB | No shrink without `VACUUM INTO` (auto_vacuum=0). |
| Disk free | **23 GB** of 196 GB total | **88% used.** Dominant operational signal. |
| Resolver | 1,846,710 ok / 199,665 pending / 39 error / 21 not_found (2.05M total, 90% resolved) | Steady. 2026-03-19 50k seed drained long ago; pending is fresh inflow. |
| `facts_export` snapshot | 3.1 GB, age 24.9 d | Parked-by-design pending DuckDB cutover. |
| Reconnects | 1339 over 3 weeks | High but operational. |

## Active production override

`/opt/driftwatch/deploy/docker-compose.override.yml` retention block:

```
ENABLE_RETENTION=1
RAW_STRIP_AGE_SEC=21600
EVENTS_RETENTION_SEC=259200
EDGES_RETENTION_SEC=604800
CLAIM_RETENTION_SEC=604800
RETENTION_STRIP_BATCH=1000
RETENTION_DELETE_BATCH=1000
RETENTION_BATCH_SLEEP_SEC=2.0

# parole-mode tuning (2026-05-05 evening)
RETENTION_INTERVAL_SEC=900            # default 3600
RETENTION_PASS_BUDGET_S=1800          # default 600
RETENTION_CHUNK_BUDGET_S=35           # default 3
# RETENTION_STREAM_LAG_THRESHOLD_S retired in code 2026-06-02 (CLEANUP_DEBT #3).
# VM override still has it pending next deploy; safe to drop from override
# at deploy time.

# containment toggles (2026-05-08)
ENABLE_FACTS_EXPORT=false
ENABLE_LONGITUDINAL_RECHECK=0
ENABLE_CLAIM_RECHECK=0

# steady-state (backfill drained)
RETENTION_PARQUET_MAX_DAYS_PER_PASS=1
```

**Drop-back-to-defaults condition** for the parole tuning: `retention_lag_s < ~3600` AND `disk_runway_days > 5` for an hour AND no re-trigger criteria fire. Currently not met (disk runway is the gating signal).

## Canonical next code slice

`CLEANUP_DEBT.md` #3 — drop `stream_lag` from the retention gate.

The `RETENTION_STREAM_LAG_THRESHOLD_S=3600` value in production is a hot-patch that effectively disables the gate (jetstream catch-up after restart inflates `stream_lag_s`; the gate fires for reasons unrelated to writer pressure). Remove the env var and the gate check in `retention_scheduler.py::_evaluate_gate`. Keep `stream_lag_s` in `current_pressure` for observability. Other gate signals (`backlog`, `queue_depth`, `rollback_lost` tripwire) remain — they reflect actual writer experience.

After this slice: cold-path Parquet/DuckDB Phase -1 research spike per `specs/gaps/gap-spec-cold-path-parquet-duckdb.md`.

## Re-trigger criteria for the staged ingest-only restart

Fire only if any of these hold for the indicated duration:

- `drop_frac > 0.1` sustained for 5 minutes
- `backlog` pinned near 5000 for 5 minutes
- `eps` materially below baseline for 5 minutes
- `stream_lag` rises and does not recover
- WAL grows unbounded
- `rollback_lost > 0` (any non-zero — tripwire)
- Writer returns to prolonged D-state with active drops

**The prepared restart**: set `ENABLE_RETENTION=0`, `ENABLE_LONGITUDINAL_RECHECK=0`, `ENABLE_FACTS_EXPORT=0` in the override. One controlled restart. Re-enable order: Phase 1 ingest-only → Phase 2 longitudinal → Phase 3 facts_export → Phase 4 retention.

## Do not

- Do not upgrade the disk as the first move. Trades cost for time without changing the bucket vocabulary.
- Do not let `ENABLE_RETENTION=0` become the new steady state. The override is parole, not exoneration.
- Do not re-route retention through the writer thread (5850d01). Path B is gated on a load harness — see `specs/gaps/gap-spec-single-writer-invariant.md`.
- Do not run full `VACUUM` on `labeler.sqlite` — needs ~120 GB free, we have 23. Use `VACUUM INTO` if reclamation is needed.
- Do not restart preemptively for the staged plan. Re-trigger criteria must fire first.
- Do not re-enable `ENABLE_FACTS_EXPORT` / `ENABLE_LONGITUDINAL_RECHECK` / `ENABLE_CLAIM_RECHECK` until cold-path cutover lands.

## Cleanup debt summary

`docs/CLEANUP_DEBT.md` items as of 2026-06-02:

1. **#3 — drop `stream_lag` from retention gate** — RESOLVED 2026-06-02 in code. VM override line removal deferred until next deploy.
2. #1 classify `database is locked` as soft abort — cosmetic, no behavior change.
3. #2 raise retention `busy_timeout` to 60 s — needs validation harness.
4. #4 reader attribution for WAL pinning — real architectural work.
5. #5 workload contention on marginal storage — subsumed by cold-path plan.
6. Longitudinal recheck queue saturation — producer-gated 2026-05-12 (`42acdb4`), latent state until cold-path cutover.

## Related

- Cleanup debt: `docs/CLEANUP_DEBT.md`.
- Cold-path plan: `specs/gaps/gap-spec-cold-path-parquet-duckdb.md`.
- Evidence-store doctrine: `specs/gaps/gap-spec-log-structured-artifact-system.md` (filed 2026-05-13).
- Single-writer scar: `specs/gaps/gap-spec-single-writer-invariant.md`.
- Continuity: `memory_query_latest(scope="driftwatch", kind="project_state")` / `next_action`.
