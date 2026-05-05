# Driftwatch Prod Status

**As of 2026-05-05 21:15 UTC. Build `f86287c` running with pressure-aware retention scheduler + emergency catch-up tuning.**

This file is overwritten on each status check. Treat the date stamp as authoritative.

## Headline

Contained but **not closed**. Retention re-enabled tonight; scheduler protects ingest correctly (`rollback_lost=0` sustained) but a workload-contention incident during catch-up exposed a deeper architectural axis. Pre-restart capture observed recovery before the prepared mitigation ran; restart held by precondition-bound authority. System is stable for the moment.

## State (post-recovery snapshot)

| Signal | Value | Reading |
|---|---|---|
| Container | Up ~30 min on `f86287c`, healthy | OK |
| `platform_health` | `degraded` (no specific gate listed) | Recovering from catch-up burst |
| Ingest eps | 83.3 vs baseline 49.4 | Above baseline (still draining replay) |
| `rollback_lost_total` | **0** sustained | Writer not losing batches ✓ |
| `drop_frac` | 0.0 in current window | Recovered from 0.99 spike ~5 min ago |
| `events_dropped_total` | 145,278 | **+17k events lost during tonight's catch-up** (real loss, post-restart replay pressure) |
| `backlog` (live queue) | 85 of 5000 | Drained from saturation |
| `queue_depth` (recheck) | 10,101 | **Longitudinal worker stuck — separate cleanup track (see `docs/CLEANUP_DEBT.md`)** |
| WAL | 7.8 MB | Bounded after checkpoint |
| DB size | 93 GB | No shrink without VACUUM (auto_vacuum=0; `freelist_count=0`) |
| Disk free | **59 GB** of 196 GB total | 69% used. Burned 1 GB during incident; archive reclaim earlier today gave +9.8 GB. |
| Brake date | ~3 days at observed ~13 GB/day burn | Watching whether retention slows growth |

## Tonight's arc (2026-05-05)

1. **17:00 UTC** — Pushed scheduler commits (`f564f8a`, `2b37b03`, `f86287c`). rsync to VM, container rebuild.
2. **17:00–17:30** — Initial deploy iterations: `stream_lag` threshold (60s → 300s → 3600s) and `chunk_budget_s` (3 → 35) tuned to match observed prune chunk reality. Chatty's framing — "match the budget to observed chunk reality" — drove the 35s value.
3. **18:00–20:00** — Reclaimed 9.8 GB by deleting `claim_history` archives older than 7 d (67 files → 4). Disk free 46 GB → 56 GB.
4. **20:00–20:45** — First retention passes. Pattern: pre-pass clear → 2–3 chunks complete → chunk overrun or `database is locked` → clean abort. `raw_stripped_partial: True` per pass; forward progress real but no full pass completed.
5. **20:50–21:00** — Catch-up burst spiked: writer thread entered kernel D-state on `wait_on_page_bit_common` (page-cache miss on 93 GB DB). drop_frac → 0.99, eps → 1.5, backlog pinned at 5000. **+17k events dropped during this window — real ingest loss.**
6. **21:00** — Operator authorized controlled restart with env-disables (`ENABLE_RETENTION=0`, `ENABLE_LONGITUDINAL_RECHECK=0`, `ENABLE_FACTS_EXPORT=0`). Pre-restart capture began.
7. **21:00–21:05** — During capture, system recovered on its own. Burst subsided, page cache filled, writer caught up. drop_frac → 0.0, eps → 83, backlog → 85, WAL → 7.8 MB. **Restart held; mitigation authority expired with the precondition.** Plan staged with explicit re-trigger criteria.

## Doctrine recorded tonight

- **Mitigation authority is precondition-bound.** Prepared corrective actions re-check the symptom that authorized them immediately before execution. If it cleared, the authority expired. Filed in continuity (`mem_394155af`).
- **Single-writer ≠ scheduling ≠ disk reclamation.** Three distinct axes; healthy ingest can coexist with disk runway problems. Filed (`mem_cf1afe52`).
- **Retention soaking the failure is containment. Retention disappearing the failure is haunted.** rollback_lost=0 is one axis of health; disk runway is another. Filed (`mem_bf77f2e2`).
- **This was workload contention on marginal storage, not lock contention.** Different architectural axis from the April lock-conflict incident. Filed in cleanup debt.

## Re-trigger criteria for the staged ingest-only restart

Fire the restart only if any of these hold for the indicated duration:

- `drop_frac > 0.1` sustained for 5 minutes
- `backlog` pinned near 5000 for 5 minutes
- `eps` materially below baseline for 5 minutes
- `stream_lag` rises and does not recover
- WAL grows unbounded
- `rollback_lost > 0` (any non-zero — tripwire)
- Writer returns to prolonged D-state with active drops

**The prepared restart:** set `ENABLE_RETENTION=0`, `ENABLE_LONGITUDINAL_RECHECK=0`, `ENABLE_FACTS_EXPORT=0` in `docker-compose.override.yml`. One controlled restart. Re-enable order phases: Phase 1 ingest-only (prove writer recovers) → Phase 2 longitudinal (prove no writer drag) → Phase 3 facts_export (prove no I/O starvation) → Phase 4 retention (only when scheduler window is safe).

## Active emergency tuning in production override

```
ENABLE_RETENTION=1
RETENTION_INTERVAL_SEC=900
RETENTION_PASS_BUDGET_S=1800
RETENTION_CHUNK_BUDGET_S=35
RETENTION_STREAM_LAG_THRESHOLD_S=3600
RETENTION_STRIP_BATCH=1000
RETENTION_DELETE_BATCH=1000
```

Drop these back to defaults once `retention_lag_s` < ~3600 AND `disk_runway_days` stays > 5 for an hour AND no re-trigger criteria fire.

## Cleanup debt opened tonight

See `docs/CLEANUP_DEBT.md`. Five items live:

1. Classify `sqlite3.OperationalError("database is locked")` as soft abort (not `-1` sentinel).
2. Raise retention `busy_timeout` to 60 s, validate it doesn't grow backlog.
3. Drop `stream_lag` from the retention gate (replace tonight's hot patch).
4. Reader attribution for WAL pinning (`lsof`/`fuser` diagnostics surface).
5. **Workload contention on marginal storage** — fewer SQLite connections, controlled read concurrency, /health caching, facts-export against snapshot, I/O budgets per loop, block-storage admitted as first-class constraint.
6. **Longitudinal recheck queue saturation** — `queue_depth` pinned at 10,101 indefinitely; `dequeued=0` per STATS window. Latent for weeks. Separate diagnostic track.

## Do not

- Do not upgrade the disk as the first move. That trades cost for time without changing the bucket vocabulary.
- Do not let `ENABLE_RETENTION=0` become the new steady state. The override is parole, not exoneration.
- Do not re-route retention through the writer thread (5850d01). Path B is gated on a load harness.
- Do not run full `VACUUM` on `labeler.sqlite` — needs ~93 GB free, we have 59. Use `VACUUM INTO` if reclamation is needed (and only after sizing per the playbook).
- Do not restart preemptively for the staged plan. Re-trigger criteria must fire first.

## Related

- Incident record: `docs/JETSTREAM_INGEST_REALITIES.md` (ends at 7398f7b containment; tonight's 05-05 evening arc not yet appended).
- Cleanup debt: `docs/CLEANUP_DEBT.md`.
- Continuity: `mem_e3b999b3` (project_state), `mem_d493daf5` (next_action), `mem_cf1afe52` / `mem_bf77f2e2` / `mem_394155af` (lessons).
- Reference labeler backport: 415a80c (docs) + 510b983 (L2 code) on `unpingable/atproto-labeler` main, 2026-05-04.
