# Cleanup Debt

Items deferred from in-flight work. Each entry is filed because forgetting it would create retrofit cost, but is **not authorization to build** — they're handles for review.

---

## 2026-05-12 — longitudinal queue: misleading metric + producer-side tax (resolved-in-part)

**Where:** `src/labeler/db.py` (`_add_recheck_txn`, `enqueue_claim_recheck`), `src/labeler/main.py` (`/health/extended.queue_depth`).

**Today:** investigating why `queue_depth` had been pinned ~10–11k for weeks revealed two things:

1. `ENABLE_LONGITUDINAL_RECHECK=0` and `ENABLE_CLAIM_RECHECK=0` in the prod compose override — both consumers are deliberately off (recorded in the override comment: *"76% of stall windows; readers (facts_export, longitudinal, identity) all active concurrently. This is reduction, not an experiment. Re-enable individually after WAL-truncate fix lands."*).
2. The producers (`_add_recheck_txn` from the consumer event path, `enqueue_claim_recheck` from the longitudinal worker — but the latter is itself off) were not gated symmetrically. The consumer kept enqueueing into `recheck_queue`. The `recheck_queue_fp_cap_hysteresis` trigger (db.py:177) trimmed back to 10k whenever the count crossed 11k. Result: pinned ring buffer, ~700–900 rows trimmed every ~3 min, **on the consumer's writer thread**, producing nothing of downstream value (driftwatch is sealed-lab/detect-only; `facts_export` is also off; no labelwatch path reads recheck output).

A 15-min sample confirmed the oscillation: floor ~10040 → ceiling ~10930 → trim → repeat.

**Fixed today:** producer-side kill switches added to both `_add_recheck_txn` and `enqueue_claim_recheck`. When the corresponding `ENABLE_*` env is not `1`, the function increments a `*_worker_disabled` counter in `queue_stats` and returns early. No queue write, no trigger, no churn. Test fixture (`tests/conftest.py`) now defaults both env vars to `1` so existing tests keep the historical contract.

**Still open (filed, not done):**

- **`queue_depth` is a misleading top-level field in `/health/extended` when the worker is disabled.** It looks like backlog; it's really "snapshot of a queue nobody is draining." Wanted shape (later):
  ```json
  "recheck_queue": {
    "count": 10041,
    "worker_enabled": false,
    "cap": 10000,
    "hysteresis_ceiling": 11000
  }
  ```
  Defer until `/health/extended` gets the broader DB-derived-field cache discussed in the 2026-05-05 workload-contention entry. Until then, the legacy top-level `queue_depth` stays for back-compat.
- **The 10k stale rows currently in `recheck_queue` will sit there indefinitely** after this fix (no enqueue → no trigger → no drain). Not harmful — they don't grow, no consumer reads them — but if we ever re-enable longitudinal we should `DELETE FROM recheck_queue WHERE scheduled_at < <cutover>` first so the worker starts from a clean slate rather than chewing through days-old fingerprints.

**Why this wasn't tonight's bigger thing:** the larger architectural fix is still the cold-path Parquet/DuckDB plan (`specs/gaps/gap-spec-cold-path-parquet-duckdb.md`). The producer-gating fix is the small, correct local move while that's pending — it removes a hidden tax on the hot writer for a system that is parked-by-design. Re-enabling longitudinal stays gated on the workload-contention work, **not** on this entry.

**Acceptance shape (record for future deploys to this codebase):**

```text
fix acceptance:        did the patch behave correctly after recovery?
deploy acceptance:     did applying the patch cause queue-boundary loss?
platform acceptance:   did the system return to steady state?
```

These are three different claims. A patch can be "fix-clean" and "deploy-dirty" simultaneously. Tonight's deploy was exactly that shape — see the 2026-05-12 addendum under the 2026-05-05 workload-contention entry for the deploy-side evidence.

Verdict for this entry:

```text
Producer-gating fix:
  PASS
  Evidence:
    - enq_attempt=0, enq_insert=0 (gate fires)
    - queue_depth frozen at 10735 across 8 samples
    - rollback_lost=0
    - WAL bounded, post-storm health recovered

Deployment envelope:
  WARN / NOT CLEAN
  Evidence:
    - restart-induced WS catch-up burst caused dropped=11677 in one window
    - rollback_lost=0 only proves writer integrity
    - "ingest remained clean" criterion denied for the deploy window

Disposition:
  keep the fix; do not revert
  see workload-contention entry (2026-05-12 addendum) for the deploy hazard
```

Keeper line earned tonight:

> **Rollback-clean is not ingest-clean.**

---

## 2026-05-05 (late evening) — workload contention on marginal storage

**Where:** architecture, not one file.

**Today:** during the retention re-enable evening, the writer thread entered kernel `D`-state on `wait_on_page_bit_common` — page-cache miss on the 93 GB DB. With ~10 SQLite connections open from one process (consumer writer, retention own-conn, longitudinal worker, facts_export source, recheck queries, /health endpoints, etc.), concurrent I/O on the Linode 8 GB / shared block-storage backend serialized at the kernel level. Drops climbed to 99% briefly before recovering when the burst subsided.

**The diagnosis:** *this is no longer SQLite lock contention. This is SQLite workload contention on marginal storage.* All the lock-level fixes from April (single-writer, batched commits, writer-owned WAL truncate) and May (pressure-aware retention scheduler) are correct AND insufficient. The next architectural axis is the storage layer.

**Want (any one of, in priority order):**
- **Fewer SQLite connections** — collapse the per-subsystem connection pool. Each loop opens its own; many of those reads could share a single read pool. The /health endpoint queries especially should not open fresh connections per request.
- **Controlled read concurrency** — semaphore around the read path so the writer isn't competing with N readers for kernel page cache.
- **Health endpoints don't open fresh DB snapshots casually** — make `/health/extended` cache its DB-derived fields with a short TTL. Today every external poll reaches into SQLite.
- **facts_export against a snapshot/replica**, not the live DB. Today its source connection reads the live labeler.sqlite and pins WAL frames.
- **Retention and longitudinal as I/O-budgeted jobs** — explicit IOPS budget per loop, not just lock-time budget.
- **Block-storage reality admitted as a first-class constraint** in the design doctrine — a doc that says "this DB is bigger than RAM, lives on shared volumes, every concurrent reader competes for the OS page cache."

**Why not now:** real architectural work spanning multiple subsystems. Tonight the writer recovered when the post-restart catch-up burst subsided; not an incident demanding immediate code change. But this is the *next* architectural surface to ratify, not the same scheduler debt.

**Tripwire that escalates this from candidate to required:** any second incident where writer enters sustained kernel I/O wait under normal (non-burst) load.

**Architectural answer filed:** see `specs/gaps/gap-spec-cold-path-parquet-duckdb.md` (filed 2026-05-05). Phased plan to move read-heavy and historical workloads to Parquet/DuckDB while SQLite stays the hot operational store. Most of the bullets above (fewer connections, controlled read concurrency, /health caching, facts/snapshot replication, I/O budgets) are subsumed once Phases 1–6 land.

**2026-05-12 addendum — restart blast radius is consequence-bearing.**

Tonight's deploy of the producer-gating fix triggered the cascade in measurable form. The fix itself is unrelated to the mechanism — restart alone is sufficient.

Sequence:

1. Container restart → cursor rewind on Jetstream → catch-up burst (~3s upstream rewind).
2. Burst floods the consumer's internal event queue while writer is paying the cost of cold page-cache misses against the 122 GB SQLite.
3. Writer falls behind → event queue backlog grows → WS reader blocks on `put` → server-side keepalive ping timeout (`websockets ConnectionClosedError 1011`) → reconnect.
4. Each reconnect re-rewinds the cursor → another catch-up burst → cascade.
5. Resolved when reads drained enough to let the writer catch up. Storm window: ~13 minutes (15:25 deploy → 15:38 last reconnect → ~15:40 lag back to 0).

Measured cost of the storm:

```text
dropped=11677  (single 60s STATS window at 15:39)
rollback_lost=0
reconnect_count delta: ~9 (88 → 97 over restart + storm)
```

This is not "restart is forbidden" — it's "restart on the current plant has a known, measurable blast radius, and we should plan deploys against that envelope." Concrete consequences:

- Deploy acceptance should include a queue-boundary loss check, not just a fix-acceptance check. See the 2026-05-12 longitudinal-queue entry above for the tri-class acceptance shape.
- The next deploy should not happen during a degraded window without explicitly accepting another ~10k events of intake loss.
- Any work that requires multiple deploy cycles (e.g. iterating on the cold-path plan) should sequence to minimize restart count.

This addendum does **not** raise an architectural escalation beyond what the parent entry already names (Phases 1–6 of the cold-path plan are still the answer). It quantifies the cost of operating with the parent entry unresolved.

Keeper that earned its rent tonight:

> Rollback-clean is not ingest-clean.

---

## 2026-05-05 (late evening) — longitudinal recheck queue saturation

**Where:** `longitudinal.py`, `/health/extended.queue_depth`.

**Today:** `queue_depth` (recheck_queue row count) has been pinned at **~10,101** through tonight's session. The longitudinal worker is dequeuing zero rows per STATS window (`dequeued=0`). Median dequeue age is ~3394 s — work that should have been processed an hour ago is still queued.

This is **not a tonight problem** — it's been latent for weeks. Tonight just made it visible because we were watching pressure signals carefully.

**Want:** separate cleanup/degradation track. Longitudinal isn't ingest-critical (its outputs feed downstream label decisions, not raw event capture), but if it stays at zero progress indefinitely, fingerprints aren't being rechecked, which is the whole point. Investigate:
- Is the longitudinal worker actually running? (Tonight we saw it failing on the same `database is locked` errors as retention.)
- If running, why is dequeued=0? Lock starvation against the writer thread? Worker logic bug?
- Is the recheck_queue itself sized wrong (10k cap reached, then no flow)?

**Why not now:** distinct system from tonight's retention scheduler work; needs its own diagnostic pass. Park it as a track, don't blend it with retention debt.

---

## 2026-05-05 — retention scheduler follow-ups

Filed during the retention re-enable + scheduler deploy. The scheduler is doing its job (zero `rollback_lost`, zero `drop_frac` across multiple aborted passes), but observation surfaced known shortcuts.

### 1. Classify `sqlite3.OperationalError("database is locked")` as soft abort — RESOLVED 2026-06-02

**Status:** RESOLVED 2026-06-02. `_LockPressure` exception added to `retention.py`; `_strip_old_raw` and `_prune_table` catch `OperationalError("database is locked"|"database table is locked")` and re-raise as `_LockPressure(partial_total)`. `_run_op` and the inline archive block in `run_retention_once_with_sched` catch and set `stats[label] = partial_total` + `stats[f"{label}_lock_pressure"] = True`. The `-1` sentinel is now reserved for actually-unexpected exceptions. Four tests in `tests/test_retention_scheduler.py::TestLockPressureClassification` cover both the lock-pressure path and the unexpected-error fallthrough.

Single-classification scope: only `lock_pressure` is distinguished (not the speculative `reader_pinned` / `busy_timeout` sub-classes — the SQLite error message doesn't distinguish, and pre-building the taxonomy would be speculative). When reader-attribution lands (#4), a sub-classification pass can refine this.

Legacy `run_retention_once` (CLI cold-pass) was left untouched; in that path a `_LockPressure` raise will be caught by `except Exception` and still recorded as `-1`. The production path uses `run_retention_once_with_sched`; the legacy path is secondary.

---

**Where:** `retention.py::_run_op` and the leaf functions invoked from it.

**Today:** any exception in a leaf is caught by `_run_op` and recorded as `stats[label] = -1`. That's a sentinel that mixes "scheduler abort" (clean), "lock-contention busy timeout" (known pressure case), and "real bug" (unexpected) into one ugly value.

**Want:**
- Catch `sqlite3.OperationalError` with `"database is locked"` text specifically.
- Treat it as a soft abort with a classification: `lock_pressure` / `reader_pinned` / `busy_timeout` (whichever fits the cause).
- Preserve partial-progress row counts already committed before the exception.
- Reserve `-1` for actually-unexpected exceptions (real bugs).

**Why not now:** it's cosmetic — current behavior is correct (the pass aborts cleanly, no events lost). Cleaner stats shape is a polish pass, not a tonight thing.

---

### 2. Raise retention's `busy_timeout` to match the consumer's — RESOLVED 2026-06-02

**Status:** RESOLVED 2026-06-02. Extracted `RETENTION_BUSY_TIMEOUT_MS` module constant in `retention.py` (default 60000, env-overridable via `RETENTION_BUSY_TIMEOUT_MS`); both `run_retention_once_with_sched` and the legacy `run_retention_once` now apply the same value via `PRAGMA busy_timeout`. Validation harness `tests/test_retention_scheduler.py::TestBusyTimeoutAndLockContentionHarness::test_real_lock_contention_classifies_as_lock_pressure` opens two SQLite connections to a file DB, holds an exclusive write transaction on one, and verifies the other's retention chunk classifies the resulting `OperationalError("database is locked")` as `lock_pressure` (not -1) — proving CLEANUP_DEBT #1 + #2 compose correctly under real OS-level lock contention, not just mocked exceptions.

Documented trade-off (in the module comment block): the scheduler's per-chunk gate fires BETWEEN chunks, not during a chunk's busy-wait. A chunk that waits the full 60s does not let the scheduler bail mid-wait — backlog accumulated during that window is not detected until the next chunk's pre-call gate. The win (fewer pointless aborts during transient contention) outweighs the cost (slightly longer backlog-detection latency under sustained contention).

No VM override change required — the current `/opt/driftwatch/deploy/docker-compose.override.yml` does not set `RETENTION_BUSY_TIMEOUT_MS`, so the new 60s default takes effect automatically on next deploy.

---

**Where:** `retention.py::run_retention_once_with_sched`, the `conn.execute("PRAGMA busy_timeout=30000")` line on the own-conn path.

**Today:** retention waits 30 s for the lock, then fails. Consumer's writer connection waits 60 s.

**Want:** bump retention's busy_timeout to 60 s as a test. The cost is that retention may stall for up to 60 s waiting for a lock — that's acceptable IF and ONLY IF retention still yields/aborts cleanly when the queue backlog grows during the wait. **Busy_timeout is not a scheduler.** Pre-validate: under simulated reader pinning, does retention's longer wait cause backlog to accumulate before the next per-chunk gate fires?

**Why not now:** changes contention semantics; needs deliberate validation. Tonight's pattern (30 s timeout → soft abort → next pass tries again) is functionally correct.

---

### 3. Drop `stream_lag` from the retention gate — RESOLVED 2026-06-02

**Status:** RESOLVED in code 2026-06-02. `STREAM_LAG_THRESHOLD_S` constant + `_evaluate_gate` check removed from `retention_scheduler.py`; `stream_lag_s` key removed from `health_state` thresholds dict; new test `test_high_stream_lag_does_not_gate` asserts 3600s lag does not skip a pass; backport gap-spec updated. VM override removal (`/opt/driftwatch/deploy/docker-compose.override.yml::RETENTION_STREAM_LAG_THRESHOLD_S`) deferred until next deploy.

---

**Where:** `retention_scheduler.py::_evaluate_gate`, the `STREAM_LAG_THRESHOLD_S` block.

**Today:** the gate trips on `stream_lag > threshold`. But `stream_lag = now - latest_event_time` is a **jetstream catch-up signal**, not a measure of writer pressure. After every container restart it inflates immediately (cursor rewind), tripping the gate even when the writer is idle. The hot-patched 3600 s threshold I set tonight effectively disables the gate; the right fix is to remove it from the code path entirely.

**Want:**
- Remove `STREAM_LAG_THRESHOLD_S` from `_evaluate_gate`.
- Keep `stream_lag_s` in `current_pressure` for observability.
- Drop the env var from production override comments.

The remaining gate signals (`backlog`, `queue_depth`, `rollback_lost` tripwire) are local to the writer's actual experience and don't false-positive on upstream events.

**Why not now:** code change + test update + redeploy. Hot patch via env override is sufficient for tonight.

---

### 4. Reader attribution for WAL pinning

**Where:** new diagnostic; probably `maintenance.py` or a new `wal_diagnostics.py`.

**Context:** when `wal.checkpoint_busy > 0` persistently, **some long-lived reader is pinning the WAL frontier**. Tonight's incident showed `wal_truncate: busy=1 log=8142 checkpointed=34` — 8000+ frames piled up because checkpoint couldn't proceed. Same architectural shape as the April WAL-bloat incident.

**Want:** automatic reader attribution when WAL frames pending grows past a threshold. Approaches:
- `lsof | grep labeler.sqlite` to identify open file descriptors.
- `fuser /mnt/zonestorage/driftwatch/data/labeler.sqlite-wal`.
- Process-level: log every `get_conn()` call site with caller info, retain the last N open handles for inspection.
- Surface in `/health/extended.wal.likely_pinning_readers` when busy > 0 for >5 min.

Likely culprits to attribute: `/health/extended` queries (cheap, but if they overlap with checkpoint they pin), longitudinal worker, dashboard reads, facts_export source connection (was the offender in April — already phase-released, but worth verifying), backup/cron jobs.

**Why not now:** real architectural work, not a small fix. Build it after Path B (writer-thread retention scheduling) gets a load harness — the diagnostic gives signal for both efforts.

---

## How this list works

- New entries go at the top, dated.
- Each entry: where, today, want, why-not-now.
- Mark items resolved or move to a CHANGELOG when done; don't quietly delete.
- An entry sitting here >30 days is a signal — either ratify it (build), close it (decide it's not needed), or split it into a smaller piece.
