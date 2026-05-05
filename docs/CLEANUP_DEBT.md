# Cleanup Debt

Items deferred from in-flight work. Each entry is filed because forgetting it would create retrofit cost, but is **not authorization to build** — they're handles for review.

---

## 2026-05-05 — retention scheduler follow-ups

Filed during the retention re-enable + scheduler deploy. The scheduler is doing its job (zero `rollback_lost`, zero `drop_frac` across multiple aborted passes), but observation surfaced known shortcuts.

### 1. Classify `sqlite3.OperationalError("database is locked")` as soft abort

**Where:** `retention.py::_run_op` and the leaf functions invoked from it.

**Today:** any exception in a leaf is caught by `_run_op` and recorded as `stats[label] = -1`. That's a sentinel that mixes "scheduler abort" (clean), "lock-contention busy timeout" (known pressure case), and "real bug" (unexpected) into one ugly value.

**Want:**
- Catch `sqlite3.OperationalError` with `"database is locked"` text specifically.
- Treat it as a soft abort with a classification: `lock_pressure` / `reader_pinned` / `busy_timeout` (whichever fits the cause).
- Preserve partial-progress row counts already committed before the exception.
- Reserve `-1` for actually-unexpected exceptions (real bugs).

**Why not now:** it's cosmetic — current behavior is correct (the pass aborts cleanly, no events lost). Cleaner stats shape is a polish pass, not a tonight thing.

---

### 2. Raise retention's `busy_timeout` to match the consumer's

**Where:** `retention.py::run_retention_once_with_sched`, the `conn.execute("PRAGMA busy_timeout=30000")` line on the own-conn path.

**Today:** retention waits 30 s for the lock, then fails. Consumer's writer connection waits 60 s.

**Want:** bump retention's busy_timeout to 60 s as a test. The cost is that retention may stall for up to 60 s waiting for a lock — that's acceptable IF and ONLY IF retention still yields/aborts cleanly when the queue backlog grows during the wait. **Busy_timeout is not a scheduler.** Pre-validate: under simulated reader pinning, does retention's longer wait cause backlog to accumulate before the next per-chunk gate fires?

**Why not now:** changes contention semantics; needs deliberate validation. Tonight's pattern (30 s timeout → soft abort → next pass tries again) is functionally correct.

---

### 3. Drop `stream_lag` from the retention gate

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
