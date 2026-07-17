# Driftwatch Runbook — "What Bad Looks Like"

Quick reference for diagnosing common failure modes.
Check `/health/extended` first, then use this guide.

---

## Consumer crash-loop

**Symptoms:** `events_per_sec: 0`, `stream_lag_s` climbing, reconnect count increasing.

**Cause:** Usually a malformed ATProto record crashing `_jetstream_to_event()`.
The crash-loop pattern: connect → receive bad event → crash → reconnect → same event → crash.

**Check:**
```bash
docker logs driftwatch 2>&1 | grep -E 'Error|Traceback|AttributeError' | tail -20
```

**Fix:** Add defensive `isinstance` guard for the malformed field, deploy, restart.
See `a0d9c99` (reply.parent boolean) for the pattern.

**Prevention:** Consumer hardening pass (filed). Every nested field access should
use isinstance guards. Bad events should be quarantined, not fatal.

---

## WAL bloat (labeler.sqlite-wal growing past 100MB)

**Symptoms:** `wal.wal_size_mb` climbing in health endpoint. `wal.checkpoint_busy > 0` persistently.

**Cause:** A long-running read transaction is pinning the checkpoint frontier.
Common culprits:
- Facts export holding source connection too long (fixed: phase-release pattern)
- Report generation with expensive queries
- External sqlite3 session left open
- Scan/derive pass during heavy ingest

**Check:**
```bash
# Current WAL size and checkpoint status
curl -s http://localhost:8422/health/extended | python3 -c \
  'import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get("wal",{}), indent=2))'

# Who has the DB open
lsof /path/to/labeler.sqlite-wal
```

**Fix:** If `checkpoint_busy` is consistently > 0 over multiple minutes, find and
kill the pinned reader. The checkpoint will catch up automatically.

**Prevention:**
- `journal_size_limit=64MB` caps the on-disk WAL file (already set)
- Facts export uses phase-release (already implemented)
- Close connections promptly after read operations

---

## Facts export not updating

**Symptoms:** `facts_export.snapshot_age_s` growing past 7200 (2 hours).
Hosting locus card shows stale data.

**Check:**
```bash
# Facts export logs
docker logs driftwatch 2>&1 | grep 'facts_export' | tail -10

# Schema mismatch?
docker logs driftwatch 2>&1 | grep 'columns but.*values' | tail -5

# Working DB exists?
ls -lh /opt/driftwatch/deploy/data/facts*.sqlite*
```

**Common causes:**
- Schema drift: code expects N columns, persisted DB has N-1. Fix: migration
  function (`_migrate_identity_facts` pattern).
- Disk full: VACUUM INTO needs ~working DB size in free space. Fix: free disk.
- Stale working DB from old container: delete facts_work.sqlite, restart.

**Fix:** Check logs for specific error. If schema mismatch, deploy migration.
If disk, free space. If corrupt working DB, delete and restart.

> **Note:** the legacy in-app facts export is parked (incident-era reduction).
> Its replacement is the Phase 3 snapshot writer below. This section stays as
> the runbook for whatever legacy cycles still exist.

---

## Facts snapshot writer (Phase 3, DuckDB-backed) — deploy + operate

> Parquet is authoritative past. DuckDB is the question engine.
> `facts.sqlite` is a compatibility projection/cache, not the source of custody.

Spec: `specs/gaps/gap-spec-facts-export-duckdb-snapshot-001.md`. The writer
materializes a labelwatch-compatible `facts.sqlite` from claim_history
Parquet partitions (`uri_fingerprint`) + the live SQLite identity projection
(`actor_identity_facts`), with atomic rename and a manifest sidecar.

**One-shot (in container):**
```bash
docker exec driftwatch python -m labeler.cli driftwatch facts-snapshot \
  --parquet-root /app/data/parquet \
  --identity-db /app/data/labeler.sqlite \
  --out /app/data/facts.sqlite
# prints the manifest JSON; also written to /app/data/facts.sqlite.manifest.json
```

**Production cadence:** host cron, hourly. Cron is the on/off switch — the
rollback path is "disable the cron", so do not bury the invocation inside the
app process. `flock -n` skips a run when the previous one is still going
(slow runs must not stack); the writer's PID-suffixed tmp files are the
second layer — even without the lock, overlap degrades to last-writer-wins
of *complete* snapshots, never a partial publish.
```cron
17 * * * * flock -n /run/lock/driftwatch-facts-snapshot.lock docker exec driftwatch python -m labeler.cli driftwatch facts-snapshot --parquet-root /app/data/parquet --identity-db /app/data/labeler.sqlite --out /app/data/facts.sqlite >> /var/log/driftwatch-facts-snapshot.log 2>&1
```

**Deploy checklist (first cutover):**
1. **Declare the disturbance first** (declaration precedes effect; NQ rejects
   retro-dating):
   ```bash
   /opt/notquery/nq-monitor maintenance declare --db /opt/notquery/nq.db \
     --host labelwatch-host --kind error_shift --start now --end now+30m \
     --reason "driftwatch facts-snapshot cutover" --declared-by labelwatch-claude
   ```
   Repeat per disturbed kind (`service_status`, `log_silence`) if the deploy
   restarts services.
2. Run the one-shot manually. Sanity-check the manifest:
   - `row_counts.actor_identity_facts` ≈ actor_identity_current count
   - `row_counts.uri_fingerprint` > 0 (unless Parquet coverage is empty —
     then the writer publishes a controlled identity-only snapshot, by design)
   - `uri_fingerprint_rows_quarantined_bogus_created_epoch` ≈ the known ~25k
     bogus-timestamp population, not ≈ the whole table
   - snapshot `uri_fingerprint` count ≥ legacy facts.sqlite count is EXPECTED:
     legacy pruned rows older than 30 days; the snapshot carries the full
     Parquet-retained history (documented divergence, parity tests pin it)
3. Watch labelwatch pick it up: `journalctl -u labelwatch | grep facts_sync` —
   next scan should log a fresh mtime and nonzero `snapshot=` once candidate
   URIs exist. Missing/stale facts degrade to a coverage caveat, never a 5xx.
4. Only after a boring cycle: install the cron.
5. Exit-criterion cleanup: remove or quarantine the legacy
   `facts_work.sqlite` (the working DB of the parked in-app exporter) so the
   old path can't half-wake.

**Rollback:** disable the cron. The last successfully renamed `facts.sqlite`
stays in place; labelwatch keeps consuming it and surfaces staleness as a
caveat. Do NOT re-enable the legacy in-app exporter as a rollback — it
depends on the hot claim-history scan path the cold-path doctrine retired.
Diagnosis runs against the manifest + writer log, not against the consumer.

**Symptoms → causes:**
- Manifest missing / stale while cron installed → check cron log; writer
  raises before rename on any failure, so a stale-but-valid snapshot is the
  expected failure shape.
- `uri_fingerprint` row count drops sharply between manifests → Parquet
  partitions missing (check `input_partition_window` in the manifest against
  `ls /app/data/parquet/claim_history/`).
- Quarantine count spikes → upstream timestamp hygiene regressed; the writer
  is doing its job. Check `specs/gaps/gap-spec-event-time-hygiene.md`.
- `duration_seconds` trending up across manifests → the V0 writer re-reads
  ALL partitions every run, so runtime grows with cold history. Watch-item:
  when it stops fitting comfortably inside the hourly slot, that's the
  forcing case for an incremental/windowed reader — file it then, don't
  pre-build it.

---

## Resolver stall

**Symptoms:** `resolver.pending` not decreasing over time. RESOLVER log line
shows `resolved=0` or high error rate.

**Check:**
```bash
# Resolver status breakdown
curl -s http://localhost:8422/health/extended | python3 -c \
  'import sys,json; d=json.load(sys.stdin); print(json.dumps(d.get("resolver",{}), indent=2))'

# Recent resolver log lines
docker logs driftwatch 2>&1 | grep RESOLVER | tail -10
```

**Common causes:**
- PLC directory rate limiting or down (check error count)
- Network issue from container
- All pending DIDs are in permanent backoff (not_found, 7-day retry)

**Fix:** If PLC is down, wait. If rate limited, reduce BATCH_SIZE. If all
permanent failures, that's expected — they'll retry after 7 days.

---

## Disk pressure (>85%)

**Symptoms:** `disk.pct_used > 85` in health endpoint. Maintenance loop logs
warnings. At 92%, emergency brake pauses event processing.

**Check:**
```bash
df -h /
ls -lh /opt/driftwatch/deploy/data/*.sqlite*
```

**Major consumers:**
- labeler.sqlite: ~54GB (stable with retention)
- labeler.sqlite-wal: 0-64MB (capped by journal_size_limit)
- facts_work.sqlite: ~7GB
- facts.sqlite: ~6GB
- facts.sqlite.tmp: transient during VACUUM INTO (~6GB)

**Fix:**
- Delete stale facts_work.sqlite if rebuilding
- Check if retention loop is running (`ENABLE_RETENTION=1`)
- Do NOT run VACUUM on main DB without 54GB+ free space
- freelist pages (reclaimable) are reused by inserts — not urgent

---

## Manual retention pressure relief

Use this **only while `ENABLE_RETENTION=0` is in effect** and disk runway is shrinking faster than the retention scheduler work can land. The playbook is parole, not a long-term operating mode.

Two paths. Pick one.

### Path A — Cold pass (preferred)

Stop the consumer, run retention against a quiet DB, compact via `VACUUM INTO`, restart.

**Trade:** ~60–120 min of downtime. Jetstream cursor + 3 s rewind recovers coverage on restart, provided downtime stays inside the upstream replay window (hours, not days).

**Preconditions** (all must hold):

```bash
# On VM (root@192.46.223.21)
df -h /mnt/zonestorage    # need free space ≥ expected compact DB size + 5 GB margin
ls -lh /mnt/zonestorage/driftwatch/data/labeler.sqlite*    # current DB size
curl -s http://localhost:8422/health/extended | python3 -c 'import sys,json; d=json.load(sys.stdin); print("rollback_lost-ish drops:", d.get("drop_frac"), "wal_mb:", d.get("wal",{}).get("wal_size_mb"))'
```

Rule of thumb for compact-DB size: current DB minus expected freelist after raw-strip. With retention disabled N days, expect ~N × 10 GB of raw to NULL out. So compact DB ≈ current − (N × 10 GB). Add 5 GB margin.

**Run:**

```bash
# 1. Stop the container (keeps data volume)
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml stop driftwatch"

# 2. Run retention as a one-shot, with retention enabled for this run only.
#    Uses the legacy sync path (consumer is stopped, no contention).
#    Container exits when run_retention_once returns. Expect 5–30 min.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml \
   run --rm -e ENABLE_RETENTION=1 driftwatch python -m labeler.cli driftwatch retention"

# 3. Inspect freelist and decide whether VACUUM INTO will fit.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "sqlite3 /mnt/zonestorage/driftwatch/data/labeler.sqlite '
     SELECT
       page_count * page_size / (1024*1024*1024.0) AS db_gb,
       freelist_count * page_size / (1024*1024*1024.0) AS freelist_gb,
       (page_count - freelist_count) * page_size / (1024*1024*1024.0) AS compact_gb
     FROM (SELECT
       (SELECT page_count FROM pragma_page_count) AS page_count,
       (SELECT page_size FROM pragma_page_size) AS page_size,
       (SELECT freelist_count FROM pragma_freelist_count) AS freelist_count
     );'"

# 4. Confirm: compact_gb + 5 GB margin <= df-free-gb. If not, STOP. Restart consumer
#    (skip step 5–7), reassess. The retention pass alone reclaims pages-on-disk via
#    reuse, just not file size — buys some runway, not a full reset.

# 5. VACUUM INTO. Writes only live pages to a fresh file. Reads main DB; large I/O.
#    Expect 20–60 min depending on compact size.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "sqlite3 /mnt/zonestorage/driftwatch/data/labeler.sqlite \
     \"VACUUM INTO '/mnt/zonestorage/driftwatch/data/labeler.compact.sqlite';\""

# 6. Verify the compact DB integrity before swapping.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "sqlite3 /mnt/zonestorage/driftwatch/data/labeler.compact.sqlite 'PRAGMA integrity_check;'"
# Expect single row: 'ok'

# 7. Atomic-ish swap. Keep .bak — do NOT delete until 24 h of healthy operation.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 "
  cd /mnt/zonestorage/driftwatch/data &&
  mv labeler.sqlite labeler.sqlite.bak &&
  mv labeler.sqlite-wal labeler.sqlite-wal.bak 2>/dev/null || true &&
  mv labeler.sqlite-shm labeler.sqlite-shm.bak 2>/dev/null || true &&
  mv labeler.compact.sqlite labeler.sqlite
"

# 8. Restart consumer.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml up -d driftwatch"

# 9. Verify.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "curl -s http://localhost:8422/health/extended | python3 -m json.tool | head -40"
```

**Rollback** (any point before step 7 swap is trivial: stop and restart on the original file):

```bash
# If the compact DB is bad or the restart fails on it, swap back:
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 "
  cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml stop driftwatch &&
  cd /mnt/zonestorage/driftwatch/data &&
  mv labeler.sqlite labeler.sqlite.bad &&
  mv labeler.sqlite.bak labeler.sqlite &&
  mv labeler.sqlite-wal.bak labeler.sqlite-wal 2>/dev/null || true &&
  mv labeler.sqlite-shm.bak labeler.sqlite-shm 2>/dev/null || true &&
  cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml up -d driftwatch
"
```

After 24 h of clean operation post-swap, delete the `.bak` files.

**Metrics to watch:**

- During retention pass (step 2): docker logs for `archived %s: N rows`, `pruned N events`, `db geometry` — pass should produce non-zero counts and `geometry.freelist_pct > 30%`.
- During VACUUM INTO (step 5): `df -h /mnt/zonestorage` should show free space dropping by approximately `compact_gb`. If free space drops to <5 GB, abort.
- After restart (step 9): `wal_size_mb < 50`, `drop_frac=0`, `rollback_lost=0`, `events_per_sec` near baseline. Coverage should reach ~100% within minutes once jetstream cursor catches up.
- Hour 1–6 post-restart: DB growth rate. Without retention re-enabled, expect ~500 MB/h (= ~12 GB/day). This is a baseline check that the file is honest, not a healthy steady state.

### Path B — Hot pass (downtime not acceptable)

Run the retention CLI while the consumer is up. Uses the legacy sync path on its own connection — contends with the writer at the SQLite lock level. Lost events surface as `rollback_lost`.

```bash
# One-shot pass, consumer stays up. Expect 20–40 min, lock-contention-bounded.
# Note: ENABLE_RETENTION isn't read by the one-shot CLI path — the gate is on
# the loop, not the CLI. The env override below is belt-and-braces only.
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 \
  "cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml \
   exec -e ENABLE_RETENTION=1 driftwatch python -m labeler.cli driftwatch retention"
```

**Trade:** no downtime, but expect on the order of tens to low hundreds of `rollback_lost` events accumulated during the pass (driftwatch's earlier L1 mode lost ~444/day under continuous retention; a single pass is shorter). Drops are instrumented; coverage during the pass becomes loss-conditioned.

**No VACUUM follow-up.** Path B reclaims pages to the freelist for reuse but does not shrink the file. It buys time against growth rate, not against the absolute disk ceiling. If you need the absolute disk ceiling moved, you need Path A.

### Do not

- Do not run full `VACUUM` on the main DB. It needs ≈ DB-size free space (currently 78 GB needed, 60 GB free). It will fail mid-pass and may corrupt or strand a partial copy. Always use `VACUUM INTO`.
- Do not re-enable `ENABLE_RETENTION=1` for the loop without the scheduler work landing. That re-introduces the 5850d01 starvation failure, or with the L2-only fallback, sustained `rollback_lost` accumulation.
- Do not delete the `.bak` files until the new file has had 24 h of clean operation.

---

## Platform health degraded

**Symptoms:** `platform_health: degraded` in health endpoint.

**Usually not a problem.** This means eps is significantly above or below the
EWMA baseline. Common during real-world traffic spikes or lulls.

**Check:** Is `stream_lag_s` also elevated? If lag is 0 and eps is just high,
it's probably a real traffic event. If lag is climbing, the consumer may be
falling behind.

---

## Quick health check one-liner

```bash
curl -s http://localhost:8422/health/extended | python3 -c '
import sys,json
d = json.load(sys.stdin)
print(f"build={d[\"build_sha\"]} health={d[\"platform_health\"]} eps={d[\"events_per_sec\"]} lag={d[\"stream_lag_s\"]}s")
w = d.get("wal", {})
if w: print(f"  wal={w.get(\"wal_size_mb\",\"?\")}MB busy={w.get(\"checkpoint_busy\",\"?\")}")
r = d.get("resolver", {})
if r: print(f"  resolver: ok={r.get(\"ok\",0)} pending={r.get(\"pending\",0)} error={r.get(\"error\",0)}")
f = d.get("facts_export", {})
if f: print(f"  facts: snap={f.get(\"snapshot_size_mb\",\"?\")}MB age={f.get(\"snapshot_age_s\",\"?\")}s")
dk = d.get("disk", {})
if dk: print(f"  disk={dk.get(\"pct_used\",\"?\")}%")
'
```
