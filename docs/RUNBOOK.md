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
