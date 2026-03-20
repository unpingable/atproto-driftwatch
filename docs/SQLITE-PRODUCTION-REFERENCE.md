# SQLite Production Reference

Practical reference for running SQLite in production. Written for the
driftwatch/labelwatch observatory stack: WAL mode, ~54GB main DB, ~13GB
sidecar, multiple concurrent readers/writers, Docker on an 8GB RAM VM.

---

## 1. PRAGMA Configuration

### Connection-time PRAGMAs (set on every connection)

```sql
PRAGMA journal_mode=WAL;           -- once per DB lifetime, but safe to repeat
PRAGMA synchronous=NORMAL;         -- safe with WAL; fsync only at checkpoint
PRAGMA busy_timeout=5000;          -- 5s default; 30-60s for batch/retention ops
PRAGMA cache_size=-64000;          -- 64MB page cache (negative = KiB)
PRAGMA foreign_keys=ON;            -- if you use them
```

### Tuning knobs

| PRAGMA | Recommended | Notes |
|--------|------------|-------|
| `busy_timeout` | 5000 (interactive), 30000-60000 (batch) | Per-connection. Higher for retention/maintenance to survive contention. |
| `cache_size` | `-64000` (64MB) | Negative = KiB. Per-connection. Don't exceed RAM/num_connections. On 8GB VM with 5 connections: 64MB each = 320MB total. |
| `synchronous` | `NORMAL` | With WAL, NORMAL is durable-enough (fsync at checkpoint, not every commit). FULL adds ~2x write latency for marginal durability gain. |
| `mmap_size` | `268435456` (256MB) | Reduces syscalls for reads. Set per-connection. Benefits read-heavy workloads. Don't exceed available address space. For a 54GB DB, mmap won't map it all — but 256MB covers hot pages. |
| `temp_store` | `MEMORY` for small DBs, `FILE` for large aggregations | Labelwatch uses FILE to avoid OOM on big GROUP BY. Driftwatch can use MEMORY if aggregations are bounded. |
| `wal_autocheckpoint` | `1000` (default, ~4MB) | Pages before auto-checkpoint attempt. Lower = more frequent but smaller checkpoints. Set to 0 to disable auto-checkpoint and manage manually. |
| `journal_size_limit` | `67108864` (64MB) | Truncates WAL file after checkpoint if it exceeds this size. Prevents leftover multi-GB WAL files on disk after a burst. |

### Current gaps in your codebase

- **driftwatch `get_conn()`**: Sets `busy_timeout=60000` but no `cache_size`, no `mmap_size`, no `journal_size_limit`. Every call opens a new connection and closes it — no pooling.
- **labelwatch `connect()`**: Sets `cache_size=-50000` (50MB), `temp_store=FILE`, `busy_timeout=5000`. Better, but `mmap_size` and `journal_size_limit` are missing.
- Neither sets `journal_size_limit` — this is why WAL files persist at multi-GB sizes after batch operations.

---

## 2. WAL Mode Configuration

### How WAL checkpointing works

WAL appends writes to `db-wal`. Checkpoint copies committed pages back to the main DB file. The WAL can only be reset (truncated) when no readers are using pages from it.

### Checkpoint types

| Mode | Behavior | Blocks writers? | Blocks readers? | When to use |
|------|----------|----------------|-----------------|-------------|
| `PASSIVE` | Checkpoint what you can, skip busy pages | No | No | Default auto-checkpoint; after batch ops |
| `FULL` | Wait for readers to finish, then checkpoint all | Briefly | No | Scheduled maintenance |
| `RESTART` | Like FULL, then reset WAL to beginning | Briefly | Briefly | When you want to reclaim WAL disk space |
| `TRUNCATE` | Like RESTART, then truncate WAL file to zero | Briefly | Briefly | Nightly cron; most aggressive cleanup |

### Preventing WAL bloat

**Root cause**: A reader holding an open transaction (even idle/implicit) pins the WAL at its read point. The checkpointer cannot advance past that point, so the WAL grows without bound.

**Your specific scenario**: Facts export opens a read connection to the main DB, runs batch queries over 30+ minutes. During that time, the consumer is writing ~105 events/sec. The WAL grows to 2-3GB because the checkpoint cannot advance past the facts export's read snapshot.

**Fixes**:
1. **Close and reopen the source connection between batches** in facts export. Each batch reads a bounded rowid range — no need for a single long transaction.
2. **Use `BEGIN IMMEDIATE` for write transactions** to fail fast on contention rather than deadlocking.
3. **Set `journal_size_limit=67108864`** (64MB) to truncate the WAL file after checkpoint succeeds.
4. **Run `PRAGMA wal_checkpoint(TRUNCATE)` in the nightly cron** (you already do PASSIVE in retention — upgrade to TRUNCATE in the nightly maintenance window).

### WAL disk space math

WAL file size = (pages since last successful full checkpoint) * page_size.
With page_size=4096 and 105 writes/sec, worst case ~1.5GB/hr if checkpoint is fully blocked.
**Budget for WAL headroom**: main DB size + 2-3GB WAL + 2-3GB for VACUUM INTO temp.

---

## 3. Connection Management

### Anti-pattern: open/close per operation (current driftwatch pattern)

```python
# Current: db.py get_conn() called per operation, conn.close() at end
def insert_event(...):
    conn = get_conn()    # opens new connection
    ...
    conn.close()         # closes it
```

**Problems**:
- Each open/close re-reads the WAL index
- No connection reuse = no page cache benefit between operations
- PRAGMAs reset on each connection (cache_size, mmap_size)

### Better: connection-per-thread with reuse

```python
import threading

_local = threading.local()

def get_conn():
    if not hasattr(_local, 'conn') or _local.conn is None:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA mmap_size=268435456")
        conn.execute("PRAGMA journal_size_limit=67108864")
        _local.conn = conn
    return _local.conn
```

### Multiple processes sharing one SQLite file

This is fine with WAL mode. SQLite's locking protocol handles it. Rules:
- Every process must set `busy_timeout` (or you get instant SQLITE_BUSY)
- Every process should set WAL mode (first one wins, but set it defensively)
- Don't use `PRAGMA shared_cache=ON` across processes — it's for threads within one process and has subtle deadlock risks
- Write transactions are serialized at the database level — keep them short

### Connection lifetime rules

1. **Reader connections**: Can live as long as you want, but **close transactions promptly**. An idle connection with an open (even implicit) transaction pins the WAL.
2. **Writer connections**: One writer at a time. Keep transactions short. Batch operations should commit every N rows (your 5000-row batch in retention.py is correct).
3. **Facts export source connection**: Close and reopen between batch cycles, or at minimum commit/rollback between batches to release the WAL read lock.

---

## 4. Schema Migration Patterns

### What ALTER TABLE supports in SQLite

- `ADD COLUMN` — instant, no table rewrite (modifies sqlite_schema only)
- `RENAME COLUMN` — instant (since 3.25.0)
- `DROP COLUMN` — may rewrite table (since 3.35.0)
- `RENAME TABLE` — instant

### What it does NOT support

- Change column type
- Add/remove constraints (NOT NULL, UNIQUE, CHECK)
- Change default values on existing columns
- Add PRIMARY KEY columns

### Safe production migration pattern (what you're doing)

```python
# This is fine and fast — no table rewrite
try:
    conn.execute("ALTER TABLE claim_history ADD COLUMN fp_kind TEXT DEFAULT 'unknown'")
except Exception:
    pass  # column already exists
```

### When you need more than ADD COLUMN

Use the "12-step" rename-and-copy pattern:
1. `CREATE TABLE new_table (...)`
2. `INSERT INTO new_table SELECT ... FROM old_table`
3. `DROP TABLE old_table`
4. `ALTER TABLE new_table RENAME TO old_table`
5. Recreate indexes and triggers

**Critical**: Wrap in a transaction. This rewrites the entire table — budget disk space for 2x table size.

### Schema versioning (labelwatch's approach is better)

Labelwatch uses explicit `meta.schema_version` + a `migrate()` function that walks
the version chain. Driftwatch uses fire-and-forget `ALTER TABLE ... ADD COLUMN` with
bare `except`. The driftwatch approach works but has no version tracking — you can't
tell what schema state a given DB is in.

### ALTER TABLE on sidecar/persisted DBs

Your `_migrate_identity_facts()` in facts_export.py is correct: check `PRAGMA table_info()`,
then `ALTER TABLE ADD COLUMN` if missing. This handles persisted working DBs that
predate the column addition. Apply this pattern to any persisted sidecar DB.

---

## 5. VACUUM and Disk Space

### VACUUM INTO (what you use for snapshots)

```python
work_conn.execute(f"VACUUM INTO '{tmp_snap}'")
os.replace(tmp_snap, snapshot_path)
```

**Disk space requirement**: VACUUM INTO needs free space equal to the *compacted* output size. For a 13GB working DB that compacts to 10GB, you need 10GB free during the operation.

**Unlike regular VACUUM**, VACUUM INTO does NOT need 2x the source DB size — it writes directly to the target file. But it does hold a read lock on the source for the duration.

### VACUUM INTO pitfalls

1. **Holds a read transaction** for the entire duration. On a 13GB DB this could be minutes. This pins the WAL on the source DB — same WAL bloat problem as long-running reads.
2. **Uses temp space**: By default, SQLite uses `/tmp` for sorting. If `/tmp` is a tmpfs (RAM-backed), a large VACUUM INTO can exhaust memory. Set `PRAGMA temp_store_directory` or `SQLITE_TMPDIR` env var to point at a disk-backed location.
3. **First-run rebuild**: If `facts_work.sqlite` doesn't exist and there's no snapshot to seed from, the first export cycle builds from scratch. For a 30-day retention window over 3M claims/day, that's ~90M rows = potentially 10-13GB. Budget disk headroom accordingly.
4. **Output is always DELETE journal mode**, not WAL. This is correct for a read-only snapshot.

### Disk space budget for your setup

| Item | Size | Notes |
|------|------|-------|
| Main DB (labeler.sqlite) | ~54GB | Stable with retention |
| WAL file | 0-3GB | Peaks during batch ops |
| SHM file | ~32KB | Negligible |
| facts_work.sqlite | ~13GB | Working copy |
| facts_work WAL | 0-500MB | Small, auto-checkpointed |
| facts.sqlite (snapshot) | ~10-13GB | Compacted |
| facts.sqlite.tmp (during VACUUM INTO) | ~10-13GB | Transient |
| Archive (JSONL.gz) | ~2-5GB | Growing slowly |
| **Total peak** | **~95-100GB** | Out of 157GB disk |

**Headroom**: ~57-62GB free. Comfortable, but a full VACUUM of the main DB (needs 54GB temp) would be tight. Never run `VACUUM` (without INTO) on the main DB in production.

### When to VACUUM

**Don't vacuum the main DB in production.** The freelist (empty pages) gets reused by new inserts. Monitor `freelist_pct` — if it exceeds 25-30%, *and* the DB is no longer growing, consider a planned maintenance VACUUM during off-hours.

**Do vacuum sidecars** via VACUUM INTO for snapshots — this is already your pattern and it's correct.

---

## 6. Index Maintenance

### PRAGMA optimize

```sql
PRAGMA optimize;           -- let SQLite decide what needs ANALYZE
PRAGMA optimize=0x10002;   -- also analyze tables that have never been analyzed
```

**When to run**:
- On connection close (ideal for short-lived connections)
- Every few hours for long-lived connections
- After schema changes (CREATE INDEX)
- After bulk data changes (retention purge, large imports)

**Your nightly cron already runs `PRAGMA optimize`** — this is correct. Consider also running it after the retention loop completes, since the data distribution may have shifted.

### ANALYZE

`PRAGMA optimize` runs ANALYZE internally when it determines it's needed. You generally don't need to run ANALYZE manually. If you do, run it on specific tables rather than the whole DB:

```sql
ANALYZE claim_history;
ANALYZE events;
```

Full `ANALYZE` on a 54GB DB can take minutes and holds a read lock.

### Index creation on live DBs

`CREATE INDEX IF NOT EXISTS` is safe but can be slow on large tables. For a 54GB DB with 100M+ rows, creating a new index can take 10-30 minutes and holds a write lock for the entire duration.

**Mitigation**: Create indexes during low-traffic periods. There's no `CREATE INDEX CONCURRENTLY` in SQLite.

---

## 7. Monitoring

### Key metrics to watch

| Metric | How to get it | Warning threshold | Emergency threshold |
|--------|--------------|-------------------|---------------------|
| WAL file size | `stat(db-wal)` or `PRAGMA wal_checkpoint(PASSIVE)` log column | >100MB | >1GB |
| Freelist ratio | `PRAGMA freelist_count / PRAGMA page_count` | >20% | >40% |
| DB file size | `PRAGMA page_count * PRAGMA page_size` | Growth >1GB/day after retention | N/A |
| Disk free | `shutil.disk_usage()` or `df` | <20% free | <10% free |
| Checkpoint success | `PRAGMA wal_checkpoint(PASSIVE)` busy column | busy=1 (couldn't complete) | busy=1 for >1hr |
| Busy timeout hits | Count SQLITE_BUSY exceptions in app logs | Any occurrence | Sustained |

### Lightweight monitoring script (cron)

```bash
#!/bin/bash
# sqlite_health.sh — run via cron every 15 minutes
DB="/opt/driftwatch/deploy/data/labeler.sqlite"
WAL="${DB}-wal"

# WAL size
WAL_MB=0
if [ -f "$WAL" ]; then
    WAL_MB=$(stat -c%s "$WAL" 2>/dev/null | awk '{printf "%.0f", $1/1048576}')
fi

# DB metrics via sqlite3
METRICS=$(sqlite3 "$DB" "
    SELECT
        PRAGMA page_count,
        PRAGMA freelist_count,
        PRAGMA page_size;
" 2>/dev/null | tr '|' ' ')

PAGE_COUNT=$(echo $METRICS | awk '{print $1}')
FREELIST=$(echo $METRICS | awk '{print $2}')
PAGE_SIZE=$(echo $METRICS | awk '{print $3}')

DB_MB=$((PAGE_COUNT * PAGE_SIZE / 1048576))
FREE_MB=$((FREELIST * PAGE_SIZE / 1048576))
FREE_PCT=$((100 * FREELIST / (PAGE_COUNT + 1)))

# Checkpoint attempt
CKPT=$(sqlite3 "$DB" "PRAGMA wal_checkpoint(PASSIVE);" 2>/dev/null)

# Disk free
DISK_FREE=$(df --output=pcent /opt/driftwatch/deploy/data | tail -1 | tr -d '% ')

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) db=${DB_MB}MB free_pages=${FREE_MB}MB(${FREE_PCT}%) wal=${WAL_MB}MB disk_used=${DISK_FREE}% ckpt=${CKPT}"

# Alerts
if [ "$WAL_MB" -gt 1024 ]; then
    echo "ALERT: WAL file is ${WAL_MB}MB" >&2
fi
if [ "$DISK_FREE" -gt 90 ]; then
    echo "ALERT: Disk ${DISK_FREE}% used" >&2
fi
```

### In-process monitoring (already partially implemented)

Your `retention.py` already collects `db_geometry` stats. Your `bake_gate.py` checks WAL checkpoint status. These are good. Consider adding:

1. **WAL size to `/health/extended`** — already there via bake_gate, but make it prominent
2. **Checkpoint age tracking** — record last successful TRUNCATE checkpoint time, alert if >24h
3. **Connection count** — not directly available in SQLite, but you can track open connections in-process

---

## 8. Checkpoint Strategy (Recommended)

### Three-tier approach

1. **Auto-checkpoint** (`wal_autocheckpoint=1000`): Default. Runs PASSIVE after every 1000 pages written. Lightweight, no blocking. Handles steady-state writes.

2. **Post-batch PASSIVE** (retention.py, already implemented): After bulk deletes/updates, run `PRAGMA wal_checkpoint(PASSIVE)`. Catches up what auto-checkpoint missed.

3. **Nightly TRUNCATE** (cron, 06:00 UTC maintenance window): Run `PRAGMA wal_checkpoint(TRUNCATE)` to fully reset the WAL file to zero bytes. This is the only way to reclaim WAL disk space.

```sql
-- Nightly cron (add to existing maintenance.sh)
PRAGMA wal_checkpoint(TRUNCATE);
PRAGMA optimize;
```

### What to do when checkpoint is blocked

If `PRAGMA wal_checkpoint(PASSIVE)` returns `busy=1`:
1. Check for long-running connections (facts export, cluster report, external scripts)
2. The blocked checkpoint is not an emergency — reads/writes still work, WAL just grows
3. Wait for the blocking reader to finish, then re-attempt
4. If the WAL exceeds 1GB, consider interrupting the blocking operation

---

## 9. Anti-Patterns

### 1. Opening connections without busy_timeout
Every connection must set busy_timeout. Without it, concurrent writes fail instantly with SQLITE_BUSY instead of retrying.

### 2. Long-running implicit transactions
```python
# BAD: implicit transaction held for entire loop
conn = get_conn()
for row in conn.execute("SELECT * FROM big_table"):
    process(row)  # takes minutes
# WAL is pinned for the entire duration

# GOOD: fetch in bounded batches
while True:
    rows = conn.execute("SELECT ... LIMIT 5000 OFFSET ?", (offset,)).fetchall()
    if not rows:
        break
    for row in rows:
        process(row)
    conn.commit()  # release WAL read lock between batches
    offset += len(rows)
```

### 3. DEFERRED transactions that upgrade to write
```python
# BAD: starts as read, upgrades to write — can deadlock with another writer
conn.execute("SELECT ...")
conn.execute("INSERT ...")  # may get SQLITE_BUSY even with busy_timeout

# GOOD: declare intent upfront
conn.execute("BEGIN IMMEDIATE")
conn.execute("SELECT ...")
conn.execute("INSERT ...")
conn.commit()
```

### 4. Running VACUUM on a live production DB
Regular VACUUM rewrites the entire DB, needs 2x disk space, holds exclusive lock for the duration. For a 54GB DB, this means ~54GB temp space and 10-30 minutes of total lockout.

### 5. Ignoring journal_size_limit
Without `journal_size_limit`, a burst of writes that generates a 3GB WAL file will leave that 3GB file on disk *even after a successful checkpoint*. The file gets reused (overwritten from the start) but never shrinks. Set `journal_size_limit` to cap the leftover file size.

### 6. Using shared_cache mode
`PRAGMA shared_cache=ON` shares page cache between connections in the same process but introduces table-level locking (worse than WAL's database-level locking) and subtle deadlocks. Don't use it.

### 7. VACUUM INTO without checking disk space
```python
# GOOD: check before attempting
import shutil
usage = shutil.disk_usage(data_dir)
work_size = os.path.getsize(work_path)
if usage.free < work_size * 1.2:  # 20% margin
    LOG.warning("skipping snapshot: %dMB free, need ~%dMB",
                usage.free // 1048576, work_size * 1.2 // 1048576)
    return
```

### 8. ALTER TABLE ADD COLUMN NOT NULL without DEFAULT
This will fail in SQLite. ADD COLUMN with NOT NULL requires a non-NULL DEFAULT value. Use `TEXT DEFAULT ''` or make it nullable.

---

## 10. Specific Scenarios from Your Stack

### Facts export WAL bloat

**Problem**: `export_once()` opens `source_conn` (main DB) and keeps it open for the entire export cycle — batch loop + hourly recompute + bounds recompute + VACUUM INTO. This can be 5-30 minutes, during which the main DB's WAL cannot checkpoint.

**Fix**: The source connection should be closed and reopened between major phases. Each phase reads a bounded range — no need for snapshot consistency across phases.

```python
def export_once(source_conn_factory, ...):
    # Phase 1: batch upsert (uses rowid range, safe to reopen)
    with source_conn_factory() as conn:
        _upsert_uri_fingerprints(conn, sidecar, ...)

    # Phase 2: hourly rollup (independent query)
    with source_conn_factory() as conn:
        _recompute_hourly(conn, sidecar, ...)

    # Phase 3: identity refresh (independent query)
    with source_conn_factory() as conn:
        _refresh_identity_facts(conn, sidecar)

    # Phase 4: snapshot (no source conn needed)
    _snapshot(sidecar, facts_path)
```

### First-run sidecar rebuild consuming disk

**Problem**: If `facts_work.sqlite` doesn't exist and no snapshot exists, the first export cycle reads the entire 14-day claim_history window. With ~3M claims/day, that's ~42M rows, producing a ~10-13GB sidecar. Combined with the main DB (54GB) and WAL headroom, this could push disk usage to 95%+.

**Fixes**:
1. **Check disk space before first-run rebuild** — skip if <20% free
2. **Limit first-run scope** — only process last 3 days instead of full 30-day window on first run, then let subsequent cycles fill in
3. **Preserve the snapshot across deploys** — your rsync already excludes `deploy/data`, so the sidecar persists. The risk is only on a fresh VM or data wipe.

### ALTER TABLE on persisted sidecar DBs

**Problem**: You add a column to `actor_identity_facts` in code, but the persisted `facts_work.sqlite` and the `facts.sqlite` snapshot don't have it.

**Current fix** (correct): `_migrate_identity_facts()` checks `PRAGMA table_info()` and does `ALTER TABLE ADD COLUMN`. This is the right pattern.

**Improvement**: Track a sidecar schema version in the `meta` table. On version mismatch, run migrations rather than checking every column individually.

### Multiple processes sharing one SQLite file

**Your scenario**: Docker container process (consumer + retention + facts export + maintenance) + external scripts (cron maintenance.sh, one-shot CLI commands).

**This is fine** with WAL mode as long as:
1. All processes set `busy_timeout` (you do)
2. All processes access the DB via the filesystem (not NFS — your Docker bind mount is fine)
3. No process holds write transactions for more than a few seconds
4. The WAL and SHM files are on the same filesystem as the DB

---

## 11. Recommended Changes (Priority Order)

### High priority
1. **Add `journal_size_limit=67108864`** to all connection init code (both projects)
2. **Add `mmap_size=268435456`** to all connection init code (both projects)
3. **Close/reopen source connection between phases in facts_export.py** to prevent WAL bloat
4. **Upgrade nightly checkpoint from PASSIVE to TRUNCATE** in maintenance cron

### Medium priority
5. **Add disk space check before VACUUM INTO** in facts_export snapshot
6. **Run `PRAGMA optimize` after retention loop** completes
7. **Switch driftwatch to connection-per-thread** instead of open/close per operation
8. **Add WAL size to /health/extended** response (prominent, not buried in bake_gate)

### Low priority (good hygiene)
9. **Add sidecar schema versioning** to facts_work.sqlite (meta table version key)
10. **Track checkpoint age** — record last successful TRUNCATE time, alert if stale
11. **Use `BEGIN IMMEDIATE`** for write transactions in insert_event to avoid upgrade deadlocks

---

## Sources

- [SQLite WAL Mode](https://sqlite.org/wal.html)
- [SQLite PRAGMA Reference](https://sqlite.org/pragma.html)
- [SQLite ALTER TABLE](https://www.sqlite.org/lang_altertable.html)
- [SQLite VACUUM](https://sqlite.org/lang_vacuum.html)
- [SQLite Checkpoint API](https://sqlite.org/c3ref/wal_checkpoint_v2.html)
- [SQLite Memory-Mapped I/O](https://sqlite.org/mmap.html)
- [SQLite ANALYZE](https://sqlite.org/lang_analyze.html)
- [Litestream Tips & Caveats](https://litestream.io/tips/)
- [Fly.io: All-In on Server-Side SQLite](https://fly.io/blog/all-in-on-sqlite-litestream/)
- [Fly.io: How SQLite Scales Read Concurrency](https://fly.io/blog/sqlite-internals-wal/)
- [phiresky: SQLite Performance Tuning](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/)
- [Fractaled Mind: Fine-tuning SQLite](https://fractaledmind.com/2023/09/07/enhancing-rails-sqlite-fine-tuning/)
- [Bert Hubert: SQLITE_BUSY Despite Timeout](https://berthub.eu/articles/posts/a-brief-post-on-sqlite3-database-locked-despite-timeout/)
- [Ten Thousand Meters: SQLite Concurrent Writes](https://tenthousandmeters.com/blog/sqlite-concurrent-writes-and-database-is-locked-errors/)
- [Simon Willison: VACUUM Disk Full](https://til.simonwillison.net/sqlite/vacum-disk-full)
- [PhotoStructure: How to VACUUM SQLite in WAL Mode](https://photostructure.com/coding/how-to-vacuum-sqlite/)
- [Oldmoe: Turn On mmap Support](https://oldmoe.blog/2024/02/03/turn-on-mmap-support-for-your-sqlite-connections/)
- [AugmentedMind: SQLite Traps and Pitfalls](https://www.augmentedmind.de/2020/08/30/sqlite-traps-and-pitfalls/)
- [SQLite Forum: Checkpoint Starvation](https://sqlite.org/forum/info/7da967e0141c7a1466755f8659f7cb5e38ddbdb9aec8c78df5cb0fea22f75cf6)
- [SQLite Forum: WAL File Grows Past Auto Checkpoint Limit](https://sqlite.org/forum/info/a188951b80292831794256a5c29f20f64f718d98ed0218bf44b51dd5907f1c39)
- [dj-lite: Configure SQLite for Production (Django)](https://github.com/adamghill/dj-lite)
