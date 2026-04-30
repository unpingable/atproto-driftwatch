# Jetstream Ingest Realities

> Notes from operating a real labeler against a real firehose.
> The 2026-04 self-shedding incident is the running case study.

Running a labeler is not "open a websocket and inspect records." It is operating a loss-sensitive stream consumer with backpressure, retention, archives, throughput ceilings, and degraded-sampling detection. A labeler can remain operational while its outputs become epistemically degraded due to dropped intake. Process liveness is not coverage and not truthfulness.

This doc captures the implementation scars. For the normative invariants those scars teach, see [labeler/INGEST_INVARIANTS.md](https://github.com/unpingable/atproto-driftwatch/tree/main).

## Case study: April 2026 self-shedding

### What happened

Between roughly **2026-04-15 and 2026-04-28**, driftwatch was silently dropping ~30-40% of jetstream events at its internal asyncio queue. `/health` reported `status=ok`. Disk was fine. Retention was fine. The event loop wasn't stuck.

The only honest signal was `platform_health.degraded(high_drop_rate)` and a sustained `drop_frac` around 0.357 in `/health/extended`. Everything else looked healthy. Archive day-files in `data/archive/claim_history/YYYY-MM-DD.jsonl.gz` quietly shrank from a stable ~270 MB/day to 39-203 MB/day, but that gradient was not visible from any single dashboard view.

### Why it took four days to localize

Three things conspired:

1. **The framing was wrong.** The first symptom was "the freelist is bloating back up after the VACUUM." It wasn't. Freelist was stable at ~2% of the database. The DB had grown 13.7 → 24 GB in five days because the post-VACUUM working set refilled to its real 7-day footprint, not because retention was failing.
2. **Retention was correctly running** (events/edges/event_versions all pruning at expected rates). One pass logged `claims_pruned: 0`, which looked alarming, but `MIN(observed_at)` showed claim_history was already at the 7-day boundary — there was simply nothing older to prune in that window.
3. **The actual signal was hiding under a different question.** "Why is the DB growing?" is not the right question when the DB is at steady state. The right question is "why is the stream being shed?" — and the answer is in `STATS` lines from the consumer, not in disk metrics.

The diagnostic moment was the `STATS` ledger:

```
STATS window=60s events_in=1,704 claims=749 ... backlog=4999 dropped=1,312 ...
```

`backlog=4999` (pegged at queue maxsize) plus a four-figure `dropped` count is unambiguous. Drops are happening at the consumer's own queue, not upstream.

### The bottleneck

Per single jetstream event, the drain task was doing this:

```
_process_event(ev):
  insert_event(...):
    get_conn()           # NEW SQLite conn + 4 PRAGMAs
    SELECT raw,ctime FROM events WHERE event_uri=?
    INSERT INTO events ...
    conn.commit()                                        # FSYNC
    add_claim_history(...):
      get_conn()         # ANOTHER new conn + 4 PRAGMAs
      INSERT INTO claim_history ...
      conn.commit()                                      # FSYNC
      conn.close()
    _add_recheck(conn, fp):
      INSERT OR IGNORE INTO recheck_queue ...
      conn.commit()                                      # FSYNC
    conn.close()
  insert_edges(edges):
    get_conn()           # third new conn + 4 PRAGMAs
    executemany INSERT INTO edges ...
    conn.commit()                                        # FSYNC
    conn.close()
```

**Three fresh `sqlite3.connect()` calls and four `commit()` calls per event.** At ~100 events/sec that's ~300 connection opens/sec and ~400 fsync barriers/sec on a single SQLite writer. Even with `synchronous=NORMAL`, each commit is a barrier. Each `get_conn()` re-runs `busy_timeout`, `journal_size_limit`, and `mmap_size=268435456` PRAGMAs. The `mmap_size` one is non-trivial as the file grows.

The drain ceiling worked out to ~20-60 events/sec. Jetstream produced ~95-105 events/sec. The asyncio queue (maxsize=5000) saturated; surplus dropped via `QueueFull`. Drop rate stabilized at the gap, ~30-40%.

### Why it appeared *after* the VACUUM

Pre-VACUUM the DB was bloated but its hot pages were warmer in memory. Post-VACUUM the file was dense and growing, and the working set started leaving page cache as `edges` (12.5M rows) and `claim_history` (7M rows) consumed the index footprint. Each `insert_event` selects on the events PK, each `insert_edges` writes to a non-indexed append table, each claim insert writes to a column with `idx_claim_history_fp(claim_fingerprint, createdAt)`. None of these are slow individually; the per-event commit cadence amplified the cost of every page miss.

The bottleneck wasn't *caused* by the VACUUM. The VACUUM made it visible by removing the freelist debt that was masking it. That's a useful pattern: **reclaiming free space is also a way to find latent throughput problems** that were being absorbed by storage slack.

### The fix (commit 2879058)

Three changes, scoped tight:

1. **Persistent writer connection** — owned by a single-thread `ThreadPoolExecutor`. Opened once inside the writer thread, kept alive for the consumer's lifetime. PRAGMAs run once.
2. **Batched drain** — drain pulls up to `BATCH_MAX_EVENTS=100` events (or `BATCH_MAX_WAIT_S=0.25` seconds, whichever comes first) and hands the whole batch to the writer thread.
3. **One transaction per batch** — `_process_batch` runs all `insert_event_txn` / `insert_edges_txn` / `add_claim_history_txn` / `_add_recheck_txn` calls inside one implicit transaction, with a single `commit()` at the end. Rollback on any exception drops the whole batch loud rather than half-writing it.

The old public functions (`insert_event`, `insert_edges`, `add_claim_history`, `_add_recheck`) stay as thin wrappers around the new `_txn` variants — open conn → call `_txn` → commit → close — so non-hot-path callers (CLI tools, demo replay, tests) are unchanged.

Result, immediate post-deploy:

- `drop_frac` 0.357 → **0.0**
- `platform_health` degraded → **ok**
- queue backlog 5000 → **1-2**
- drain throughput ~20-60 eps → **52-62 eps** (matches production rate, with headroom)

### What the fix did *not* prove

Recovery is not the same as resilience. Specifically:

- **Recheck enqueue came back online** (`_fp_passes_enqueue_gate` had been platform-gated during degradation). Through the 24h recovery horizon the queue caught up and stayed plateaued — recheck did not become the next bottleneck. That was an open question at fix-time, not at recovery-stamp time.
- **Edges have no dedupe and no unique constraint.** The 12.5M edges table includes duplicates. Not a hot-path issue, but worth a deliberate swing later.
- **Archives produced during the degraded window cannot be backfilled.** The shed events are gone. Stamp the window, condition any quantitative claims derived from it, and move on.

### What the fix decisively proved

Naive ingest design — read event, write event, write claims, write edges, repeat — produces a self-shedding stream processor on first contact with a real firehose. **The naive design is not a starting point that gets refined; it's a design that silently fails under load while reporting health.** Persistent writer + batched transaction is not a performance optimization. It is correctness infrastructure for any labeler that intends to make claims about the world.

### Recovery stamped 2026-04-29

The fix landed 2026-04-28T09:23 UTC. Recovery was stamped ~27h later, after the criteria-with-horizon held the full 24h window.

Criteria met:

- `drop_frac < 0.05` sustained for 24h — held at **0.0** the entire window
- `recheck_queue_depth` plateaued rather than ran away
- No `batch failed` / rollback spam in writer thread logs (`docker compose logs --since 24h labeler` clean)
- DB/WAL behavior sane at stamp time: WAL **64 MB**, no checkpoint stalls
- `platform_health=ok`, no `gate_reasons`, `coverage_pct=0.9724`, `events_per_sec=100.5` vs `baseline_eps=103.4`
- Disk pressure cleared as a separate VACUUM win (44.8% used, 108 GB free)

Criterion deferred (not load-bearing for the 24h rule):

- Archive day-files in `data/archive/claim_history/YYYY-MM-DD.jsonl.gz` returning to ~270 MB/day baseline. The retention pipeline writes archives ~14 days after the day, so the first archive from the recovered window (`2026-04-28.jsonl.gz`) lands ~**2026-05-12**. Until then this criterion is structurally untestable. After that date, if archives don't return to baseline, reopen.

Residual follow-ups carried as non-blocking:

- `facts_export work_size_mb` running ~2.4× snapshot size. By design — work DB is long-lived WAL with retention by `DELETE`; snapshot is `VACUUM INTO`-compacted. Could add `PRAGMA incremental_vacuum` if disk pressure returns. Not warranted now.
- `resolver.pending` elevated (~332k vs 50k seeded 2026-03-19). Tracked under the resolver seed-queue draining work, not under this incident.

The 2026-04-15..28 window remains loss-conditioned. Don't backfill, don't quote rates from it without flagging.

## Operational doctrine

These follow from the case but are not specific to it.

- **A labeler can be live and lying.** Liveness is process state. Coverage is intake completeness. Truthfulness is the relationship between claimed outputs and observed reality. They are three axes; a green light on one does not imply the others.
- **Drops at your own queue boundary are not "upstream loss."** They are self-shedding. Report them as such.
- **Mark degraded windows.** Don't let them blend into history. Future readers (you, in a month) need to know which artifacts come from sampling rather than coverage.
- **Recovery requires sustained criteria, not a snapshot.** A clean `drop_frac=0.0` two minutes after a deploy is not recovery. The earlier degradation note stays open until the criteria-with-horizon are met.
- **VACUUM exposes throughput problems that storage slack was hiding.** This is a feature, not a bug. Plan for the post-VACUUM throughput surprise, not just the post-VACUUM disk-size win.

## Pointers

- Fix commit: `2879058` (`consumer: batched writer thread + persistent SQLite conn`)
- Closed incident record + recovery receipts: `project_driftwatch_degraded_sampling_2026_04.md` (auto-memory, not in repo)
- Durable lesson extracted from this incident: `lesson_self_shedding_queue_boundary.md` (auto-memory)
- Cross-constellation lesson: `lesson_operationally_up_epistemically_degraded.md` (auto-memory)
- Normative invariants: `labeler/INGEST_INVARIANTS.md` in the reference labeler repo
