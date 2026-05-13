# Update 2026-05-07 — cold-path gap spec evidence + tripwire status

Companion to `gap-spec-cold-path-parquet-duckdb.md` (filed 2026-05-05). Records evidence and tripwire status as of 2026-05-07 mid-day. **Still candidate, not authorization to build.**

## Tripwire status

The 2026-05-05 spec listed four tripwires that would escalate the gap from candidate to required. Status check after the 2026-05-06..07 24h sampler window:

### 1. Second workload-contention incident — **evidence pending stall capture**

The 24h geometry sampler window (2026-05-06T16:25Z → 2026-05-07T16:15Z) captured **10 distinct episodes** with `drop_frac > 0.6`, `events_per_sec ≈ 1.5`, `backlog = 5000`, WAL between 47 MB and 603 MB. Pattern repeats roughly daily, not just under burst load.

Whether this qualifies as "writer in sustained kernel I/O wait under non-burst load" depends on what stall captures show. The trigger-based stall watcher deployed today (`/usr/local/bin/driftwatch_stall_watcher.py`, cron */2 min) captures `py-spy dump`, `/proc/<pid>/task/*/wchan`, lsof, fuser on trigger. First captures expected within 2-4h.

**Tentative status:** likely tripwire-met pending py-spy/wchan evidence.

### 2. Disk runway < 30 days for two consecutive weeks — **tripwire met**

Disk runway has been < 10 days since the 2026-04-23 retention-disabled period. Current readings oscillate between 2.48 and 6.16 days over 24h. Net disk loss continues at ~8.7 GB/day despite the pressure-aware retention scheduler (`f86287c`) doing bounded partial work.

This was already known on 2026-05-05; the 2026-05-06..07 window confirms the slope did not flatten.

**Status:** tripwire met.

### 3. New subsystem adding long-lived SQLite reader — not triggered

No new subsystem proposed. The existing readers (longitudinal, facts_export, retention, platform_health, maintenance) are the suspects, not new additions.

**Status:** not triggered.

### 4. Linode obsolescence forcing migration anyway — not triggered

Current 8GB/157GB plan is adequate (now resized to 196GB). No migration deadline.

**Status:** not triggered.

## New evidence the 2026-05-05 spec did not have

### Calm-window concurrency from py-spy

A single `py-spy dump` against the driftwatch container in a calm window (eps=89.8, drop_frac=0, WAL=6.8 MB) showed three SQLite operations active concurrently:

- Thread 13 (`ThreadPoolExecutor-0_2`): `get_claim_history → rule_provenance_laundering → recheck_once` — **longitudinal**
- Thread 24 (`ThreadPoolExecutor-0_5`): `_snapshot → export_once → _run` — **facts_export**
- Thread 16 (`dw-writer_0`): `insert_event_txn → _process_batch` — **writer**

This is direct evidence of the shared-substrate hypothesis from the 2026-05-05 spec. Three subsystems against one 99 GB DB at the same moment, with the system reading as healthy. The py-spy attribution is independent of `lsof` (which would only say "uvicorn"), so subsystem identity is preserved across attribution methods.

### Quantified loss bucket separation across 24h

Yesterday's spec discussed "drop_frac" loss qualitatively. The 24h window now quantifies the two distinct buckets:

| | 24h window |
|---|---|
| `rollback_lost_total` (writer batch integrity) | **0** delta |
| `events_dropped_total` (queue-boundary intake loss) | **+1,562,966** |
| Sample windows with `drop_frac > 0.01` | 26 / 98 (27%) |
| Sample windows with `drop_frac > 0.1` | 24 / 98 |
| Stall episodes with eps ≈ 1.5 + backlog 5000 | 10 |
| WAL p99 | 603 MB |

Writer batch integrity is preserved (`rollback_lost = 0` since 2026-04-25). Intake completeness is **not** preserved at the queue boundary. The Jetstream cursor advances regardless of consumer drops, so most of those 1.56M events/day are not recoverable via cursor replay.

This sharpens Phase 3's framing (`facts_export` source moves to Parquet first): the operational payoff is not just "remove WAL pinning" — it is "remove a measurable per-day intake loss that operators currently can't see without explicit instrumentation."

## What this update does NOT change

- Phased plan. Phase 0 → 7 unchanged.
- Anti-patterns. Tiny-file hell, big-bang migration, DuckDB-as-substrate, Parquet-as-transactional all still anti-patterns.
- Hot writer path. Jetstream → spool → SQLite stays SQLite.

## What this update changes

- The "candidate, non-binding" header should be re-read as "candidate with one tripwire met, second tripwire evidence-pending."
- Phase 3 (facts_export → Parquet) becomes the first concrete priority once stall captures attribute a meaningful share of WAL pinning to facts_export. py-spy already shows facts_export as one of the three concurrent readers in a calm window.
- The `gap-spec-cold-path-parquet-duckdb.md` "Phase 0 — stabilize" acceptance criterion ("ingest_loss=0") was achieved for `rollback_lost` but **not** for `events_dropped_total`. Phase 0 is in flight, not complete.

## Decision gate before any Phase 1 work begins

**Evidence required before promoting the gap spec from candidate to authorization-to-build:**

1. At least 3 stall captures from the watcher across different times-of-day showing consistent multi-subsystem reader concurrency at trigger time.
2. py-spy attribution of the dominant pinning subsystem (most likely facts_export or longitudinal).
3. Isolation toggle protocol run (see `docs/ISOLATION_TOGGLE_PROTOCOL.md`) — disable suspect subsystem for a window, observe whether stall frequency drops materially. If yes, that subsystem's Phase moves first. If no, the substrate-level argument is strengthened.
4. Explicit user decision to authorize Phase 1 work. The spec author's framing stands: *the duck enters with papers.*

## Related

- `gap-spec-cold-path-parquet-duckdb.md` — original spec, 2026-05-05
- `docs/ISOLATION_TOGGLE_PROTOCOL.md` — operational protocol for subsystem-level attribution
- Continuity: `mem_4ffb4a1d` (current project_state, 2026-05-07), `mem_6f09dbc4` (solution-family-exhaustion hypothesis), `mem_d99dd5ee` (diagnostic-reads-are-workloads lesson), `mem_b219ffb7` (intake vs rollback loss vocabulary constraint)
