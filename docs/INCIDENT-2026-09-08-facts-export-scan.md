# Facts export: a non-covering scan delayed snapshots

Date: 2026-09-08. Status: local repair qualified; production outcome pending.

The production completion campaign deployed rc.5 and rolled it back after a
two-hour window with queue drops and stale facts. Subsequent baseline observation
showed that restored HTTP availability did not establish restored facts export.
The exporter remained inside `_recompute_bounds` for hours after startup.

A function-only Python stack capture located the active exporter in that query.
SQLite's query plan was `SCAN uri_fingerprint USING INDEX idx_uri_fp`. This index
contains fingerprints but not the timestamps needed for MIN and MAX. The query
therefore visits table rows in fingerprint order. The deployed sidecar is many
gigabytes; the old comment describing it as a small table was misleading.

The repair adds `NOT INDEXED` to this full-table aggregate. SQLite scans the table
sequentially and groups using its sorter, which can spill to temporary storage.
No retained rows, aggregate definitions, schema, index, or snapshot publication
boundary changes. Temporary-sort space and subsequent VACUUM INTO space remain
operational requirements.

`tests/test_facts_bounds_scan.py` compares the aggregate against the original
indexed query, including timestamp ties, a zero timestamp, replacement of old
bounds, and an empty table. Its plan assertion rejects the observed non-covering
index scan. This and the existing export suite passed: 17 tests.

A synthetic 200,000-row, 50,000-group comparison used the same 42,889,216-byte
database, a 64 KiB SQLite cache, FILE temporary storage, and SQLite 3.45.1. Results
were identical (SHA-256
`7ff8cc4a56db5d2a7de92cc14da3dc014b02c1f79a38c41356361d6316cbed86`).
The indexed query took 0.622 seconds and the sequential query 0.301 seconds.
The OS cache was warm; these timings do not establish production speed or a
completed production snapshot. The private campaign retains the script and
selected operational evidence.

This is separate from the queue/checkpoint repair described in the campaign's
ingestion history. Both baseline and rc.5 exhibited operational trouble; these
observations alone do not attribute a regression to rc.5. A new release must
demonstrate its actual facts publication cadence before acceptance.

## Production follow-through

Candidate `v0.1.0-rc.6` (`9ec9da593b6a435de273396626ecaca3755268e7`)
completed one production export at 2026-09-08 21:32:50 UTC. Function-only samples
had identified retention pruning at 19:21, deletion of prior bounds at 20:18,
and `VACUUM INTO` at 20:47. The last phase demonstrates that the repaired bounds
query completed, rather than merely that the process stayed alive.

Aggregate-only completion logs measured 9,973.9 seconds total: batch 1,719.9,
prune 1,238.8, identity 459.6, hourly 1,224.8, bounds 1,704.4, and snapshot copy
3,106.9 seconds. These are one run's phases, not isolated before/after benchmark
measurements. Bounds deletion and recomputation share one phase timer.

The original two-hour acceptance window ended before publication and remains
incomplete on that gate. A finite followup and separate closeout established
the new 15,532,933,120-byte snapshot and one indexed fingerprint aggregate match.
The exported source watermark was still 18:46:36, approximately 2h50m old when
validated at 21:36:38. New publication therefore does not establish current
source coverage, sustainable export cadence, or eliminated capacity pressure.
The next bounded maintenance task is export catch-up/cost qualification using
these phase receipts, without restarting the safe ingest path merely to repeat
already completed work. See the [ingestion outcome](findings/2026-09-08-completed-prefix-recovery.md)
for accounting, recovery, and remaining status-policy limitations.
