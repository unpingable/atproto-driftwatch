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
