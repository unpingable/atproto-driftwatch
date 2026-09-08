# Completed-prefix recovery after queue saturation

Date: 2026-09-08. Status: local correctness qualification; production outcome
must be appended after the candidate observation window.

## What failed and what the counter means

The production rc.5 observation reported 37,694 queue drops and the integrator
restored the captured baseline without replacing its database. That restored
availability, not a proof of complete ingestion. Subsequent baseline observation
also found queue drops. The defect below exists in both baseline `7a68b027` and
rc.5 `3a81b95ec29af29d17ef83efdfd3385bf159cc37`.

`consumer.py` advanced `_last_cursor` when receiving JSON. Queue admission could
then fail before normal event persistence. The drain periodically saved that
received cursor after committing an earlier batch; reconnect and shutdown also
used it. Identity/account processing ran outside the queue and swallowed errors.
Replaying already committed events could append duplicate edges, and replaying
an older version after a newer version could reverse the current record.

The drop counter counts failed local queue admissions. It does not identify the
events, their exact time interval, an independent retained copy, or permanent
loss. This campaign has not reconstructed all 37,694 events. The older April
incident's statement that its archived losses could not be backfilled is a
dated finding about that incident, not evidence about this September interval.
See [Jetstream ingest realities](../JETSTREAM_INGEST_REALITIES.md).

## Source boundary and repair

Primary source checked at Jetstream legacy revision
`8a65de4eda28bed1cafcbcf25b0cd46ac6f2148b`:

- [`RunSequencer`](https://github.com/bluesky-social/jetstream-legacy/blob/8a65de4eda28bed1cafcbcf25b0cd46ac6f2148b/pkg/consumer/consumer.go)
  assigns a clock value, persists, and emits serially.
- [`Clock.Now`](https://github.com/bluesky-social/jetstream-legacy/blob/8a65de4eda28bed1cafcbcf25b0cd46ac6f2148b/pkg/monotonic/clock.go)
  strictly advances even on equal or backward wall-clock samples.
- [`ReplayEvents`](https://github.com/bluesky-social/jetstream-legacy/blob/8a65de4eda28bed1cafcbcf25b0cd46ac6f2148b/pkg/consumer/persist.go)
  scans ascending timestamp keys from an inclusive lower bound. Its trimming
  uses a configured TTL; this does not establish current hosted retention.
- The [README](https://github.com/bluesky-social/jetstream-legacy/blob/8a65de4eda28bed1cafcbcf25b0cd46ac6f2148b/README.md)
  recommends resuming from processed event time with a small rewind.

This establishes the examined protocol implementation, not the running upstream
host's exact revision or a retention SLA. The deployment must retain the same
source endpoint. Switching it requires explicit custody reconciliation.

All source kinds now enter the bounded FIFO and single writer. Identity changes,
post processing, replay receipts, and cursor persistence share a transaction.
Claim-processing exceptions propagate on this path. Failed batches remain ahead
of later work and retry. Queue saturation awaits capacity while yielding the
event loop; cancellation leaves the cursor behind unadmitted input. Reconnect
waits for admitted work before choosing its persisted boundary. Shutdown never
saves the received cursor.

Each successful source batch checkpoints its completed frontier minus five
seconds. Receipts contain endpoint, timestamp and SHA-256 of canonical source
JSON, not raw research content. They deduplicate replay including identity
journals, edges and record updates. Receipts older than the resume boundary
expire; input older than that boundary is refused rather than reapplied. The
receipt limit is 100,000: exceeding it rolls back and holds the checkpoint.
Timestamp ties and backward arrival within the retained window are tested.
The guarantee is conditional on the examined source ordering and replay
availability; arbitrarily reordered or expired upstream history is not repaired.

The previous consumer's initial checkpoint may already have skipped work. This
repair cannot retrospectively certify that starting boundary. Unknown intervals
remain unknown; it does not reset historical loss evidence.

## Decisive comparison and reproduction

`tests/test_consumer_resume.py::test_actual_drain_completed_prefix` executes the
actual drain from both exact old source refs and the repair against identical
one-slot synthetic input and controlled writer scheduling. Only timestamp
10,000,000 commits while 11,000,000 and 12,000,000 remain unresolved. Both old
refs save 12,000,000 and reject one admission. The repair saves 5,000,000 and
holds admission. This is a correctness comparison, not a production throughput
benchmark.

The reconnect fixture replays missing work, repeats committed work, updates an
existing record, and repeats old input without undoing the update. Additional
tests cover identity rollback, account state, ties, retained-window reordering,
receipt expiry/capacity, and exact baseline DB code reading/writing candidate
state with `quick_check=ok`.

```bash
PYTHONPATH=src python -m pytest -q tests/test_consumer_resume.py \
  tests/test_identity.py tests/test_db.py tests/test_ops_status.py
PYTHONPATH=src python tools/demo_offline.py
```

Historical-source comparison tests require the two retained Git objects. Other
tests and the meaningful detect-only demonstration use temporary state.

## Operational interpretation and rollback

Producer facts now expose received/admitted/completed attempt counts, replay
duplicates, interrupted unadmitted attempts, outstanding admitted work, queue
waits, and pending admission/batch retry. A recovered retry is not counted as
permanent loss. Pending admission or batch retry makes stream coverage degraded.
Normal short-lived queued work remains visible; queue depth alone does not
establish missing coverage. Existing loss counters retain their meanings.

Two additive SQLite tables are ignored by baseline code; no old tables are
rewritten. Baseline source can start and write candidate state without a data
rewind, but rollback also restores the baseline's unsafe checkpoint behavior.
Keep post-cutover observations and the incident interval when rolling back.

The separate [facts-export scan incident](../INCIDENT-2026-09-08-facts-export-scan.md)
records the observed capacity contributor and bounded exporter repair. Local
correctness, upstream replay availability, production throughput, and production
facts freshness remain distinct claims until observed.
