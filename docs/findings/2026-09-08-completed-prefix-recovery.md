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

## Release and observed outcome (2026-09-08 UTC)

The repair shipped as immutable `v0.1.0-rc.6`, source
`9ec9da593b6a435de273396626ecaca3755268e7`. Two isolated clean Linux/amd64
builds using the pinned Python 3.11 base and dependency inputs produced identical
complete image/config identity
`sha256:70c60bb3403c5f2d57c5f9b789306d5933ec3dc751b837c9bde9830d3c88d80a`
and identical complete Docker-save archives
`sha256:d463e8d31ed00e4f3c059eaf20f55acd16f466f4d6349f05e4e01eef400a19ef`.
No differing archive component was excluded. This is bounded runtime-artifact
reproducibility, not a claim that every third-party dependency was rebuilt.

The release passed 59 focused tests and the nine-finding synthetic demonstration.
Network-disabled artifact recovery exercised SQLite backup/restore, startup,
cursor continuity, replay, duplicates, updates, identity changes, and meaningful
findings. The exact captured baseline also read and started against synthetic
candidate-written state without a database rewind. Separately, an offline Git
bundle reconstruction recovered the exact source/tree in a clean worktree.
Source reconstruction, synthetic application recovery, and live operation are
different receipts; none reconstructs the historical missing-event interval.

The deployed candidate was observed from 18:46:34 through 20:46:34 without a
container restart or OOM. At the final accounting sample, 741,655 source envelopes
were received/admitted, 741,650 completed, and five remained explicitly queued.
There were 211 queue waits and 4,177 replay duplicates, with zero reported queue
loss, rollback loss, or batch failures. A saturated queue plus one pending
envelope at the one-hour checkpoint recovered within ten minutes; the degraded
status remained visible during that episode. All outstanding work was accounted.
These observations support the repaired admission/checkpoint invariant under
this load, not comprehensive upstream coverage or recovery of earlier loss.

The original two-hour gate remained **14/15**, because the new facts snapshot
had not yet published. A separate, finite followup ending at 21:35 preserved
that result. Its final scheduled sample was still copying; its terminal correctly
said no publication had been observed. A subsequent one-off closeout and retained
completion log established that atomic publication actually completed at
21:32:50. The new snapshot was 15,532,933,120 bytes. One indexed fingerprint's
stored minimum/maximum/count matched recomputation; this is representative
validation, not a full-table audit.

Publication time is not input freshness: the snapshot's export watermark was
18:46:36, checkpoint rowid 88,332,365. At the 21:36:38 validation, the source was
at rowid 88,673,083, a 340,718-rowid gap and approximately 2h50m export age.
Rowid distance is not a missing-event count. Export elapsed 9,973.9 seconds;
the [exporter incident history](../INCIDENT-2026-09-08-facts-export-scan.md)
records its measured phases. Timely catch-up remains maintenance work even
though snapshot generation now completed without restarting ingestion.

Additional limitations remain explicit: evaluator observations were absent,
and the inherited internal-slack policy assumes `auto_vacuum=none` while the
actual SQLite store reports `auto_vacuum=2`. Its five-million-page floor was
not changed in this campaign. That applicability question is separate from
measured filesystem headroom and ingestion correctness. Producer operation,
bounded live status acquisition, downstream fixture qualification, and authority
to act on observations remain separate claims.
