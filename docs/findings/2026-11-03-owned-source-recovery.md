# Owned source and recovery reconciliation — 2026-11-03

This is source integration for [issue 1](https://github.com/unpingable/atproto-driftwatch/issues/1)
and Driftwatch's part of [issue 2](https://github.com/unpingable/atproto-driftwatch/issues/2).
It does not select a production replacement or establish sustained operation.

## Current consumer correction — 2026-11-03

This record preserves the earlier source integration and its tests. The
historical facts-snapshot and live Labelwatch-reader requirements below are
superseded by the [current operating boundary](../RUNBOOK.md). No current
Labelwatch or Facts API consumer uses that bridge. The current named Driftwatch
consumer is the private native cluster report and summary; its dated query,
source coverage and resource limits require actual evidence.

The later finite ingestion occurrence failed its connection and drain limits,
and the private native report reached its query deadline. Those outcomes remain
failed. They do not close issue 1 or establish fresh or sustained operation.
Current acceptance must name the selected source/image, completed prefix,
truthful gaps, enabled workload, capacity and recovery boundary. An unconsumed
exporter or retired compatibility reader is not a prerequisite for ordinary
delivery or current consumer acceptance.

## The source boundary

The live GitHub breakwater branch was still
`9fcb3cdb8c4b17f8663b216ea0750705204db958` on November 3. Its owned local
successor was `7f4147c775be411b0865f49759aabb3b56589032`, thirteen commits
ahead, with separate uncommitted archive/retention changes. July main is not
the replacement donor.

The selected production source was re-observed read-only on November 3 as
`15a5fdd30e111f5a23b40a3f418924605593a46d`. It diverges from breakwater at
`6525e5e627d57b1ec48712df80c2d42429e131b6`, and already contains completed-prefix
accounting, fail-closed admin authentication, operator-pause enforcement and
snapshot-workspace admission. Deploying the local breakwater head would lose
those contracts.

The integration branch `fix/selected-source-recovery-20261103` therefore starts
at that exact selected source and carries the existing hourly-rollup,
lineage/recovery and data-contract commits (`3c6ee4f`, `42c2f8a`, `7f4147c`),
followed by the bounded integration corrections below.
Conflict resolution retains the selected source's connection diagnostics,
committed-prefix reconnect cursor, five-second replay overlap, earned-baseline
provenance and workspace admission before any lineage-triggered rebuild.

The separate dirty archive work remains at the original checkout, unchanged.
Its current author/custodian has not been established. It is excluded from this
candidate, and no destructive-retention claim is adopted from it. In
particular, source integration does not qualify recurring retention or the
forward-only multipart archive layout described by that unfinished work.

## Current runtime observation

At `2026-11-03T16:24:07Z`, the selected image was
`sha256:82ef11e736f498b2504be672788aaa6576cec99302549c1fcdac6f8dcfd44d86`.
The container was running and healthy, with operator pause enabled and main
ingestion, label ingestion, facts export, retention, maintenance and both
rechecks disabled. The durable cursor remained `1789150814352505`, updated
`2026-09-13T06:04:34.334513+00:00`.

The facts snapshot was 15,537,885,184 bytes, last modified
`2026-09-12T04:46:46.807224+00:00`, about 52.48 days old. It carried no source
generation or coverage-floor metadata. The primary store was 107,477,565,440
bytes with an empty WAL and no lineage tables. The old source is therefore
still selected; the recovery integration has not been deployed.

The observation used pinned SSH host checking and no agent forwarding. Only
whitelisted image/feature and aggregate metadata was retained. SQLite reads
used immutable mode after verifying the selected WAL was empty, avoiding SHM
or journal creation. No service, configuration, data or credential was changed.

## Recovery evidence and its limit

The existing total-loss regression runs the real consumer against a local
Jetstream substitute, destroys only replica SQLite/WAL/SHM files, and restarts
without supplying the old frontier. The new generation reports unknown
earlier continuity and a new substantiated floor. A later restart uses the
atomic durable cursor with its replay overlap. Retained facts watermarks and
health baselines cannot silently carry the old lineage into the new one.

Additional checks cover an interrupted sidecar rebuild, workspace refusal
before rebuilding, and rebuilding current DID/PDS interpretation while
retaining the configured admin boundary. These checks establish local
behavior. They do not prove production ingestion, archive custody, rollout,
rollback after new writes, or current output freshness.

The existing snapshot-to-Labelwatch test uses the real scan consumer from
Labelwatch source `aef40df17c17ef9746e4d03cd080ad096808945c`. Its fixture
checks URI/fingerprint and current-identity semantics, including timestamp
quarantine. An additional real-reader test now consumes the changed legacy
exporter after a source-generation rebuild: it derives the replacement URI
with the correct fingerprint and lag, excludes the prior store's URI, and
releases its attachment and temporary tables. Neither test makes a claim
about longer-running downstream reports.

The first complete integrated suite passed 610 tests on Python 3.12.3. An earlier
complete run retained four evaluator failures caused by replay fixtures
leaving ancient stream lag in the process-global health monitor. The ordered
replay/evaluator pair reproduced that failure offline before a fixture-only
isolation repair and passed afterward. No production health gate or evaluator
expectation was weakened. Compile checks, local documentation links and the
offline detect-only demonstration also passed.

## Review counterexample and bounded correction

The first independent review accepted `d45bf55` for its tested initialized-
store scope and noted two further export paths. Its verdict and evidence are
retained. A subsequent synthetic reproduction established that a bound export
with five old rows could read a lineage-less replacement with seven rows,
retain all five old-store URIs, skip the first five replacement URIs, and
erase its generation binding. A standalone exporter can encounter a store
from an older image without running application initialization first; this
transition was therefore a real gap in the broader replacement claim.

The correction refuses a missing source lineage whenever a known export
binding exists, before working-store writes or rebuild. It preserves the
prior generation, watermark and both working and published facts; it never
initializes the source. Regression cases use replacement row counts below,
equal to and above the old watermark. A second regression reproduces and
closes the new source-open/read exception path's working-connection leak.
Both corrections were reproduced offline before repair. They change no
production selector, pause control, retention protocol or observation verdict.
The successor complete suite passed 617 tests, with no failures, errors or
skips. The six new boundary regressions failed before the corrections and
passed afterward; the real-reader suite passed all three cases.

## Remaining ordinary operating acceptance

1. At execution time, re-observe the exact selected image, configuration,
   pause controls, durable
   cursor, stored gap records, facts mtime, database/WAL allocation and free
   space. The November observations above also age. Queue/drop/lag
   zero while paused do not establish observation. Preserve the recorded gaps.
2. Freeze one source/image and its rollback, with the effective operator pause
   and unset-token refusal verified. Refresh source reconstruction and copied-
   state application recovery using the existing release/recovery procedure.
   An old backup or a source test is not a current rollback receipt.
3. Choose and qualify the enabled retention/export configuration. The dirty
   archive repair is unresolved; this candidate does not authorize its
   deletion paths. Do not reuse a historical compacted generation or a
   historical capacity reading as current admission.
4. In the admitted deployment window, establish five new-session stream
   windows, current consumer evidence within 120 seconds, zero known loss and
   an advancing completed-prefix cursor. Verify a controlled restart preserves
   the replay boundary and discloses every unrecovered interval. A source replay
   request older than the provider's actual floor still needs an explicit gap.
5. Observe the selected retention cadence and physical plus live-page growth;
   a zero-growth pathname can be consuming its freelist. Verify WAL and export
   workspace headroom on every actual filesystem. Then require a newly atomic
   facts snapshot within two hours and exercise the installed local Labelwatch
   facts reader. Disabled evaluators and schedules remain explicitly disabled,
   rather than becoming failed or passing observations by inference.

The named consumers are the Jetstream reader and completed-prefix writer,
the source-bound facts exporter and atomic local snapshot, Labelwatch's
`scan._sync_driftwatch_facts`, and the existing operations-status acquisition.
The snapshot writer's older Parquet path still lacks a generation/floor in its
manifest, as disclosed in [the data contract](../../DATA-CONTRACT.md). It must
not be substituted for the lineage-aware legacy exporter without preserving
that limitation.

The existing production sequence and copied-state recovery procedures own the
execution and rollback boundaries. Historical cross-product campaigns confer
no new admission. This work adds no such dependency, archive platform or
production mutation. Issue 1 stays open until fresh sustained-operation
evidence satisfies the selected configuration.

## Proposition and decision owner

For this exact source integration, losing the observation store must not
substantiate continuity before the next recorded coverage floor, and retained
derived state must not suppress observations from a replacement generation.
The owner decides production selection and acceptance; this branch supplies
reviewable source and replica evidence. Bounded real-consumer regressions and
the explicit state transitions suffice here; a separate formal model would
not establish the missing production observations or resource admission.
