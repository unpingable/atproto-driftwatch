# Repository-declared operations visibility

Driftwatch declares its expected semantic attention surface at
`.ops/concerns.toml` (`project.concerns/v1`). A generic consumer reads this
semantic inventory independently of acquisition. The separate
`.ops/observation.toml` (`project.observation-binding/v1`) binds one
project-level `project.ops.status/v1` producer without Driftwatch-specific
knowledge:

```bash
PYTHONPATH=src python3 -m labeler.cli driftwatch ops-status --format json
PYTHONPATH=src python3 -m labeler.cli driftwatch ops-status --format text
```

The status is producer-local observation. It is not NQ admission, Pulse
qualification, or Nightshift attention policy. The manifest contains no
notification severity, recurrence, retry, or response cadence.

## Declared propositions

| Concern | Exact local proposition and basis | Becomes unknown, stale, or degraded when |
|---|---|---|
| `driftwatch.observation.stream_coverage` | A current-session consumer fact (max age 120s) reports a connected Jetstream transport, the configured warmup window count (five by default), health `ok`, and no known drop, rollback loss, parse failure, or recalibration. | No fact (`ABSENT`), restart has not re-established the configured window count (`UNKNOWN`), fact is old (`STALE`), or connected observation is lagging/dropping/failing (`DEGRADED`). |
| `driftwatch.observation.cursor_continuity` | The SQLite `cursors` table contains a durable restart cursor updated within 15m. | Cursor absent/unreadable, old, or live session has advanced beyond a stale durable cursor. Process restart does not renew it. |
| `driftwatch.evaluation.freshness` | The independently enabled longitudinal evaluator emitted a completed run fact within 180s and recorded adequate inputs. | Evaluator never ran, fact is stale, run failed, or current inputs were inadequate. |
| `driftwatch.drift.bounded_current_state` | The last current bounded candidate evaluation reports one of `DRIFT_OBSERVED`, `NO_DRIFT_OBSERVED`, `UNKNOWN`, `FAILED`, or `NOT_ESTABLISHED`. | Stale, failed, missing-side/no-candidate, or inadequate-basis evaluation retains its distinct state. Only an adequate current run over loaded candidates may say `NO_DRIFT_OBSERVED`. |
| `driftwatch.evaluation.execution` | A bounded evaluator pass completed without per-candidate failures. Queue depth/oldest age are reported as rolling-hotset facts only. | Evaluator fact is absent/stale/unknown/failed or candidate failures occurred. Depth alone never asserts backlog convergence. |
| `driftwatch.persistence.sqlite_continuity` | The configured DB passes bounded read, required-table, schema-metadata, page/freelist, and non-mutating `BEGIN IMMEDIATE` write-intent probes. It explicitly reports `integrity_check_performed=false`; full integrity belongs to the quiesced backup/restore receipt. | The file is absent or unreadable, required continuity tables are missing, or write intent cannot be acquired. This is not an integrity or “database healthy” claim. |
| `driftwatch.persistence.volume_capacity` | The data volume is below the existing 85% warning and 92% emergency-brake thresholds. | Sampling fails or either exact bound is crossed. |
| `driftwatch.persistence.sqlite_slack` | The `auto_vacuum=none` DB has at least 5,000,000 freelist pages available for internal reuse, matching the production NQ early-warning check. | SQLite geometry is unavailable or internal reuse pages fall below the floor. This is intentionally different from Labelwatch's freelist-bloat predicate. |
| `driftwatch.output.facts_snapshot_freshness` | `facts.sqlite` exists and its atomic snapshot mtime is within 2h. | The artifact was never produced or is stale. The artifact is explicitly observational, not authoritative identity truth. |

The semantic question IDs are the `driftwatch.question.*/v1` values in the
manifest. `driftwatch.profile.local_status/v1` names this local representation;
it is not an NQ profile.

## Drift state and observation basis

`NO_DRIFT_OBSERVED` means only that no Driftwatch rules matched in the bounded
candidate set evaluated by a recent successful run under adequate current
stream observation. It does not mean universal absence of drift. A missing
comparison side yields `NOT_ESTABLISHED`; evaluator failure yields `FAILED`;
inadequate coverage yields `UNKNOWN`; and old results yield `STALE`.

Absence of observed activity is not evidence of absence unless adequate
observation coverage supports that inference. In particular:

```text
no incoming events != no upstream events
endpoint equality != uninterrupted convergence
no new decision rows != no drift
```

Driftwatch does not currently observe an authoritative desired-state
reconciler or causal actor. No reconciliation/convergence concern was invented.
The recheck queue is documented and implemented as a capped rolling hotset, not
a backlog, so its depth cannot establish convergence or stall by itself.

Missing required observation is visible state, not implicit success. An absent
observation has `local_state: "ABSENT"` and `observation_present: false`.

## Restart and currentness

The EWMA baseline checkpoint remains useful historical input, but the ops
surface also carries a new random session ID and an independent
`session_windows_seen` counter. Even when a recent baseline is restored, the
status remains `UNKNOWN` until five new-session windows and a live connection
re-establish the observation basis. Historical evaluation and cursor facts age
normally and are not renewed by process existence.

## NQ seam

The two files under `deploy/nq-checks/` are genuine production NQ saved checks
for volume free space and SQLite internal slack. They remain valid narrow
resource checks, but they do not admit this manifest/status envelope or cover
stream/evaluator/drift state. NQ's available check-pack contract also says
family-specific observation output is not yet a versioned immutable monitor
artifact.

Accordingly this work does not claim full NQ integration. It provides an
adapter-ready status representation and deterministic tests. A future adapter
must exercise NQ's real immutable admission path and preserve project,
question, source, producer session, observation time, validity basis, and
domain disposition. Pulse remains responsible for downstream currentness and
blindness qualification; Nightshift retains recurrence and attention.

Monitor now performs passive discovery, explicitly trusted bounded execution,
exact structural matching, and the declaration/observation left join. Its
`MISSING_REQUIRED_OBSERVATION` is distinct from Driftwatch explicitly
reporting `UNKNOWN`, and it does not reinterpret
`NO_DRIFT_OBSERVED`. This is still not generic NQ semantic admission.
