# Gap: driftwatch self-health contract + NQ evidence-loss finding

Status: CANDIDATE — named 2026-07-13 after a live incident. Not built.

## Forcing case (a real incident, not speculation)
2026-07-13: driftwatch shed **19–26% of its stream for ~97 minutes** (WAL pinned
22MB→6.8GB by an orphaned reader; write path slowed; consumer dropped events).
Everything that mattered was *exposed* by `/health/extended` and the STATS log —
but **nothing promoted it to a finding.** It surfaced only because an operator
asked for a manual health check and an agent went digging (`lsof`/`ps`
archaeology). That is diagnosability, not observability. Textbook
"operationally up, epistemically degraded" (see workbench design constraint) —
green liveness while the observatory fails at its actual job.

## Two follow-ups

### 1. Driftwatch self-health contract (this repo)
Export explicit, baseline-aware signals as first-class health fields (some exist
raw in `/health/extended`; promote them to *typed* signals with their baselines):
- drop_frac **vs its own baseline** (sustained elevation, not instantaneous)
- WAL size **and growth rate** (MB/min), + checkpoint-blockage (`checkpoint_busy`
  climbing while `checkpoint_done` flat)
- disk-runway acceleration
- reconnect churn rate (jetstream keepalive timeouts spiking)
- **commit-progress**: is the write/commit path actually persisting observed
  claims, distinct from ingest liveness

### 2. NQ finding logic (agent_gov / NQ — separate repo, not built here)
Detect the **composite shape**, not raw counters:
> Driftwatch is operationally live but persistently unable to commit observed
> claims, causing active evidence loss.

Shape = { service up ∧ ingest nominal ∧ epistemic throughput degraded ∧ storage
pressure increasing }.

## Discipline (learned the hard way this session)
**Raw counters lie; find on the composite semantic.** `dequeued=0` and
`queue_depth=10735` looked like a "wedged drain" but are the healthy baseline
(recheck consumer disabled by design) — present when green. `WAL>5GB` is a
symptom, not the finding. The finding is *evidence loss*: `dropped>0` sustained,
i.e. observed claims not being committed. Any signal used by NQ must be defined
against its baseline, or it will both false-alarm and false-reassure. See
workbench law [[lesson_measure_the_set_before_calling_it]].

## Root-cause note
This specific incident's trigger was an orphaned host-side `sqlite3` (a local
`timeout ssh` that left the remote query running) — see
[[lesson_ssh_local_timeout_orphans_remote]]. That is a separate operational scar;
the observability gap here would hold regardless of what caused the stall.
