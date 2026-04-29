# driftwatch — Failure Modes

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc names failure modes the architecture already knows about — encoded in invariants, gates, recovery paths, and incident lessons. It is the architectural-frame counterpart to `../RUNBOOK.md` and `../JETSTREAM_INGEST_REALITIES.md`.

The point: "the architecture handles X" should be a checkable claim, not a story.

## Ingest failures

### Drop-aware coverage (Jetstream queue overflow)

**Shape**: Jetstream produces events faster than consumer processes them. Queue fills; events drop.

**What the architecture knows**: drops happen, especially under maintenance or VACUUM. Outputs computed during drop windows are loss-conditioned, not ground truth.

**Architectural mitigation**: bounded queue with explicit drop counter. `drop_frac` tracked and surfaced in STATS. Reports condition aggregate outputs on observed loss. Platform health gate (`WARMING_UP→OK↔DEGRADED`) hard-gates rechecks during degraded windows.

**Active incident** (2026-04-23..): `drop_frac` 30-40% post-VACUUM. Archives and tables for that window are loss-conditioned views. See `project_driftwatch_degraded_sampling_2026_04.md` in memory.

### Cursor drift on reconnect

**Shape**: WebSocket disconnect; cursor advances past unprocessed events.

**What the architecture knows**: gapless replay matters more than dedup-perfectness.

**Architectural mitigation**: cursor persistence + 3s rewind on reconnect. Backstop scrape every 6h via labeler-lists if applicable.

### "Dead but optimistic" daemon

**Shape**: consumer catches an event, fails to write, continues running. Looks healthy; silently broken.

**What the architecture knows**: a daemon that swallows write failures is worse than a crashed one.

**Architectural mitigation**: DB write failure → crash. systemd restarts. Cursor + 3s rewind covers gap.

## Storage failures

### Disk pressure runaway

**Shape**: SQLite + WAL + archives + facts.sqlite all on one disk. Without a brake, writes continue until disk full.

**What the architecture knows**: 8GB Linode plan, 157GB disk, shared with labelwatch + PDS + governor + caddy. Disk is the constrained resource.

**Architectural mitigation**: maintenance loop monitors disk; warn at 85%, emergency brake at 92% (`.disk_pressure` flag file). Consumer pauses event processing while flag exists. Retention loop runs aggressively under pressure.

### WAL bloat

**Shape**: long-lived reader pins WAL; checkpoint can't truncate; WAL grows.

**Architectural mitigation**: `busy_timeout=60s`, partial index on `events.raw`, retention batch tuning. Nightly cron: `PRAGMA optimize` + WAL checkpoint at 06:00 UTC. Periodic VACUUM (manual; the most recent reduced DB from 57GB to 13.7GB on 2026-04-23).

### Schema migration drift

**Shape**: code expects schema N+1; DB at N. Or vice versa.

**Architectural mitigation**: `init_db()` walks migrations sequentially. Idempotent. `init_db()` only at startup (lesson; un-migrated to memory: don't call inside hot loops).

### Fingerprint version migration

**Shape**: `FP_VERSION` change without dual-emit transition causes audit gap.

**What the architecture knows**: `FP_VERSION` is a contract. Changing it without warning breaks downstream consumers and invalidates aggregates.

**Architectural mitigation**: `FP_VERSION` constant; bump = migration with documented rationale. Transition period emits both old and new fingerprints (audit only, never live). `fingerprint_config_hash()` recorded in every `label_decision`.

## Sampling / coverage failures

### Wrong denominator ("0.2% coverage" bug)

**Shape**: comparing 40k resolved actors against 2M label events; the populations don't match.

**What the architecture knows**: coverage claims without specified populations are nonsense.

**Architectural mitigation**: three named populations (live-observed, labeled-target seed, overlap). Documentation requires denominator specification. Memory entry on the bug.

### Half-life noise floor

**Shape**: half-life computed on <8 hourly bins is meaningless.

**Architectural mitigation**: `cluster_report()` requires ≥8 hourly bins for half-life output. Below that, reports as null/insufficient.

### AppView/HTTP read health gap

**Shape**: platform health covers Jetstream stream only. AppView and HTTP reads have separate failure modes (rate limits, 401s, latency spikes) not currently tracked.

**Status**: known gap, see `../../THEORY_TO_CODE.md` known-gaps section.

## Detection / publication failures

### Repeated identical findings

**Shape**: same `det_id` fires every scan, drowns out new signals.

**Architectural mitigation**: `CooldownFilter` in `detection.py`. Suppresses repeat det_ids for `COOLDOWN_WINDOWS` (5). Re-emits on severity escalation or score Δ ≥ `COOLDOWN_SCORE_DELTA` (0.5). `detections_suppressed` count in report header.

### Singleton noise

**Shape**: one-off posts fingerprint-matching nothing else trigger spurious enqueue.

**Architectural mitigation**: enqueue gate requires multi-author OR contains-link OR reply-thread membership. Recheck queue is a rolling hotset, not a backlog.

### Accusation-shaped output drift

**Shape**: dashboard language drifts from "concentration anomaly" toward "bot farm" because the latter is more vivid.

**What the architecture knows**: language *is* surface area. Calling something a "bot farm" commits driftwatch to a verdict it doesn't have evidence for.

**Architectural mitigation**: signature-not-category language is a documented invariant. See `SIGNAL_MODEL.md`. Output review for verdict-shaped phrasing is part of the publication discipline.

## Cross-population skew failures

### Vintage / cohort confounds

**Shape**: cross-population comparison shows a "skew" that turns out to be cohort or activity bias, not the observable.

**Architectural mitigation**: `../../specs/core/ADMISSIBILITY-PROTOCOL.md` formalizes the sequence for taking an observed skew from "interesting pattern" to "publishable claim, or honestly retired." Required steps: state claim, identify field semantics, enumerate confounds, define exclusion rules *before* rerun, recompute, classify outcome (admissible / narrowed / dissolved / deferred).

**Reference instance**: `../VINTAGE-ADMISSIBILITY.md` (hosting-locus / vintage).

### Resolver as crawler

**Shape**: resolver scope creeps from "resolve labeled targets" to "resolve everything in sight."

**What the architecture knows**: indiscriminate crawling is hostile to the network and produces dossier-shaped storage.

**Architectural mitigation**: resolver works from a seeded queue (`seed_targets.py`) — labeled targets from labelwatch, plus live-observed identity events. Not free-form crawl. Provenance tracked per DID (`identity_source`: live / labelwatch_seed / both).

## Baseline / state failures

### EWMA baseline lost on restart

**Shape**: process restart resets in-memory EWMA; first hour post-restart is hot-baselined and produces noise.

**Architectural mitigation**: baseline persistence — checkpoint/restore EWMA stats across restarts (2026-03-16). `WARMING_UP` state on cold start.

### Hard-coded thresholds drift from data

**Shape**: thresholds set when data looked one way; data shifts; thresholds no longer separate signal from noise.

**Architectural mitigation**: thresholds are config (recorded as `config_hash` in every receipt). Threshold changes are auditable. EWMA-based baselines adapt automatically; static thresholds need explicit review.

## The doctrine layer

### Operationally up vs epistemically degraded

A live, green system can be silently producing loss-conditioned outputs. Liveness ≠ coverage ≠ truthfulness.

**Architectural mitigation**: drop-aware coverage stats; platform health states (WARMING_UP, OK, DEGRADED); STATS heartbeat surfaces lag and drop_frac; reports condition outputs on observed loss. The dashboard does not say "everything is fine" without saying which axis it's measuring.

### Tolerability horizon

Sometimes the system is bad-but-tolerable; sometimes it's actually broken. Both states must be declarable.

**Architectural mitigation**: platform health distinguishes DEGRADED from broken. Drop_frac surfaced. Reports flag conditional outputs explicitly.

### Co-presence is not corroboration

Two label decisions appearing together does not validate either. A receipt next to another receipt is not evidence that either is correct.

**Architectural mitigation**: each receipt attests to one decision and its inputs. Cross-reference is operator judgment; the architecture does not claim corroboration.

### Crash loud on write failure

A daemon that hides write failures looks healthy and is silently broken. Loud crashes are recoverable; silent rot isn't.

**Architectural mitigation**: discovery streams and consumers crash on DB write failure. systemd restart with cursor rewind covers the gap.

## Cross-reference

- `OVERVIEW.md` — system context.
- `SIGNAL_MODEL.md` — what the signals can and can't decide.
- `PUBLIC_SURFACES.md` — what gets surfaced and the gates.
- `DATAFLOW.md` — where each gate sits in the pipeline.
- `../RUNBOOK.md` — operator procedures (recovery).
- `../JETSTREAM_INGEST_REALITIES.md` — Jetstream-specific operational notes.
- `../../specs/core/ADMISSIBILITY-PROTOCOL.md` — published-claim discipline.
- `../VINTAGE-ADMISSIBILITY.md` — hosting-locus / vintage instance.
- Memory: `lesson_operationally_up_epistemically_degraded.md`, `project_tolerability_horizon.md`, `constraint_copresence_not_corroboration.md`, `project_driftwatch_degraded_sampling_2026_04.md`.
