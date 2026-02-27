# Driftwatch Roadmap (Conservative, Operator-First)

This roadmap is intentionally narrow. It favors reversible steps, observable outcomes,
and explicit stop conditions to prevent scope creep.

## Phase 0 — Sealed Lab (current)

Goal: Ingest, fingerprint, and compute. No emits, no labeler registration.

- Jetstream consumer running, ~500 eps steady state
- Fingerprint pipeline with kind tracking (entity/quantity/domain/span/text)
- Cluster reports with burst scoring, half-life estimation, single-author detection
- Longitudinal rechecks with drift rules (assertiveness, provenance laundering)
- Platform health watermark gating rechecks during incomplete data
- Facts export sidecar for labelwatch consumption
- Decision ledger receipting every label decision with full provenance

Exit condition:
Cluster reports show real signal — half-life estimates stabilize, burst detection
catches actual coordinated posts, fp_kind distribution is healthy.

## Phase 1 — Data Validation (next)

Goal: Confirm the fingerprint pipeline and cluster analysis produce trustworthy output.

- Validate fp_kind distribution (entity/quantity should dominate, text < 20%)
- Confirm half-life estimates require 8+ hourly bins before reporting
- Verify burst detection against known coordinated campaigns (retrospective)
- Tune complexity gate if low-signal posts are leaking through
- Evidence tightening: quote spans + prior claim IDs in evidence hashes

Exit condition:
You can point at a cluster report and explain every entry to a non-author.

## Phase 2 — Regime Detection (patience required)

Goal: Detect shifts in claim distribution over time.

- Baseline accumulation (needs days/weeks of data)
- Rolling window divergence detection (label distribution vs baseline)
- Regime taxonomy formalization (QUIET / ACTIVE / SURGE / ALARM) with hysteresis
- Cluster-aware recheck promotion (burst score drives recheck priority)

Exit condition:
Regime shifts are detected and explicable, not just statistical noise.

## Phase 3 — Observatory UX (only if watched)

Goal: Visibility, not control.

- Inspection UI for cluster reports and decision ledger
- Recent decisions, quarantines, regime state
- No charts unless someone checks them daily

Exit condition:
The UI answers "what just happened?" in under 10 seconds.

## Seams / Spec Work

Patterns borrowed from PCAR (agent_gov), adapted for ATProto.
Full analysis: [docs/ATPROTO_SEAMS.md](docs/ATPROTO_SEAMS.md)

**Near-term:**
- Adopt PCAR-D canonicalization profile for receipt hashing
- Add `receipt_hash` to `label_decisions` (content-addressed decision identity)
- Draft effect taxonomy: detect / warn / quarantine / suggest / emit

**Later:**
- Receipt chain (`prev_receipt_hash`) for tamper-evident audit trail
- Typed claim envelopes in decision ledger (explicit claim_type)
- Cross-project receipt verification / replay tool
- Casefile / annotation ledger for human review notes
- Domain separation tags for hash namespacing

## Explicit Anti-Roadmap

- No large corpora ingestion
- No policy enforcement or moderation
- No ML classifiers
- No LLM-in-the-loop decisions
- No "intelligence" claims
- No per-account profiling

This is a forensic instrument, not a referee.

## LLM Boundary (Future Use)

Allowed (off the hot path):
- Authoring and maintenance: fixtures, docs, refactor suggestions (validated by tests).
- Analysis and inspection: summarize existing ledger entries for humans.
- Simulation/red-teaming: hypothetical failures and adversarial scenarios.

Disallowed (inside the governor):
- Proposing labels
- Deciding commits vs proposals
- Resolving disputes
- Adjusting thresholds or budgets
- Selecting evidence or spans
- Scheduling rechecks

One-liner:
LLMs may assist humans around the labeler, but never participate in labeling,
scheduling, or enforcement decisions.
