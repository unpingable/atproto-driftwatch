# Labeler ROADMAP (Conservative, Operator-First)

This roadmap is intentionally narrow. It favors reversible steps, observable outcomes,
and explicit stop conditions to prevent scope creep.

## Phase 0 — Freeze the Contract (now / pre-release)

Goal: Make it boring and legible.

- Fingerprint format, `FP_VERSION`, and decision ledger are schemas, not features.
- No semantic changes without fixtures + budget tests.
- Emit stays detect-only by default.

Exit condition:
You can diff two runs and explain every difference.

## Phase 1 — Operate Without Lying (first live runs)

Goal: If something goes wrong, it fails quietly and explainably.

- Document inspection surfaces:
  - `/health`
  - `/health/extended`
  - decision ledger anatomy
- Single, explicit fail-closed path:
  - reason → quarantine → suppress emit
- Ops checklist:
  - env flags
  - budget defaults
  - recheck cadence
  - how to shut it off in 30 seconds

Exit condition:
A non-author can tell whether the system is healthy without reading the code.

## Phase 2 — Evidence Tightening (Reactive Only)

Goal: Improve precision only when disputes appear.

- Tighten evidence hashes (quote spans + prior claim IDs).
- Add small, targeted near-miss fixtures (10–20 cases).
- No broad corpus expansion.

Explicit rule:
If no one is arguing with the labels, do nothing.

Exit condition:
Disputes are resolved by pointing at receipts, not intuition.

## Phase 3 — Semantic Scheduling (Opt-In, Constrained)

Goal: Allow deeper longitudinal reasoning without global blast radius.

- Claim-group rechecks:
  - enabled per-rule
  - hard caps
  - quarantine on spillover
- Root-based scheduling remains the default.

Important framing:
This is a performance optimization, not a correctness upgrade.

Exit condition:
You can turn it off instantly and the system still behaves sanely.

## Phase 4 — Minimal Operator UX (Only If Watched)

Goal: Visibility, not control.

- Inspection UI, not a dashboard:
  - recent decisions
  - recent quarantines
  - rule activity
- No charts unless someone checks them daily.

Exit condition:
The UI answers “what just happened?” in under 10 seconds.

## Explicit Anti-Roadmap

- No large corpora ingestion
- No policy enforcement or moderation
- No ML classifiers
- No LLM-in-the-loop decisions
- No “intelligence” claims

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
