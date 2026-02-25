# Design Notes (Governance Extensions, Non-Operational)

This document captures optional extensions that are **explicitly out of the MVP runtime path**.
They are recorded here to keep future changes coherent and reversible.

## 1. Authority Delegation (Multi-Operator Ledger)

Goal: allow multiple operators to propose while requiring quorum to commit.

Proposed schema extensions (append-only, no runtime use yet):
- `label_decisions.signer_id` (operator identity)
- `label_decisions.decision_group_id` (UUID grouping a quorum set)
- `label_decisions.commit_state` (`proposed` | `quorum_met` | `committed` | `rejected`)
- `label_decisions.commit_rule` (e.g., `quorum:2-of-3`)

Behavioral intent:
- Operators may create proposals.
- Commit occurs only when quorum is met.
- Ledger remains the authoritative record; runtime logic must remain deterministic.

## 2. Strain Detection (Observability-Only)

Goal: detect systemic stress without semantic inference.

Signals (non-authoritative):
- Rule spike: per-rule activation rate > N× baseline
- Queue pressure: `recheck_requests` growth > threshold
- Quarantine frequency: repeated quarantine within rolling window
- Claim-group churn: repeated claim-group rechecks for same fingerprint

Response (non-binding):
- Emit **warnings only**. Do not auto-adjust thresholds.
- Operators decide whether to reduce emit or pause.

## 3. Label Composition (Receipt-First)

Intent:
- If multiple rules fire, **each** produces a decision receipt.
- Emission policy may choose a subset, but receipts are complete.

## 4. Temporal Coherence (Recheck Scheduling Only)

Intent:
- Staleness should enqueue a recheck, not mutate labels directly.
- Possible triggers: evidence hash change, claim-group growth, or time-based TTL.

## 5. Fingerprinting and Migration Policy

- Fingerprint version is a contract: `FP_VERSION` is a constant and must change only for intentional semantic changes.
- When fingerprint behavior changes, bump `FP_VERSION` and document the rationale.
- Migration rule: if you need a transition period, emit both old and new fingerprints for audit only, never for live labels.
- Configuration safety: a hash of relevant knobs is recorded in the label decision ledger (`label_decisions.config_hash`).

## Non-Goals
- No LLM-in-the-loop decision making.
- No automatic policy enforcement.
