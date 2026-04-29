# Driftwatch — Scope

*An infotoxin observatory. Measuring information half-life and correction resistance in the wild.*

> **For high-level orientation, start at [`docs/architecture/OVERVIEW.md`](../architecture/OVERVIEW.md).**
> This document is the single-page summary: objects, output by stage, non-goals, data posture, queue semantics. The architecture tree expands each section into its own doc; this remains as the at-a-glance overview.

---

## Objects

```
post → fingerprint (claim cluster) → variants (fingerprint_version) → label_decision (receipted)
```

- **Post**: a single ATProto event with text, edges, timestamps.
- **Fingerprint**: stable cluster ID derived from claim signals (quantities, entities, spans). Survives surface-level mutations (whitespace, emoji, URL params, case, hedging). Keyed in `claim_history`.
- **Variant**: same fingerprint, different `fingerprint_version` or `evidence_hash`. Tracks mutation within a cluster.
- **Label decision**: receipted judgment with rule_id, evidence hashes, config hash, decision trace. Append-only ledger.

## Outputs

### Stage 0 — Sealed Lab (current)

Ingest + fingerprint + compute. **No emits. No labeler registration.**

- Fingerprint-level time series (posts, unique authors, confidence, provenance mix per time bin)
- Half-life estimates per claim cluster (peak → decay)
- Burst / synchrony scores (z-score volume, author/post ratio, variant entropy)
- Regime shift candidates (label distribution divergence from rolling baseline)
- Cluster reports: top fingerprints by burst score with half-life, author growth, variant spread

### Stage 1 — Reference Implementation

Reproducible runs on a corpus + report output. **Still no emits.**

- Instant vs confirmed label distinction (confirmed requires persistence thresholds)
- Correction elasticity (requires correction event detection — deferred)
- Generated artifacts: cluster reports, regime shift reports

### Stage 2+ — Deliberate Fork (if ever)

Public labeler registration, live emission, appeals, governance. **Explicit decision, not an accident.**

## Non-Goals (Stage 0–1)

- No public label emission onto the ATProto network
- No per-account scoring, "top offenders" views, or per-DID discourse-weather forecasts (volatility, risk class, behavioral prediction). Weather is aggregate system state; per-poster forecasting is dossier production. If the tables can answer dossier-shaped questions, the API still must not.
- No raw text retention beyond ingest processing
- No ML classifiers or LLM-in-the-loop decisions
- No always-on production daemon (yet)

These non-goals are **fuses, not walls**. If the instrument works well and the governance question gets answered, some may be revisited deliberately. The point is: nothing drifts into institution territory by accident.

## Data Posture

- **Aggregate-first**: cluster curves, label-rate vectors, variant families — not profiles.
- **Hashes over text**: `claim_history` stores fingerprints + evidence hashes, not raw content.
- **Expiry**: TTL anything account-adjacent that isn't needed for unique-author counts.
- **Receipted**: every label decision records why it fired, with what evidence, under what config.

## Queue Semantics

The recheck queue (`recheck_queue` table) is capped at 10k entries and functions as a **rolling hotset**, not a backlog. The queue holds recently-seen claim fingerprints; a DB trigger trims the oldest entries when the cap is exceeded. A singleton gate prevents one-off posts from entering the queue — fingerprints must either appear from multiple authors, contain links, or be part of a reply thread. Queue depth pinned at 10k is normal operation, not a sign of system overload.

## Relationship to Labelwatch

Labelwatch watches **labeler behavior** (are labelers consistent, accountable, governed?).
Driftwatch watches **information drift** (do claims persist, mutate, resist correction, and who amplifies them?).

Same observatory family, different instruments.

---

*Part of the Δt framework research program (Beck 2025–2026).*
