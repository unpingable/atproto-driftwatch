# driftwatch — Architecture Overview

**Status**: v0 starter — expect expansion.
**Last updated**: 2026-04-28

## What this is

driftwatch is an ATProto substrate observatory. It watches how claims propagate across Bluesky — fingerprinting, cluster analysis, drift detection, half-life estimation, burst/synchrony scoring — and maps hosting infrastructure onto labeled populations (identity enrichment, PDS resolution, hosting-locus analysis).

It runs in **sealed-lab mode**: detect-only, no labels emitted to ATProto. Outputs are reports, the facts-bridge sidecar to labelwatch, and detect-only audit records. Public emission is a deliberate Stage 2+ decision, not an accident.

It does not produce per-account scoring, "top offenders" views, poster behavioral profiles, or per-DID discourse-weather forecasts. See `PUBLIC_SURFACES.md` (TODO) and `../driftwatch/SCOPE.md`.

## Five questions

1. **What is this system?** A claims-anchored observatory that fingerprints posts into stable cluster IDs, tracks how clusters propagate and mutate, and computes structural signals (burst, synchrony, half-life, variant entropy, single-author concentration) over the resulting clusters. See `SIGNAL_MODEL.md`.
2. **What are its organs?** Single-process Python service running:
   - Jetstream consumer (WebSocket; posts + reposts; queue-bounded, drop-aware)
   - Fingerprint pipeline (entity/quantity/domain/span/text precedence)
   - Sensor array (cluster analysis, drift detection, platform health, cooldown filter)
   - Maintenance + retention loops (archive-before-delete, observed_at, JSONL+gzip)
   - Facts export sidecar (read-only bridge to labelwatch)
3. **What are the admissible outputs?** Cluster reports, drift detection findings, hosting-locus analysis, facts-bridge sidecar. Detect-only by default; quarantine path for suppressed emits. See `PUBLIC_SURFACES.md` (TODO).
4. **What boundaries are intentional?** Aggregate-first not profile-first; sealed-lab; no per-account scoring; no LLM-in-the-loop; no automatic policy enforcement; "fuses, not walls." See `PUBLIC_SURFACES.md` (TODO).
5. **What failure modes does the architecture already know about?** Drop-aware coverage, degraded-sampling-conditioned outputs, vintage admissibility (cohort/temporal confounds), disk pressure brake, baseline persistence across restarts, WAL bloat. See `FAILURE_MODES.md` (TODO).

## System diagram

```
        ┌────────────────────┐
        │   Jetstream WSS    │
        │  (posts, reposts)  │
        └─────────┬──────────┘
                  │
                  ▼
        ┌────────────────────┐
        │  Consumer + Queue  │
        │  (drop-aware)      │
        └─────────┬──────────┘
                  │
                  ▼
        ┌────────────────────┐
        │ Fingerprint        │
        │ entity/quantity/   │
        │ domain/span/text   │
        └─────────┬──────────┘
                  │
                  ▼
        ┌────────────────────┐
        │  claim_history     │
        │  (append-only,     │
        │   observed_at)     │
        └─────────┬──────────┘
                  │
       ┌──────────┼──────────────────┐
       ▼          ▼                  ▼
   ┌──────┐  ┌─────────┐     ┌──────────────┐
   │Sensor│  │Platform │     │   Maint +    │
   │Array │  │ Health  │     │  Retention   │
   └───┬──┘  └─────────┘     └──────────────┘
       │
       ▼
   ┌──────────────────────────────────┐
   │ Cluster reports (HTML/JSON)      │
   │ Drift detection (longitudinal)   │
   │ Facts export (sidecar →          │
   │   labelwatch via ATTACH)         │
   └──────────────────────────────────┘
```

See `diagrams/` for rendered mermaid versions: [system-overview](diagrams/system-overview.md), [dataflow](diagrams/dataflow.md), [publication-boundary](diagrams/publication-boundary.md), [signal-model](diagrams/signal-model.md).

## Core invariants

- **Observation only** — no content moderation, no user profiling, no enforcement.
- **Aggregate-first** — cluster-level and host-family-level analysis, not per-account dossiers.
- **Append-only events** — `identity_events` and `claim_history` are journals, not mutable state.
- **Fieldwise reducer** — `actor_identity_current` is converged from sparse events; newer events never erase prior known fields. MISSING sentinel distinguishes "absent" from "explicitly cleared."
- **Provenance tracked** — every DID has `identity_source`: `live`, `labelwatch_seed`, or `both`.
- **Receipt hashing** — label decisions include receipt hashes for audit trail.
- **Platform health gating** — rechecks suppressed during degraded ingest (EWMA baseline, coverage watermark).
- **Emit gated** — `LABELER_EMIT_MODE=detect-only` by default. Must explicitly enable.
- **Fingerprint version is a contract** — `FP_VERSION` change = migration; transition periods emit both old and new (audit only, never live).
- **Language may propose; only evidence commits state** — rules propose via drift detection; commits require evidence-backed receipts.

## Populations (always specify denominator)

- **Live-observed** — DIDs seen in Jetstream identity/account events.
- **Labeled-target seed** — DIDs imported from labelwatch's labeled-target population for resolver enrichment.
- **Overlap** — DIDs in both populations.

Coverage claims must specify which population. "0.2% coverage" was a real bug from comparing 40k resolved actors against 2M label events.

## Where to go next

| Question | Doc |
|----------|-----|
| What do the structural signals actually mean? | `SIGNAL_MODEL.md` |
| How does data flow through? | `DATAFLOW.md` (TODO) |
| What's the boundary against poster surveillance? | `PUBLIC_SURFACES.md` (TODO) |
| What does the architecture already know can go wrong? | `FAILURE_MODES.md` (TODO) |
| What's the scope and stage gating? | `../driftwatch/SCOPE.md` |
| Where to find code for what concept? | `../../THEORY_TO_CODE.md` |
| What's deferred / non-runtime? | `../../DESIGN_NOTES.md` |
| Cross-population skew publication discipline | `../../specs/core/ADMISSIBILITY-PROTOCOL.md` |
| Vintage / hosting-locus pitfalls | `../VINTAGE-ADMISSIBILITY.md` |
