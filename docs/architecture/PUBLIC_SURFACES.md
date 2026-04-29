# driftwatch — Public Surfaces

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc names what driftwatch exposes externally, the stage gating that governs it, and the boundary against per-account dossier production. Sealed-lab posture is the default; public emission is a deliberate stage transition, not an accident.

## Surfaces inventory

| Surface | Shape | Reachability | Stage | Key gates |
|---------|-------|--------------|-------|-----------|
| Cluster reports | HTML + JSON | https://driftwatch.sp00ky.net | 0–1 | atomic dir swap; aggregate-only; signature-not-category language |
| Drift detection findings | report rows + JSONL | dashboard + archive | 0–1 | platform health gate; cooldown filter; cluster-level only |
| Hosting-locus analysis | report cards | dashboard | 0–1 | aggregate by host family; population denominators required |
| Facts-bridge sidecar | `facts.sqlite` (read-only ATTACH) | local labelwatch | 0–1 | unidirectional; whitelist of fields (see `../../CLAUDE.md`); 30d retention; 72h overlap window |
| Detect-only audit trail | `label_decisions` table | internal only | 0–1 | not published; admissible as receipt history |
| ATProto label emission | `applyLabels` calls | ATProto | 2+ (gated) | `LABELER_EMIT_MODE=detect-only` by default; explicit confirm step required |

## Stage gating

Driftwatch is staged. Each stage gates which surfaces become live.

### Stage 0 — Sealed Lab (current)

Ingest + fingerprint + compute. **No emits. No labeler registration.** Outputs: cluster reports, drift findings, hosting-locus analysis, facts-bridge.

### Stage 1 — Reference Implementation

Reproducible runs on a corpus + report output. **Still no emits.** Adds: instant-vs-confirmed label distinction, correction elasticity (deferred), generated artifacts.

### Stage 2+ — Deliberate Fork (if ever)

Public labeler registration, live ATProto emission, appeals, governance. **Explicit decision, not an accident.** Currently gated by `LABELER_EMIT_MODE` env var; default is `detect-only`. Flipping the gate requires an explicit confirm step in `emit_mode.py`.

The non-goals listed in `../driftwatch/SCOPE.md` are *fuses, not walls* — they may be revisited deliberately if the instrument works well and the governance question gets answered. They never drift open by accident.

## The aggregate / per-cluster / per-DID distinction

Driftwatch surfaces are categorized:

1. **Aggregate** (cluster-level, host-family-level, population-level) — *default-permitted at all stages*.
2. **Per-cluster** (specific fingerprint cluster reports) — *permitted at all stages*. The cluster is the unit; author lists are collapsed to counts.
3. **Per-DID** — *forbidden as a surface*. Driftwatch's storage tracks DIDs (it has to, to compute author entropy and count distinct authors), but no surface exposes per-DID rollups.

The architectural distinction: a cluster report saying "this cluster had 800 distinct authors" is aggregate. A per-DID lookup saying "this account participated in clusters X, Y, Z" is dossier-shaped, regardless of how truthfully it could be computed.

## The forbidden shape

```
GET /poster/{did}/clusters           # forbidden
GET /poster/{did}/weather            # forbidden
GET /poster/{did}/automation_score   # forbidden
GET /poster/{did}/discourse_profile  # forbidden
```

Or any equivalent per-handle behavioral or participation forecast. Driftwatch has no such endpoint and no plan for one.

## The load-bearing rule

> **If the tables can answer dossier-shaped questions, the API still must not.**

`claim_history` rows know who posted. The fingerprint pipeline must know, to compute distinct-author counts. That does not mean a `claim_history` query joinable on `did` should be exposed. The publishable contract is *not* "everything the storage can compute."

## Sealed lab in detail

Sealed-lab posture means:

- `LABELER_EMIT_MODE=detect-only` (env var, default).
- All decisions land in `label_decisions` (the receipt ledger) regardless of mode.
- No `applyLabels` calls hit ATProto.
- Quarantined emits (suppressed by budget/policy) still record full payload for inspection.
- Reports are generated; they are not pushed to ATProto.

This is *not* a placeholder for inevitable Stage 2. It is the operating mode unless and until governance questions are answered.

## Output language: signatures, not categories

Driftwatch outputs describe signatures, not verdicts:

- "concentration anomaly" not "bot farm"
- "low variant entropy in cluster X" not "copy-paste campaign"
- "synchrony spike" not "coordinated attack"
- "single-author cluster (likely_automation)" not "automated account"

The verdict — "this is automation, this is laundering, this is organic" — sits with whoever reads the report. The architecture is built to *not* render that verdict. See `SIGNAL_MODEL.md` for why.

## Adding a new surface — checklist

Any new surface must answer:

1. **Aggregate, per-cluster, or per-DID?** Per-DID → stop.
2. **Stage 0–1 or Stage 2+?** Stage 2+ requires the explicit emit-mode confirm step.
3. **What's the receipt?** Surfaces without receipts can't be verified.
4. **What population is it computed over?** Live-observed, labeled-seed, or overlap. Coverage must specify.
5. **What language does it use?** Signatures, not categories.
6. **What's the doc?** Update the inventory table above.

## Facts-bridge contract

The facts-bridge sidecar (`facts.sqlite`) is the one cross-system surface. It is unidirectional (driftwatch → labelwatch), read-only on the labelwatch side, and exposes only a whitelisted subset:

- `uri_fingerprint` — post URI → fingerprint dedup
- `fingerprint_hourly` — hourly aggregation (72h overlap window)
- `fingerprint_bounds` — time bounds per fingerprint
- `actor_identity_facts` — thin resolved-host projection: `did, handle, pds_endpoint, pds_host, resolver_status, resolver_last_success_at, is_active, identity_source`

Schema changes to this sidecar are **data contract changes**. They cross a project boundary; treat them accordingly.

## Cross-reference

- `../driftwatch/SCOPE.md` — stage descriptions, non-goals, queue semantics, relationship to labelwatch.
- `OVERVIEW.md` — system context.
- `SIGNAL_MODEL.md` — what the structural signals can and can't decide.
- `../../specs/core/ADMISSIBILITY-PROTOCOL.md` — discipline for taking a published claim from "interesting" to "admissible or retired."
- `../VINTAGE-ADMISSIBILITY.md` — vintage / hosting-locus pitfalls.
- `../../CLAUDE.md` — facts-bridge schema reference.
