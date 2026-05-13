# GAP: Formal Claim-Admissibility Gate for Labelwatch / Driftwatch

> Status: candidate / proposed architecture.
> Scope: Labelwatch / Driftwatch pipeline, reference atproto-labeler backport path.
> No hot-path solver/prover execution. No moderation-truth oracle. No automatic claim promotion.
> This GAP names where formal admissibility tooling (**Z3 first, Lean later**) belongs in the pipeline and where it must not be allowed to spread.

## Summary

Labelwatch and Driftwatch produce observations, features, findings and public-facing claims. The operational failure mode is not merely producing wrong data. It is allowing a lower-rung artifact to be consumed as a higher-rung claim.

The gate splits into two layers with different tools doing different work:

- **Z3 (Phase 1, near-term)** — finite SMT check over evidence flags, freshness windows, detector outputs, and the claim/forbidden-claim matrix. Answers *"is there any satisfying assignment where this forbidden claim emits?"* Returns counterexamples when the policy is wrong. Operationally cheap; CI-only.
- **Lean (Phase 2, later)** — durable doctrine kernel: claim-ladder types, impossible-transition theorems, algebra of admissibility. Encodes WHY the ladder exists, not just which configs pass. Companion formal repo.

Neither runs on the hot path. Runtime consumes generated, checked-in policy tables and emits admissibility receipts.

The gate should not prove that a labeler is malicious, that a user is a bot, that intent exists, or that a moderation judgment is substantively correct. It should prove narrower boundary properties such as:

* this evidence type licenses this claim kind
* this evidence type does **not** license that claim kind
* forbidden claims cannot be emitted from insufficient observations
* claim projections preserve the distinction between observation, feature, finding and claim
* public artifacts carry proof/denial receipts about admissibility status

Keeper lines:

> Z3 checks the policy table. Lean protects the ontology.

> Use Z3 to catch bad claim configurations. Use Lean to prove why the ladder exists.

Corollary:

> Detection is empirical. Interpretation is statistical. Claim admissibility is theorem-shaped — but the first useful enforcement is SMT-shaped.

## Motivation

The Labelwatch / Driftwatch work has repeatedly surfaced the same boundary problem:

* raw observations are mistaken for features
* features are mistaken for findings
* findings are mistaken for public claims
* advisory signals are mistaken for determinations
* missing or stale evidence is collapsed into negative claims
* health metrics are treated as globally green despite scoped failure buckets

Examples from the current operating history:

* `rollback_lost=0` proved writer rollback safety, not clean intake.
* bounded WAL proved checkpoint behavior, not disk runway safety.
* retention progress proved logical lifecycle movement, not physical disk reclamation.
* labeler silence can indicate upstream quiet, service failure, collector failure, or metric artifact.
* raw PDS distribution does not license current hosting-locus claims without admissible vintage/control evidence.

These are not merely code bugs. They are claim-boundary failures.

Formal tooling is useful here because the boundary can be made explicit: a given evidence family either can or cannot license a given claim family. Z3 is enough to enforce the boundary as a finite policy check; Lean adds durable doctrine over time.

## Pipeline Ladder

The pipeline should preserve the following ladder:

```text
Observation → Feature → Finding → Claim
```

### Observation

A directly collected or witnessed fact.

Examples:

* label event observed from a labeler endpoint
* Jetstream event received
* DID document advertises a service endpoint
* health endpoint returned a counter
* SQLite sample recorded page_count/freelist_count

Observation is not interpretation.

### Feature

A derived measurement from observations.

Examples:

* 7d/30d label ratio
* median JSD
* author/post ratio
* WAL size trend
* queue-boundary drop rate
* partition row count

Feature is not finding.

### Finding

A structured result that interprets features under a declared detector standard.

Examples:

* `gone_dark`
* `degrading`
* `likely_automation`
* `coverage_low`
* `retention_throttled_progress`
* `wal_frontier_pinned`

Finding is not public claim.

### Claim

A statement intended for publication, downstream consumption, operator action or cross-tool routing.

Examples:

* "this labeler is silent"
* "this labeler is operationally degraded"
* "this evidence supports a reviewable coordination finding"
* "this incident is contained, not closed"
* "current data does not license a hosting-locus claim"

Claim requires admissibility.

## Problem

The system currently has strong empirical tooling and growing operational instrumentation, but it lacks a formal boundary that prevents a finding from being projected into an over-strong claim.

Without such a boundary, downstream artifacts can accidentally launder authority:

```text
raw count → metric → detector result → public statement → operator belief
```

The failure is subtle because every individual transformation may look reasonable. The overclaim appears only at the projection boundary.

This GAP proposes that Lean govern that projection boundary.

## Non-Goals

This GAP does not propose:

* running Lean in the hot ingest path
* proving detector correctness end-to-end
* proving moderation judgments are true
* proving human intent
* proving bot identity from behavioral features
* proving centralized command from synchronized activity alone
* proving current hosting locus from raw PDS distribution alone
* replacing statistical validation, sampling, testing or operational receipts
* making Lean a runtime dependency for labeler availability
* blocking ingestion if proof infrastructure is unavailable

Neither Z3 nor Lean makes empirical uncertainty disappear. They prevent uncertainty from being projected into a stronger claim category than the evidence licenses.

## Required Boundary

The formal layer (Z3 in Phase 1, Lean in Phase 2) should bind the following transition:

```text
Finding + EvidenceKind + ClaimKind → AdmissibilityVerdict
```

Suggested verdict space:

```text
admissible_claim
advisory_only
inadmissible_claim
requires_more_evidence
out_of_scope
```

Minimal version:

```text
allowed
advisory_only
denied
```

The exact names can change. The important property is that a public claim cannot be emitted without an admissibility verdict attached.

## Example Claim Families

### Licensed / potentially admissible claims

Examples of claims that may be admissible from Labelwatch / Driftwatch evidence, depending on detector receipts:

* labeler emitted no observed labels during a declared window
* labeler endpoint returned a specific HTTP status during probe window
* 7d/30d activity ratio crossed a declared threshold
* label pair surfaced in a conflict/co-occurrence report
* structural coordination signal is reviewable under declared thresholds
* retention pass made bounded partial progress
* WAL truncation was skipped under pressure according to configured rule

### Forbidden or high-risk claims

Claims that should be theorem-denied from the current evidence families unless a stronger evidence family is introduced:

* actor intent
* bot identity
* centralized command
* definitive coordinated inauthentic behavior
* current physical hosting locus from raw PDS distribution alone
* moderation correctness from label presence alone
* user/account moral status from labeler behavior
* service death from silence alone
* global platform health from one green subsystem metric

Keeper line:

> Behavioral features do not compose into intent claims by quantity alone.

## Solver Layer

The formal layer has two tools doing different work. Build Phase 1 (Z3) first; Phase 2 (Lean) comes when claim vocabulary hardens.

### Phase 1 — Z3 policy checker (near-term)

The genuinely-new framing of this layer:

> Z3 is not checking whether the finding is true. It is checking whether the finding is allowed to become that claim.

This is an inversion of the usual SMT-for-verification move. Most verification asks "given system S, can bad state B happen?" This asks "given evidence E, can overclaim C be emitted?" Small but nasty inversion, and the right shape for the claim-boundary problem.

Z3 is the right fit because the immediate problem is finite: given a fixed set of evidence flags, freshness/coverage windows, detector outputs and a forbidden-claim list, can a forbidden claim be emitted under any satisfying assignment?

Encode the admissibility policy as SMT constraints over boolean / bounded-integer variables:

```text
evidence_kind         (enum)
claim_kind            (enum)
freshness_secs        (int)
coverage_pct          (int 0..100)
threshold_hit         (bool)
source_count          (int)
vintage_control_present (bool)
endpoint_probe_present  (bool)
operator_receipt_present (bool)
rollback_lost           (int)
events_dropped          (int)
```

Then for every forbidden claim, prove `unsat`:

```text
Can "service_dead" be emitted when:
  label_count_7d   = 0
  endpoint_probe_200 = true
  did_service_present = true
?
→ Z3: unsat   (no model exists; forbidden claim cannot license under this evidence)

Can "ingest_clean" be emitted when:
  rollback_lost = 0
  events_dropped > 0
?
→ Z3: unsat
```

When a forbidden claim *is* satisfiable, Z3 returns a model — the counterexample. That counterexample is the bug in the policy. This is the gold path: the failure mode is *informative*, not opaque.

Z3 outputs feed a generated, checked-in policy table that runtime consumes (see Runtime Behavior below).

CI integration:

* a Python script enumerates each forbidden claim and asks Z3 for `unsat`
* CI fails if any forbidden claim becomes satisfiable
* CI also verifies the generated policy table matches the SMT model (no drift between solver output and runtime table)

#### Starter vocabulary (Labelwatch v1)

Enough vocabulary already exists to begin Phase 1 with a deliberately tiny matrix. Do not wait for perfect vocabulary — Z3 is how we discover whether the vocabulary is good enough.

Evidence / signals:

* observed labels in window
* 7d/30d activity ratio
* endpoint probe result
* DID advertises labeler service
* queryLabels auth/result state
* collector health
* drop / coverage / read health

Findings:

* `gone_dark`
* `degrading`
* `active`
* `no_data`
* query failure / auth failure
* collector degraded

Forbidden / downgraded claims (must be `unsat` under current evidence families):

* service dead
* labeler abandoned
* moderation judgment invalid
* upstream outage
* ingest clean
* platform healthy

#### Starter policy checks

Four worked assertions that should be `unsat` in the first Z3 spec. Each one is a concrete bug pattern we've already observed or narrowly avoided:

```text
gone_dark + endpoint_200       → cannot emit service_dead
rollback_lost=0 + events_dropped>0 → cannot emit ingest_clean
7d/30d_low + known_flaky_labeler   → advisory only, not critical determination
collector_no_data              → cannot infer upstream silence
```

Fixtures should use real cases: Hailey silence (gone_dark + endpoint 200 + DID service present), the 2026-05-12 producer-gating deploy storm (rollback_lost=0 + dropped=11677), and collector-health degradation patterns.

#### Implementation outline

Phase 1 is a small CI artifact, not a daemon. No runtime, no sidecar, no MCP plumbing yet — just CI that refuses overclaiming fixtures.

Suggested layout:

```text
labelwatch/z3/
  claim_policy.py            # Z3 spec + forbidden-claim assertions
  test_claim_admissibility.py # pytest harness running Z3 against each fixture
  cases/
    hailey_silence.json
    queue_drop_vs_rollback.json
    collector_no_data.json
    endpoint_alive_but_quiet.json
```

First acceptance, narrow and concrete:

* Z3 denies `service_dead` from silence alone (Hailey fixture).
* Z3 denies `ingest_clean` when queue drops occurred (deploy-storm fixture).
* Z3 denies `upstream_silent` when collector health is `NO_DATA`.
* Z3 allows `labeler_quiet_in_window` from observed zero-label window.
* Z3 allows `advisory_degraded` from low 7d/30d ratio with scoped wording.

That's enough to prove the pattern. Expand the matrix later when this earns its keep.

### Phase 2 — Lean doctrine kernel (later)

Lean is for the higher-altitude invariants: the algebra of the ladder, the impossibility of certain transitions, durable doctrine independent of any particular detector vocabulary.

A first Lean kernel should be intentionally small.

Illustrative types:

```lean
inductive EvidenceKind
  | observedLabelEvent
  | endpointProbe
  | didServiceRecord
  | activityWindowRatio
  | distributionFeature
  | synchronyFeature
  | vintageControlledDistribution
  | operatorReceipt
  | healthCounter

inductive ClaimKind
  | labelerSilentInWindow
  | endpointUnavailable
  | activityDegraded
  | reviewableCoordinationSignal
  | currentHostingLocus
  | actorIntent
  | botIdentity
  | centralizedCommand
  | globalPlatformHealthy

inductive Verdict
  | admissible
  | advisoryOnly
  | denied
  | requiresMoreEvidence
```

Then define an admissibility relation:

```lean
Admits : EvidenceKind → ClaimKind → Verdict
```

Theorems should include denial lemmas for known overclaims:

```lean
activity_ratio_does_not_admit_intent
synchrony_does_not_admit_centralized_command
raw_pds_distribution_does_not_admit_current_hosting_locus
health_counter_does_not_admit_global_platform_health
silence_alone_does_not_admit_service_death
rollback_clean_does_not_admit_ingest_clean
```

Positive lemmas should remain narrow:

```lean
endpoint_probe_admits_endpoint_probe_claim
activity_ratio_admits_activity_degraded_advisory
vintage_controlled_distribution_may_admit_hosting_locus_advisory
operator_receipt_admits_operational_state_claim
```

The relationship between the two layers:

```text
Lean  ─ defines the type boundary, proves no transition exists between categories
        without the right constructor/basis. Encodes WHY.

Z3    ─ given Lean's category structure, checks concrete claim/evidence
        policies for satisfiability of forbidden configurations. Encodes WHICH.

Runtime ─ consumes checked-in tables/receipts derived from both. Doesn't call
          either solver/prover.
```

Z3 catches policy bugs fast. Lean prevents the ontology from drifting underneath the policy.

## Integration Points

### 1. CI gate

The initial integration is CI-only. Neither Z3 nor Lean runs on the hot path.

Phase 1 (Z3, near-term):

* Z3 policy spec lives in `specs/admissibility/` or equivalent.
* CI script enumerates forbidden claims, calls Z3, fails build if any forbidden claim is satisfiable.
* CI emits/verifies the generated policy table that runtime consumes.
* No runtime z3 dependency.

Phase 2 (Lean, later):

* Lean files live in a companion proofs repo, referenced by commit/hash.
* CI verifies the kernel builds.
* CI verifies that Lean's category structure agrees with the Z3 policy enumeration (no claim kind in Z3 unknown to Lean, no Lean denial-lemma without a Z3 forbidden-claim check).

### 2. Claim projection tests

Before a public artifact is emitted, tests should assert that each claim projection maps to a policy-table-governed claim kind.

Example:

```text
finding: labeler_gone_dark
claim: labeler emitted no observed labels during 7d window
verdict: admissible or advisory_only
```

But:

```text
finding: labeler_gone_dark
claim: labeler service is dead
verdict: denied unless endpoint probes license a stronger claim
```

### 3. Admissibility receipts in artifacts

Public or downstream artifacts should include a lightweight receipt:

```json
{
  "claim_kind": "activityDegraded",
  "evidence_kind": "activityWindowRatio",
  "policy_table": "labelwatch-admissibility@<commit>",
  "kernel": "LabelwatchClaimAdmissibility@<commit-or-null>",
  "verdict": "advisory_only"
}
```

`policy_table` is always present (Z3-derived). `kernel` is populated once Phase 2 lands; null/absent until then.

This is not a full proof object. It is a traceable boundary receipt.

### 4. Documentation cross-reference

Docs that describe findings should identify:

* detector threshold
* evidence family
* permitted claim family
* forbidden stronger claims
* Z3 forbidden-claim assertion name (Phase 1) and Lean theorem/lemma name if applicable (Phase 2)

## Placement in Labelwatch / Driftwatch

Potential file locations:

```text
specs/gaps/gap-spec-formal-claim-admissibility-pipeline.md   (this spec)
specs/admissibility/forbidden_claims.smt2                    (Phase 1 Z3 source)
specs/admissibility/policy_table.json                        (CI-generated, checked in)
docs/architecture/CLAIM_LADDER.md
docs/architecture/ADMISSIBLE_CLAIMS.md
```

Lean artifacts (Phase 2) may live outside the repo, but the consuming repo must reference them explicitly:

```text
companion proofs repo: ~/git/lean/LeanProofs/...
claim kernel module: LabelwatchClaimAdmissibility.lean
```

If proofs live in a companion repo, the Labelwatch / Driftwatch repo must pin:

* module name
* commit hash or tag
* expected theorem names
* last verified date

A hidden companion proof is not a release artifact.

## Runtime Behavior

Runtime invokes neither Z3 nor Lean.

Runtime consumes the CI-generated, checked-in policy table:

```json
{
  "labeler_gone_dark": {
    "allowed_claims": ["labelerSilentInWindow"],
    "advisory_claims": ["activityDegraded"],
    "denied_claims": ["endpointUnavailable", "serviceDead"]
  }
}
```

The mapping is checked in and verified by CI against the Z3 spec on every build.

This keeps runtime boring and proof-boundaries inspectable.

Keeper:

> Z3 catches the bad config in CI. Lean protects the ontology over time. Runtime stays a lookup.

## Acceptance Criteria

This GAP is closed in two phases.

**Phase 1 closed when (Z3):**

1. A Z3 policy spec defines evidence flags, freshness/coverage variables and claim kinds.
2. At least five forbidden-claim assertions exist, each provable `unsat` against the spec.
3. At least three positive policies exist for narrow permitted claims.
4. CI fails if any forbidden claim becomes satisfiable, with the Z3 counterexample surfaced in the failure.
5. A generated, checked-in policy table is consumed by claim-projection tests in Labelwatch / Driftwatch.
6. Public artifacts include an admissibility receipt with `policy_table` commit ref.
7. A finding cannot be projected into a forbidden claim without a failing test or CI break.
8. Docs distinguish observations, features, findings and claims.

**Phase 2 closed when (Lean):**

9. A small Lean module defines evidence kinds, claim kinds and admissibility verdicts.
10. The module builds in CI.
11. Denial lemmas mirror the Phase-1 forbidden-claim assertions (no Z3 forbidden-claim without a Lean impossibility theorem).
12. Companion proof repo/path is named explicitly in release documentation.
13. Receipts populate the `kernel` field with the Lean commit ref.

## Stabilization Gates: when Phase 2 (Lean) earns its keep

Stabilization is not a vibe, but it is not a single metric either. It is when the vocabulary survives contact with emitted claims, Z3 counterexamples and consumer usage without changing shape.

Phase 2 (Lean) should begin only when all six of the following hold:

1. **Emitted claim inventory exists.** Every public/report claim type has been listed.
2. **Claim/evidence matrix survives use.** Z3 checks run in CI and catch no surprising holes for a few iterations.
3. **Renames slow down.** `FindingKind` / `ClaimKind` / `EvidenceKind` are not being renamed every incident.
4. **Counterexamples stop teaching ontology.** Z3 may still catch mistakes, but it no longer reveals new categories.
5. **Consumers stop forcing reinterpretation.** Labelwatch UI / reports / downstream tools are not treating claims as stronger than intended.
6. **Lean would add structure, not patch policy.** The remaining thing worth proving is categorical (Observation ≠ Feature ≠ Finding ≠ Claim), not "this flag combination is forbidden."

Until all six hold, Lean would be tracking a moving target. Keep using Z3.

## Trigger Conditions for Ratification

Ratify this GAP when any of the following happen:

1. Labelwatch / Driftwatch emits public reports with claim-bearing language beyond raw counters.
2. A detector result is used by another tool as evidence for action.
3. A downstream consumer treats a finding as a stronger claim than intended.
4. A postmortem identifies an overclaim caused by artifact-kind collapse.
5. The reference atproto-labeler backport begins exporting generic findings for other operators.
6. Claim families stabilize enough that a small Lean kernel can remain stable for more than one release (Phase 2 trigger; Phase 1 should start earlier).

## Relationship to Existing Doctrine

### Workflow routing boundary

Workflow routing says claims route by type to competent tools.

This GAP says Labelwatch / Driftwatch must prove the claim type is admissible before routing or publishing it.

### Solution-family exhaustion

Solution-family exhaustion detects when repeated fixes indict a premise.

This GAP prevents the exhaustion finding from being emitted as stronger architecture claims without admissibility status.

### Thin-slice ratchet

Thin-slice ratchet identifies under-action caused by conservatism amplification.

This GAP prevents the pattern name from becoming authority. The pattern may be advisory; it does not by itself authorize action.

### Coverage honesty

Coverage honesty requires all loss buckets and scope boundaries to be named.

This GAP turns that into claim discipline: a green metric can only support the scoped claim it actually measures.

### Verifier (MCP sidecar, future — out of scope here)

Labelwatch's Z3 gate is **not** the Verifier sidecar. It is the training ground for Verifier-shaped thinking on a bounded surface:

```text
Labelwatch Z3 gate (this spec):
  local claim-admissibility smoke alarm
  finite evidence/claim matrix
  CI-level guard against overclaiming
  teaches integration patterns

Verifier sidecar (separate, future):
  constellation-level admissibility checker
  talks to NQ / Nightshift / Governor
  larger ontology
  higher consequence radius
  probably needs richer proof/SMT boundary
```

> Labelwatch is the smoke alarm. Verifier is the fire suppression system.
>
> Smoke alarms are allowed to be annoying. Fire suppression is not allowed to improvise.

This spec proposes only the smoke alarm. Verifier is a separate (future) GAP. The deliberate posture: prove the inversion pattern works on a small surface here, before any constellation-level system inherits it.

## Worked Examples

### Example 1: Hailey silence

Evidence:

* no observed labels in 7d window
* endpoint probe returns HTTP 200
* DID advertises labeler service

Admissible claim:

> hailey.at emitted no observed labels during the measured window.

Advisory claim:

> hailey.at is quiet relative to its prior activity window.

Denied claim:

> hailey.at service is dead.

Reason:

Endpoint and DID evidence contradict a service-death claim. Silence alone does not license service death.

### Example 2: Queue-boundary drops

Evidence:

* `rollback_lost=0`
* `events_dropped_total` increased
* `drop_frac > 0`

Admissible claim:

> writer rollback integrity was preserved.

Denied claim:

> ingest was clean.

Reason:

Rollback safety does not imply intake completeness.

### Example 3: PDS distribution

Evidence:

* observed PDS distribution
* no vintage control
* no current hosting testimony

Admissible claim:

> observed distribution across reported PDS values.

Denied claim:

> current hosting locus.

Reason:

Raw distribution without vintage/control evidence does not license current hosting-locus claims.

### Example 4: Retention progress

Evidence:

* Parquet partition written
* retention pass aborted after budget
* raw_strip/prune skipped or partial

Admissible claim:

> cold-path capture progressed.

Denied claim:

> retention completed.

Reason:

Archive success and full retention completion are distinct claims.

## Risks

### Cathedral-for-a-garden-shed

The emotional calibration: don't make this feel like building formal methods for the labeler nobody reads. Make it feel like adding a smoke alarm to the shed because the shed keeps producing public claims about fire.

Mitigation:

Z3 first, Lean only when the vocabulary stabilizes. If Phase 1 alone catches the bugs, Phase 2 is constitution rather than scaffolding.

### Proof theater

Either layer can become decorative if it proves toy properties unrelated to emitted claims.

Mitigation:

Every Z3 forbidden-claim assertion must correspond to a real claim projection we actually produce. Every Lean theorem must bind to a Z3 assertion. No orphan formality.

### Over-formalization

The system may try to encode detector statistics in Z3 or Lean.

Mitigation:

Keep empirical thresholds outside both. The solver/prover governs claim admissibility, not statistical truth. Threshold values are detector config; whether the *threshold-hit signal* licenses a claim kind is policy.

### Runtime fragility

Putting either solver/prover in runtime would make formal tooling part of labeler availability.

Mitigation:

CI-only. Runtime consumes the checked-in policy table and receipts.

### Stale policy / kernel drift

Code may drift from the Z3 spec or the Lean kernel.

Mitigation:

CI verifies (1) the generated policy table matches the Z3 model, (2) once Phase 2 lands, every Z3 forbidden-claim has a Lean denial lemma, and (3) artifact receipts include both `policy_table` and `kernel` commit refs.

### Claim-kind explosion

Too many claim kinds make either layer unusable.

Mitigation:

Start with the forbidden overclaims we've already observed (service-dead, ingest-clean, current-hosting-locus-from-raw-distribution, intent-from-behavior, global-health-from-one-counter) and the small set of public claims that already exist. Grow on demand.

## Open Questions

1. Does the Phase 1 Z3 spec live inside the labelwatch/driftwatch repo (`specs/admissibility/`) or in a shared formal companion?
2. SMT-LIB vs Python z3-py for the spec source? (SMT-LIB is portable; z3-py is more legible in this codebase.)
3. What is the minimal claim-kind vocabulary for Labelwatch v1?
4. Should public artifacts expose admissibility verdicts directly, or only include internal receipts?
5. Should the reference atproto-labeler consume the same policy table, or a reduced subset?
6. How should evidence freshness and TTL interact with claim admissibility (Z3 variable vs. runtime check)?
7. Should `advisory_only` be a Z3 verdict (third state alongside admissible/denied) or a downstream policy layer?
8. (Phase 2) Does the first Lean module live in the existing `LeanProofs` repo or inside the labelwatch/driftwatch repo?

## Keeper Lines

> Z3 checks the policy table. Lean protects the ontology.

> Z3 is for shaking the claim table until the bad claims fall out. Lean is for proving why the table has legs.

> Use Z3 to catch bad claim configurations. Use Lean to prove why the ladder exists.

> Use Z3 now. Leave Lean as the constitution, not the bouncer.

> Z3 catches the bad config in CI. Lean protects the ontology over time. Runtime stays a lookup.

> Don't build the cathedral. Build the smoke alarm.

> The first solver job is not correctness. It is admissibility.

> A detector may be right and still not license the claim you want to emit.

> Labelwatch is the smoke alarm. Verifier is the fire suppression system. Smoke alarms are allowed to be annoying; fire suppression is not allowed to improvise.

> Detection is empirical. Interpretation is statistical. Claim admissibility is theorem-shaped — but the first useful enforcement is SMT-shaped.

> Observation is not feature. Feature is not finding. Finding is not claim.

> Behavioral features do not compose into intent claims by quantity alone.

> A green metric licenses only the scoped claim it actually measures.

> Rollback-clean is not ingest-clean.

> Guardrails for not accidentally turning counters into accusations.

> Lean gates the shape of claims; it does not sit in the ingest loop.

---

## Backport addendum — reference atproto-labeler

**Status: DO NOT BACKPORT (the formal machinery itself — neither Z3 nor Lean).**

Do not port the Z3 policy checker or the Lean doctrine kernel into the generic/reference labeler yet.

Port the doctrine first:

* Observation is not feature.
* Feature is not finding.
* Finding is not claim.
* A green metric licenses only the scoped claim it actually measures.
* Behavioral features do not compose into intent claims by quantity alone.
* Claim projection must name what evidence licenses the emitted claim.
* Rollback-clean is not ingest-clean.

Do not:

* add Z3 or Lean as runtime dependencies
* add Z3 CI to the reference implementation until at least one downstream operator consumes admissibility receipts
* add Lean CI to the reference implementation until claim families stabilize
* make the reference labeler depend on the companion proofs repo for ordinary operation
* expose admissibility receipts in generic output until the policy table is real and tested

Acceptable near-term backport:

* `docs/architecture/CLAIM_LADDER.md`
* `docs/architecture/ADMISSIBLE_CLAIMS.md`
* tests that prevent obvious overclaims in emitted reports
* explicit forbidden-claim language in docs
* optional placeholder field names for future admissibility receipts, but not active schema unless needed

Formal integration belongs first in labelwatch/driftwatch as a candidate claim-admissibility gate, because that is where the actual public/reporting claims and failure history exist. Phase 1 (Z3) earns its keep on the smoke-alarm framing: the shed keeps producing public claims, the alarm catches the ones that license themselves wrongly.

Reference labeler should receive the reduced operating doctrine, not the formal apparatus, until:

* emitted claim families stabilize in the source project
* the Z3 policy table has proven useful (caught at least one real overclaim, ideally pre-publish)
* the reference implementation has consumers who benefit from admissibility receipts
* (later) a first Lean kernel exists and has remained stable across a release
