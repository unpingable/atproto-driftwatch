# Comparison-Basis Binding for Claim Drift

**Status**: candidate. Non-authorizing. Nothing here changes production behavior.
**Date**: 2026-07-26
**Scope**: the claim-state comparison path only — `claim_history` → `rule_assertiveness_increase` → `label_decisions`.

## The job

Driftwatch's claims-anchored design (`SIGNAL_MODEL.md`) rests on knowing *that two posts say the same thing*. Once two posts are in the same cluster, the next question is *what changed between them*, and that question is answered by a comparison. This note is about the comparison: what it is computed from, what identifies the things compared, and what the resulting delta is allowed to be called downstream.

`PUBLIC_SURFACES.md` already states the principle in the subject dimension:

> The publishable contract is *not* "everything the storage can compute."

That discipline is enforced for *who* a signal is about (per-DID surfaces are forbidden even though storage could compute them). It is not yet enforced for *what a delta was computed against*. This note proposes the same discipline in the basis dimension.

---

## 1. Current implementation inventory

Verified against the tree at commit `3e2a490`.

### Storage

| Table | Written by | Relevant columns |
|---|---|---|
| `events` | `db.insert_event_txn` (`db.py:341`) | `event_uri` (PK), `ctime`, `raw`. `raw` and `ctime` are **overwritten in place** on edit (`db.py:386`). |
| `event_versions` | `db.py:382` | pre-edit `raw` snapshot, `version_ts`. |
| `claim_history` | `claims.add_claim_history_txn` (`claims.py:416`) | `authorDid`, `claim_fingerprint`, `createdAt`, `evidence_hash`, `post_uri`, `post_cid`, `fingerprint_version`, `evidence_class`, `fp_kind`, `observed_at`. Append-only. |
| `label_decisions` | `db.insert_label_decision` (`db.py:716`) | `decision_id`, `subject_uri`, `label`, `rule_id`, `fingerprint_version`, `inputs_json`, `evidence_hashes_json`, `decision_trace`, `config_hash`, `status`. |

### The comparison path

```
claim_history row(s)                       claims.get_claim_history        claims.py:438
  → prior selection by declared timestamp   rule_assertiveness_increase     rules.py:122-126
  → prior raw fetched by event_uri          rules.py:129
  → both endpoints scored                   claims.compute_claim_state_from_post   claims.py:465
  → three deltas                            claims.compare_claim_states     claims.py:487
  → threshold test                          rules.py:144
  → LabelRecord                             rules.py:145
  → decision receipt                        longitudinal.py:267-303 → db.py:716
  → HTTP                                    main.py:341 /recent-decisions, main.py:332 /labels/{subject_uri}
```

### The coarse view

`compute_claim_state_from_post` (`claims.py:465`) reduces a post to three fields:

- `confidence` — `assertiveness_score` (`drift/diff.py:5`), which is `min(1.0, len(claim.modal) / 3.0)`: a **count** of regex hits.
- `evidence_hash` — re-derived from signals at comparison time via `evidence_hash_from_signals`, not the `evidence_hash` stored on the `claim_history` row (which was derived by `evidence_hash_from_raw`).
- `attribution_present` — membership test against `ATTRIBUTION_TOKENS` (`claims.py:462`).

`MODAL_RE` (`drift/extract.py:9`) is:

```python
r"\b(definitely|confirmed|proved|certainly|sure|guaranteed|reported|reportedly|according to)\b"
```

Certainty markers and attribution markers are in the same alternation and are counted the same way.

### What is recorded and not consumed

| Recorded | Where | Consumed by the comparison? |
|---|---|---|
| `claim_history.post_cid` | `claims.py:423`, returned at `claims.py:454` | **No.** Prior content is fetched by `event_uri` (`rules.py:129`). Only readers are retention/parquet column lists. |
| `claim_history.fingerprint_version` | `claims.py:423`, returned at `claims.py:455` | **No.** Never compared between the two endpoints. |
| `claim_history.evidence_hash` | `claims.py:423` | **No.** Both endpoints re-derive a different hash at comparison time. |
| `event_versions.raw` | `db.py:382` | **No.** Grep across `src/` finds writes in `db.py` and prunes in `retention.py`; no analysis path reads it. |
| `compare_claim_states` → `attribution_removed` | `claims.py:492` | **No.** `rules.py:144` reads only `confidence_delta` and `evidence_changed`. |
| Jetstream `time_us` | `consumer.py:105` | **Discarded.** Used only as a fallback when the record omits `createdAt` (`consumer.py:138`). No witnessed observation time is stored for post events. |

### What the ordering key actually is

`consumer._jetstream_to_event` sets `"createdAt": record.get("createdAt", ctime)` (`consumer.py:138`) — the **author-declared** record timestamp. That value flows to `events.ctime`, to `claim_history.createdAt`, and to `Post.createdAt` (`longitudinal.py:55`, and the same line in the other two post loaders at `:96` and `:130`). Prior selection (`rules.py:123`) orders on it.

So claim-drift ordering is entirely on subject-declared time. The one witnessed clock available at ingest — the relay's `time_us` — is not retained for posts.

---

## 2. Confirmed problem statement

Three findings, at three different rungs. All were verified by execution, not by reading.

**(a) The view cannot separate the property the label names.** `assertiveness_increase_possible` claims a direction of change in assertiveness. The view is a count of modal-regex hits in which "reportedly" and "confirmed" are interchangeable. Two texts that differ in exactly the target property score identically. This is the strongest finding: it is not a coarseness that costs recall, it is a view under which the target property is not a function of the observation.

**(b) Basis information is discarded, not merely unexposed.** `post_cid` identifies the exact record version witnessed; the fetch ignores it and keys on the mutable `event_uri`, whose `raw` is overwritten on edit. `event_versions` retains the true prior content and is read by nothing. The relay's observation time is dropped at normalization. These are lost coordinates, not unsurfaced ones.

**(c) The receipt records one endpoint of a two-endpoint computation.** `label_decisions.inputs_json` is `_decision_inputs_for_post(p.text)` (`longitudinal.py:267`, and `:373` on the claim-group path) — signals of the *current* post only. The prior appears only as a URI string inside `decision_trace.evidence`. The delta value, the threshold that was applied (`ASSERTIVENESS_DELTA`, env-tunable at `rules.py:141`), the prior's CID, and the prior's score are not in the receipt. Two decisions made under different thresholds are indistinguishable after the fact.

Answering the classification question directly: **all three of "failing to expose", "discarding", and "computing a claim the history cannot justify" are present**, and they sit at different rungs. (c) is a receipt gap. (b) is a storage/normalization gap. (a) is a claim-admissibility gap, and it is the one that matters, because it is the only one that makes an emitted label wrong rather than merely unauditable.

### Secondary observations, not the subject of this note

- **Retention window mismatch.** `events` prunes at 7d, `claim_history` at 14d (`retention.py:74,76`). For rows aged 7–14 days the prior-raw fetch returns nothing and `rules.py:131` returns no labels. Absence of the basis is indistinguishable from absence of drift.
- **Score granularity vs. threshold.** The score moves in steps of 1/3; the default threshold is 0.2 (`rules.py:141`). Any single net modal-token gain clears it. The threshold has no resolution below one token.
- **Cap collision.** `min(1.0, …)` means 3 modal tokens and 8 modal tokens both score 1.0.

---

## 3. Collision specimens

Executable at `tests/test_comparison_basis_collisions.py`. They are characterization tests: they assert current behavior and are expected to fail when the view is enriched.

All four specimen texts share one fingerprint (`133894d963c2d580`), so they are the same claim by driftwatch's own identity.

### Specimen A — the view collision

| Text | `modal` | score |
|---|---|---|
| `Reportedly 200 people were affected in Springfield.` | `["Reportedly"]` | 0.3333 |
| `Confirmed: 200 people were affected in Springfield.` | `["Confirmed"]` | 0.3333 |

Equal under the view. The target property — is the author attributing or asserting — separates them. No decoder from this view to that property exists, so no threshold tuning and no downstream postprocessing of this score recovers it.

### Specimen B — false positive (sign inversion)

```
prior:   200 people were affected in Springfield.
current: Reportedly 200 people were affected in Springfield, according to officials.

confidence_delta = +0.6667   evidence_changed = False   →  assertiveness_increase_possible FIRES
```

The author moved from a bare assertion to an attributed one. Assertiveness went *down*. The label says it went up.

### Specimen C — false negative, with the disambiguator computed and dropped

```
prior:   Reportedly 200 people were affected in Springfield.
current: Confirmed: 200 people were affected in Springfield.

confidence_delta = 0.0   attribution_removed = True   →  no label
```

This is the textbook case the rule is named for. `compare_claim_states` computes `attribution_removed=True`; `rules.py:144` never reads it.

### Specimen D — the prior endpoint is reconstructed from mutable storage

Insert a post, then edit it (same URI, new CID, unchanged declared `createdAt`):

- `claim_history` gains two rows, distinguishable only by `post_cid`.
- Both rows carry the **same** `createdAt`, so `h["createdAt"] < post.createdAt` (`rules.py:123`) can never select one as the other's prior. Post edits — the paradigm drift event — are structurally unreachable by this rule.
- `SELECT raw FROM events WHERE event_uri = ?` for the *first* row returns the *edited* text. The reconstructed "prior state" is the post-edit state.
- `event_versions` holds the true pre-edit content, and nothing reads it.

---

## 4. Proposed minimal record

**No new table.** `label_decisions` already exists, already carries `inputs_json` / `decision_trace` / `config_hash` / `fingerprint_version`, and is already the audit surface. The proposal is an additive JSON block inside `decision_trace` for comparison-shaped rules only.

```json
{
  "comparison_basis": {
    "basis_version": "cb.v1",
    "prior":   {"post_uri": "at://…", "post_cid": "bafy…", "declared_at": "…", "fingerprint_version": "fp.v3"},
    "current": {"post_uri": "at://…", "post_cid": "bafy…", "declared_at": "…", "fingerprint_version": "fp.v3"},
    "ordering": {"key": "declared_at", "witnessed": false, "tie": "none"},
    "source": {"prior_content_from": "events.raw", "exact_version_confirmed": false},
    "projection": {"fields": ["confidence", "evidence_hash", "attribution_present"],
                   "scorer": "assertiveness_score/modal_count.v0"},
    "delta": {"confidence_delta": 0.667, "evidence_changed": false, "attribution_removed": false},
    "gate": {"threshold": 0.2, "threshold_source": "env:ASSERTIVENESS_DELTA"},
    "qualification": "coarse_view_cannot_separate_attribution_from_certainty"
  }
}
```

The load-bearing fields are the unglamorous ones:

- **`exact_version_confirmed`** — false whenever prior content came from `events.raw` without a CID match. This is the honest encoding of Specimen D. It should be false today for every decision.
- **`ordering.witnessed`** — false while ordering is on subject-declared time.
- **`qualification`** — a non-empty string is a refusal to promote this delta past descriptive, not a caveat on a claim that still gets made.

`basis_version` exists so a later enrichment is legible in the receipt rather than silently changing what old rows meant.

---

## 5. Where the boundary sits

Driftwatch already has a ladder in `specs/gaps/gap-spec-formal-claim-admissibility-pipeline.md`:

```
Observation → Feature → Finding → Claim
```

The comparison path maps onto it, and the boundary this note is about is the Feature→Finding edge:

| Rung | Here | Basis obligation |
|---|---|---|
| Observation | a `claim_history` row + the record version it names | identity is `(post_uri, post_cid)`, not `post_uri` |
| Feature | `compare_claim_states` output | **descriptive.** "Modal-token count rose by 0.667 between these two observations under this projection" is true and needs no collision proof. |
| Finding | `assertiveness_increase_possible` | **semantic.** Names a direction of change in a property. Requires that the projection separate that property — i.e. no exhibited collision. |
| Claim | `/recent-decisions`, `/labels/{subject_uri}`, cluster reports | requires the Finding plus its basis |

The rule to propose, in one line:

> A descriptive delta may be computed from any projection. A delta may only be named as a change in some property if no collision under that projection separates the property — and the receipt must carry the basis either way.

Specimen A is an exhibited collision for the property `assertiveness_increase_possible` names. Under this rule that label is currently a Feature published as a Finding.

A descriptive delta does **not** owe an impossibility proof. Nothing here asks for one before emitting a number.

---

## 6. Compatibility and migration

- **Additive only.** `decision_trace` is already free-form JSON (`db.py:744` writes `decision_trace or ""`). Adding a key breaks no reader; `main.py:347` selects the column and returns it verbatim.
- **No schema change** for the receipt block. Binding prior content by CID would need either a `(event_uri, cid)` lookup path over `event_versions` or a CID column on `event_versions` — that is a later slice with a real migration, not part of the receipt work.
- **Retention interaction.** Any basis field naming a row that retention may prune must be treated as possibly-absent at read time. Prefer recording the identifiers at decision time over expecting to re-resolve them later.
- **Existing rows.** Decisions written before `cb.v1` have no `comparison_basis` key. Absence means unknown basis, not confirmed-good basis; any future consumer must default to the conservative reading.
- **`FP_VERSION` is already a contract** (`SIGNAL_MODEL.md`: "migrations require dual-emit periods"). Comparing two endpoints across a fingerprint-version boundary is the same class of problem and should reuse whatever that migration discipline settles on.

---

## 7. Non-goals

- **No `Adoption` or `Action` rung.** Driftwatch has no consumer-action seam. Adding one would be empty structure.
- **Not a redesign of the assertiveness scorer.** The note establishes that the current view cannot support the current label. Choosing a better view is a separate decision with its own evidence.
- **Not a proposal to delete or silence `assertiveness_increase_possible`.** Detect-only mode means nothing is emitted to ATProto today; the disposition of the rule is the operator's call.
- **Not general to all driftwatch signals.** `driftmetrics.py` is history-shaped already — time-binned series, half-life from peak, JSD against a rolling baseline. It has its own basis questions (bin boundaries, baseline window) and this note does not address them.
- **No hot-path solver, prover, or admissibility engine.** See the formal-claim-admissibility gap spec for where that would belong if it were ever built.
- **No new vocabulary import.** Terms here are driftwatch's own.

---

## 8. Recommended implementation sequence

Ordered by ratio of evidence gained to blast radius. Each step stands alone.

1. **Land the specimens.** Done in this change. Zero production surface.
2. **Read the field that is already computed.** `rules.py:144` ignores `attribution_removed`. Consuming it converts Specimen C from a silent false negative into a firing case. Smallest real behavior change in the file, and the only one this note would argue for on its own merits.
3. **Emit `comparison_basis` in `decision_trace`** for `rule_assertiveness_increase`. Receipt-only; changes no decision. Makes Specimen D's `exact_version_confirmed: false` visible in `/recent-decisions` instead of implicit.
4. **Bind prior content to `post_cid`.** Look up `event_versions` when the current `events.raw` CID does not match the history row's `post_cid`; set `exact_version_confirmed` accordingly. This is the first step with a migration cost and should not start before 3 is deployed and read.
5. **Decide the ordering key.** Either retain a witnessed observation time at ingest, or state explicitly that ordering is subject-declared and mark `ordering.witnessed: false` permanently. Retaining `time_us` is cheap at normalization but is a schema change on the hot path — do not fold it into 4.
6. **Revisit the view.** Only after 2–5, and only with the collision specimens as the acceptance test.

Steps 2 through 6 are proposals. None is authorized by this note.

---

## 9. Unanswered questions

1. Is `rule_assertiveness_increase` producing decisions in production today, and at what rate? `budgets.py:49` aggregates `label_decisions` by `rule_id`; that count has not been read for this rule. The severity of Specimen B depends on it.
2. Should the 7d/14d retention mismatch be closed, or should the comparison explicitly refuse when the basis is outside the events window? These give the same output today and different receipts.
3. Does `facts_export` carry anything derived from this path into labelwatch? If a comparison-derived signal crosses that contract, the basis obligation crosses with it. Not checked.
4. Is `post_cid` reliably populated on historical rows, or is it `""` for older ingests (`claims.py:423` writes `post_cid or ""`)? Step 4 is only meaningful where it is present.
5. Does the same basis gap exist in `rule_provenance_laundering` (`rules.py:11`), which also reconstructs prior text by URI at `rules.py:49`? It looks structurally identical. Not verified.
6. What is the right disposition for a Finding whose collision is exhibited — suppress, downgrade to Feature, or publish with a mandatory qualification? This note names the boundary but does not choose the remedy.

---

## 10. Authority statement

This note is a **candidate**. It does not alter production behavior, claim semantics, publication authority, roadmap status, or release status. It authorizes no implementation. The proposed `comparison_basis` record is a sketch for review, not a schema; `cb.v1` is a placeholder name with no force.

The findings in §2 and the specimens in §3 are executable and were verified by running them. The design in §4–§8 is not verified by anything and should be read as proposal only.

---

## Provenance

The distinction this note applies — that a computation being possible over a view does not make its result usable as a stronger claim, and that a coarse view's insufficiency is exhibited by a collision rather than argued — comes from the V16 governed-transition-boundary results in `~/git/lean` (`target_collision_blocks_explicit_factorization`, `derived_view_cannot_restore_target`, `endpoint_equality_does_not_preserve_route_force`, `computationally_sufficient_product_can_be_refused`). Those theorems are the reason the specimen format is "exhibit a collision pair" rather than "argue the heuristic is weak."

They are cited as rationale. No part of this note depends on the reader knowing them, and no vocabulary from that work is imported into driftwatch.
