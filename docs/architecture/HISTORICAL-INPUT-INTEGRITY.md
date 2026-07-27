# Historical-Input Integrity in the Drift Rules

**Status**: candidate. Non-authorizing. Nothing here changes production behavior.
**Date**: 2026-07-26
**Scope**: historical-content resolution for the drift rules — `claim_history` → `post_uri` → `events.raw`.
**Companion**: `COMPARISON-BASIS-BINDING.md` (commit `92ec821`), which covers the scoring view. This note covers the input to it.

## Executive finding

**Classification: SHARED-HISTORICAL-LOADER-DEFECT.**
**Readiness: REQUIRES-LEGACY-DATA-POLICY, secondarily REQUIRES-SCHEMA-DESIGN.**

`rule_provenance_laundering` reproduces the defect, in both directions, and it is not rule-local. Every drift rule that reasons about a prior claim receives that claim's *current* representation. The substitution happens in the shared post loaders in `longitudinal.py`, which build the thread handed to all five rules, and is duplicated in two rule-local fallback lookups.

The consequence is stronger than "stale inputs". Because `events.raw` is overwritten in place on edit and nothing reads `event_versions`:

> A later edit by the observed party retroactively creates or destroys labels on a *different* post that never changed.

Both directions are executable (§5). Editing an earlier post to remove attribution erases a `provenance_laundering_possible` label (score 0.9) from a later post. Editing an earlier post to *add* attribution manufactures one. The label is attached to a post whose content, at the time of observation and now, is identical.

The evidence that would resolve every case correctly is already captured. `claim_history.post_cid` names the version witnessed; `event_versions` retains the pre-edit record. Neither is read by any analysis path.

---

## 1. Exact dataflow for `rule_provenance_laundering`

Verified against the tree at commit `92ec821`.

### Entry

`longitudinal.recheck_once` (`longitudinal.py:159`) dequeues fingerprints, calls `_load_posts_for_fingerprint(conn, fp)` (`longitudinal.py:204`), and passes the result to `apply_all_rules(p, posts)`. `apply_all_rules` (`rules.py:152`) calls `rule_provenance_laundering` first (`rules.py:154`). The claim-group path at `longitudinal.py:343` does the same via `_load_posts_for_claim_group`.

### Branch 1 — thread-local priors (`rules.py:14`, `:35-37`)

```python
priors = [p for p in thread if p.authorDid == post.authorDid and p.uri != post.uri]
for prior in reversed(priors):
    if _check_prior_text(prior.text, prior.uri):
```

`prior.text` comes from the loader. The loader built it like this (`longitudinal.py:69-105`):

```sql
SELECT post_uri, createdAt FROM claim_history WHERE claim_fingerprint = ? ORDER BY createdAt ASC
```
then, per row:
```sql
SELECT raw, ctime FROM events WHERE event_uri = ?
```

Three things happen in those nine lines:

1. **`post_cid` is not selected.** The history row's version identity is dropped before content resolution begins.
2. **`createdAt` is selected and discarded** — `for post_uri, _created in rows` (`longitudinal.py:77`). `Post.createdAt` is then taken from the *current* record (`longitudinal.py:96`), not from the history row.
3. **Dedup by URI** (`longitudinal.py:76-80`). Two witnessed versions of one post collapse to one `Post`, carrying the current record's `cid` (`longitudinal.py:94`) — an identity that contradicts the history row it came from.

### Branch 2 — `claim_history` fallback (`rules.py:39-58`)

Runs when no thread-local prior matched.

```python
fp = fingerprint_text(post.text)
history = get_claim_history(post.authorDid, fp)      # claims.py:438
for h in reversed(history):
    if h["createdAt"] < post.createdAt:              # rules.py:47
        rows = conn.execute("SELECT raw FROM events WHERE event_uri = ?",
                            (h["post_uri"],)).fetchall()   # rules.py:49
        raw = json.loads(rows[0][0])
        if _check_prior_text(raw.get("text", ""), h["post_uri"]):
```

`get_claim_history` *does* return `post_cid` (`claims.py:454`) and `fingerprint_version` (`claims.py:455`). `rules.py:49` uses neither. The lookup is by `post_uri` alone, against the table that is overwritten on edit.

### Projection

`_check_prior_text` (`rules.py:19-32`) reduces both sides to:

- `prior_has_attr` / `post_has_attr` — membership in `ATTRIBUTION_TOKENS` (`rules.py:7`);
- `comparable_claim_texts(prior_text, post.text)` (`drift/diff.py:14`) — normalized substring or >4-char token overlap;
- `signal_overlap` — set intersection on dates, quantities, entities from `extract_claim_signals`.

Fires when `prior_has_attr and not post_has_attr and (strong_text or signal_overlap)`.

### Receipt and emission

`LabelRecord(label="provenance_laundering_possible", score=0.9, evidence=[{"prior": prior_uri, "post": post.uri}])` (`rules.py:30`). Passes the 0.4 score filter (`rules.py:160`). Receipt written by `longitudinal.py:267-303` → `db.insert_label_decision` (`db.py:716`): `inputs_json` is the *current* post's signals; the prior appears only as a URI string in `decision_trace.evidence`. **No CID on either endpoint.** Reachable at `/recent-decisions` (`main.py:341`) and `/labels/{subject_uri}` (`main.py:332`). `LABELER_EMIT_MODE=detect-only` means no ATProto emission.

### The scenario, confirmed

```text
v1: URI U, CID C1, text T1   (attributed)
v2: URI U, CID C2, text T2   (attribution removed)

claim_history holds rows for both C1 and C2
events currently stores T2 for U
rule requests historical content for the C1 row
rule receives T2                                    ← CONFIRMED, both branches
```

---

## 2. Inventory of historical-content consumers

Complete enumeration of `FROM events` in `src/` plus every `claim_history` reader.

| # | Site | What it resolves | Class |
|---|---|---|---|
| 1 | `longitudinal.py:82` `_load_posts_for_fingerprint` | `claim_history` rows → `events.raw` by URI | **MUTABLE_URI_LOOKUP** |
| 2 | `longitudinal.py:116` `_load_posts_for_claim_group` | same, undeduped | **MUTABLE_URI_LOOKUP** |
| 3 | `rules.py:49` `rule_provenance_laundering` fallback | `claim_history` row → `events.raw` by URI | **MUTABLE_URI_LOOKUP** |
| 4 | `rules.py:129` `rule_assertiveness_increase` | `claim_history` row → `events.raw` by URI | **MUTABLE_URI_LOOKUP** |
| 5 | `longitudinal.py:25,33` `_load_posts_for_root` | thread by reply-root, no `claim_history` | **METADATA_ONLY** — current-thread semantics by construction; still current-representation, but makes no historical claim |
| 6 | `db.py:349` `insert_event_txn` | its own prior row, to decide insert vs update | **METADATA_ONLY** |
| 7 | `db.py:672` | `event_uri` list for unlabeled events | **METADATA_ONLY** |
| 8 | `main.py:328` `/strain/top` | `GROUP BY author` counts | **METADATA_ONLY** |
| 9 | `retention.py:352,1443` | rowid/MIN(ctime) for pruning | **METADATA_ONLY** |
| 10 | `driftmetrics.py:68,363,379,414` | `claim_history` aggregates — no content resolution | **METADATA_ONLY** |
| 11 | `facts_export.py:107,110,136,290` | `claim_history` → fingerprint/URI aggregates for the labelwatch bridge | **METADATA_ONLY** |
| 12 | `retention.py` archive/parquet paths | `claim_history` columns incl. `post_cid`, copied verbatim | **IMMUTABLY_BOUND** (carries the identity; does not resolve content) |
| 13 | `maintenance.py:71`, `preflight.py:69` | counts and date ranges | **METADATA_ONLY** |

**IMMUTABLY_BOUND content resolvers: none.** Sites 1–4 are the whole population, and 1–2 feed all five rules.

`event_versions` readers in `src/`: **zero**. Written at `db.py:382`, pruned by `retention.py`, read by nothing.

### Rules affected, by evidence

| Rule | Historical input | Substitution-sensitive? |
|---|---|---|
| `rule_provenance_laundering` (`rules.py:11`) | prior text (thread + fallback) | **Yes** — both directions, §5 specimens 1–3 |
| `rule_repeat_claim_no_new_evidence` (`rules.py:63`) | prior `externalLinks`/`embeds` | **Yes** — §5 specimen 6 |
| `rule_assertiveness_increase` (`rules.py:108`) | prior text via fallback | **Yes** — established in `COMPARISON-BASIS-BINDING.md` §3 specimen D |
| `rule_quote_mismatch` (`rules.py:82`) | current post only | No |
| `rule_time_inconsistency` (`rules.py:91`) | current post only | No |

---

## 3. Defect boundary

### Where the narrowest shared layer sits

Sites 1–4 perform the same three steps: take a `claim_history` row, resolve `post_uri` against `events`, build a `Post`. The narrowest layer that covers all four is a single resolution helper consuming a history row and returning either a version-bound `Post` or a typed absence. Using existing repo vocabulary, something shaped like:

```python
def load_post_for_claim_row(conn, row) -> tuple[Optional[Post], str]:
    """row is a claim_history row carrying post_uri + post_cid.
    Returns (post, resolution) where resolution is one of:
      "exact"          — events.raw CID matches row.post_cid
      "from_versions"  — matched in event_versions
      "unavailable"    — outside retention, or no CID on the row
    """
```

Sites 1 and 2 become loops over it. Sites 3 and 4 call it directly instead of running their own SQL.

### Per-boundary assessment

| Boundary | Callers | Identity available? | Legacy fallback | Fixes ordering (B)? | Receipt-consumable? | Migration | Silent-label-change risk |
|---|---|---|---|---|---|---|---|
| Per-rule patch (`rules.py:49`, `:129`) | 2 | yes (`post_cid` already returned) | per-rule | no | yes | none | **high, and incomplete** — leaves sites 1–2, which feed every rule |
| **Shared row resolver** (above) | 4 | yes | one place | **yes** — see below | yes | none for the resolver itself | **high** |
| `Post` dataclass gains version fields | all rule code | yes | n/a | partly | yes | touches `drift/models.py` | medium |
| Ingest-side: stop overwriting `events.raw` | whole write path | yes | n/a | n/a | n/a | **schema + retention redesign** | very high |

The shared row resolver is the right boundary. It is the only one that covers all four sites without touching the write path.

### Does ordering (defect B) have to move with it?

**Yes, at the same layer, because the loader is where ordering is currently corrupted.** `longitudinal.py:77` and `:114` select `claim_history.createdAt` and discard it, then take `Post.createdAt` from the current record (`:96`, `:130`). A repair that binds content to the right version but still stamps it with the current record's declared timestamp would produce a `Post` whose text and timestamp come from different versions — worse than today. The resolver must carry the history row's `createdAt` through.

This does **not** mean resolving the broader ordering question. Whether ordering should move off subject-declared time to a witnessed clock is a separate decision (`COMPARISON-BASIS-BINDING.md` §8 step 5) and is not required here. What is required is that the timestamp and the content come from the same row.

### Defects kept separate

| | Defect | This note | Status |
|---|---|---|---|
| A | historical content substitution | **primary target** | confirmed, §5 |
| B | comparison ordering ambiguity | only the loader-local part (timestamp/content mismatch) | must move with A |
| C | incomplete comparison receipt | out of scope | `COMPARISON-BASIS-BINDING.md` §4 |
| D | semantically invalid scoring | out of scope | `COMPARISON-BASIS-BINDING.md` §2(a) |
| E | dead computed signals (`attribution_removed`) | out of scope | `COMPARISON-BASIS-BINDING.md` §8 step 2 |

A and B-local share a repair. C, D, E do not, and are not proposed here.

---

## 4. Specimens and results

Executable at `tests/test_historical_input_integrity.py` — 8 tests, all passing against current behavior. All texts share fingerprint `133894d963c2d580`.

| # | Scenario | Expected historical text | Text actually supplied | Consequence |
|---|---|---|---|---|
| 1 | P1 (attributed) → P2 (bare), then P1 edited to bare | `Reportedly 200 people…` (`cid-p1-v1`) | `200 people…` (`cid-p1-v2`) | **False negative.** `provenance_laundering_possible` @0.9 fires, then disappears. P2 unchanged throughout. |
| 2 | Q1 (bare), Q2 (bare), then Q1 edited to attributed | `200 people…` (`cid-q1-v1`) | `Reportedly 200 people…` (`cid-q1-v2`) | **False positive.** A 0.9-score label appears on Q2, which never changed. |
| 3 | Same as 2, rule called with `thread=[current]` | as above | as above | Fallback path at `rules.py:49` reproduces it **independently of the loader** — the defect is at both layers. |
| 4 | One post, two witnessed versions | two rows, `cid-v1-old` + `cid-v1-new` | one `Post`, `cid=cid-v1-new` | **Untraceable.** The `Post` asserts a version identity contradicting its source row. |
| 5 | Same data, both loaders | — | `_load_posts_for_fingerprint` → 1 post; `_load_posts_for_claim_group` → 2 identical posts | The two loaders disagree on the same rows; the claim-group path duplicates the current version to stand in for two different witnessed ones. |
| 6 | R1, R2 (both bare, no links), then R1 edited to add a link | no link | link present | **False negative.** `repeat_claim_no_new_evidence` @0.6 erased. |
| 7 | Any edit | — | — | `event_versions` holds the pre-edit record, text and CID both recoverable from the JSON. Nothing reads it. |

Specimen 2 is the one that matters most for disposition: an author who retroactively *improves* the sourcing on an old post causes an accusation-shaped label to attach to a newer post of theirs. The incentive is backwards.

### Contradictions with the earlier note

None. Every finding in `COMPARISON-BASIS-BINDING.md` that this investigation touched was re-verified and held. Two clarifications, neither a reversal:

- That note's open question 5 asked whether `rule_provenance_laundering` shared the defect. **Answer: yes**, and more broadly than asked — the shared loaders are affected, not just the rule's own lookup.
- That note attributed the mutable lookup to `rules.py:129` and `rules.py:49`. Those are real but are the *minority* of the affected sites. `longitudinal.py:82` and `:116` are the substrate, and the earlier note did not name them. That is an omission being corrected, not a claim being retracted.

---

## 5. Candidate shared-loader design

Sketch for review. Not a specification.

The resolution ladder, in the order it would be tried:

1. `events.raw` CID == `row.post_cid` → `exact`. The common case: unedited posts.
2. `event_versions` row for `event_uri` whose `raw` JSON `cid` == `row.post_cid` → `from_versions`.
3. `row.post_cid` is empty (legacy) or no version matches within retention → `unavailable`.

Only `exact` and `from_versions` should be usable as a prior. `unavailable` must be a typed absence, not a fallback to current text — falling back is exactly today's behavior.

Two properties worth fixing in the sketch now, because they are cheap to state and expensive to retrofit:

- **The returned `Post` carries its version identity and its history row's timestamp**, not the current record's. This is what makes B-local go away.
- **The resolution status is a return value, not a log line.** It is what a future receipt would record, and what distinguishes "no prior existed" from "a prior existed and we could not read it."

The second is the whole point. Today those two cases are the same code path and the same output: no label.

---

## 6. Legacy data and migration

**No schema migration is required to *read* correctly. One is likely required to read *efficiently*.**

- `event_versions` is `(event_uri, version_ts, raw)` (`db.py:111`) with **no index and no CID column**. Resolution by CID means either a JSON extract per candidate row or an added column plus index. At current volumes this is a real cost decision, not a formality.
- **The retention windows make exact resolution impossible for part of the corpus.** `events` and `event_versions` prune at 7d (`retention.py:74`); `claim_history` at 14d (`retention.py:76`). For history rows aged 7–14 days there is no record to resolve against — today that returns no label, and after a repair it must return `unavailable`. This is why the readiness classification is REQUIRES-LEGACY-DATA-POLICY: the substrate cannot answer, and what the system should say when it cannot answer is a decision, not an implementation detail.
- **`post_cid` fill rate on existing rows is unmeasured.** It is written as `raw.get("cid")` from the Jetstream commit (`consumer.py:94`, `db.py:365`), defaulting to `""` (`claims.py:423`). Live-ingested rows should carry it. Rows from seeds, replays, or pre-column ingests may not. Not checkable from this workstation; needs a production count.
- **Backfill is not possible.** A history row with `post_cid=""` cannot have its identity reconstructed after the fact. Those rows are permanently `unavailable`.

### Risk of silently changing labels

**High, and it is the main reason not to implement from this note.** Specimens 1, 2, and 6 all flip. A repair changes which labels exist in `label_decisions`, which changes `/recent-decisions`, `budgets.py:49` rule counts, and any downstream reading of rule activation. `LABELER_EMIT_MODE=detect-only` bounds the blast radius to driftwatch's own surfaces — nothing reaches ATProto — but it does not make the change invisible.

Any implementation should shadow-evaluate first: compute both resolutions, record the divergence, change no label. That measurement does not exist and would need to come before, not after.

---

## 7. Sequencing recommendation

Proposals. None authorized by this note.

1. **Land the specimens.** Done in this change. No production surface.
2. **Measure.** Two counts, neither of which changes behavior: `post_cid` fill rate on `claim_history`, and the rate at which a `claim_history` row's `post_cid` disagrees with the current `events.raw` CID. The second is the real incidence of this defect in production and is currently unknown. Everything below should wait on it.
3. **Shadow resolver.** Add the resolution ladder, call it alongside the existing path, record `exact` / `from_versions` / `unavailable` and whether the resolved text differs. Change no label.
4. **Decide the `unavailable` policy.** Refuse, or degrade with a marker. This is the legacy-data policy the readiness classification names, and it is an operator decision.
5. **Cut sites 1–4 over**, ordering timestamps with content at the same time.
6. **Then** — and only then — the receipt work in `COMPARISON-BASIS-BINDING.md` §8, which can finally record a real endpoint identity.

Note the inversion against the earlier note: its step 3 (emit `comparison_basis`) assumed the endpoints were correctly identified. They are not. This note's steps 2–5 are a prerequisite for that receipt to be worth writing.

---

## 8. Non-goals

- **No production fix.** No loader change, no rule change, no schema migration, no receipt-format change, no threshold change, no consumption of `attribution_removed`, no publication or dashboard change.
- **Not a proposal to stop honoring edits.** Driftwatch should see the current version. The defect is that it sees *only* the current version while claiming to compare against a historical one.
- **Not an ingest redesign.** Making `events` append-only would fix this and is out of proportion to it. `event_versions` already captures what is needed.
- **Not a disposition for the affected rules.** Whether `provenance_laundering_possible` should keep firing while this is open is the operator's call.
- **Not general to non-drift paths.** `driftmetrics.py`, `facts_export.py`, and the retention/parquet paths are metadata-only or identity-carrying and are unaffected.
- **Not a claim about production incidence.** Everything here is reproduced in a test database. How often real posts are edited within the recheck window is unmeasured — see §7 step 2.

---

## 9. Unanswered questions

1. What is the `post_cid` fill rate on production `claim_history`, and what fraction of rows disagree with the current `events.raw` CID? Without this, severity is unknown.
2. How often does the recheck queue re-evaluate a fingerprint *after* one of its posts was edited? The defect only bites when both happen.
3. Should `_load_posts_for_claim_group`'s duplicate `Post` objects (specimen 5) be treated as a separate bug? They inflate the prior set for every rule that iterates `priors`, independently of substitution.
4. `_load_posts_for_root` (`longitudinal.py:20`) reads `events` directly with no `claim_history` involvement. It is classified METADATA_ONLY here because it makes no historical claim — but the rules receiving its output cannot tell which loader they were fed by. Does the resolution status need to travel with the thread?
5. Does the labelwatch bridge carry anything derived from these labels? `facts_export.py` reads `claim_history` for aggregates only, but `label_decisions` was not traced across the contract.
6. Is there a legitimate reading under which "compare against the current version of the prior post" is the intended semantics? The rule names — *laundering*, *no new evidence* — suggest not, but this note assumes rather than establishes it.

---

## 10. Authority statement

This note is a **candidate**. It does not alter production behavior, rule semantics, label publication, roadmap status, or release status. It authorizes no implementation. The loader sketch in §5 is illustrative; its names have no force.

The findings in §1–§4 are executable and were verified by running them. The design and sequencing in §5–§7 are proposal only and are verified by nothing.

## Provenance

Follows `COMPARISON-BASIS-BINDING.md` (commit `92ec821`), whose open question 5 asked whether `rule_provenance_laundering` shared the defect. The rationale for demanding an exhibited specimen rather than an argued weakness is unchanged and is recorded there.
