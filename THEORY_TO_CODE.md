# Theory → Code

Mapping from governance concepts to source locations. Not exhaustive — just
enough to navigate.

## Invariants

- **Language may propose; only evidence commits state**
  → `src/labeler/longitudinal.py` (rules propose via drift detection)
  → `src/labeler/db.py::insert_label_decision()` (evidence-backed receipts on commit)

- **Emit is gated; default is detect-only**
  → `src/labeler/emit_mode.py` (mode resolution + confirm gate)
  → `src/labeler/emitter.py::record_emit_decision()` (audit path for all modes)

- **Fingerprint version is a contract**
  → `src/labeler/claims.py::FP_VERSION` (constant; bump = migration)
  → `src/labeler/claims.py::fingerprint_config_hash()` (config hash recorded in ledger)

- **Containment preserves records**
  → `src/labeler/db.py::insert_quarantine_emit()` (suppressed emits stored with full payload)
  → `src/labeler/cli.py` (quarantine list/show for inspection)

## Receipt semantics

- Decision ledger schema → `src/labeler/db.py` (`label_decisions` table)
- Receipt fields: `rule_id`, `fingerprint_version`, `inputs_json`, `evidence_hashes_json`, `config_hash`, `decision_trace`
- Receipts on commit → `src/labeler/db.py::insert_label_decision()`
- Receipts on expiry → `src/labeler/db.py::expire_label_decisions()`

## Drift detection (longitudinal)

- Claim fingerprinting → `src/labeler/claims.py::fingerprint_text()`
- Claim history tracking → `src/labeler/db.py` (`claim_history` table)
- Delta computation → `src/labeler/drift/diff.py`
- Drift rules (assertiveness increase, provenance laundering) → `src/labeler/drift/rules.py`
- Recheck scheduling → `src/labeler/longitudinal.py`, `src/labeler/recheck_queue.py`

## Budget enforcement

- Per-rule activation budgets → `src/labeler/budgets.py`
- Budget breach → quarantine trip → `src/labeler/emit_mode.py`
- Rolling window checks use `label_decisions` as source of truth

## Stability testing

- Mutation classes (whitespace, punctuation, casefold, URL, emoji, small edit) → `src/labeler/stability.py::MUTATIONS`
- Stability report computation → `src/labeler/stability.py::compute_stability_report()`
- Threshold evaluation → `src/labeler/stability.py::evaluate_stability()`
- Release rail (quarantine → promote) → `src/labeler/cli.py`

## Conformance fixtures

- Golden labels → `tests/golden/expected_labels.jsonl`
- Golden assertiveness → `tests/golden/expected_assertiveness.jsonl`
- Golden provenance laundering → `tests/golden/expected_laundering.jsonl`
- Adversarial fingerprints → `fixtures/fingerprint_adversarial.jsonl`
- Known separation pairs → `fixtures/fingerprint_known_pairs.jsonl`
- Stability transforms → `fixtures/fp_stability_transforms.jsonl`

## Replay

- Demo replay (seeds events from fixture) → `src/labeler/demo_replay.py`
- Golden test regeneration → `scripts/regenerate_golden.py`
- All golden/regression tests are deterministic, no network dependencies

## Known gaps

- No `THEORY_TO_CODE` crosswalk for the Δt framework papers specifically (this repo predates the complete series)
- Strain detection is design-only (`DESIGN_NOTES.md`), not implemented
- Authority delegation / multi-operator quorum is design-only
- No failure gallery (adversarial fixtures exist but aren't framed as "how it fails")
