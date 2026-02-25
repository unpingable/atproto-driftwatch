# Contributing (Normative)

## 0. Conformance Language
This document uses **MUST**, **SHOULD**, and **MAY** as defined in RFC 2119.

## 1. Golden File Updates (Drift Detector)

Contributors MUST:
- Treat rule changes as semantic changes with explicit rationale.
- Prefer targeted tests before golden updates.

When updating the drift detector behavior, follow this process to update the golden file safely:

1. Run the deterministic detector locally and inspect results:
   - `make regenerate-golden`
   - The script runs `labeler.drift.cli.run` on `fixtures/posts.jsonl` and writes `tests/golden/expected_labels.jsonl`.
2. Review changes in the generated file carefully. Golden updates require an explicit PR with:
   - A clear rationale in the PR description explaining why behavior changed (bugfix, tightened rule, new rule, or intentional change).
   - A note about any nondeterminism fixed to make the golden stable.
3. Update tests if expected behavior changed. Prefer adding targeted unit tests for rule changes before altering the golden.
4. CI will run the golden test and full test suite. Ensure all tests pass locally before pushing.

If you need to regenerate the golden file as part of a PR, add a short section in the PR text explaining the justification and include a link to relevant issue/ticket.

## 2. Fingerprint Contract
- `FP_VERSION` MUST only change for intentional semantic changes.
- Any change to fingerprint behavior SHOULD include a migration note.

## 3. Ledger Receipts
- Any committed label MUST have a decision record in `label_decisions`.
- Decision traces SHOULD remain short and stable.
