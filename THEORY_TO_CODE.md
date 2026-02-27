# Theory → Code

Mapping from governance concepts to source locations. Not exhaustive — just
enough to navigate.

## Core pipeline (Driftwatch-specific)

- **Jetstream ingestion**
  → `src/labeler/consumer.py` (WebSocket consumer, event parsing, cursor persistence)
  → `src/labeler/db.py::insert_event()` (claim_history insert with fp_kind)

- **Claim fingerprinting**
  → `src/labeler/claims.py::fingerprint_text_with_kind()` (hash + kind)
  → `src/labeler/drift/extract.py` (entity, quantity, span extraction)
  → Precedence: multi-word entity > quantity+context > URL domain > single-word entity > spans > normalized text

- **Cluster analysis**
  → `src/labeler/driftmetrics.py::cluster_report()` (burst score, half-life, regime shifts)
  → fp_kind distribution, single-author detection, evidence class tracking

- **Platform health watermark**
  → `src/labeler/platform_health.py` (EWMA baseline, degradation triggers, recovery hysteresis)
  → Gates rechecks during incomplete data periods

- **Facts export (Driftwatch → Labelwatch bridge)**
  → `src/labeler/facts_export.py` (SQLite sidecar, atomic snapshot, incremental update)

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
- Platform context stamped in decision traces → `src/labeler/longitudinal.py`

## Drift detection (longitudinal)

- Claim fingerprinting → `src/labeler/claims.py::fingerprint_text()`
- Claim history tracking → `src/labeler/db.py` (`claim_history` table, `fp_kind` column)
- Delta computation → `src/labeler/drift/diff.py`
- Drift rules (assertiveness increase, provenance laundering) → `src/labeler/drift/rules.py`
- Recheck scheduling → `src/labeler/longitudinal.py`, `src/labeler/recheck_queue.py`
- Enqueue gate (platform health + singleton) → `src/labeler/db.py::_fp_passes_enqueue_gate()`

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

## Known gaps

- Regime shift detection is computed but not yet validated against known campaigns
- Half-life estimation needs 8+ hourly bins to be meaningful
- Per-kind EPS baseline (cheap to add — kind counters exist in STATS)
- AppView/HTTP read health tracking (platform health covers stream only)
- No failure gallery (adversarial fixtures exist but aren't framed as "how it fails")
