# Bluesky Labeler (Reference Implementation)

A governed ATProto labeler with drift detection, longitudinal tracking, and a
decision ledger. Built as a reference implementation — not a production service,
but a demonstration of patterns that auditable labelers should adopt.

## Why this exists

ATProto provides cryptographic provenance: content-addressed CIDs prove **what
was written**, and DID-based identity proves **who signed it**. What ATProto does
not provide is governance: why a label was applied, what evidence supported it,
what regime the decision entered, or how to audit the process after the fact.

Most labelers treat this gap as someone else's problem. This implementation
treats it as the core problem, and shows what a receipted, auditable labeling
pipeline looks like in practice.

## What it demonstrates

**Decision ledger.** Every label commit produces a receipt in `label_decisions`
with rule ID, fingerprint version, input evidence hashes, config hash, and
decision trace. Labels are not just applied — they are receipted.

**Emit modes (safe by default).** Labeling defaults to detect-only. Emission
requires explicit opt-in (`LABELER_EMIT_MODE=emit` + `LABELER_EMIT_CONFIRM=true`).
A quarantine mode suppresses emits while preserving audit records. Per-rule
activation budgets auto-trip quarantine if a rule spikes beyond its rolling
window cap.

**Longitudinal tracking.** Claims are fingerprinted and tracked over time by
`(authorDid, claim_fingerprint)`. Drift rules detect assertiveness increases
(confidence rises without new evidence) and provenance laundering (attribution
removed in later edits). Rechecks are scheduled, not ad-hoc.

**Fingerprint stability.** A stability testing CLI runs mutation classes
(whitespace, punctuation, casefold, URL params, emoji, small edits) against
fixtures and enforces collision rate, churn rate, and drift thresholds. A
release rail gates promotion on stability report results.

**Containment without erasure.** Quarantined emits are stored with full payload
and audit context. Nothing is silently dropped. The quarantine CLI and API
endpoints allow inspection and release with provenance.

## Quickstart

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# Run locally
python -m labeler.main

# Run tests
pytest -q
```

### Docker Compose (demo)

```bash
docker compose up --build -d
curl http://localhost:8000/health

# Seed demo events and watch worker + simulated labeler
docker compose run --rm replay
docker compose logs -f worker
```

The simulated labeler returns 429s for initial requests to exercise retry and
cooldown behavior. The shared `./data` directory holds the DuckDB database.

## CLI

```bash
# Quarantine inspection
python -m labeler.cli quarantine list --limit 50
python -m labeler.cli quarantine show <emit_id>

# Fingerprint stability testing
python -m labeler.cli stability-test --input fixtures/fingerprint_extended.jsonl --out out/stability_report.json

# Release rail (quarantine -> promote)
python -m labeler.cli release quarantine --report out/stability_report.json
python -m labeler.cli release promote --in out/release_manifest_quarantine.json
```

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `LABELER_EMIT_MODE` | `detect-only` | `detect-only`, `emit`, or `quarantine` |
| `LABELER_EMIT_CONFIRM` | `false` | Must be `true` to enable live emission |
| `LABELER_EMIT_MAX_PER_RECHECK` | — | Cap emits per recheck; exceeding trips quarantine |
| `LABELER_EMIT_AUDIT_DIR` | `out/` | Override audit file output directory |
| `LABELER_RULE_BUDGETS` | — | Per-rule caps (e.g., `provenance_laundering:5,assertiveness_increase:3`) |
| `LABELER_RULE_BUDGET_WINDOW_HOURS` | `24` | Rolling window for budget enforcement |
| `ENABLE_LONGITUDINAL_RECHECK` | `0` | Enable longitudinal recheck loop |
| `ENABLE_CLAIM_RECHECK` | `0` | Enable claim-group recheck scheduling |
| `CLAIM_RECHECK_MAX_PER_RUN` | — | Cap claim-group work per recheck loop |
| `ADMIN_API_TOKEN` | — | Protect admin endpoints; open access if unset |
| `DB_BACKEND` | `sqlite` | `sqlite` or `duckdb` |

## Invariants

- Language may propose; only evidence commits state (ledger recorded).
- Fingerprint version is a contract; changes require explicit bump and migration.
- Emit is gated; default is detect-only and auditable.
- Containment preserves records; nothing is silently dropped.

## Architecture

```
firehose -> consumer -> events/edges/cursors (append-only)
                          |
                    claim_history (fingerprinted, longitudinal)
                          |
                    drift rules (assertiveness, provenance laundering)
                          |
                    label_decisions (receipted)
                          |
                    emit_mode gate -> audit log / quarantine / live emit
```

## Design notes

See [DESIGN_NOTES.md](DESIGN_NOTES.md) for governance extensions (authority
delegation, strain detection, label composition, temporal coherence) that are
explicitly out of the runtime path but recorded for future coherence.

## Related work

This implementation is an artifact of a broader research program on temporal
coherence and governance in hierarchical systems. The conceptual framework is
developed in the Δt Framework preprint series (Beck, 2025–2026), starting with
[The Coherence Criterion](https://zenodo.org/records/17726790). The governance gap addressed here — provenance is not
governance — is formalized in the ATProto governance transfer proof.

## License

Unless otherwise noted, this repository is licensed under MIT OR Apache-2.0, at your option. Contributions are accepted under the same terms.
