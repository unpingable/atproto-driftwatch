# Driftwatch

*An infotoxin observatory — measuring information half-life and correction resistance in the wild.*

## Why this exists

ATProto provides cryptographic provenance: content-addressed CIDs prove **what
was written**, and DID-based identity proves **who signed it**. What ATProto does
not provide is governance: why a label was applied, what evidence supported it,
what regime the decision entered, or how to audit the process after the fact.

Most labelers treat this gap as someone else's problem. Driftwatch treats it as
the core problem. It watches how claims propagate, mutate, and persist across
Bluesky — fingerprinting them, tracking them longitudinally, and producing
cluster reports with burst detection, half-life estimation, and regime shift
analysis.

**Currently running in sealed-lab mode: detect-only, no labels emitted, no
labeler registration.**

## What it does

**Jetstream consumer.** Real-time WebSocket connection to the Bluesky Jetstream
firehose, ingesting posts and reposts (~500 events/sec steady state).

**Claim fingerprinting.** Posts are hashed into canonical fingerprints with kind
tracking — entity, quantity, domain, span, or text — using a precedence-based
pipeline that survives surface-level mutations (whitespace, emoji, URL params,
case, hedging). Complexity gate filters low-signal posts.

**Cluster analysis.** Fingerprints are grouped into clusters with burst scoring,
unique-author counts, half-life estimation, and evidence class tracking
(none/link/embed/facet/mixed). Single-author automation detection flags
likely-bot clusters.

**Longitudinal tracking.** Claims tracked over time by `(authorDid,
claim_fingerprint)`. Drift rules detect assertiveness increases (confidence
rises without new evidence) and provenance laundering (attribution removed in
later edits). Rechecks are scheduled, not ad-hoc.

**Platform health watermark.** EWMA-based stream health monitor gates rechecks
during incomplete data periods. Three degradation triggers (low coverage, high
lag, consumer backlog) with recovery hysteresis prevent analysis against
partial ingest.

**Decision ledger.** Every label commit produces a receipt in `label_decisions`
with rule ID, fingerprint version, input evidence hashes, config hash, and
decision trace. Labels are not just applied — they are receipted.

**Identity enrichment.** Captures Jetstream identity/account events into an
append-only log with a fieldwise reducer producing converged current state.
Async DID resolver extracts PDS endpoints from DID documents (did:plc via PLC
directory, did:web via .well-known). Provenance tracked: `live` (observed via
Jetstream), `labelwatch_seed` (seeded from labeled-target population), `both`.

**Facts export.** SQLite sidecar export for [Labelwatch](https://github.com/unpingable/atproto-labelwatch)
consumption — URI→fingerprint mappings, hourly aggregates, fingerprint bounds,
and actor identity facts (DID→PDS host mapping with resolver status).
Atomic snapshot via `VACUUM INTO` + `os.replace()`.

## Quickstart

```bash
# Development (with simulated labeler)
docker compose up --build -d
curl http://localhost:8000/health

# Seed demo events
docker compose run --rm replay
docker compose logs -f worker
```

### Production

```bash
cd deploy
cp .env.example .env
# Edit .env with your config
docker compose -f docker-compose.prod.yml up -d --build
curl http://localhost:8422/health/extended
```

## CLI

```bash
# Cluster report
python -m labeler.cli driftwatch report

# Quarantine inspection
python -m labeler.cli quarantine list --limit 50
python -m labeler.cli quarantine show <emit_id>

# Fingerprint stability testing
python -m labeler.cli stability-test --input fixtures/fingerprint_extended.jsonl

# Release rail (quarantine -> promote)
python -m labeler.cli release quarantine --report out/stability_report.json
python -m labeler.cli release promote --in out/release_manifest_quarantine.json
```

## Configuration

Key environment variables (full list in source):

| Variable | Default | Purpose |
|----------|---------|---------|
| `LABELER_EMIT_MODE` | `detect-only` | `detect-only`, `emit`, or `quarantine` |
| `LABELER_EMIT_CONFIRM` | `false` | Must be `true` to enable live emission |
| `JETSTREAM_WS` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream endpoint |
| `ENABLE_LONGITUDINAL_RECHECK` | `0` | Enable longitudinal recheck loop |
| `ENABLE_CLAIM_RECHECK` | `0` | Enable claim-group recheck scheduling |
| `ENABLE_FACTS_EXPORT` | `0` | Enable facts sidecar for labelwatch |
| `MIN_CLAIM_ALPHA_TOKENS` | `3` | Minimum tokens for fingerprint complexity gate |
| `ENABLE_RETENTION` | `0` | Enable periodic retention loop (prune old data) |
| `ENABLE_MAINTENANCE` | `0` | Enable maintenance loop (label expiry, disk monitoring) |
| `ADMIN_API_TOKEN` | — | Protect admin endpoints; open access if unset |

## API

| Endpoint | Auth | Purpose |
|----------|------|---------|
| `GET /health` | — | Simple OK |
| `GET /health/extended` | — | Queue depth, platform health, coverage, lag, emit mode, disk |
| `GET /health/preflight` | — | Startup checks: disk, DB, tables, WAL (503 on fail) |
| `GET /health/bake` | — | Baseline trustworthiness: consumer, retention, DB growth |
| `GET /metrics` | — | Prometheus metrics |
| `GET /strain/top` | — | Top authors by event count |
| `GET /labels/{uri}` | — | Labels for a post URI |
| `GET /recent-decisions` | admin | Recent label decisions (filterable by rule_id) |
| `GET /quarantine/recent` | admin | Recent quarantined emits |

## Architecture

```
Jetstream (WebSocket)
    → consumer (posts + reposts + identity + account events)
        |
        |— posts/reposts → claim_history (fingerprinted, fp_kind tracked)
        |       |
        |    fingerprint pipeline
        |    (entity > quantity > domain > span > text)
        |       |
        |    cluster analysis (driftmetrics)
        |       |— burst score / half-life / regime shifts
        |       |— single-author detection
        |       |
        |    longitudinal rechecks (platform health gated)
        |       |
        |    drift rules → label_decisions (receipted)
        |       |
        |    emit_mode gate → detect-only / quarantine / emit
        |
        |— identity/account → identity_events (append-only log)
                |
             fieldwise reducer → actor_identity_current
                |
             async DID resolver → pds_endpoint, pds_host
                |
             facts_export → facts.sqlite → labelwatch
                (uri_fingerprint + fingerprint_hourly +
                 fingerprint_bounds + actor_identity_facts)

  enrichment:
      seed_targets — import labeled-target DIDs from labelwatch
                     for resolver pickup (provenance: live/seed/both)

  operational:
      preflight checks → startup validation
      retention loop → prune old events/edges/claims
      maintenance loop → label expiry, disk monitoring
      disk pressure brake → pause processing at 92% disk
      STATS heartbeat → periodic observability line
```

## What this system is and is not

**Is:** A substrate observatory that watches how claims propagate and how
hosting infrastructure maps onto labeled populations. Aggregate-first.

**Is not:** A moderation tool, a user profiler, an enforcement mechanism, or a
trust score. It does not decide who is bad. It measures where weirdness clusters.

**Populations:**
- **Live-observed actors** — DIDs seen via Jetstream identity/account events.
- **Labeled-target actors** — DIDs seeded from labelwatch's labeled-target population.
- **Overlap** — DIDs present in both (tracked as `identity_source = 'both'`).
- Coverage metrics always specify which population they refer to.

**Host family is not operator identity.** Co-location on the same PDS does not
imply shared intent, coordination, or culpability. A host family may contain one
person's VPS, a community instance, a bridge, or disposable infrastructure.

**Current-state enrichment is not historical truth.** A DID's PDS today may differ
from its PDS at the time it was labeled. Temporal claims require timestamped
identity snapshots (future work).

## Invariants

- Language may propose; only evidence commits state (ledger recorded).
- Fingerprint version is a contract; changes require explicit bump and migration.
- Emit is gated; default is detect-only and auditable.
- Containment preserves records; nothing is silently dropped.
- Aggregate-first: cluster-level analysis, not per-account profiling.
- Only add enrichment that changes a live analytic question.

## Observatory family

Driftwatch watches **information drift** — do claims persist, mutate, resist
correction?

[Labelwatch](https://github.com/unpingable/labelwatch) watches **labeler
behavior** — are labelers consistent, accountable, governed?

Same family, different instruments.

## Project docs

- [SCOPE](docs/driftwatch/SCOPE.md)
- [DESIGN_NOTES](DESIGN_NOTES.md)
- [THEORY_TO_CODE](THEORY_TO_CODE.md)
- [ROADMAP](ROADMAP.md)
- [CONTRIBUTING](CONTRIBUTING.md)
- [PROVENANCE](PROVENANCE.md)

## Related work

This implementation is an artifact of a broader research program on temporal
coherence and governance in hierarchical systems. The conceptual framework is
developed in the Δt Framework preprint series (Beck, 2025–2026), starting with
[The Coherence Criterion](https://zenodo.org/records/17726790). The governance
gap addressed here — provenance is not governance — is formalized in the ATProto
governance transfer proof.

## License

Unless otherwise noted, this repository is licensed under MIT OR Apache-2.0,
at your option. Contributions are accepted under the same terms.
