# CLAUDE.md

Driftwatch is an ATProto substrate observatory. It watches how claims propagate across Bluesky (fingerprinting, cluster analysis, drift detection) and maps hosting infrastructure onto labeled populations (identity enrichment, PDS resolution, hosting-locus analysis). It runs in sealed-lab mode: detect-only, no labels emitted.

## Architecture rules

- **Observation only** — no content moderation, no user profiling, no enforcement.
- **Aggregate-first** — cluster-level and host-family-level analysis, not per-account dossiers.
- **Append-only events** — `identity_events` and `claim_history` are journals, not mutable state.
- **Fieldwise reducer** — `actor_identity_current` is converged from sparse events; newer events never erase prior known fields. MISSING sentinel distinguishes "absent" from "explicitly cleared."
- **Provenance tracked** — every DID in `actor_identity_current` has `identity_source`: `live` (observed via Jetstream), `labelwatch_seed` (seeded from labeled targets), `both`.
- **Receipt hashing** — label decisions include receipt hashes for audit trail.
- **Platform health gating** — rechecks suppressed during degraded ingest (EWMA baseline, coverage watermark).
- **Emit gated** — `LABELER_EMIT_MODE=detect-only` by default. Must explicitly enable.

## Populations (know which one you're measuring)

- **Live-observed**: DIDs seen in Jetstream identity/account events (~60k and growing).
- **Labeled-target seed**: DIDs imported from labelwatch's labeled-target population for resolver enrichment.
- **Overlap**: DIDs in both populations.
- Coverage must always specify denominator. "0.2% coverage" was a real bug caused by comparing 40k resolved actors against 2M label events.

## Facts bridge contract

`facts.sqlite` is the unidirectional export to labelwatch. Schema:

- `uri_fingerprint` — post URI → fingerprint dedup
- `fingerprint_hourly` — hourly aggregation (72h overlap window)
- `fingerprint_bounds` — time bounds per fingerprint
- `actor_identity_facts` — thin resolved-host projection:
  - `did, handle, pds_endpoint, pds_host, resolver_status, resolver_last_success_at, is_active, identity_source`

Labelwatch ATTACHes this read-only. Changes to this schema are data contract changes.

## Key files

```
src/labeler/
  consumer.py      — Jetstream WebSocket consumer, identity event capture, resolver dispatch
  identity.py      — MISSING sentinel, IdentityDelta, parse/apply identity events, fieldwise reducer
  resolver.py      — Async DID resolver: PLC directory + did:web, batch processing, backoff
  seed_targets.py  — Import labeled-target DIDs from labelwatch for resolver pickup
  facts_export.py  — SQLite sidecar export (fingerprints + identity facts)
  fingerprint.py   — Claim fingerprinting pipeline (entity/quantity/domain/span/text)
  detection.py     — Cluster analysis, burst scoring, cooldown filter
  db.py            — Schema, migrations, connection management
  main.py          — App startup, periodic task wiring
```

## What not to do

- Don't add enrichment without a named analytic question it answers.
- Don't treat host family as operator identity (rake #2).
- Don't treat current-state PDS as historical-at-time-of-label PDS (rake #7).
- Don't let the resolver become an indiscriminate crawler.
- Don't produce accusation-shaped outputs — "concentration anomaly" not "bot farm."
- Don't skip first-run operational testing on schema/export changes (facts export first-run is expensive).

## Commands

```bash
pytest tests/ -v                           # run all tests
python -m labeler.cli driftwatch report    # cluster report
python -m labeler.cli driftwatch summary   # human-readable summary (1h default)
python -m labeler.cli driftwatch preflight # startup checks
python -m labeler.seed_targets --help      # seed resolver from labelwatch
```
