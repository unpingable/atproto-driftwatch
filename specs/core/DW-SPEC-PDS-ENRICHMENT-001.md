# DW-SPEC-PDS-ENRICHMENT-001: PDS Enrichment and Hosting-Locus Concentration

> This module stores historical observations of DID→service-endpoint mappings
> over time; it does not treat current resolution as historical truth.

## One-sentence description

Capture historical DID→PDS observations in Driftwatch using Jetstream identity
events plus lazy resolution, then build a read-only hosting-locus concentration
report.

## Purpose

Build a historical enrichment layer that records **DID→PDS endpoint observations
over time**, then use it to answer a narrow question:

> Do existing abuse-adjacent signals cluster non-randomly by hosting locus after
> controlling for base rates?

This is **not** a "bad PDS detector." It is:

- historical infrastructure enrichment
- concentration analysis
- evidence substrate for later infra-trust work

## Why this exists

Current systems can see suspicious accounts, labels, and behavioral clusters.
They cannot reliably answer where those accounts were hosted **when the
suspicious behavior happened**, because DID resolution is time-varying and
current-state lookup rewrites the past.

ATProto identity resolution gives you the current PDS from the DID document's
`#atproto_pds` service entry, but that host can change over time without
changing the DID, especially for `did:plc` migrations. Query-time resolution
rewrites history unless you preserve observations as they happen.

mary-ext's atproto-scraping repo is useful as a rolling host census (~2,973
active PDS instances, ~198 active labelers as of 2026-03-17) but explicitly
drops instances after 14 days of inactivity. Phone book, not chain of custody.

## Existing substrate

Driftwatch already has key pieces in place:

- **Jetstream connection** receives `identity` events (currently dropped by
  `_jetstream_to_event()` which only processes `kind == "commit"`)
- **claim_history** contains `authorDid` for every ingested post — ~3M rows/day,
  ~565k unique DIDs/day, with ~2-3 weeks of clean daily archives on disk
- **Sidecar pattern** proven by labelwatch's discovery_stream.py

The critical insight: identity events are already arriving for free. We're just
throwing them away.

## v1 scope — four deliverables only

1. **Stop dropping `identity` events** — capture them from Jetstream
2. **Persist DID→endpoint observations** — historical, timestamped, sourced
3. **Add lazy/backfill resolution for priority DIDs** — async sidecar
4. **Produce one read-only concentration report** — no policy actions

## Explicit non-goals for v1

- Operator correlation across multiple PDSes
- Enforcement, policy actions, or automated trust decisions
- Reputation scoring or "bad PDS" ontology
- Full historical reconstruction
- Inline resolution in the ingest hot path

## Terminology

Use deliberately boring language:

- **PDS enrichment**: historical DID→endpoint observations
- **hosting locus**: endpoint / hostname / registrable domain used for aggregation
- **concentration sensor**: baseline-adjusted clustering of suspicious signals by
  hosting locus
- **infra-risk subject**: optional later label for an endpoint/domain under
  investigation (v2+)

Avoid: "bad PDS", "malicious host", "infrastructure reputation" — until you have
receipts and a recovery story.

## Architecture

### Components

**A. Identity event capture** — in existing consumer, stop filtering out
`kind: "identity"` events. Record DID→endpoint observations directly.

**B. Resolver sidecar** — async worker for unseen/stale/priority DIDs. Deduped
queue, cached resolution state, rate-limited fetches. Gap-filler, not primary
signal.

**C. Enrichment store** — observation table + resolution cache.

**D. Concentration report** — read-only analytics joining existing suspicious
signals to hosting loci.

### Data flow

1. Event arrives in Driftwatch.
2. If `kind: "identity"`: extract service endpoint, record observation directly.
3. If `kind: "commit"`: extract `authorDid`, submit to enrichment queue if
   unseen/stale.
4. Sidecar resolves DID document out of band.
5. Service endpoint observation recorded with timestamp and metadata.
6. Analytics layer joins suspicious-signal subjects to historical observations.
7. Report computes concentration metrics by endpoint/domain.

### Key principle

**Identity events as primary update signal. Resolver as gap-filler and backfill
mechanism.** This keeps resolution load manageable at firehose speed.

## Data model

### `did_pds_observation`

| Column | Type | Notes |
|--------|------|-------|
| did | TEXT NOT NULL | |
| service_endpoint | TEXT NOT NULL | full endpoint URL |
| endpoint_host | TEXT NOT NULL | hostname extracted |
| registrable_domain | TEXT | e.g. swarmchat.ai from pds.swarmchat.ai |
| observed_at | TEXT NOT NULL | ISO timestamp |
| source | TEXT NOT NULL | `identity_event`, `resolver`, `plc_log`, `manual_seed` |
| doc_fingerprint | TEXT | hash of relevant DID doc fields |
| status | TEXT NOT NULL | `ok`, `no_service`, `fetch_error`, `parse_error` |

Semantics: each row is an **observation**, not an eternal truth claim. Repeated
observations of the same (did, service_endpoint) pair are useful — they confirm
persistence.

### `did_resolution_cache`

| Column | Type | Notes |
|--------|------|-------|
| did | TEXT PRIMARY KEY | |
| last_endpoint | TEXT | |
| last_resolved_at | TEXT | |
| last_fingerprint | TEXT | |
| next_refresh_after | TEXT | |
| last_status | TEXT | |

Purpose: operational cache / refresh scheduler. Not analytical ground truth.

## Resolution policy

### Triggers

- DID first seen (not in cache)
- Cached entry stale (past `next_refresh_after`)
- Identity event received (cheap refresh)
- Manual investigation / backfill request

### Refresh cadence

- Successful resolution: refresh after 24h
- Repeated stable fingerprint: back off to 72h, then 7d
- Failure: exponential backoff with cap
- Identity event: immediate update (no resolver call needed)

### Rate limiting

- Per-host concurrency limits against plc.directory
- Global QPS cap
- Jittered retries
- Dedupe: one DID cannot create N simultaneous fetches

### Storage rule

**Store observations, not ownership.** That one rule prevents a lot of later
self-betrayal.

## Aggregation levels

All analytics must support at least three grouping keys:

1. `service_endpoint` — precise but brittle
2. `endpoint_host` — useful for operational clustering
3. `registrable_domain` — catches multi-endpoint sharding

## First sensor: hosting-locus concentration

### Question

Are suspicious accounts overrepresented on some hosting loci relative to
network baseline?

### Inputs

From Driftwatch:
- Accounts in suspicious behavioral clusters
- Burst cohorts
- Timing-correlation clusters

From Labelwatch (via facts bridge):
- Accounts with negative labels
- Count of distinct labelers per account
- Severity mix

### Normalization controls (must-have)

- Total accounts observed per locus
- Account age distribution
- Activity volume
- Recent account-creation bursts

Without these you'll just rediscover that big hosts have more of everything.

### Initial metrics

- `% observed accounts with negative labels` per locus
- `% observed accounts in suspicious clusters` per locus
- Concentration ratio vs network baseline
- Rolling-window persistence
- New-account burst score
- Endpoint churn score

### Output

Per hosting locus:
- Locus identifier (endpoint / host / domain)
- Baseline population
- Suspicious-signal counts
- Adjusted concentration score
- Persistence over time
- Evidence summary
- Confidence / caveat note

**No automated action in v1.**

## Backfill strategy

Priority queue for resolver backfill:

1. DIDs in current suspicious clusters
2. DIDs with negative labels (from labelwatch)
3. High-activity DIDs
4. Everything else (background, best-effort)

Historical archive (~2.4GB compressed JSONL, ~565k unique DIDs/day) provides
the candidate set. One month of clean data is sufficient to bootstrap.

## Use of mary-ext dataset

Allowed:
- Bootstrap known endpoint inventory
- Annotate open-registration / version / online status
- Diff host appearance/disappearance
- Sanity-check whether a host is isolated or common

Not allowed:
- Authoritative DID→endpoint history
- Sole evidence of hosting attribution
- Trust score input without corroboration

## Validation plan

### Test buckets

- One-user personal PDSes (should not flag)
- Open-registration third-party PDSes (may flag — is that signal or structure?)
- Bluesky-hosted endpoints (baseline)
- Known noisy but benign communities (must not false-positive)
- Reported suspicious hosts (e.g. pds.swarmchat.ai)

### Questions to answer

- Does the sensor just rediscover "open registration exists"?
- Do large hosts dominate raw counts but not normalized rates?
- Does migration create false concentration?
- Do results persist across windows or vanish as noise?
- Can manual inspection explain flagged cases?

### Success condition

The first win is **useful triage**, not truth with a crown on it.

## Milestones

### M1 — Identity capture and storage

- Driftwatch ingests Jetstream `identity` events (stop dropping them)
- `did_pds_observation` and `did_resolution_cache` tables exist
- Identity events produce observation rows
- Schema migration in `db.py`

### M2 — Resolver sidecar

- Async resolver worker with queue, dedupe, rate limiting
- Resolves unseen/stale DIDs from commit events
- Backfill mode for priority DIDs from existing archives
- Hard rate limits and exponential backoff

### M3 — Baseline analytics

- Endpoint/host/domain rollups
- Unresolved-rate tracking
- Endpoint churn view
- "How many accounts have we observed per hosting locus?"

### M4 — Concentration report

- Join suspicious signals to hosting loci
- Baseline-adjusted concentration metrics
- Read-only analyst report
- Manual spot-checks on flagged loci

### M5 — Validation pass

- False-positive review against test buckets
- Sensitivity notes
- Threshold recommendations or refusal to threshold
- Documented limits
- Decision: continue, refine, or stop

## Acceptance criteria (v1 complete)

- Driftwatch ingests and stores Jetstream `identity` events
- DID→endpoint observations are persisted with timestamp and source
- Resolver sidecar supports lazy resolution for unseen/stale/priority DIDs
- Existing suspicious-cluster DIDs can be queued for backfill
- A report exists showing suspicious-signal concentration by endpoint/host/domain
- No automated trust or enforcement decisions are made from this data in v1

## Risks

1. **History rewriting** — sloppy refresh semantics mutate the past by accident
2. **Base-rate stupidity** — big hosts look "worse" simply because they are big
3. **Ontology creep** — people try to turn "concentration" into "bad actor" too
   early
4. **Resolver pain** — identity fetches become expensive if caching is dumb
5. **Capture risk** — the moment this smells like infra trust, you've recreated
   soft centralization politics

## Open questions

1. Should host annotations (open-registration status, version) be a separate
   table or columns on `did_pds_observation`?
2. What historical backfill window justifies the resolver cost?
3. Do we need separate "endpoint" and "service authority" subjects later?
4. When, if ever, does this graduate from concentration analysis to attestation?
5. Can PLC operation log polling supplement identity events for better coverage?

## Filed

2026-03-17. Triggered by user reports of suspicious activity from
pds.swarmchat.ai (open-registration third-party PDS).
