# driftwatch — Dataflow

**Status**: historical starter architecture, originally recorded 2026-04-28.

The diagrams below retain historical component context. Current operator
procedures are in [`../RUNBOOK.md`](../RUNBOOK.md), and current coverage and
recovery claims are defined in [`../../DATA-CONTRACT.md`](../../DATA-CONTRACT.md).
Background features depend on explicit activation; this document is not a
record that they are running or accepted. The old Labelwatch facts bridge is
unconsumed, and ingestion does not schedule DID document resolution.

## The job

This doc traces how data moves through driftwatch — Jetstream to claim_history to cluster analysis to surfaced outputs. Each stage names its inputs, outputs, gates, and where it can fail.

For concept-to-code mapping, see `../../THEORY_TO_CODE.md`. For component detail, see CLAUDE.md.

## The pipeline (end-to-end)

```
[Jetstream] → consumer → fingerprint → claim_history → sensor array → cluster reports
                                              │              │
                                              │              ├─→ drift detection (longitudinal)
                                              │              ├─→ recheck queue
                                              │              └─→ facts export → labelwatch
                                              │
                                              └─→ maintenance + retention loops
```

## Stage 1 — Jetstream ingestion

**Job**: receive ATProto post and repost events.

**`consumer.py`** opens a WebSocket to `wss://jetstream2.us-east.bsky.network/subscribe` filtered to `app.bsky.feed.post` and `app.bsky.feed.repost` collections. Its cursor checkpoints the completed source prefix with replay overlap. A source may no longer retain the requested history; restart does not prove continuous coverage.

**Queue architecture**: reception admits events to a bounded queue. Durable processing advances the completed prefix transactionally. Backpressure, unresolved work and a failed drain must remain visible; queue capacity is not permission to silently discard received events.

**Drop-awareness**: drop count and `drop_frac` tracked. Reports condition outputs on observed loss — *outputs are not ground truth, they are loss-conditioned views*. See `FAILURE_MODES.md` (degraded sampling, 2026-04-23..).

**Outputs**: events parsed into `claim_history` rows.

## Stage 2 — Fingerprinting

**Job**: turn post text into a stable cluster ID.

**Pipeline** (`fingerprint.py`, `claims.py`, `drift/extract.py`):

```
post text
   │
   ▼
extract spans, entities, quantities, URLs, embeds, facets
   │
   ▼
apply precedence:
   multi-word entity > quantity+context > URL domain >
   single-word entity > spans > normalized text
   │
   ▼
fingerprint hash + fp_kind ∈ {entity, quantity, domain, span, text}
```

**Year exclusion**: bare 19xx/20xx stripped from quantity extraction (treadmill avoidance).

**Entity stopwords**: English function words (That/This/They/etc) — language-agnostic, not curated treadmill.

**Complexity gate**: `MIN_CLAIM_ALPHA_TOKENS=3`, or surviving entity/URL/embed/facet. Below this, posts skip fingerprinting (single-word noise).

**Versioning**: `FP_VERSION` is a contract. Changing it = migration. Transition periods emit both old and new fingerprints (audit only, never live).

**fp_kind**: returned as part of the fingerprint result; surfaces as a STATS canary if `unknown` exceeds 5%.

**Outputs**: `claim_history` rows with `(fingerprint, fp_kind, fingerprint_version, evidence_hash, observed_at, createdAt, did, ...)`. Append-only. `observed_at` is the trusted timestamp; `createdAt` is from the firehose and may be wrong.

## Stage 3 — Cluster analysis (sensor array)

**Job**: compute structural signals over claim clusters.

**Per cluster** (`detection.py`, `driftmetrics.py::cluster_report()`):

- author/post ratio
- variant entropy
- half-life (peak → decay; needs ≥8 hourly bins)
- burst score (z-score volume vs rolling baseline)
- synchrony score
- single-author detection (`likely_automation` if 1 author + ≥10 posts)
- evidence class (none/link/embed/facet/mixed)
- fp_kind distribution

**Sensor array architecture**: kill switch (`SENSOR_ARRAY_ENABLED`), per-sensor disable list (`SENSOR_DISABLED_IDS`), capabilities envelope (`get_capabilities()` → `/health/extended`), cooldown filter (re-emit on severity escalation or score Δ ≥ `COOLDOWN_SCORE_DELTA`).

**Platform health gate**: `platform_health.py` — EWMA baseline, WARMING_UP→OK↔DEGRADED state. Hard-gates rechecks during incomplete data periods.

**Outputs**: cluster reports (HTML/JSON), drift findings.

## Stage 4 — Longitudinal drift detection

**Job**: detect claim mutation over time.

**`longitudinal.py`** + `drift/diff.py` + `drift/rules.py` compute deltas across versions of the same fingerprint. Rules: assertiveness increase, provenance laundering, evidence drop, author shift.

**Recheck queue** (`recheck_queue.py`): capped at 10k entries, functions as **rolling hotset**, not a backlog. DB trigger trims oldest when cap exceeded. Singleton gate prevents one-off posts from entering — fingerprints must be multi-author OR contain links OR be in a reply thread. Queue depth pinned at 10k is normal.

**Enqueue gate** (`db.py::_fp_passes_enqueue_gate()`): platform health + singleton gate. If platform health is DEGRADED, enqueue is suppressed (rechecks during incomplete data are misleading).

**Receipts**: each rule firing produces a `label_decisions` ledger row with `(rule_id, fingerprint_version, inputs_json, evidence_hashes_json, config_hash, decision_trace)`. Receipts on commit and on expiry.

## Stage 5 — Maintenance + retention

**Job**: keep the database from eating the disk.

**Maintenance loop** (`maintenance.py`, `ENABLE_MAINTENANCE=1`, 6h default):

- label expiry (`expire_labels_by_ttl`, 30d soft expire)
- disk monitoring (warn 85%, emergency brake 92%, `.disk_pressure` flag file)
- claim_history growth stats

**Retention loop** (`retention.py`, `ENABLE_RETENTION=1`, 1h default):

- `events.raw` NULLed after 24h (biggest disk win)
- `events` rows deleted after 7d
- `edges` after 14d
- `event_versions` after 7d
- `claim_history`: archive to `data/archive/claim_history/YYYY-MM-DD.jsonl.gz`, then DELETE after 14d

Archive keys on `COALESCE(observed_at, createdAt)` — trusted timestamp, not firehose timestamp.

**Disk pressure brake**: when `.disk_pressure` flag exists, consumer pauses event processing. Maintenance runs first; ingest stays paused until pressure relieves.

## Stage 6 — Identity resolution + facts export

**Historical job**: resolve DIDs to handles and PDS hosts for an earlier Labelwatch bridge. No current bridge consumer or automatic resolver is scheduled by ingestion.

**Resolver helpers** (`resolver.py`) are unscheduled. They must not block the queue drainer. A future named enrichment consumer requires an explicit bounded lifecycle and shutdown contract; cancellation of an asyncio await does not stop a synchronous executor callable.

**`actor_identity_current`**: fieldwise reducer from `identity_events` (append-only journal). MISSING sentinel distinguishes "absent" from "explicitly cleared." Newer events never erase prior known fields. Each row tracks `identity_source`: `live`, `labelwatch_seed`, or `both`.

**Historical facts export** (`facts_export.py`) retains the earlier SQLite sidecar implementation. Current Labelwatch does not consume it. Its presence does not establish an active export, retention obligation or accepted recovery path.

## Stage 7 — Reports + bridge

**Cluster reports**: native report and summary functions filter an authored-time window. They require an admitted resource budget and truthful source coverage. A short requested window alone does not prove an instant query or current source data. See the canonical runbook for the time-range query requirement and private report limitations.

**Drift findings**: rules-fired-this-window summaries, surfaced as report cards. Decisions land in `label_decisions` ledger regardless of emit mode (sealed-lab posture).

**No public ATProto emission** at Stage 0–1. See `PUBLIC_SURFACES.md`.

## Populations (always specify denominator)

Coverage claims must specify which population:

- **Live-observed** — DIDs seen in Jetstream identity/account events.
- **Labeled-target seed** — DIDs imported from labelwatch.
- **Overlap** — DIDs in both.

"0.2% coverage" was a real bug from comparing 40k resolved actors against 2M label events. The denominator was wrong.

## STATS heartbeat

The consumer emits a periodic STATS line:

```
coverage=X% health=Y baseline_eps=Z lag=Ws
```

Plus per-rule activations, queue depth, drop_frac, fp_kind distribution. Used to detect degraded states quickly.

## Cross-reference

- `OVERVIEW.md` — system map.
- `SIGNAL_MODEL.md` — what the cluster signals mean.
- `PUBLIC_SURFACES.md` — what gets surfaced and what doesn't.
- `FAILURE_MODES.md` — what can go wrong at each stage.
- `../../THEORY_TO_CODE.md` — concept → file map.
- `../../CLAUDE.md` — architecture rules + file inventory.
- `../JETSTREAM_INGEST_REALITIES.md` — Jetstream-specific operational notes.
- `../RUNBOOK.md` — operator procedures.
