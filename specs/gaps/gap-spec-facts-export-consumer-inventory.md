# Facts-export consumer inventory + Phase 2.5 migration shape decision

> **Status: scoping document.** Filed 2026-06-10 in response to the
> cold-path Parquet/DuckDB phase plan. This document is the **Phase 2
> consumer inventory** required before the Phase 2.5 (A vs B) migration
> shape decision; the decision itself is scoped here so the next slice can
> ratify or amend it without a second discovery pass.
>
> Composes with:
> - `gap-spec-log-structured-artifact-system.md` (phase plan; this is Phase 2 of 7)
> - `gap-spec-facts-export-duckdb-productionization.md` (Phase 3 implementation gap)
> - `gap-spec-cold-path-parquet-duckdb.md` (umbrella doctrine)
> - `labelwatch/specs/core/HOSTING-LOCUS-DATA-CONTRACT.md` (the data contract this inventory characterizes)

## Why this document exists

The cold-path phase plan blocks Phase 3 (DuckDB-backed facts export) on a
Phase 2.5 decision: does Phase 3 produce **(A)** a compatibility
`facts.sqlite` snapshot for labelwatch to ATTACH unchanged, or **(B)** a
direct DuckDB/Parquet artifact that labelwatch learns to read natively?

Either path requires knowing exactly **what labelwatch reads from
`facts.sqlite` today**: which tables, which columns, what query shapes,
what freshness contract. Phase 2 is that knowing-step; this is the
artifact.

## What labelwatch actually reads from facts.sqlite

### Table 1 — `actor_identity_facts`

**Producer:** `driftwatch/src/labeler/facts_export.py` line 64–74.
Full schema:

```sql
CREATE TABLE actor_identity_facts (
    did                        TEXT PRIMARY KEY,
    handle                     TEXT,
    pds_endpoint               TEXT,
    pds_host                   TEXT,
    resolver_status            TEXT,
    resolver_last_success_at   TEXT,
    is_active                  INTEGER,
    identity_source            TEXT     -- added via _ensure_actor_identity_facts_schema
);
```

**Consumer:** `labelwatch/src/labelwatch/hosting.py`.

**Columns actually read by labelwatch** (verified by grep `aif\.\w+`):

| Column | Used by labelwatch | Used for |
|---|---|---|
| `did` | ✓ | JOIN key on `le.target_did = aif.did` |
| `handle` | ✓ | display in per-host drilldown |
| `pds_host` | ✓ | provider classification, hosting locus distribution |
| `resolver_status` | ✓ | coverage caveats (unresolved/active filter) |
| `pds_endpoint` | ✗ | currently unused by any labelwatch query |
| `resolver_last_success_at` | ✗ | currently unused |
| `is_active` | ✗ | currently unused (read inside `query_population_comparison` but only via SELECT 1 sanity probe) |
| `identity_source` | ✗ | currently unused |

**Query shapes** (`hosting.py` line refs):

```sql
-- Coverage sanity probe (every consumer)
SELECT 1 FROM drift.actor_identity_facts LIMIT 1                       -- L118, L303, L388, L496

-- Population denominator
SELECT COUNT(*) FROM drift.actor_identity_facts                        -- L517
SELECT COUNT(*) FROM drift.actor_identity_facts WHERE pds_host = ?     -- variants

-- Hosting locus join (the load-bearing pattern)
SELECT … FROM label_events le
LEFT JOIN drift.actor_identity_facts aif ON aif.did = le.target_did    -- L133, L245, L319, L407, L526, L551, L561

-- Host-family enumeration
SELECT DISTINCT pds_host FROM drift.actor_identity_facts               -- L505
```

**Freshness contract (observed, not specified):**
- `_sync_driftwatch_facts` in `labelwatch/scan.py` checks the mtime of `facts.sqlite` and refuses to derive if stale (line 687, 698).
- Reports surface a coverage caveat ("facts not attached" / "no facts data") when the table is missing or empty — they do NOT 5xx.
- Hosting locus produces `snapshot=0` (descriptive zero, not error) when no rows JOIN.

### Table 2 — `uri_fingerprint`

**Producer:** `driftwatch/src/labeler/facts_export.py` line 41–47.

```sql
CREATE TABLE uri_fingerprint (
    post_uri      TEXT PRIMARY KEY,
    fingerprint   TEXT,
    created_epoch INTEGER,
    rowid_src     INTEGER
);
CREATE INDEX idx_uri_fp ON uri_fingerprint(fingerprint);
```

**Consumer:** `labelwatch/src/labelwatch/scan.py` (the derive pass).

**Columns actually read by labelwatch:**

| Column | Used by labelwatch | Used for |
|---|---|---|
| `post_uri` | ✓ | JOIN against labelwatch's candidate URIs |
| `fingerprint` | ✓ | stored into `derived_label_fp.claim_fingerprint` |
| `created_epoch` | ✓ | computes `lag_sec_claimed` (post→label latency) |
| `rowid_src` | ✗ | producer-internal pagination key, never read |

**Query shape** (`scan.py:792–797`):

```sql
CREATE TEMP TABLE tmp_drift_fp AS
SELECT uf.post_uri, uf.fingerprint, uf.created_epoch
FROM drift.uri_fingerprint uf
JOIN tmp_candidate_uris c ON c.uri = uf.post_uri;
```

The JOIN is bounded by labelwatch's candidate URIs (i.e., URIs that have
label_events without an existing fingerprint). After the temp materializes,
labelwatch `DETACH`es drift immediately so inode rotation can happen
without pinning (see scan.py:801–814 for the careful detach dance).

**Freshness contract:** labelwatch only re-derives when both (a) the
facts.sqlite mtime is fresh and (b) there are candidate URIs to look up.
Stale facts produce derive-pass=0 rows, surfaced as a freshness caveat.

### Tables NOT read by any labelwatch query

Driftwatch's `facts_export` produces five tables; labelwatch reads only
two of them. For completeness:

| Table | Producer purpose | Currently read by labelwatch? |
|---|---|---|
| `actor_identity_facts` | DID → host mapping | ✓ (hosting.py) |
| `uri_fingerprint` | post URI → claim fingerprint | ✓ (scan.py derive) |
| `fingerprint_hourly` | bucketed claim volume | ✗ |
| `fingerprint_bounds` | per-fingerprint first/last/total | ✗ |
| `meta` | export config + cursor state | ✗ |

`fingerprint_hourly` and `fingerprint_bounds` are presumably used by
driftwatch's own reports / NQ; this inventory only certifies the
**labelwatch consumer**. The phase 2.5 decision should classify them
("dropped from Phase 3 output", "preserved for driftwatch-side reads", or
"deferred until first external consumer surfaces").

## Surface area summary

```
Tables read by labelwatch:        2 of 5    (actor_identity_facts, uri_fingerprint)
Columns read from those tables:   7 of 12   (4 of 8 from identity, 3 of 4 from uri_fp)
Distinct query shapes:            ~8        (3 identity coverage + 4 identity join + 1 uri_fp join)
Hot-path freshness contract:      mtime-gated; empty-result-acceptable; no 5xx on missing
```

The consumer footprint is **small and well-localized** (`hosting.py` for
identity, `scan.py` for fingerprint). The Phase 3 producer doesn't have to
reproduce all five tables for the consumer to be served — only the
identity facts + uri_fingerprint slices, with the columns enumerated above.

## Phase 2.5 — Migration shape decision

With the inventory above, the Phase 2.5 (A vs B) trade-off becomes
concrete enough to ratify.

### Option A — Compatibility `facts.sqlite` snapshot

Phase 3 produces a fresh `facts.sqlite` at retention time by reading
Parquet (via DuckDB) and writing SQLite. Labelwatch consumes it
**unchanged** via the existing ATTACH path.

**Pros:**
- Zero labelwatch code change. Read path is byte-identical to today.
- Failure modes are familiar (stale mtime, empty rows, missing file).
- Easy rollback: turn off the DuckDB writer; labelwatch falls back to the
  last good `facts.sqlite` until it ages out.
- The two tables labelwatch reads + the three it doesn't ALL get
  reproduced cheaply — no consumer triage needed.

**Cons:**
- We're still writing a SQLite database. Doctrine ("past the SQLite
  boundary, don't write a database; write the storage shape your workload
  keeps trying to become") points away from this.
- Two storage formats in flight: hot SQLite, Parquet for past, snapshot
  SQLite for derived. The snapshot SQLite is a third state to track.
- The labelwatch consumer's freshness contract stays implicit (mtime
  comparison), not explicit (manifest version + receipt).

**Implementation cost (estimated):**
- DuckDB-backed writer: reads Parquet partitions, INSERTs into a fresh
  SQLite. ~1 small module, atomic rename pattern already shipped for
  retention.
- No labelwatch changes.

### Option B — Direct Parquet/DuckDB consumption

Phase 3 produces Parquet artifacts; labelwatch learns to read them via
either embedded DuckDB or a small projection module.

**Pros:**
- Doctrine alignment: SQLite for present, Parquet for past, DuckDB for
  questions.
- One canonical artifact shape; no compatibility-snapshot copy.
- Explicit custody (manifests + receipts) at the consumer boundary.
- Sets the consumption pattern that future consumers (NQ, external
  observers) inherit; the next consumer is cheaper.

**Cons:**
- Labelwatch needs a new read path. The two query patterns above must be
  reproduced (per-DID JOIN for hosting locus; per-URI JOIN for derive pass).
- DuckDB embedded in labelwatch is a new runtime dependency (~50 MB
  binary, but well-bounded).
- The `tmp_candidate_uris` JOIN against `drift.uri_fingerprint` is the
  trickiest pattern — it requires writing labelwatch-side temp data and
  reading Parquet in the same query. DuckDB handles this (it can JOIN
  across attached SQLite + Parquet), but failure modes are new.

**Implementation cost (estimated):**
- Labelwatch side: replace `attach_facts(...)` with `attach_parquet(...)`
  helper that uses DuckDB to expose `drift.actor_identity_facts` and
  `drift.uri_fingerprint` as views over Parquet partitions. ~1 small
  module, plus updates to all `hosting.py` callsites (use the helper) and
  `scan.py` (replace the ATTACH dance with a DuckDB temp join).
- Driftwatch side: cap the Phase 3 producer at the consumer slice
  (identity + uri_fp). Leave the other three tables either deprecated or
  driftwatch-only.

### Recommendation (not ratification — for the user to decide)

**Path A** is the smaller blast radius. It ships Phase 3 without
touching labelwatch. The doctrinal cost (writing another SQLite) is
tolerable because the snapshot is **derived from Parquet** — the
authoritative artifact is still Parquet; the SQLite is a cache. That
framing keeps the doctrine without forcing the consumer rewrite.

**Path B** is the doctrinally cleaner long-term move and the right
shape for the third+ consumer. But it bundles two changes (producer
rewrite AND consumer rewrite) into one slice. If labelwatch's derive
pass hits an unexpected DuckDB issue, both observatories are blocked.

**Suggested sequencing:** ship A as Phase 3, learn from it, then propose
Path B as Phase 3.5 once we've watched the snapshot writer behave for
a couple of cycles. Path B is filed but not started.

This recommendation is non-binding. The user can ratify A, B, or "A now,
B as standing follow-up." All three are valid.

## What this document does NOT do

- Does not implement either path. Phase 3 is the next slice once 2.5 is
  ratified.
- Does not change the labelwatch data contract (`HOSTING-LOCUS-DATA-CONTRACT.md`).
- Does not drop the three unused tables from facts_export — that's
  separately staffable once Phase 3 ships.
- Does not commit to a manifest format for facts artifacts. Path B would
  need one; Path A inherits the implicit mtime contract.
- Does not address the bogus-timestamp leak from Phase 1 (claim_history
  Parquet capture). That leak propagates into uri_fingerprint reads if
  Path B is chosen; Path A inherits whatever the snapshot writer chooses
  to filter.

## Open questions parked for Phase 3

1. **Retention of the compatibility snapshot (if Path A).** How long does
   each snapshot live before expiring? Hourly cycle? Daily? Mtime-only?
   Manifest-tracked?
2. **DuckDB vs pyarrow for the writer (if Path A).** DuckDB can read
   Parquet + write SQLite in one process; pyarrow + sqlite3 is two
   libraries. Trade-off: DuckDB's footprint vs the operational simplicity
   of a single SQL surface.
3. **JOIN against labelwatch's temp candidate URIs (if Path B).** DuckDB
   supports cross-source JOIN, but the scan.py:792-814 dance assumes
   ATTACH semantics. Adapting this is more nuanced than the per-DID
   identity lookup.
4. **Bogus timestamp policy.** Phase 1 carries a known leak of ~25k
   bogus-year rows per maintenance log. Does Phase 3 quarantine those at
   the writer (uri_fingerprint excludes year < 2020), or pass through
   and let consumers decide? Labelwatch's lag calc would treat a 1997
   created_epoch as 25 years of lag — silently wrong, not loudly wrong.

## Acceptance for this scoping slice

- [x] Consumer inventory: which tables, which columns, which queries — concrete from grep + code reads, not inferred.
- [x] Phase 2.5 (A vs B) trade-off named with implementation cost estimates.
- [x] Recommendation surfaced but not ratified.
- [x] Open questions for Phase 3 enumerated.

The user can ratify (or amend) Path A/B without a second discovery pass.

## Ratification — 2026-06-10

**Decision: A now → B later, with bogus-timestamp quarantine pulled into A.**

Phase 3 produces a compatibility `facts.sqlite` snapshot from Parquet via
DuckDB. Labelwatch consumes it unchanged through the existing ATTACH path.
Path B (direct Parquet/DuckDB consumption by labelwatch) is filed as
Phase 3.5 / standing follow-up after snapshot writer behavior is observed.

### Doctrine framing (load-bearing)

> Parquet is authoritative past.
> DuckDB is the question engine.
> `facts.sqlite` is a compatibility projection/cache, not the source of custody.

This phrasing matters. Without it, a future re-read of Phase 3 will mistake
the snapshot for canonical state and the cold-path doctrine slips. Cite this
framing in the Phase 3 slice spec and the writer module's header.

### Amendment: bogus-timestamp quarantine is in-scope for Phase 3

Open question #4 (bogus timestamp policy) is **resolved at the writer
boundary, not deferred**. Phase 1 carries ~25k bogus-year rows; labelwatch's
lag calc would treat a 1997 `created_epoch` as 25 years of lag — silently
wrong, not loudly wrong. Silent wrong is the failure mode the doctrine is
trying to prevent.

Policy:

```
valid_created_epoch:
  >= 2020-01-01
  <= generated_at + 1 day
```

Rows outside the band are excluded from `uri_fingerprint` and counted in
the receipt. No silent pass-through.

### Why A and not B (preserved for review)

- Inventory shows labelwatch reads 2 of 5 facts tables, 7 of 12 columns,
  with the consumer footprint localized to `hosting.py` and `scan.py`.
  Small surface, well-localized — exactly the shape where the compatibility
  snapshot is cheap and the consumer rewrite is the optional follow-up.
- B bundles producer migration + consumer migration + new runtime failure
  modes + the `tmp_candidate_uris` JOIN rewrite. Shipping all four in one
  slice means a DuckDB issue blocks both observatories.
- The `tmp_candidate_uris` JOIN against `drift.uri_fingerprint` is the
  Path B site to learn cheaply about. Phase 3.5 spike targets that JOIN
  first; if it works, identity facts is paperwork; if it doesn't, we
  learned at low cost.

### What this ratification does NOT authorize

- No direct labelwatch DuckDB dependency.
- No `scan.py` rewrite.
- No drop of the three currently-unread tables (`fingerprint_hourly`,
  `fingerprint_bounds`, `meta`) without a separate decision. Preserve only
  if cheap; explicit decision required to drop.
- No new labelwatch consumer contract.

### Phase 3 slice handle

Filed as a sibling spec: see `gap-spec-facts-export-duckdb-snapshot-001.md`
for scope, manifest fields, acceptance tests, and non-goals. That spec
inherits this ratification verbatim.

### Open questions parked (still parked, narrowed)

1. Retention of compatibility snapshot — still open, default to atomic
   rename + last-good-on-disk semantics until a manifest cadence is
   needed.
2. DuckDB vs pyarrow writer — defer to Phase 3 implementer; either is
   acceptable as long as the manifest is emitted.
3. `tmp_candidate_uris` JOIN — parked for Phase 3.5, not blocking Phase 3.
4. Bogus timestamp policy — **resolved above**, no longer open.
