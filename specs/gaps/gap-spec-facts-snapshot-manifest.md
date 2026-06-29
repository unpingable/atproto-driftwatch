# Gap spec: facts snapshot custody / freshness manifest

**Status:** gap-spec, doc-only. **Not authorized for implementation.**
**Filed:** 2026-06-29
**Component:** `src/labeler/facts_export.py` → `facts.sqlite` sidecar (the
unidirectional Driftwatch → Labelwatch data bridge).
**Scope discipline:** this is a **separate border** from the resolver backlog lane
(`gap-spec-resolver-backlog-lane.md`). The backlog lane answers *resolver work
scheduling*; this manifest answers *artifact custody / freshness / admissibility*.
They touch the same `actor_identity_facts` projection but must not be conflated or
implemented together. This spec is also **not** the Parquet/DuckDB cutover
(`gap-spec-cold-path-parquet-duckdb.md`); the manifest is a small receipt that the
compatibility `facts.sqlite` should carry **now**, independent of that migration.

## Problem

Labelwatch ATTACHes `facts.sqlite` read-only and depends on it for hosting-locus
enrichment of labeled targets. The bridge currently ships **without a manifest**: the
consumer cannot tell, from the artifact alone, how fresh the snapshot is, what schema
it speaks, how much of the labeled-target population is actually resolved (the
denominator), or what the snapshot is structurally unable to testify to. That is how a
green coverage number commits perjury — the consumer renders "hosting locus" without
the freshness / coverage / refusal context that makes it honest.

Driftwatch already states the relevant refusals in prose (current PDS host is **not**
historical PDS-at-label-time; host family is **not** operator identity; coverage must
state a denominator). The gap is that these refusals live in READMEs, not in the
artifact. READMEs are where nuance goes to nap.

## Non-goals

- **Not** the Parquet/DuckDB canonical-artifact migration. Short term: keep the
  compatibility `facts.sqlite`; just add the manifest beside it.
- **Not** a schema change to the existing facts tables (`uri_fingerprint`,
  `fingerprint_hourly`, `fingerprint_bounds`, `actor_identity_facts`). The manifest is
  a **sidecar receipt**, not a column change — so it is not itself a data-contract
  break.
- **Not** a resolver behavior change, **not** a Labelwatch rendering change (rendering
  is a downstream follow-on, named below but out of scope here).

## Proposed manifest

A small receipt-like artifact written atomically beside each `facts.sqlite` snapshot
(e.g. `facts.manifest.json`), produced by `facts_export.py` as the final step of the
existing atomic-snapshot swap:

```json
{
  "artifact": "facts.sqlite",
  "produced_at": "2026-06-29T00:00:00Z",
  "producer": "atproto-driftwatch",
  "schema_version": "<facts contract version>",
  "snapshot_hash": "<sha256 of the swapped file>",
  "source_db_high_water": "<max observed_at / cursor at export time>",
  "actor_identity_rows": 0,
  "uri_fingerprint_rows": 0,
  "resolver_pending_total": 0,
  "resolver_pending_gt_72h": 0,
  "resolver_pending_gt_168h": 0,
  "oldest_pending_hours": 0,
  "coverage": {
    "live_observed_actors":  {"resolved": 0, "total": 0},
    "labelwatch_seed_actors":{"resolved": 0, "total": 0},
    "both":                  {"resolved": 0, "total": 0}
  },
  "cannot_testify": [
    "historical_pds_at_label_time",
    "operator_identity",
    "account_intent",
    "global_hosting_truth"
  ]
}
```

The `cannot_testify` list is **load-bearing**, not decoration. It encodes Driftwatch's
standing refusals into the artifact so the consumer can render them verbatim:

- `historical_pds_at_label_time` — current resolver state ≠ where the DID was hosted
  when labeled (Driftwatch rake #7). The single most over-read caveat; it is *the*
  caveat for any hosting-locus view.
- `operator_identity` — host family ≠ who operates the infrastructure (rake #2).
  Co-location implies neither coordination nor culpability.
- `account_intent`, `global_hosting_truth` — the snapshot is a partial, point-in-time
  projection, not a census.

The `coverage` block carries a **denominator per population**, honoring the Driftwatch
invariant that "0.2% coverage" was a real bug from a missing denominator. Resolver
aged-tail metrics (`resolver_pending_*`, `oldest_pending_hours`) are included so the
consumer can see when enrichment is running behind — directly tied to the aged-tail
coverage defect this whole thread tracks.

## What the consumer can then surface (follow-on, out of scope here)

Documented so the manifest's purpose is legible; **not** authorized by this spec:

```
Facts snapshot fresh as of <produced_at>.
Coverage: <X>/<Y> labeled-target actors resolved.
Cannot testify: historical PDS at label time; operator identity.
```

and, in hosting-locus cards, the honest sentence instead of the sexy one:

> "Labeler X labeled DIDs that currently resolve to host family Y under partial
> resolver coverage" — **not** "Labeler X targeted host Y."

## Open questions

1. **Hash scope:** hash the swapped `facts.sqlite` bytes, or a content digest over the
   logical rows (stable across VACUUM / page reordering)? Byte hash is cheaper;
   content digest is more meaningful for "did the data change."
2. **High-water definition:** `max(observed_at)`, the Jetstream cursor, or both?
   Needs to be a value Labelwatch can compare against wall-clock to compute staleness.
3. **Coverage denominators:** the `total` for each population — is it the count in
   `actor_identity_current` by `identity_source`, or the labeled-target roster
   Labelwatch seeded? They differ; the manifest must say which.
4. **Failure surfacing:** if the manifest write fails, does the snapshot swap abort
   (manifest is part of the atomic unit) or proceed with a stale/absent manifest? Lean
   abort — a snapshot without a manifest is an untestable artifact.
5. **Versioning:** does `schema_version` track the facts table contract, the manifest
   shape, or both (two fields)?

## Recommended first implementation slice

Emit the manifest **read-only alongside** the existing export with the cheap fields
first (`produced_at`, `producer`, `schema_version`, `snapshot_hash`, row counts,
`cannot_testify`) and make the manifest write part of the atomic swap unit. Add the
`coverage` block second, once open question #3 (denominators) is ratified — that is the
field most likely to mislead if computed against the wrong denominator.
