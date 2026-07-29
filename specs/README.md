# driftwatch — Specs

Normative, authoritative documents. The decision rule is borrowed from agent_gov's `DOC_TAXONOMY`:

> **Could implementation be judged wrong by this document?**
>
> If yes → here. If no → `docs/`.

## Layout

```
specs/
  core/         shipped canonical specs (architecture, protocol, invariant)
  gaps/         explicit backlog — things we know are missing
  research/     non-committed lines of inquiry — not created yet (no dir on disk)
```

## core/

| File | What it specifies |
|------|-------------------|
| `ADMISSIBILITY-PROTOCOL.md` | Procedural spec: required steps for taking observed cross-population skews from "interesting pattern" to "publishable claim or honestly retired" |
| `DW-SPEC-PDS-ENRICHMENT-001.md` | PDS-side enrichment behavior — resolver scope, identity provenance, not-a-crawler discipline |

## gaps/

Full index re-synced 2026-07-29 (the table had drifted to 2 of 20 entries).
Grouped by family; all are candidate/backlog unless a status line inside says
otherwise.

**Storage, cold path, and artifacts**

| File | Gap |
|------|-----|
| `gap-spec-storage-layout-labelwatch-driftwatch.md` | Cross-project storage layout decisions |
| `gap-spec-off-host-backup-labelwatch-driftwatch.md` | Off-host backup posture (depends on storage layout) |
| `gap-spec-cold-path-parquet-duckdb.md` | Cold-path architecture: SQLite=present, Parquet=past, DuckDB=questions |
| `gap-spec-cold-path-phase-3.5-forward-parquet-capture.md` | Forward Parquet capture of claim_history at retention time |
| `gap-spec-cold-path-update-2026-05-07.md` | Dated evidence + tripwire status update to the cold-path spec |
| `gap-spec-log-structured-artifact-system.md` | Log-structured artifact system (manifests as custody) |

**Facts export (labelwatch bridge)**

| File | Gap |
|------|-----|
| `gap-spec-facts-export-consumer-inventory.md` | Consumer inventory + Phase 2.5 migration shape decision |
| `gap-spec-facts-export-duckdb-productionization.md` | Productionize the DuckDB facts export |
| `gap-spec-facts-export-duckdb-snapshot-001.md` | Snapshot-001: Parquet-backed compatibility facts.sqlite |
| `gap-spec-facts-snapshot-manifest.md` | Custody / freshness manifest for facts snapshots |
| `gap-spec-facts-snapshot-scale-containment.md` | Attempt-1 OOM postmortem + attempt-2 containment design |

**Health, witnessing, and observability**

| File | Gap |
|------|-----|
| `gap-spec-self-health-contract.md` | Exporter-side typed self-health signals + NQ evidence-loss finding (named 2026-07-13) |
| `gap-spec-witness-coverage-requirements.md` | What "full" external witness coverage means — host/APM split, incident-mapped acceptance (2026-07-28) |
| `gap-spec-observatory-read-instrumentation.md` | Read classification + cohort forecast instrumentation |
| `gap-spec-resolver-backlog-lane.md` | Resolver aged-tail drain lane |

**Data integrity and admissibility**

| File | Gap |
|------|-----|
| `gap-spec-single-writer-invariant.md` | Single-writer invariant for labeler.sqlite |
| `gap-spec-event-time-hygiene.md` | Claimed vs observed time (named 2026-07-13; schema change) |
| `gap-spec-bounded-specimen-export.md` | Bounded read-only specimen export (named 2026-07-13) |
| `gap-spec-formal-claim-admissibility-pipeline.md` | Z3-then-Lean claim-admissibility gate (candidate architecture) |

**Other**

| File | Gap |
|------|-----|
| `gap-spec-atproto-labeler-backport.md` | Backport plan for the reference atproto-labeler implementation |

## Adding a new spec

1. Apply the rule. If the doc could be a basis for "the implementation is wrong against this," it's a spec.
2. Place it: `core/` if shipped, `gaps/` if explicit backlog, `research/` if speculative.
3. Update this README's table.

Step 3 drifted for ~3 months (2 of 20 entries indexed by 2026-07-29) because
it is a manual step nothing checks. If it drifts again, prefer a generated
index over another manual re-sync — a stale index is worse than none, since
it reads as an authoritative "these are the gaps" while hiding 90% of them.

## Architecture vs specs

`docs/architecture/` is the orientation surface — overviews, dataflow, signal model, public surfaces, failure modes. It explains how the system is shaped and why.

`specs/` is the binding contract. Implementation can be judged wrong against a spec; it cannot be "judged wrong" against an explanation.

Both refer to each other. Neither replaces the other.

## Adapted from

agent_gov's `docs/DOC_TAXONOMY.md` (north-star, partially adhered to in source).
