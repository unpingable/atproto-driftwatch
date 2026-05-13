# Gap spec: facts_export DuckDB productionization

**Status:** proposed / engineering scope. Filed 2026-05-11 evening, after the janitor pass on stale facts artifacts and the explicit naming of cleanup-vs-cutover.

**This is not authorization to build.** It is the named handle so future work doesn't have to reconstruct the boundary from filesystem archaeology.

Companion to:
- `gap-spec-cold-path-parquet-duckdb.md` (Phase 3 is the parent: facts_export source moves to Parquet)
- `gap-spec-cold-path-phase-3.5-forward-parquet-capture.md` (recent-data path Phase 3 requires)
- `gap-spec-atproto-labeler-backport.md` Layer F (cross-repo backport gate, same prerequisites)

## Architecture sentence

> The duck has papers but no badge reader. Cleanup is not cutover. Prototype validation is not production integration.

## Premise

As of 2026-05-11:

- `ENABLE_FACTS_EXPORT=false` in prod (`docker-compose.override.yml`). No SQLite-backed export is running.
- `facts.sqlite` still exists at `/mnt/zonestorage/driftwatch/data/facts.sqlite` (3.0 GB, mtime 2026-05-08 17:03). Frozen-stale.
- Labelwatch `_sync_driftwatch_facts` still ATTACHes this file every ~65 min. Every cycle reports `snapshot=0` (no joining rows produced from stale data), no errors.
- `facts_work.sqlite` (5.2 GB) and `facts_proto.sqlite` (1.5 GB) plus SQLite sidecars were removed in the 2026-05-11 janitor pass. 6 GB reclaimed.
- The DuckDB prototype (`/usr/local/bin/facts_export_duckdb_prototype.py`, receipt `facts_export_duckdb_prototype.json` 2026-05-08 18:53) ran in `dry_run: true` with `no_prod_cutover: true`. Internal consistency vs Parquet verified row-for-row, but parity vs labelwatch consumer queries was NOT demonstrated.

The remaining work is to replace the stale SQLite-backed facts_export artifact with a DuckDB/Parquet-backed production path — without smuggling in a hot-DB reader and without breaking labelwatch consumption.

## First required step (gating all implementation)

**Consumer inventory.** Before any export code is written, walk labelwatch's `_sync_driftwatch_facts` and answer:

- Where does labelwatch ATTACH `facts.sqlite`?
- Which tables are read? (`uri_fingerprint`, `fingerprint_hourly`, `fingerprint_bounds`, `actor_identity_facts`, others?)
- Which columns within each table?
- What joins/queries does labelwatch run against the attached schema?
- What is the freshness contract — how stale can facts be before labelwatch behavior degrades materially?
- What happens when `facts.sqlite` is stale? (Observed: `snapshot=0`, no error.)
- What happens when `facts.sqlite` is empty? Missing entirely?
- Which facts actually drive UI / health / report output, vs which are dormant?

This inventory defines the parity target. Without it, "DuckDB facts_export works" is unfalsifiable.

## Known facts (current state)

- `ENABLE_FACTS_EXPORT=false` in prod
- `facts.sqlite` stale (frozen 2026-05-08) but still attached by labelwatch each cycle
- `facts_work.sqlite` and `facts_proto.sqlite` removed 2026-05-11 (6 GB reclaimed)
- DuckDB prototype is dry-run / one-off only; no production module exists
- `actor_identity_current` not represented in Parquet — identity facts skipped in prototype
- Date-partitioned Parquet avoids the rowid checkpoint orphaning scar (`mem_15ad4324`)
- Phase 1 historical mirror covers `2026-04-10..05-04`; recent-data path is Phase 3.5 (separate spec)

## Non-goals

- No production cutover yet.
- No labelwatch schema change yet.
- No hot DB scans introduced — `gap-spec-cold-path-parquet-duckdb.md` premise stands.
- No fixing the old rowid-checkpoint path unless rollback explicitly requires it.
- No drag-the-prototype-tonight. The prototype is internal-consistency proof, not a service.

## Acceptance for future productionization

When implementation work is authorized:

1. Consumer inventory complete (per "First required step" above), and durably filed.
2. Parity target defined by actual labelwatch reads, not by internal Parquet row counts.
3. DuckDB-backed export either produces a labelwatch-consumable artifact (likely still named `facts.sqlite`), **or** labelwatch is deliberately changed to consume DuckDB/Parquet directly (with the schema and consumer change reviewed together).
4. At least one full snapshot cycle passes parity against the consumer queries — not just internal row counts.
5. Identity facts (`actor_identity_current` → `actor_identity_facts`) have an explicit handling decision: Phase 4 cold path, sidecar source split, or deliberate exclusion with consumer impact assessed.
6. Recent-data path resolved (Phase 3.5 — see companion spec). Latency contract documented (likely retention-cadence ≈ 1 day, acceptable for facts_export but document it).
7. Rollback path documented: how does prod return to the previous state if the cutover misbehaves? What artifact does labelwatch consume during rollback?
8. Janitor pass for the retained stale `facts.sqlite` performed once the new artifact is producing.

## Open decisions

- **Output format**: continue producing `facts.sqlite` (smallest consumer change) vs teach labelwatch to read DuckDB / Parquet directly (larger but cleaner).
- **Cadence**: cron-driven snapshot vs service module with internal scheduler.
- **Freshness contract**: defined by consumer inventory, not assumed from Parquet capture cadence.
- **Schema drift handling**: how does this path tolerate Parquet schema evolution without silently producing wrong joins?

## Cross-repo backport

Per `gap-spec-atproto-labeler-backport.md` Layer F, the reference impl (atproto-labeler) waits until this productionization completes successfully here. Backport must adopt the date-partition identifier choice, not rowid (the bucket-migration scar).
