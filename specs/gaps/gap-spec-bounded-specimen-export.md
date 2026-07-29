# Gap: bounded read-only specimen export

Status: CANDIDATE — named 2026-07-13, not built. Read-only; low risk.

## Purpose
A boring, clean custody boundary: export one bounded graph component from
driftwatch that any offline analysis can consume. **NOT** integration with
provenance-semantic-correlation, NOT a service feature, NOT emit. Just: driftwatch
supplies bounded observations; the lab adjudicates them elsewhere.

The `driftwatch-001` calibration hand-rolled this (frozen selection rule + JSON
pull + digest). This gap promotes that ad-hoc extraction into a stable,
receipted tool.

## Inputs
- root URI or record key
- max records / max depth (hard cap)

## Output (a self-describing capsule)
- raw post text **while still retained** (<24h); absent for stripped rows
- reply / quote / repost edges within the component
- **claimed** timestamp (producer) and **observed** timestamp (ingestion) —
  the latter depends on `gap-spec-event-time-hygiene.md`
- stable content digest + an extraction receipt (query shape, cap, timestamp,
  row count, dropped-row count)

## Discipline
- Read-only; no writer path, no mutation-capable credentials.
- Bounded + index-friendly access only (the calibration learned this the hard
  way: global `GROUP BY` over `events` times out and risks a WAL-pin; use cheap
  index-bounded lookups).
- Prefer an inert artifact (JSONL/JSON) over a live DB handle for downstream.
- Selection rule frozen and recorded before content is read (no outcome-shaping);
  aggregate claims (thread size, "below threshold") measured over the full set,
  never a first-inspected element — see workbench law "measure the set before
  calling it."
- If any bound truncates coverage (cap hit, rows dropped), the receipt says so
  explicitly. Silent truncation reads as "covered everything."

## Sequencing
Comes after event-time hygiene (for a meaningful observed timestamp), then park.
The next real consumer is v1 (temporal claim-lineage), which will ask this
exporter for a component where posts revise / contradict / delete / apparently
correct each other.
