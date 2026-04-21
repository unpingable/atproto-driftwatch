# Vintage-stratified hosting-locus admissibility — exclusion rules and
# minimum publishable effect

**Pre-registered**: 2026-04-20, before Phase D rerun.
**Backfill provenance**: `plc_export_runs.run_id = plc_export_20260420_152312Z_db628ce9`
(target_dids at launch: 1,012,453; PLC /export paginated from cursor=0).
**Rule**: this document is finalized before any vintage-stratified analytic
output is produced. Changes after Phase D require a new dated revision and
an explicit note explaining why the rule changed.

## Why this document exists

The 2026-04-19 admissibility check established that the seed:live shard
distribution gap inside `*.host.bsky.network` was a vintage confound
(account-creation cohort), not a hosting-locus observable. The column
`pds_host` is operationally faithful; the *claim* layered on top of it
("hosting locus concentration") was overinterpreted. Different sin.

The Phase D rerun asks a narrower, publishable question:

> After controlling for vintage, is there still seed vs live non-major host
> skew among genuinely-independent PDSes (not bsky-network shards)?

To keep that rerun from becoming an ad-hoc interpretation exercise, this
document fixes — in advance — which rows are eligible, which strata are
compared, which effect sizes count as non-noise, and which robustness checks
the result has to pass before it is publishable.

## Eligibility filters

Applied to `actor_identity_current` in this order. A row must pass every
filter to enter the rerun.

1. **DID method**: `did LIKE 'did:plc:%'`. Non-PLC DIDs (did:web et al.) are
   excluded because PLC genesis timestamps do not exist for them.
2. **Vintage**: `did_created_at IS NOT NULL AND vintage_source IN
   ('plc_export', 'plc_audit_log')`. Rows with NULL vintage,
   `unsupported_method`, or missing backfill coverage are excluded.
3. **Vintage repair**: rows with `vintage_source = 'repair_override'` are
   included and annotated in the report header with the count and reasons.
4. **Resolver status**: for any analysis that references `pds_host`,
   `resolver_status = 'ok'` is required. Unresolved rows are counted
   separately as the "resolver-coverage tail" but excluded from comparisons.
5. **Freshness**: for host-based claims, `resolver_last_success_at` must be
   within 7 days of the analysis run. Staler rows are excluded to avoid
   reintroducing the staleness confound the 04-19 check ruled out.
6. **Source**: `identity_source IN ('labelwatch_seed', 'live')`. The `both`
   bucket (~1.5k rows as of 04-19) is reported separately with sample counts
   but not used in the primary seed-vs-live comparison.

## Strata

### Vintage bucketing

Buckets are derived empirically from the vintage × shard distribution
after backfill completes. The boundary-selection rule is fixed before
buckets are chosen:

- Sort DIDs by `did_created_at`.
- Identify points where the running fraction of accounts assigned to
  `jellybaby.us-east.host.bsky.network` (or the current dominant shard) shifts
  by ≥20 percentage points within a ≤30-day rolling window.
- Bucket boundaries are placed at those regime-change points.
- If fewer than two regime changes are detected, fall back to equal-width
  quarterly buckets.
- Bucket boundaries, once chosen, are fixed for the rerun and stored as
  constants in the module with the date chosen and the run_id that produced
  the underlying vintage data.

This is the empirical-but-not-ad-hoc rule. The method is fixed in advance
even if the specific dates cannot be.

### Host classes

- **major**: `pds_host LIKE '%.bsky.network' OR pds_host = 'bsky.social'`
- **non_major**: resolved, not matching major rule
- **unresolved**: excluded from host-based comparisons (see filter 4)

### Provider family (for non-major tail)

Non-major PDSes are rolled up to registrable-domain family using the
`extract_host_family` heuristic from `labelwatch/hosting.py`. Individual
shards inside a family are not compared directly; that's the mistake the
04-19 check exposed.

## Minimum sample floor

A (vintage_bucket × identity_source × host_family) cell must contain at
least **100 resolved DIDs** to be included in the comparison table.
Cells below the floor are reported as "sparse — insufficient sample" and
not used for effect-size calculations.

Floor justification: 100 resolved DIDs at a non-major family gives a
binomial standard error on the non-major share of roughly
sqrt(p(1-p)/100) ≈ 5 percentage points at p=0.5, which matches the effect
floor below. Smaller samples cannot distinguish effect-floor-sized signals
from noise.

## Minimum publishable effect

A seed-vs-live non-major host skew is **publishable** only if **all** of
the following hold *inside the same vintage bucket*:

1. **Percentage-point floor**: seed non-major share minus live non-major
   share, computed on identical `(vintage_bucket, host_family)` cells,
   is at least **5 percentage points** (absolute, either direction).
2. **Ratio-of-ratios floor**: the ratio `(seed_share_on_family_X /
   live_share_on_family_X)` divided by the overall
   `(seed_total_on_family_X / live_total_on_family_X)` is at least
   **1.5×** (or ≤ 0.67× for under-representation).
3. **Sample floor**: both seed and live cells for that family meet the
   100-DID floor above.
4. **Minimum bucket count**: the effect must appear in at least **two**
   vintage buckets, not just one. A one-bucket effect may be an artifact
   of bucket boundary choice.

If no family meets all four conditions in any bucket, the published
finding is: **"vintage control collapses the non-major host skew."**

## Robustness checks

Applied to any family that clears the effect floor, before publishing:

1. **Bucket-boundary sensitivity**: shift each vintage bucket boundary by
   ±14 days. Effect must survive (still clear sample floor and effect
   floor) in at least 3 of 5 perturbations.
2. **Freshness sensitivity**: restrict to `resolver_last_success_at <
   48h`. Effect must still appear (possibly at reduced magnitude). If
   the effect is present only in stale rows, report as "freshness-tied"
   and do not publish as a cohort claim.
3. **Exclusion-rule sensitivity**: rerun with `both`-source DIDs
   included. Effect direction must be preserved (magnitude may shift).

## Publishable language

- Effect clears all gates: "After vintage control, `<family>` shows
  elevated labeled-target density vs overall population in vintage
  bucket(s) `<B>` at effect size `<N>%`."
- Effect collapses: "After vintage control, no non-major host family
  retains seed-vs-live skew meeting the pre-registered effect floor.
  The 04-19 shard-level observation was explained by cohort vintage."
- Mixed: some families clear, others collapse. Each family reported
  individually, with the full effect/sample/robustness stats. No
  aggregate claim.

## Non-goals for this rerun

The following are explicitly out of scope for Phase D:

- **Activity bias** (second-order confound: live rows select for active
  accounts). Flag in findings; do not architect around it until Phase D
  completes.
- **did_pds_observation historical log** (spec'd in
  DW-SPEC-PDS-ENRICHMENT-001 but not built). Designing this is deferred
  until Phase D tells us whether a historical-host log is actually the
  next bottleneck.
- **Hosting-locus narrative beyond "vintage-controlled seed vs live
  non-major skew"**. No attestation, no trust scoring, no provider
  ranking.

## Provenance footer (filled at Phase D time)

| Field | Value |
|-------|-------|
| Rerun executed at | 2026-04-21T18:47:45Z |
| Backfill run_id | plc_export_20260420_152312Z_db628ce9 |
| target_dids at backfill launch | 1,012,453 |
| vintaged DIDs at rerun | 669,040 eligible (post-filter) |
| vintage coverage (% of resolved) | ~68% of resolved rows had vintage+fresh by policy; 160,802 after 7d freshness filter |
| Bucket boundaries chosen | 13 quarterly boundaries, 14 buckets (doctrine fallback — single joint jellybaby+stropharia regime detected, below 2-event minimum) |
| Families clearing effect floor | 0 |
| Verdict | no_non_major_signal_after_vintage_control |
| Findings artifact | out/admissibility/2026-04-21_findings.md |
| Numeric artifact | out/admissibility/2026-04-21_1847Z_vintage_admissibility.json |

## Different-sin language

In any writeup, the field `pds_host` is described as **operationally
correct** (current location of the DID's PDS). The earlier *claim*
built on `pds_host` — that seed:live shard distribution revealed
hosting-locus signal — was **overinterpreted**. Readers should not
walk away thinking the column is useless; the claim was.
