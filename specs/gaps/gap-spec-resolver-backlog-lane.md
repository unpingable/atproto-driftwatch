# Gap spec: resolver backlog lane (aged-tail drain)

**Status:** gap-spec, doc-only. **Not authorized for implementation.**
**Filed:** 2026-06-29
**Triggered by:** `docs/resolver-pending-aged-tail.md` second read (2026-06-29) —
pre-registered tripwires T1 (`pending_gt_168h` still rising) and T2
(`oldest_pending_hours` > 256h) tripped; T3 (count rising) passed. The forcing case
flipped the dedicated-backlog-lane fix from **candidate** to **warranted**.
**Disposition:** warranted, not urgent. Spec now; build after ratification.

## Problem

The DID resolver sweep (`src/labeler/resolver.py` `resolve_batch` /
`fetch_unresolved_batch`) is fair (oldest-first) but hard-capped at `BATCH_SIZE=20` ×
`RESOLVE_INTERVAL_S=60` = **~1,200 DIDs/hr**. That budget roughly tracks fresh inflow,
so the resolver keeps the *count* flat (and drains it when inflow dips) but makes
near-zero net progress on the standing aged sediment. Over the 06-24..06-29 window the
oldest floor slid 10.4d → 11.0d and the >7d cohort grew +21k while the total *fell* —
coverage of the aged tail degraded while the headline number improved.

The fix is **not** a global budget bump. T3 passed: fresh resolution keeps pace when
inflow dips, so the live lane is adequately provisioned. The rot is confined to the
sediment layer. A blunt `BATCH_SIZE` increase would over-serve the part that is
already fine and risk stealing from fresh resolution. The diagnosis-aware fix is a
**separate, capped lane** that drains the aged tail on its own budget while live
resolution stays priority and stable.

## Evidence

- `docs/resolver-pending-aged-tail.md` — first read (2026-06-25), second read
  (2026-06-29), full ruled-out-hypotheses analysis (selection is provably fair;
  EXPLAIN + top-15 confirm oldest-first; no version skew).
- Sampler series: `/mnt/zonestorage/driftwatch/data/resolver_pending_samples.jsonl`
  (`query_version=resolver_pending_v1`), hourly.
- The verbatim guardrail this spec exists to honor: *"The falling total is not
  evidence that the problem solved itself; it is evidence that arrivals temporarily
  fell below the fixed resolver ceiling."* `pending_total` must **not** be the
  primary success metric for this lane.

## Non-goals

- No incident response. This is a coverage defect, not an availability event.
- No `BATCH_SIZE` bump as a substitute for the lane.
- No change to the **fresh** resolver lane's ordering, WHERE, or priority.
- No emit / label / moderation / enforcement semantics change (observatory is
  detect-only; that invariant is untouched).
- No automatic remediation, no infinite drain loop, no crawler behavior.
- No Labelwatch dashboard or facts-export change (facts custody is a *separate*
  border — see `gap-spec-facts-snapshot-manifest.md`).

## Pre-implementation requirement: tail composition decomposition

**Before the lane is built, decompose the pending pool.** The second read proves the
tail is worsening; it does *not* prove what the tail is made of. Implementing a drain
lane over an undifferentiated queue risks building a retirement home for poison items
and mistaking "old work" for "old *resolvable* work." Required decomposition (extend
the sampler or a one-shot read-only query; preserve the population separation the
Driftwatch README already mandates — `live` / `labelwatch_seed` / `both`):

```
age_bucket:      <24h | 24–72h | 72–168h | 168–240h | 240–336h | >336h
source:          live | labelwatch_seed | both | retry/requeue | unknown
attempt_count:   0 | 1 | 2–3 | 4+
last_error_kind: never_attempted | transient_network | resolver_timeout
                 | did_doc_missing | malformed_did | deleted_or_unavailable
                 | rate_limited | permanent-ish | unknown
```

This also closes the second read's open caveat (inflow drop not decomposed: organic
Jetstream dip vs. seed-import wave). The lane's eligibility predicate (below) is
provisional until this decomposition runs.

## Eligibility

- **Primary target:** pending items older than **168h** (the seven-day boundary is
  the observed failure line). Oldest-first within the lane.
- **Optional secondary band:** 72h–168h, admitted **only** when `pending_gt_168h` is
  below a configured threshold *or* the lane has spare capacity. Do **not** start with
  72h as the main target.
- **Excluded:** anything the tail-composition pass classifies as poison (see
  Quarantine). The lane drains *resolvable* sediment, not immortal failures.

## Scheduling model

- Backlog lane runs **independently** from the fresh resolver lane (separate task /
  loop, separate cap, separate counters).
- **Fresh lane remains priority** for current arrivals at all times.
- The backlog lane **must not** cause fresh `pending_total` to rise. If fresh pending
  begins rising, the backlog lane yields (see Failure criteria + kill switch).

## Capacity model (the central decision)

The lane must explicitly declare what kind of capacity it is. **Decision for this
spec:**

- **Primary: opportunistic unused capacity.** The lane consumes resolver headroom
  that exists only when fresh inflow is below the ~1,200/hr ceiling — exactly the
  slivers during which the aged pool currently drains by luck. This makes the luck
  systematic without contending with fresh work.
- **Optional: a small additive cap**, configurable, engaged only when explicitly
  authorized — for deliberate drawdown of the standing sediment.
- **Avoid: reserved capacity carved from the existing budget** (stealing from fresh
  resolution), unless explicitly ratified. T3 passed, so there is no standing
  justification to degrade fresh resolution.

Initial cap must be **conservative and configurable**. The cap's *type*
(opportunistic / reserved / additive) must be a named config value, not implicit.

## Quarantine / dead-letter class

The backlog lane must **not** become a retirement home for immortal failures. Items
that cannot resolve must leave the normal oldest-first lane into a **visible, counted,
inspectable** quarantine class. Quarantine is **not success** and **not a silent
drop**.

Resolver status vocabulary (extends current `ok` / `not_found` / `error` / NULL):

```
pending | in_progress | resolved | deferred | quarantined | dead_lettered
```

Quarantine criteria (provisional; tune against the tail-composition pass):

```
quarantine if:
  - age > 336h AND attempts >= 3
  - or permanent-ish last_error_kind
  - or malformed DID / invalid input
  - or deleted / unavailable upstream reference
  - or repeated timeout from same DID / provider family beyond threshold
```

Invariant:

> The backlog lane drains eligible sediment. Quarantine isolates poison.
> Neither may pretend to be resolution.

Note the **latent poison-pill smell** already filed in the parent doc:
`resolve_batch` increments `stats["error"]` on exception *without* writing
`resolver_last_attempt_at`, so a throwing DID is re-selected forever. The quarantine
class is the structural fix for that recirculation path — attempts must be recorded
even on failure, and exhausted items must exit the lane.

## Receipts / observability

- Per-cycle log line for the backlog lane, distinct from the fresh
  `RESOLVER resolved=N` line (e.g. `BACKLOG resolved=N quarantined=M deferred=K`).
- Extend the sampler (`query_version` bump) with: backlog-lane attempts/hr, resolved/hr,
  quarantined count, dead_lettered count, and the source-population split.
- First-class separation of **capacity vs. inflow weather** in the daily read, so the
  "charismatic liar" (a draining total) cannot be mistaken for a capacity win:

```
resolver_capacity_observed: resolved_per_hour
arrival_weather:            new_pending_per_hour
effective_surplus:          resolved_per_hour - new_pending_per_hour
aged_tail_delta:            pending_gt_168h delta/hour
```

## Tripwires (lane-on; inherit the parent doc's lane-off tripwires)

Once the lane is live, judge it by the aged-tail axes, never the total:

- **Working:** `pending_gt_168h` stops growing over a rolling window; then
  `oldest_pending_hours` stops increasing, then declines.
- **Not working:** `gt168` keeps rising despite the lane; or the oldest floor keeps
  sliding; or attempts rise while resolved/hr does not (poison signature).
- **Harming:** fresh `pending_total` rises because the backlog lane starves live work
  → lane must yield immediately.

## Rollback / kill switch

- The lane is gated behind an env flag (default **off**), in keeping with Driftwatch's
  emit-gated / flag-gated convention.
- Setting the flag off must immediately and cleanly stop the backlog lane with the
  fresh lane unaffected.
- Quarantine writes are reversible (a quarantined item can be re-admitted by clearing
  its status) — quarantine is a holding pen, not a tombstone.

## Success criteria

```
- pending_gt_168h stops growing over a rolling window
- oldest_pending_hours stops increasing, then declines
- fresh-resolution lag remains stable
- pending_total is NOT the primary success metric
```

## Failure criteria

```
- fresh pending rises because backlog starves live work
- gt168 continues rising despite the lane
- oldest floor continues sliding
- lane burns capacity on retries / poison without progress
```

## Open questions

1. **Source decomposition (blocking):** what fraction of the >168h tail is
   `labelwatch_seed` vs. `live`? Seed-driven sediment may have different SLO than
   live-observed identities, and the inflow drop driving the current drain is
   undecomposed.
2. **Opportunistic detection:** how does the lane *measure* headroom — observed
   inflow below ceiling for N consecutive cycles, or a queue-depth signal on the fresh
   lane? Needs a concrete, cheap signal.
3. **Quarantine thresholds:** the 336h/3-attempt numbers are provisional; ratify
   against the tail-composition histogram.
4. **Provider-family rate limiting:** repeated timeouts "from the same provider
   family" needs a definition (PLC directory vs. did:web host) before it can gate
   quarantine.
5. **Schema:** does the quarantine vocabulary live in `resolver_status` (string
   widening, a facts-bridge data-contract change) or a sidecar column? The facts
   bridge exports `resolver_status` — widening it touches the Labelwatch contract.

## Recommended first implementation slice

**Measurement before mechanism.** Slice 1 is the tail-composition decomposition
(read-only query / sampler extension) — it is the blocking prerequisite, carries zero
behavior risk, and converts the provisional eligibility/quarantine thresholds into
ratifiable numbers. No lane code until that histogram exists and the capacity-model
decision is ratified.
