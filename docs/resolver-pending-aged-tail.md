# Issue: DID resolver maintains high throughput while starving aged never-attempted live identities

**Status:** open, low priority (not an availability incident — coverage defect)
**Filed:** 2026-06-24
**Component:** `src/labeler/resolver.py` (`resolve_batch` / `fetch_unresolved_batch`), driven by `src/labeler/consumer.py`
**Severity:** not sev-worthy. Ingest, WAL, lag, drop_frac all green. This is biased *coverage*, not a failure.

## One-line

The resolver's backlog sweep is fair (oldest-first) but throttled to ~1,200 DIDs/hr.
That roughly matches fresh inflow (~1,256/hr), so it keeps the count flat but makes
near-zero net progress draining the ~324k standing sediment of never-attempted live
identities. The aggregate "throughput" looked healthy (and a string-comparison query
bug inflated it ~18×), but it never reflected coverage of the aged tail. Green
throughput is not coverage.

## Snapshot that opened this (2026-06-24 ~19:20 UTC)

`actor_identity_current` resolver state:

| status | count |
|---|---|
| ok | 2,310,141 |
| **(null = pending)** | **323,784** |
| not_found | 748 |
| error | 41 |

Of the 323,784 pending: **100% are `resolver_last_attempt_at IS NULL`
(never_attempted)**, **all `identity_source='live'`**.

Pending age distribution (by `first_seen_at`):

| age | count | share |
|---|---|---|
| <1h | 1,195 | 0.4% |
| 1–6h | 5,596 | 1.7% |
| 6–24h | 19,811 | 6.1% |
| 1–3d | 64,492 | 19.9% |
| 3–7d | 123,859 | 38.3% |
| >7d | 108,830 | 33.6% |
| **oldest** | — | **250.2h ≈ 10.4 days** |

Oldest pending `first_seen_at` ≈ **2026-06-14T09:25**, which aligns with the
labelwatch-relocation / disk-pressure event. Treat the 10.4-day floor as a
**suspected observability floor (a scar), not a proven cap**, until the time
series proves the floor holds or advances.

## What was ruled out (evidence, not vibes)

Candidate hypotheses and their disposition:

1. **Fresh-first ordering (`ORDER BY ... DESC`)** — REFUTED. The selection query is
   `ORDER BY CASE WHEN resolver_status IS NULL THEN 0 ELSE 1 END, first_seen_at ASC`
   (never-resolved first, then *oldest* first). Fair by intent.
2. **LIMIT-before-sort / planner lying** — REFUTED. `EXPLAIN QUERY PLAN` shows
   `SEARCH ... USING INDEX idx_actor_identity_resolver` + `USE TEMP B-TREE FOR ORDER BY`.
   The sort is honored.
3. **Predicate / eligibility drift** — REFUTED. Of 108,820 never-attempted pending
   >7d, **108,820 (100%)** match the candidate `WHERE`. Running the resolver's exact
   candidate query, the **top 15 handed out next are the oldest 2026-06-14
   never-attempted rows** (age 250.1h, `last_attempt=NEVER`). They are eligible AND
   front-of-line.
4. **Version skew (running build older, unfair query since fixed)** — REFUTED.
   Running build `08281a7`; local HEAD `20d753c` is newer overall, but **no commit
   touched `resolver.py` between them**. The fair ordering has existed since the
   resolver's birth commit `6c64c0c`; there was never a `DESC` variant.

So selection is exonerated. The bug is **not** where the DIDs are chosen.

## Root cause (proven from logs)

The sweep is healthy and fair but **budget-starved**:

```
src/labeler/resolver.py
  BATCH_SIZE        = 20    # DIDs per cycle
  RESOLVE_INTERVAL_S = 60   # seconds between cycles
```

Container logs confirm it runs as designed — exactly 20 resolved every ~60s:

```
19:28:59  RESOLVER resolved=20 ok=20 not_found=0 error=0
19:29:58  RESOLVER resolved=20 ok=20 not_found=0 error=0
19:30:59  RESOLVER resolved=20 ok=20 not_found=0 error=0
```

**Ceiling = 20 × 60/hr = 1,200 DIDs/hr**, confirmed three independent ways: the
`RESOLVER resolved=20` log cadence, the `BATCH_SIZE`×interval math, and the sampler's
`attempts_last_hour=1200` (correct `julianday()` comparison).

Corrected flow (seed sample 2026-06-24T19:35Z):

- sweep drain ≈ **1,200/hr** (oldest-first)
- fresh pending inflow ≈ **1,256/hr** (`new_pending_last_hour`)
- net ≈ **+56/hr** — near-equilibrium, which is why `pending_total` looks flat.

So the sweep roughly keeps pace with *new* arrivals but makes near-zero net progress
on the ~324k standing sediment. Spending its entire 1,200/hr budget oldest-first, it
*does* advance the front slowly (oldest age 250.2h → 249.99h over ~15 min — a fixed
floor would have *gained* time), but at this rate draining the standing backlog (if
inflow stopped) is ~270h ≈ 11 days, and with continuous inflow the sediment barely
moves. This is a **capacity/throttle** problem, not a fairness problem.

## Measurement caveat (how the first read fooled us)

The initial ad-hoc diagnosis reported "~22k attempts/hr, all succeeding." That number
was a **query bug**, not real throughput: comparing `resolver_last_attempt_at >
datetime('now','-1 hour')` as *strings* fails because stored timestamps use a `T`
separator (`...T18:...`) while `datetime('now',...)` emits a space (`... 18:...`), and
`'T'` (0x54) sorts after `' '` (0x20) — so it matched every row attempted *today*, not
the last hour. Always compare these timestamps with `julianday()`, never string `>`.

Independent of that bug, three *other* writers also touch
`resolver_last_attempt_at/_success_at` — `scripts/vintage_admissibility.py`,
`scripts/admissibility_check.py`, `src/labeler/facts_export.py` — so any rate derived
from those columns can conflate writers. The clean, un-conflated signals are
`pending_total`, `oldest_pending_hours`, and the age cohorts.

## Latent code smell (not the current root cause, but file it)

`resolve_batch` increments `stats["error"]` on exception **without** calling
`_write_resolution`, so a DID whose `resolve_did` *throws* never gets
`resolver_last_attempt_at` written and is re-selected forever. Not the active cause
here (logs show error=0), but it is a poison-pill recirculation path that would
produce a never-attempted tail under a different failure mode. Worth a guard.

## Open question the sampler answers

Single snapshots cannot distinguish:

- **(bounded)** sweep front in equilibrium at a fixed ~10-day lag, or
- **(losing ground)** backlog + oldest age growing monotonically, or
- **(scar)** 10.4-day floor is just the 2026-06-14 reset horizon and pre-reset
  pending vanished from accounting.

The hourly sampler (`scripts/resolver_pending_sampler.sh`, JSONL,
`query_version=resolver_pending_v1`) records `pending_total`,
`oldest_pending_hours`, and the >24h/>72h/>168h cohorts over time.

**The precise failure mode** is not "green throughput hides bad selection" — it's
"green throughput hides insufficient *surplus* capacity." Sweep budget (~1,200/hr) ≈
live inflow (~1,256/hr), so the aged pool only drains during the slivers when inflow
dips below budget. That distinction picks the fix: do **not** touch
ordering/fairness/WHERE.

Trend-witness decision rule (from the sampler series):

- `oldest_pending_hours` falls **and** >7d cohort shrinks → leave it; sweep is winning.
- `oldest_pending_hours` flatlines near the floor → known **bounded coverage lag**;
  tolerable, note it.
- `oldest_pending_hours` rises **or** >7d cohort grows → add resolver **surplus
  capacity**.

If capacity is needed, in rough order of preference:

1. **Dedicated backlog lane** with its own capped budget — fresh resolution stays
   stable while the tail drains independently. (Best operationally.)
2. **Adaptive batch size** keyed on `oldest_pending_hours` / >7d cohort — spend more
   only when the tail is deep.
3. **Raise `BATCH_SIZE`** — blunt; steals nothing but is undifferentiated.
4. **Shorten `RESOLVE_INTERVAL_S`** — noisier (more wakeups), least preferred.

## Coverage SLO candidate (non-binding, names the crime — do not enforce yet)

Filed per name-early/ratify-lazily. Not active; a handle for later review:

```
oldest_pending_hours < 168h
  or
pending_gt_168h == 0  (sustained)
```

If the trend witness shows the floor is bounded and tolerable, this stays a candidate.
If it shows the tail growing, this is the threshold the surplus-capacity fix should
restore.

## Sampler output

`/mnt/zonestorage/driftwatch/data/resolver_pending_samples.jsonl` on the VM,
appended hourly (cron, minute 7). One JSON object per line; `query_version` gates
schema evolution.
