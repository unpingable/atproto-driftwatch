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

## First read — 2026-06-25 (20.5h, 22 hourly samples)

Span `2026-06-24T19:35Z → 2026-06-25T16:07Z`. Mean flow **1,152 attempts/hr vs
1,150 new/hr** — drain and inflow matched to within noise.

| metric | start → end | Δ (20.5h) |
|---|---|---|
| `pending_total` | 323,807 → 323,742 | −65 (flat) |
| `oldest_pending_hours` | 249.99 → 249.70 | −0.29 (pinned ~10.4d) |
| `pending_gt_24h` | 297,206 → 296,102 | −1,104 |
| `pending_gt_72h` | 232,641 → 242,512 | **+9,871 (+481/h)** |
| `pending_gt_168h` | 108,810 → 110,817 | **+2,007 (+98/h)** |

Floor is **held, not frozen**: 20.5h of wall-clock elapsed; a frozen floor would have
*gained* +20.5h of age, but `oldest_pending_hours` moved −0.29h. The sweep advances
the front ~1h of `first_seen` per wall-clock hour — holds the ~10.4d lag constant,
never closes it.

Verdict — the nasty middle case (not broken enough to touch, not healthy enough to
ignore):

- **Headline health:** stable (count flat, no leak)
- **Coverage lag:** bounded (~10.4d floor held)
- **Staleness distribution:** worsening (>72h +481/h, >7d +98/h)
- **Capacity verdict:** matched, **no surplus**
- **Action:** observe longer; do **not** tune blindly

Sharp read: **the resolver is behaving correctly under the wrong capacity envelope.**
Fair sweep, stable floor, no leak — but zero slack, so the mid-distribution mass ages
into uglier buckets faster than the front drains. Green dashboard, asbestos in the
walls.

Caveat on the >7d growth: much of it is likely the **original 3–7d mass (123,859 rows,
first_seen ≈ 2026-06-17..21) crossing the 7-day line**, which finishes ≈ **2026-06-28**.
That is a wave, not necessarily a permanent slope. Re-read ≈ **2026-06-29** (after the
wave should have cleared) to distinguish transient from linear decay.

### Tripwires (added now, so the next read isn't a judgment call)

1. **`pending_gt_168h` still rising after ≈2026-06-28** (old 3–7d wave should have
   crossed) → not transient; **file the fix.**
2. **`oldest_pending_hours` rising materially above ~250h** → floor no longer held;
   **fix sooner.**
3. **`pending_total` also rising** → capacity is no longer merely "no surplus," it is
   **losing**; fix is not optional.

Fix preference holds: **dedicated backlog lane beats a `BATCH_SIZE` bump** — a
`BATCH_SIZE` bump is aspirin (treats the symptom blindly); the backlog lane is
diagnosis-aware (keeps fresh resolution stable while the tail drains on its own
budget). Bump `BATCH_SIZE` only if the *whole* resolver turns out to be uniformly
underprovisioned (i.e. fresh resolution itself starts lagging), not just the tail.

## Second read — 2026-06-29 11:10 EDT (115.5h, 117 samples)

Resolved the open question the first read left: **the >7d growth was not a clean
transient wave, and the floor did not hold.** Two of the three pre-registered
tripwires tripped.

**Provenance of this read.** The observation-only backstop
(`scripts/resolver_second_read_check.sh`) fired from its date-guarded cron at
**11:10 EDT / 15:10 UTC**, pulled the sampler JSONL read-only, and wrote
`reports/resolver-pending-second-read-2026-06-29.txt` (**117 rows**, series end
`2026-06-29T15:07Z`). For the interpretation below I then pulled the live series
directly (**118 rows, 116.5h**, end `2026-06-29T16:07Z`) — one hour newer; the two
agree within an hour of drift. The cron did exactly its job: observe, compute blunt
deltas, write the artifact, change nothing. It did **not** edit docs, commit, decide,
or touch resolver config.

| metric | first read (06-25T16:07) | second read (06-29T16:07) | Δ over ~96h |
|---|---|---|---|
| `pending_total` | 323,742 | **301,962** | **−21,780** (was flat; now draining) |
| `oldest_pending_hours` | 249.70 (10.4d) | **264.49 (11.0d)** | **+14.79h** (floor slid up) |
| `pending_gt_72h` | 242,512 | 237,174 | −5,338 (flat) |
| `pending_gt_168h` | 110,817 | **132,292** | **+21,475** (still climbing) |

### Tripwire verdict

| wire | condition | result |
|---|---|---|
| **T1** | `pending_gt_168h` still rising after the ~06-28 wave-crossing | **TRIP** |
| **T2** | `oldest_pending_hours` materially above ~250h (>256h) | **TRIP** |
| **T3** | `pending_total` also rising (capacity *losing*, not merely no-surplus) | **pass** |

### Interpretation

- **Total count is draining because inflow dipped, not because capacity improved.**
  The resolver is still pinned at its ~1,200/hr ceiling; mean `new_pending` fell
  1,117/h (day 0) → 880/h (day 4). The fixed budget gained surplus only because
  arrivals dropped below it. **The falling total is not evidence that the problem
  solved itself; it is evidence that arrivals temporarily fell below the fixed
  resolver ceiling.** It reverses the moment inflow climbs back.
- **The aged tail kept worsening on both axes that measure coverage.** `gt168` grew
  +21,475, and its windowed growth did **not** decelerate through the predicted
  06-28 wave-crossing: +69 → +204 → +241 → +50 → **+473/h** (last window steepest).
  `gt72` is flat while `gt168` swells — the middle cohorts are waterfalling across
  the 7-day line faster than the oldest-first front drains them out. This is
  **sustained slow degradation, not a one-time wave.**
- **The oldest floor slid** 249.70h → 264.49h (+14.79h over 116.5h wall-clock). The
  front advances only ~0.88h per real hour, so it is losing ground on the floor —
  slowly, but directionally.

### Decision flip: candidate → warranted

The first read pre-registered the decision rule *"`oldest_pending_hours` rises **or**
the >7d cohort grows → add resolver surplus capacity"* and three tripwires so this
read would not be a judgment call. **Both rule conditions fired and T1+T2 tripped**;
the disambiguating window (does the wave clear by 06-28?) resolved toward *sustained*.
The forcing case for the fix is therefore **met**. The dedicated-backlog-lane fix
moves from **candidate** to **warranted**.

**Warranted is not urgent.** This remains a low-priority coverage defect on an
emit-disabled observatory: no user impact, count actually improving, fresh resolution
keeping pace. The response is **spec the lane now, build after ratification** — not
an incident, not a `BATCH_SIZE` bump, not a scheduler change today. T3 passing is
load-bearing here: fresh flow keeps up when inflow dips, so the rot is in the sediment
layer, which argues for a *separate tail lane*, not a global knob. Design captured in
`specs/gaps/gap-spec-resolver-backlog-lane.md`.

### Composition — decomposed 2026-06-29 (read-only tail-composition report)

The second read's open caveat ("inflow drop not decomposed: organic Jetstream dip vs.
a `labelwatch_seed` import wave") is now **answered**, by the Slice-1 read-only report
(`src/labeler/resolver_tail_composition.py`, run read-only against prod; receipt at
`reports/resolver-tail-composition-2026-06-29.json`). Pending pool = 301,765:

| dimension | result | reading |
|---|---|---|
| source population | **100% `live`** (0 `labelwatch_seed`, 0 `both`) | **seed-import-wave hypothesis refuted** — the pool is organic live-observed identities, not a seed backlog |
| attempt status | **100% never-attempted** | pure under-capacity sediment, **not poison/failures** |
| `>336h` bucket | **0** (oldest 264h ≈ 11.0d) | accumulation is **bounded under 14d** — corroborates the 2026-06-14 reset-horizon floor (the scar), not an unbounded leak |
| quarantine candidates | **all 0** | nothing to quarantine in the pending pool |
| terminal poison (excluded from pending) | `error=42`, `not_found=749` | negligible; lives outside the pending pool |

**Correction to the second read:** the draining total being driven by a finishing
`labelwatch_seed` import is **ruled out** — there are zero seed rows in the pool. The
drain is organic inflow weather against the fixed ceiling, exactly as the verbatim
guardrail states; the *composition* it drained from was entirely live.

**Consequence for the fix:** the backlog lane is a **pure opportunistic-throughput
drain**. The quarantine/dead-letter machinery in the spec is correct but currently a
**no-op** (no poison to isolate as of 2026-06-29) — it is instrument-only until
attempted/error rows appear in the pool. Draining does not depend on wiring quarantine
first.

Schema gaps the report surfaced (not inferred): no attempt-count column (only
attempted-vs-never is computable), so the quarantine `attempts>=3` and
`repeated_timeout` rules and the 1/2-3/4+ attempt buckets are **unavailable**;
`retry/requeue` is not a real `identity_source`; `deleted_or_unavailable` is not
separable from `did_doc_missing`. These are the next open questions if the lane ever
needs attempt-level discrimination.

## Sampler output

`/mnt/zonestorage/driftwatch/data/resolver_pending_samples.jsonl` on the VM,
appended hourly (cron, minute 7). One JSON object per line; `query_version` gates
schema evolution.
