# Gap spec: observatory read instrumentation (cohort forecast + read classification)

**Status:** CANDIDATE handle, **not ratified, not authorized to build.** Filed
2026-06-29 as a review handle while the shape is fresh (name-early / ratify-lazily).
Two non-binding candidates that emerged from the resolver aged-tail reads; neither has
a forcing *case* to implement yet. Build only on explicit greenlight.

**Provenance:** the 2026-06-29 resolver aged-tail thread
(`docs/resolver-pending-aged-tail.md`, second read + composition). Both candidates
exist to stop a draining top-line number from reading as a coverage win — the failure
the second read nearly committed.

---

## Candidate A — cohort forecast ("aging waterfall")

**Problem it would solve.** Current tripwires are *retrospective*: they fire after
`gt168` has already grown or the floor has already slid. Picking a backlog-lane cap
(`gap-spec-resolver-backlog-lane.md`) is therefore vibes-based — we know the tail is
worsening but not by how much next, nor how much surplus would arrest it.

**Shape (non-binding sketch).** A tiny model over the existing sampler series:

```
given: new_pending_per_hour, resolved_per_hour,
       pending_72_168h, pending_gt_168h, oldest_pending_hours
forecast:
  expected gt168 in 24h / 48h / 72h
  expected oldest-floor movement
  required surplus to hold gt168 flat
  required surplus to reduce gt168 by N/day
```

**Why candidate, not build:** the 2026-06-29 composition showed the tail is pure
never-attempted live sediment bounded <14d, so a simple opportunistic drain may suffice
without a forecast. Promote this only if the lane needs a *sized* cap, or if the floor
starts sliding faster than linear.

## Candidate B — read classification taxonomy

**Problem it would solve.** The backlog-lane spec already records the
capacity-vs-weather **metrics** (`resolver_capacity_observed`, `arrival_weather`,
`effective_surplus`, `aged_tail_delta`). What's missing is the **verdict label** that
turns those numbers into a disposition, so a human (or a future automated read) can't
mistake a weather-driven drain for a capacity win.

**Shape (non-binding sketch).** Classify each read as exactly one of:

```
capacity_win   : resolved_per_hour increased AND aged_tail improves
weather_win    : new_pending_per_hour decreased, total drains, but aged_tail does NOT improve
coverage_loss  : oldest floor rises OR gt168 grows
poison_signature: attempts rise but resolved_per_hour does not
```

The 2026-06-29 second read was textbook **`weather_win`** (total fell −21.8k while
`gt168` grew +21.5k) — the taxonomy would have labeled it as such instead of leaving
the disambiguation to prose.

**Why candidate, not build:** one classifier is cheap, but it earns its keep only once
reads are frequent/automated enough that prose disposition stops scaling. Promote when
the composition read becomes recurring (it is currently ad hoc, by design — "cron
observes; agent interprets; human authorizes").

---

## Non-goals (both candidates)

- No automated remediation; these inform a human read, they do not act.
- No scheduler / cron / resolver / emit change.
- Not a prerequisite for the backlog lane's first drain (that lane is already
  build-ready pending ratification per its own spec).
