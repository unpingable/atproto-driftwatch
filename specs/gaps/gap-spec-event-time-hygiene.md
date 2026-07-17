# Gap: event-time hygiene (claimed vs observed time)

Status: CANDIDATE — named 2026-07-13, not built. Production change (schema +
writer + deploy); confirm scope before executing.

## Problem (measured, not assumed)
`events.ctime` is derived from producer-controlled `createdAt` and is poisoned:
real rows carry **year-2999** timestamps (`max(ctime) = 2999-05-26…`). Any
temporal query that treats `ctime` as recency is wrong. Surfaced during
provenance-semantic-correlation calibration `driftwatch-001`, which had to work
around it with "`raw IS NOT NULL` ⟹ ingested within ~24h" as the only trustworthy
recency proxy.

## Shape (not authorization to build)
- **Preserve** producer time as *claimed* time (`createdAt` / current `ctime`) —
  it is the record's own assertion and must not be discarded.
- **Add** a distinct **observed/ingestion** timestamp stamped by the consumer at
  write time (monotonic, not producer-controlled).
- Bounded extraction / recency queries **default to observed time**; claimed time
  is available but never the ordering key.
- Backfill is impossible for existing rows (no ingestion time was recorded) —
  historical rows carry observed_time = NULL / unknown; only forward rows get it.

## Fence
Claimed time is testimony; observed time is custody. Do not let a producer
assert its own ingestion order. (Kin to the observatory's actor_identity /
observed_at doctrine.)

## Dependency
Enables `gap-spec-bounded-specimen-export.md` to emit a *meaningful* observed
timestamp per record. Until this lands, the exporter can only emit claimed time
plus a coarse "raw-not-null ⟹ <24h" bound.
