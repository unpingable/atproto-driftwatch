# Gap spec: facts-snapshot scale + containment (attempt-1 postmortem, attempt-2 design)

**Status:** postmortem + design. Filed 2026-07-17 after attempt-1 OOM'd on the
live VM. Product horizon ratified (14 days). Attempt-2 NOT yet built — no third
live-fire was attempted Friday night; this is the handle for the next session.

Companion to `gap-spec-facts-export-duckdb-snapshot-001.md` (the slice this
corrects) and `gap-spec-cold-path-parquet-duckdb.md` (umbrella).

## What happened (attempt 1, 2026-07-17 ~22:12Z)

Deployed the Phase 3 snapshot writer to the VM and ran the one-shot backfill
detached **inside the live driftwatch container**. The writer's python process
grew to **6.5 GB anon-rss on a 7.8 GB, zero-swap-headroom host** and was
OOM-killed at ~295s (`EXIT=137`). No snapshot published (atomic rename never
reached — the old May-8 facts.sqlite stayed intact). driftwatch survived
because the kernel OOM-killer picked the snapshot process, not uvicorn.

## The decisive measurement

`approx_count_distinct(post_uri)` over the real tree:
**195,780,181 distinct URIs / 202,397,607 rows = 96.7% unique.**

> This is not a deduplication dataset. It is a ~196M-row identity map wearing a
> dedup hat because the fixtures lied politely.

Consequences:
- A blocking `arg_max(...) GROUP BY post_uri` builds ~196M string-keyed groups.
  No `memory_limit` spill strategy makes that hash table fit; it is the OOM.
- A full-history `uri_fingerprint` would be ~196M rows / ~15-20 GB of SQLite —
  to serve lookups for a few thousand *recent* label→post URIs. Absurd.

## Corrections banked (operator, 2026-07-17) — these are doctrine

1. **"Unharmed" ≠ "never at risk."** A 6.5 GB process on a 7.8 GB no-swap host
   put *everything* in the OOM lottery. The kernel happened to shoot the right
   hostage. Do not launder a lucky outcome into a safety claim.
2. **`memory_limit` is an execution hint, not containment.** DuckDB's limit
   bounds *some* operator state. Python objects, SQLite, allocator overhead,
   and high-cardinality aggregate state live outside that promise. Verified
   honored (DuckDB reported 1.3 GiB) yet rss hit 6.5 GB.
3. **Declaration precedes effect; containment *limits* effect. Different
   doctrines.** The NQ maintenance declaration made the disturbance
   *interpretable*. It did nothing to *bound* it. A risky run needs both.
4. **Resource enforcement failed; custody did not.** The rails that worked —
   rsync dry-run (prevented destructive `--delete`), atomic publication
   (prevented a corrupt snapshot), NQ declaration (made the blip legible) —
   are all *custody* rails. What failed was *resource enforcement*. Keep the
   distinction; they are fixed by different mechanisms.

## Attempt-2 design (build next, with a clear head, NOT Friday-live)

### Containment (the enforcement fix)
Run the backfill in a **separate cgroup / one-shot container**, never detached
inside the live service container:
- hard `memory.max` (e.g. 2 GB), `memory.swap.max=0`, `memory.oom.group=1`
- bounded CPU/IO weight
- so a runaway is killed *by its own cgroup*, isolated from driftwatch.
Compose: a profile/one-shot service sharing the data volume, or
`docker run --rm --memory=2g --memory-swap=2g --cpus=1.5` against the same
image. Cron invokes the one-shot container, not `docker exec` into the service.

### Streaming (the memory-shape fix)
Kill the blocking aggregate. Since 96.7% is unique, dedup is a rare-case
concern, not the workload:
- DuckDB does a **streaming projection + filter** (window + epoch band), no
  GROUP BY. Bounded pipeline memory.
- `fetchmany` → SQLite `INSERT ... ON CONFLICT(post_uri) DO UPDATE SET
  fingerprint=excluded.fingerprint, created_epoch=excluded.created_epoch
  WHERE excluded.created_epoch > uri_fingerprint.created_epoch` — dedup-if-newer
  on disk, matching the arg_max rule for the ~3.3% dupes.
- **Stream `actor_identity_current` too** — 3.2M rows means `_copy_identity`'s
  `fetchall` is independently disqualified. fetchmany + executemany batches.

### Horizon (the product fix) — RATIFIED: 14 days
`uri_fingerprint` bounded to `created_epoch >= now - fingerprint_horizon_days`.
- **14 days** initial (operator disposition 2026-07-17). ~4.5× the consumer's
  72h overlap window. 7d too tight for delayed labels/reporting; 30d is
  inherited legacy folklore until the lag distribution proves otherwise.
- Env-configurable (`DRIFTWATCH_FACTS_FINGERPRINT_HORIZON_DAYS`, default 14).
- ~31M rows / ~2.5 GB facts.sqlite — tractable on the box.

### Make the bound visible (do not silently delete epistemology)
Manifest gains:
- `fingerprint_horizon_days`, `fingerprint_cutoff_epoch`
- `oldest_source_created_epoch`, `newest_source_created_epoch`
- `recent_label_uris_outside_horizon` — count of URIs from recent label events
  whose post falls before the cutoff (i.e. coverage the horizon is costing us)
- observed **label-lag distribution** (p50/p90/p99 of label_ts − post_created)
  — this is the data that decides whether 14 becomes 7, 30, or something
  stranger. The horizon is a hypothesis; the manifest is how it gets tested.

### Parity contract rewrite
The snapshot-001 parity test currently asserts "snapshot carries FULL history"
(the retention-horizon divergence). **That is now wrong.** Rewrite to:
> identical consumer-visible results **within the declared horizon**, plus an
> explicit out-of-horizon fixture (a valid row older than the cutoff is present
> in legacy, absent from the snapshot, and counted — not silently dropped).

## Current VM state (safe resting point, left as-is Friday)
- New image deployed + healthy (duckdb 1.3.0 in image; needed for the fix
  anyway). The OOM-ing writer path exists but is invoked by nothing (no cron).
- Old `facts.sqlite` (May-8, 5 uri_fingerprint rows) intact. labelwatch already
  hard-skips it on staleness — no consumer change from attempt-1.
- No cron installed. NQ maintenance windows self-expire ~00:08Z.
- Nothing to roll back on the VM.
