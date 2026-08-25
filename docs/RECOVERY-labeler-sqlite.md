# Recovering labeler.sqlite from freelist exhaustion

Companion to `INCIDENT-2026-08-12-volume-exhaustion.md`. That document records
what happened; this one records how to get out of it and what was repaired so
it cannot recur silently.

Tooling: `scripts/recover_labeler_db.py`.

## The situation this addresses

Measured 2026-08-25 on production:

| | |
|---|---|
| file size | 206,922,838,016 B (192.7 GiB) |
| page_size / page_count | 4096 / 50,518,271 |
| freelist_count | 35,536,691 (135.6 GiB, 70.3%) |
| live data | ~57.2 GiB |
| auto_vacuum | **NONE (0)** |
| journal_mode | wal |
| volume | `/mnt/zonestorage`, 196 GiB, 100% full |

Retention works. It performs logical DELETEs, which return pages to SQLite's
in-file freelist and never shrink the file. With `auto_vacuum=NONE` there is no
mechanism to hand those pages back to the filesystem, so the freelist grows
monotonically until the volume fills. The four mechanisms are distinct and
should not be collapsed into "disk got full":

1. **logical retention/deletion** — working
2. **freelist accumulation** — the trapped 135.6 GiB
3. **physical compaction** — never ran
4. **filesystem pressure** — the consequence

Pressure-controller aborts (`pressure_returned:backlog>1500`) are a *symptom*:
the scheduler correctly refusing to add write pressure. They did not cause this.

## Why external scratch is mandatory

`VACUUM` needs room for a full copy. `VACUUM INTO` needs room for a compact
copy. The data volume has 0 bytes free and cannot stage its own rebuild;
`/` has ~28 GiB against a ~69 GiB requirement (57.8 GiB live × 1.2 margin).

`incremental_vacuum` is the one route that needs no scratch — and it is
unavailable here, because it is a no-op unless the database is
`auto_vacuum=INCREMENTAL`, and converting an existing database to that mode
itself requires a rebuild. That is the deadlock: **the only cheap fix requires
having already applied the expensive one.**

## Verified SQLite semantics

Measured on both deployed runtimes (container SQLite 3.46.1, host 3.37.2),
identical results:

```
PRAGMA auto_vacuum=INCREMENTAL;
VACUUM INTO '/scratch/labeler.rebuilt.sqlite';
```

* source stays `auto_vacuum=0` and **byte-identical** (sha256 unchanged)
* destination is written as `auto_vacuum=2`
* destination passes `PRAGMA integrity_check`
* destination is compacted (10025 → 5020 pages on the fixture)

So a single `VACUUM INTO` produces a replacement already configured for
in-place reclamation. No second conversion pass is needed.

### The one-page trap

`PRAGMA incremental_vacuum(N)` reclaims **one page**, not N, when issued via
Python's `conn.execute()` — the pragma runs inside the driver's implicit
transaction and is stepped once. `.fetchall()` does not help. Measured with
freelist 15036, N=1000:

| invocation | pages reclaimed |
|---|---|
| `conn.execute("PRAGMA incremental_vacuum(1000)")` | 1 |
| `conn.execute(...).fetchall()` | 1 |
| `conn.executescript("PRAGMA incremental_vacuum(1000);")` | 1000 |
| `sqlite3` CLI | 1000 |

All three retention call sites used the broken form. Had the database been
flipped to mode 2 without fixing them, retention would have reclaimed 4 KB per
pass — roughly 384 KB/day against a 135.6 GiB freelist. Use
`maintenance.incremental_vacuum_chunk()`, which is tested against this
specifically.

## Procedure

```bash
# 1. preflight — mutates nothing, safe against live production
./scripts/recover_labeler_db.py --scratch /mnt/recovery \
    --expect-page-count 50518271

# 2. execute
./scripts/recover_labeler_db.py --scratch /mnt/recovery \
    --expect-page-count 50518271 --execute
```

Scratch sizing:

* **~250 GiB** (live × 1.2 + the 193 GiB original) — fully reversible, default,
  recommended.
* **~70 GiB** (live × 1.2 only) — the verified compact copy on scratch is the
  sole rollback artifact. Requires `--accept-no-original-backup`.

The tool refuses on: missing source; source already mode 2; unexpected
page_size; page_count drift >25% from `--expect-page-count`; scratch missing,
unwritable, or on the database's own filesystem; scratch too small; destination
already present; docker unavailable; failed quiesce; leftover DB writers; failed
`integrity_check`; replacement not reporting mode 2; per-table row-count
mismatch.

Ordering note: the original occupies 193 GiB of a 196 GiB volume, so the
replacement cannot be staged beside it. It is built on scratch and copied back,
which leaves a window where the data volume holds no database. Rollback closes
that window — that is why the backup copy is the default and skipping it is an
explicit flag.

## After restart, qualify — do not trust `status: ok`

Recovery is not "the process came back". Watch for:

* free space on `/mnt/zonestorage`, and DB size/freelist
* EPS against the ~405 baseline (was ~93)
* coverage (was ~23%)
* resolver backlog *falling* (was ~224k)
* `events_dropped_total` flat
* gate reasons clearing `platform_low_eps`
* retention passes completing instead of aborting

If throughput stays low or the backlog does not drain once storage is healthy,
that is a **separate surviving pipeline problem**, not a reason to mutate the
database further.

## What was repaired so this cannot recur silently

* **Runway metric** was a pure derivative and returned `None` at zero free
  bytes, with both alarm branches gated on `is not None`. It now evaluates an
  absolute floor first, so a full volume reports zero runway and `critical`.
* **Emergency brake** was armed only inside `run_maintenance_once()`, which
  never ran because `ENABLE_MAINTENANCE=false`. Disk *reporting* was inline and
  worked; disk *acting* sat in a disabled loop. The brake is now derived from
  the same sample that feeds reporting, with hysteresis and a defined release.
* **Brake state location** was `DATA_DIR/.disk_pressure` — on the volume whose
  exhaustion it signals. It is now in-process; arming requires no write to the
  full filesystem.
* **auto_vacuum** is set explicitly for new databases instead of inheriting
  SQLite's `NONE`. Existing mode-0 databases are deliberately left alone.
* **Reclaim status** distinguishes `ok` / `noop` / `mode_incompatible` /
  `failed`. A database structurally unable to reclaim space is a fault, not an
  idle pass, and `/health/extended` surfaces it under `reclaim`.
* **Recovery-capacity invariant** (`recovery_capacity`) compares live bytes ×
  margin against *qualified* workspace. It is a level, not a derivative, so
  unlike runway it cannot go silent at saturation.
* **Growth rate** is published as `db_growth_gb_per_day` rather than living in
  a log line someone was expected to read daily.
* **Retention lag** now measures from the oldest *believable* row.
  `events.ctime` comes from record-supplied `createdAt` and is caller-
  controlled; a single 2002-04-01 row drove `retention_lag_s` to ~11.8 years.

### Deploying the brake repair on an already-full volume

Be deliberate about this. On a volume at 100%, the repaired brake engages
immediately and cannot release until the rebuild lands, because release
requires usage below `DISK_RELEASE_THRESHOLD`. The consumer pauses ingest
entirely while engaged — a full observation blackout for the duration.

That may well be correct: it stops freelist consumption, which is what is
driving toward hard write failure. But it is an operational decision.
`DISK_BRAKE_ENABLED=0` disarms it. Disarming is deliberately **loud** — pressure
is still evaluated, `brake_would_engage` is still reported, and health carries
`brake_disarmed_warning`. A disarmed brake must never again look identical to a
brake that simply is not tripping.

## Open follow-ups

* `/var/lib/labelwatch/labelwatch.db` — 70.3 GiB, **freelist 0**, so it does not
  have this pathology today, but it is `auto_vacuum=NONE` and therefore carries
  the same latent exposure the moment it starts bulk-deleting.
* `events.ctime` accepts values from 2002. Retention-lag now floors them; nobody
  validates them at ingest.
