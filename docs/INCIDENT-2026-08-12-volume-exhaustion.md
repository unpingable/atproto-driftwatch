# Incident: volume exhaustion and nine-day blind period

**2026-08-12 04:31 UTC → 2026-08-21 22:47 UTC.** Detected 2026-08-20 by audit,
not by alert.

> Driftwatch was blind for nine days while its process-level health remained
> green; recovery proved that its original storage envelope was viable, but its
> ability to enforce and observe that envelope had failed.

That is the incident. Everything below is detail.

---

## 1. What happened

`/mnt/zonestorage` (196 G, the Driftwatch data volume) reached 0 bytes free on
2026-08-12. The consumer entered a hot loop, retrying ~4 failed commits per
second, each ending `sqlite3.OperationalError: database or disk is full` and
rolling back its batch. The cursor froze at `2026-08-12T04:31:02Z`.

Throughout, `/health` returned `{"status":"ok"}` and Docker reported the
container `healthy` with `Restarts: 0`. Nothing alerted. The condition was found
on 2026-08-20 during an unrelated read-only audit.

**Loss: 57,199,140 events (`rollback_lost_total`) plus 613,541 dropped, across
8.4 days.**

### Why the volume filled

After the 2026-07-27 rebuild the database was recreated fresh (~1.8 GiB) on an
empty volume, and retention windows were widened to **30d across the board** as a
deliberate growth-curve experiment. The override carried its own tripwire:

> "if steady-state ingest exceeds ~6GB/day the 30d windows will not fit the
> volume and must be revisited **BEFORE the runway closes**."

Observed growth was **~12.1 GB/day** — twice the stated threshold — from the
second day onward. `maintenance.log` records the ramp: 11 G on 07-28, 193 G on
08-12, straight-line, no spike. With 30-day windows nothing became age-eligible
for pruning until 2026-08-26; the volume filled on 08-12, **fourteen days early**.

The experiment's own falsification criterion fired around 07-29/30 and was not
acted on. Nothing was watching for it.

### Why nothing alerted

Two independent failures, both structural:

1. **The volume was never collected.** The Debian/Ubuntu `prometheus-node-exporter`
   package patches the upstream default exclude regex to
   `^/(dev|proc|run|sys|mnt|media|var/lib/docker/.+)($|/)`. Upstream does **not**
   exclude `/mnt`. So from the day the volume was mounted at `/mnt/zonestorage`
   (2026-04-17) it was structurally invisible to node_exporter, and therefore to
   NQ. No alert was ever possible. Choosing that mount path silently opted the
   volume out of monitoring.

2. **The volume was not in the check surface.** NQ's `hosts_current` / `v_hosts`
   carry **one filesystem per host** — the root fs. The built-in `disk critical`
   check (`disk_used_pct > 95`) therefore reported `PASS` while the data volume
   sat at 100%. Fixing (1) alone does not fix (2): metrics landed in
   `series`/`metrics_current` but no saved check read them.

### This was the second occurrence

`specs/gaps/gap-spec-witness-coverage-requirements.md`, filed **2026-07-28**
after a 3d20h outage of exactly this shape (2026-07-23…27), opens by describing
"zero ingest, cursor frozen, volume at 0 bytes free" while `/health` returned ok,
and notes the witness "had no driftwatch service subject and no
`/mnt/zonestorage` disk subject". Sixteen requirements (H-1…A-11) were written.
None were implemented. The identical failure recurred fifteen days later and ran
**2.2× longer**.

---

## 2. The envelope was never the problem

`maintenance.log` contains the decisive evidence. Under the pre-experiment
windows (events 3d / edges 7d / claims 7d) the database held:

| Period | Size | Duration |
|---|---|---|
| 2026-03-09 … 04-06 | 54 G | ~4 weeks, flat |
| 2026-04-15 … 04-23 | 54 G | ~1 week, flat |
| **2026-05-10 … 07-13** | **120 G** | **65 consecutive days, flat** |

A 65-day flat line on a 196 G volume is a genuine equilibrium, not drift.

Growth resumed on 07-14 and ran to 187 G by 07-23 — coincident with the
documented incident chain (07-13 WAL pinned by an orphaned reader; 07-18…20
retention self-lock; 07-23 volume full). **The windows equilibrate fine on this
hardware; retention failing to run is what breaks them.** Any postmortem that
concludes "the retention windows were too generous" is reading the wrong signal.

---

## 3. Recovery

Sequenced deliberately; every step declared to NQ before execution.

| # | Action | Result |
|---|---|---|
| 1 | Copy the 13 G `cold-offload` Parquet archive off-host | 61/61 partitions verified byte-for-byte by independent md5; 13,235,032,844 bytes |
| 2 | Stop the container | Hot loop halted |
| 3 | Retire `facts.sqlite` (May-8, superseded) — copy to root disk, md5-verify, remove from volume | **3.1 G freed** — the only expendable object on the full volume |
| 4 | Revert retention: events 30d→3d, edges 30d→7d | Confirmed in the loop banner |
| 5 | Restart with `FIREHOSE_AUTO_START=0` | Retention drains without writer contention |
| 6 | `RETENTION_BATCH_SLEEP_SEC` 2.0→0.25 for the drain only | Reclaim 6.5 → 168 GiB/day |
| 7 | **H-1**: remove `/mnt` from node_exporter's exclude regex | 7 filesystem series live in NQ, inodes included |
| 8 | **G1**: two saved NQ checks (see §5) | Live-fire verified — finding emitted in ~10s |

Step 1 came first because it was the only irreversible-loss risk: the July
rebuild destroyed the database those rows came from, and with no off-host backup
it was the sole surviving copy of May–June claim history.

### Ordering constraint that mattered

Retention frees space by deleting rows, and deletes are writes. At zero bytes
free every retention pass aborted before it could release a page — a self-sealing
failure. Space had to come from outside the database first (step 3), which is why
freeing 3.1 G was load-bearing rather than housekeeping.

### An error made during recovery

While sizing the drain, a `COUNT(*)` over `events` was run. Its SSH session timed
out but **the remote query kept running**, holding a read snapshot for ~14
minutes. That pinned the WAL: 63 MB → 1.1 GB at 2 MB/s, consuming a third of the
headroom just freed. It was initially misread as retention misbehaving. Killing
the stray reader let `wal_checkpoint(TRUNCATE)` succeed (`0|0|0`) and the WAL
stayed at 8–64 MB thereafter.

This is the same failure shape as 2026-07-13 — an orphaned reader pinning the
WAL — which is exactly why requirement **A-5** exists. Do not run unbounded
`COUNT(*)` against this database; use `SELECT NOT EXISTS(SELECT 1 FROM t)` and
`LIMIT`-bounded counts.

---

## 4. Drain and resume

The drain ran **24 hours**, watched continuously with guards that would have
aborted on disk, WAL, or container failure. None tripped.

| Signal | At convergence (08-21 20:41) |
|---|---|
| `events` / `edges` | 37 rows / 9 rows — drained |
| live data | **17.97 GiB** |
| freelist | 174.75 GiB reclaimed |
| `page_count` | 50,518,271 — unchanged |
| WAL | 0 bytes, clean checkpoint |

**17.97 GiB is the monotonic baseline** — `identity_events`,
`actor_identity_current` and 30 days of `claim_history`, the tables no retention
window bounds. This is only measurable at the drain floor and is the number that
governs long-term capacity. Note the drain *cannot* reveal steady-state size:
with ingest off every row ages past the 3d/7d windows, so events and edges go to
zero regardless.

Ingest resumed 2026-08-21 22:47 UTC after re-declaring maintenance windows and
reverting the two drain-only settings.

| Signal | Catch-up | Steady state |
|---|---|---|
| cursor gap | 28.02 h | **0.5 s** |
| `drop_frac` | 0.26 – 0.88 | **0.0** |
| dropped / 60s | 42k – 130k | **0** |
| `gate_reasons` | lag_high, high_drop_rate | **[] empty** |
| eps | 389 – 966 | **94.9** (pre-incident baseline 82.8) |
| `rollback_lost` | 0 | **0** |
| `page_count` | 50,518,271 | **50,518,271** |

**The capacity model held exactly.** Across ~2¼ hours of ingest including a
25×-real-time replay of 28 hours of backlog, `page_count` never moved. Writes
reused reclaimed pages instead of extending the file, so the 3.03 GiB of
filesystem headroom was never touched. The 6.6 M events shed during catch-up were
the designed pressure response to 25× volume through a 5,000-slot queue — not
write failure, which `rollback_lost=0` throughout confirms.

---

## 5. What changed permanently

- **node_exporter** — `/etc/default/prometheus-node-exporter` now sets
  `--collector.filesystem.mount-points-exclude` without `/mnt`. Backup at
  `.bak.20260820`. Both `avail` (non-root, reads 0) and `free` (root, the number
  matching the writer) are now exported, satisfying **H-2** as well as **H-1**.
- **Two NQ saved checks** — canonical source in `deploy/nq-checks/`, reinstallable
  via `install-nq-checks.py`. The aggregator evaluates saved checks every
  generation (~60s); no cron is required.
- **Production override** — captured to `deploy/docker-compose.override.prod.yml`.
  It previously existed only on the VM.
- **`facts.sqlite` retired** — at `/opt/driftwatch/retired/facts.sqlite.2026-05-08`.
  Labelwatch already hard-skipped it on staleness and degrades to a `no_facts`
  caveat; verified healthy after removal.

### Warning horizon, and why there are two checks

With `auto_vacuum=none` the file does not extend until internal slack is
exhausted. So if retention stops, `df` reads perfectly flat for ~4.3 days at
12 GB/day and only moves in the final ~6 hours.

| Stage | Budget |
|---|---|
| equilibrium → file high-water mark | ~4.3 days |
| high-water mark → hard wedge | ~6 hours |
| **total warning window** | **~4.5 days** |

A disk-free check alone therefore buys ~6 hours. `driftwatch-db-slack.sql`
watches the freelist — the thing that actually depletes first — and restores
~1.5–2 days. Last time nobody looked for eight days; detection must land well
inside 4.5.

---

## 6. Still open

- **G3 / requirement A-1 — cursor-age check.** `now − last_cursor.updated_at`
  beyond ~30 min should be a finding regardless of every other green light. Disk
  is only one way to go blind; this single signal catches the whole class. Needs
  plumbing to get the value out of `/health/extended` into NQ. **Not done.**
- **The remaining witness requirements** (H-3…H-5, A-2…A-11) from
  `gap-spec-witness-coverage-requirements.md`.
- **No off-host backup** — `gap-spec-off-host-backup-labelwatch-driftwatch.md`
  remains open. The 13 G archive copy made during recovery is a one-off copy on a
  workstation, **not a backup system**.
- **`platform_health` reads `degraded`** with empty gates, zero drop and zero lag:
  the EWMA baseline was learned during catch-up (`baseline_eps=405`) while real
  throughput is ~95. Self-correcting reporting artifact, not a fault.
- **`freelist pressure` check FAILs** — accurate, ~166 GiB genuinely reclaimable.
  Resolves as the DB refills. Not silenced.
- **Compaction window closed.** At the drain floor, live data was 17.97 GiB
  against ~21 GiB free on root — `VACUUM INTO` was briefly feasible for the first
  time. It is now 26.1 GiB and climbing. The file cannot be compacted in place;
  that remains the case `gap-spec-cold-path-parquet-duckdb.md` exists to answer.
- **`CLAIM_RETENTION_SEC` left at 30d**, deliberately. Claim pruning archives to
  Parquet on the same volume before deleting (~250 MB per day-partition); moving
  to 7d would write ~2.2 GiB into ~3.0 GiB of headroom. Revisit only with real
  headroom. Do **not** set `ENABLE_RETENTION_PARQUET_CAPTURE=0` as a shortcut on
  build `70109c8` — that activates the legacy JSONL writer which deletes with a
  positional predicate unrelated to what it archived (see `ARCHIVE_PROTOCOL.md`).

---

## 7. Lessons

1. **Liveness is not observation.** `/health` returning ok, and Docker reporting
   `healthy`, are statements about a process, not about whether the observatory
   is observing. Requirement A-11 forbids sourcing health from either. Both lied
   for nine days.
2. **Collection is not detection.** Making the volume visible to NQ did not make
   it alertable; the check surface read a different table. Half-closing this gap
   would have felt like a fix and prevented nothing.
3. **An experiment with a stated tripwire needs something watching the tripwire.**
   The 6 GB/day threshold was written down and was crossed on day two.
4. **Packaging defaults are load-bearing.** A distro patch to one regex decided
   that this volume could never be monitored, silently, four months before it
   mattered.
5. **A guardrail you have not seen fire is not a guardrail.** Both new checks were
   live-fire tested by temporarily moving the threshold until they tripped.
