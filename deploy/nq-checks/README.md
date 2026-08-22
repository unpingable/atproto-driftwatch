# NQ saved checks for Driftwatch

Canonical source for the checks that watch Driftwatch's storage envelope.
They exist in production only as rows in `saved_queries` inside
`/opt/notquery/nq.db`; these files are the durable copy.

## Install / refresh

```bash
python3 install-nq-checks.py --db /opt/notquery/nq.db --dry-run   # inspect
python3 install-nq-checks.py --db /opt/notquery/nq.db             # apply
```

Idempotent: updates by `name` if present, inserts otherwise.

## No scheduling required

The NQ **aggregator evaluates saved checks itself every generation (~60s)** and
emits `check_failed` findings. `nq-monitor check --db ...` is a manual
convenience for inspecting results, not the scheduler. There is deliberately no
cron entry or systemd timer.

## The two checks

| File | Fires when | Warning horizon |
|---|---|---|
| `driftwatch-volume-free-space.sql` | `/mnt/zonestorage` free < 1.5 GiB | ~6 hours |
| `driftwatch-db-slack.sql` | `labeler.sqlite` freelist < 5M pages (~19 GiB) | ~1.5–2 days |

Both are `check_mode = non_empty`: any returned row is a failure.

**They are a pair, and the second one is the important one.** `labeler.sqlite`
runs `auto_vacuum=none`, so retention frees pages inside the file and the file
never shrinks. If retention stops, the DB eats internal slack for ~4.3 days while
`df` reads perfectly flat, and disk-free only moves in the final ~6 hours. The
freelist check watches what actually depletes first.

## Two traps

- **Use `node_filesystem_free_bytes`, not `node_filesystem_avail_bytes`.** On this
  volume `avail` reads 0 permanently — ext4 reserves 5% and the production writer
  runs as root, so `free` is the number matching the effective writer (requirement
  H-2).
- **Do not rely on `v_hosts` / the built-in `disk critical` check for this volume.**
  `hosts_current` carries one filesystem per host (the root fs). During the
  2026-08-12 outage, with the data volume at 100% and 0 bytes available,
  `disk critical` reported `PASS`.

## Verifying a change

A check you have not seen fire is not a check. To live-fire test, temporarily
widen the threshold so current readings breach it, confirm a `check_failed`
finding appears (it takes ~10s), then restore and confirm it clears.

## Provenance

Installed 2026-08-20 as guardrail **G1** following the 2026-08-12…08-20 blind
period. Background: `docs/INCIDENT-2026-08-12-volume-exhaustion.md`.
Requirements: `specs/gaps/gap-spec-witness-coverage-requirements.md` (H-1, H-2).
