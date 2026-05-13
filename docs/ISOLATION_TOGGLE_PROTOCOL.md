# Subsystem isolation toggle protocol

**Status:** documented, not yet executed. Filed 2026-05-07.

## Purpose

When stall captures from `/usr/local/bin/driftwatch_stall_watcher.py` show multi-subsystem reader concurrency but py-spy attribution rotates between candidates (no single dominant pinner), the next step is **isolation by toggle**: disable one suspect subsystem at a time, observe whether stall frequency drops, restore, repeat.

This protocol exists to make those toggles disciplined — not "let's see what happens if I turn off facts_export."

## Suspects and their kill switches

All env vars set on the VM via `/opt/driftwatch/deploy/docker-compose.override.yml`. Toggles take effect on container restart (~30s ingest gap).

| Subsystem | Disable env var | Notes |
|---|---|---|
| `facts_export` | `ENABLE_FACTS_EXPORT=0` | Removes the snapshot path that pins WAL during long source reads. Probably the highest-impact toggle. |
| `longitudinal` | `ENABLE_LONGITUDINAL_RECHECK=0` | Removes `recheck_once` thread-pool runs that scan claim_history. |
| `maintenance` | `ENABLE_MAINTENANCE=0` | 6h-cadence maintenance pass. Lowest churn; toggle last. |
| `retention` | `ENABLE_RETENTION=0` | **Highest-risk toggle.** Disabling means disk runway collapses. Only run for short windows (≤30 min) and only after the others have been ruled out. The 2026-04-23..05-05 incident scar is exactly here. |

## Pre-toggle checklist

Before any toggle:

1. Confirm stall watcher is running and capturing (`crontab -l | grep driftwatch_stall`).
2. Confirm geometry sampler is running (`crontab -l | grep driftwatch_geometry`).
3. Note current values for: `events_dropped_total`, `rollback_lost_total`, `aborted_passes`, `disk_free_gb`, `disk_runway_days`. These are the deltas we'll compare against the toggle window.
4. Pick a **window length** appropriate to the toggle:
   - `facts_export`, `longitudinal`, `maintenance`: 4 hours minimum (need to span at least one day/night transition or one stall-prone window).
   - `retention`: **30 minutes maximum**, and only after others have been tried. Disk pressure compounds fast.
5. Confirm there is no concurrent maintenance / experiment / archive cleanup. One variable at a time.
6. Capture `/health/extended` and a baseline `py-spy dump --pid <PID>` immediately before the toggle. File path: `/var/lib/driftwatch_isolation/baseline_<subsystem>_<timestamp>.txt`.

## Toggle order (default)

1. **`facts_export` first** — strongest prior, lowest disruption. If this materially drops stall frequency, Phase 3 of the cold-path gap spec becomes the immediate priority and we may stop here.
2. **`longitudinal` second** — second-strongest prior. Same evaluation gate.
3. **`maintenance` third** — lowest prior, but cheap and informative. If toggling this changes anything we learn we underestimated it.
4. **`retention` last and briefly** — only run if the above three together have not isolated the dominant pinner. Strict 30-min window. Disk runway compresses during this; the question being answered is whether retention's own snapshot is the dominant pinner, not whether retention can be disabled in production.

## Observation window

For each toggle:

- **Trigger frequency:** count stall watcher captures during the toggle window vs same length pre-toggle window. A ≥50% reduction is suggestive; ≥80% is strong.
- **Drop fraction:** compare 15-min sample drop_frac distribution from `geometry_samples.jsonl` for the toggle window vs prior 24h baseline. Look at p50, p90, max.
- **WAL distribution:** same — p50, p90, p99 from samples in the window.
- **Disk slope:** compute disk_free_bytes regression over the window. If toggling adds load elsewhere (e.g., disabling facts_export means less disk free released) we should see it.
- **Restore-and-recur:** after the window, restore the env var and observe for an equal window. Stalls should return at prior frequency. If they don't, something else changed (variable contamination — invalidates the toggle).

## Discriminating outcomes

| Pattern | Interpretation |
|---|---|
| Stall frequency drops sharply with one toggle, returns on restore | That subsystem is the dominant pinner. Phase 3/4 of cold-path spec moves to authorization-to-build for that subsystem. |
| Stall frequency drops modestly with several toggles, no single dominant | Substrate-level contention. Multiple subsystems contribute. Cold-path spec promotion strengthens; Phase 3 still likely first because facts_export is removable as a unit. |
| No toggle changes stall frequency | Reconsider — either the trigger thresholds are wrong, or the pinner is something we haven't enumerated (e.g., the retention scheduler itself, or an internal SQLite checkpoint thread). Re-examine py-spy captures with finer granularity. |
| Stalls get worse with a toggle | Some subsystem is *protecting* the writer somehow (unlikely but possible — e.g., retention may currently be releasing pages that prevent worse fragmentation). Investigate before drawing any architectural conclusion. |

## Anti-patterns

- **Toggling more than one variable at once.** "Let me turn off both facts_export and longitudinal to be sure" is exactly how we learn nothing.
- **Skipping baseline captures.** Without pre-toggle py-spy + counters, we can't compute deltas.
- **Running retention-off windows longer than 30 minutes.** The 2026-04-23..05-05 arc is the scar tissue.
- **Treating one toggle window as conclusive.** Stall episodes are roughly Poisson-ish (~10/day) — one 4h window with zero stalls might be noise. Want at least one stall during the window for the absence-of-stalls case to be informative.
- **Running toggles during known burst periods** (Bluesky-known posting peaks, etc.) — confounds the signal.

## Rollback

Each toggle has a clean rollback: restore the env var, restart the container.

```bash
# example for facts_export
ssh -i ~/git/claude/ssh/linode root@192.46.223.21 "
  sed -i 's/ENABLE_FACTS_EXPORT: \"0\"/ENABLE_FACTS_EXPORT: \"1\"/' \
    /opt/driftwatch/deploy/docker-compose.override.yml &&
  cd /opt/driftwatch/deploy && docker compose -f docker-compose.prod.yml -f docker-compose.override.yml up -d
"
```

`ENABLE_RETENTION=0` rollback is the **most time-sensitive**. Restore on a 30-minute hard timer regardless of what evidence has accumulated.

## Decision gate

The protocol's purpose is not to *fix* anything. It is to produce **evidence** that informs the cold-path gap spec promotion decision. After running enough toggles to have a signal:

- Document findings in `gap-spec-cold-path-update-<date>.md` (companion file).
- Bring findings to the user for the explicit authorization-to-build decision on Phase 3 (facts_export → Parquet) or Phase 4 (longitudinal split), per `gap-spec-cold-path-parquet-duckdb.md`.
- Do NOT begin Phase 1+ implementation work without that explicit authorization.

## Related

- `specs/gaps/gap-spec-cold-path-parquet-duckdb.md` — the architectural target this protocol gathers evidence for.
- `specs/gaps/gap-spec-cold-path-update-2026-05-07.md` — current evidence/tripwire status.
- `/usr/local/bin/driftwatch_stall_watcher.py` (on VM) — capture mechanism.
- `/usr/local/bin/driftwatch_geometry_sample.py` (on VM) — geometry sampler that feeds before/after comparison.
- Continuity memory: `mem_4ffb4a1d` (current project_state), `mem_6f09dbc4` (solution-family exhaustion hypothesis).
