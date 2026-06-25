#!/usr/bin/env bash
# resolver_second_read_check.sh  --  OBSERVATION-ONLY second-read backstop.
#
# Doctrine: cron observes; agent interprets; human authorizes.
# This script MUST NOT: edit docs, git add/commit/push, or change resolver
# config. It does exactly three things:
#   1. pull (read-only) the sampler JSONL from the VM
#   2. compute fixed deltas vs the 2026-06-25 first read + dumb tripwires
#   3. write ONE dated artifact under reports/
# Then a human pings the agent for the reasoned read. Nothing here decides
# "fix vs tolerate" -- the thresholds below are deliberately blunt.
#
# One-shot: invoked by a date-guarded crontab line for 2026-06-29 ~15:10 UTC.
set -euo pipefail

REPO=/home/jbeck/git/atproto-nutrition/driftwatch
OUT="${SECOND_READ_OUT:-$REPO/reports/resolver-pending-second-read-2026-06-29.txt}"
KEY="$HOME/git/claude/ssh/linode"
REMOTE=root@192.46.223.21
JSONL=/mnt/zonestorage/driftwatch/data/resolver_pending_samples.jsonl

mkdir -p "$REPO/reports"
TMP="$(mktemp)"; ERR="$(mktemp)"
trap 'rm -f "$TMP" "$ERR"' EXIT

# BatchMode so cron never hangs on an auth prompt.
if ! scp -i "$KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=20 \
       "$REMOTE:$JSONL" "$TMP" 2>"$ERR"; then
  {
    echo "resolver second-read backstop -- PULL FAILED at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "could not scp $REMOTE:$JSONL"
    sed 's/^/  ssh: /' "$ERR"
    echo
    echo "ACTION: ping the agent; pull the JSONL manually for the reasoned read."
  } > "$OUT"
  exit 0   # observe-only: never fail loudly from cron
fi

python3 - "$TMP" "$OUT" <<'PY'
import json, sys, datetime as dt
rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
out = open(sys.argv[2], "w")
def p(*a): print(*a, file=out)

# --- first-read reference (end of the 2026-06-25 first read) ---
FR = {"ts": "2026-06-25T16:07:01Z", "pending_total": 323742,
      "oldest_pending_hours": 249.70, "pending_gt_72h": 242512,
      "pending_gt_168h": 110817}

def t(s): return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
def slope(key, n=24):
    seg = rows[-n:] if len(rows) >= 2 else rows
    if len(seg) < 2: return 0.0
    a, b = seg[0], seg[-1]
    h = (t(b["ts_utc"]) - t(a["ts_utc"])).total_seconds() / 3600 or 1
    return (b[key] - a[key]) / h

last = rows[-1]
span_h = (t(last["ts_utc"]) - t(rows[0]["ts_utc"])).total_seconds() / 3600

# --- blunt tripwire thresholds (intentionally dumb; agent reasons from raw #s) ---
T1_gt168_margin, T1_gt168_slope = 2000, 10.0    # still meaningfully above FR AND rising
T2_oldest_material            = 256.0           # materially above the ~250h floor
T3_pending_margin, T3_slope   = 5000, 50.0      # count rising, not just no-surplus

s168  = slope("pending_gt_168h")
spend = slope("pending_total")
sold  = slope("oldest_pending_hours")

t1 = (last["pending_gt_168h"] > FR["pending_gt_168h"] + T1_gt168_margin) and (s168 > T1_gt168_slope)
t2 = last["oldest_pending_hours"] > T2_oldest_material
t3 = (last["pending_total"] > FR["pending_total"] + T3_pending_margin) and (spend > T3_slope)
def mark(b): return "TRIP" if b else "pass"

p("=" * 72)
p("RESOLVER PENDING -- SECOND-READ ARTIFACT (OBSERVATION ONLY)")
p("cron observes; agent interprets; human authorizes.")
p("This file is mechanical. It does NOT decide fix-vs-tolerate. Ping the agent.")
p("=" * 72)
p(f"generated_utc : {dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}")
p(f"series        : {rows[0]['ts_utc']} -> {last['ts_utc']}  ({len(rows)} rows, {span_h:.1f}h)")
p(f"first_read_ref: {FR['ts']}")
p("")
p("-- latest sample --")
for k in ("pending_total","oldest_pending_hours","pending_gt_24h","pending_gt_72h",
          "pending_gt_168h","attempts_last_hour","new_pending_last_hour"):
    p(f"  {k:<22} {last.get(k)}")
p("")
p("-- delta vs first read (2026-06-25T16:07Z) --")
for k in ("pending_total","oldest_pending_hours","pending_gt_72h","pending_gt_168h"):
    d = last[k] - FR[k]
    p(f"  {k:<22} {FR[k]:>10} -> {last[k]:>10}   d={d:>+10.2f}")
p("")
p("-- recent slopes (per hour, last <=24 samples) --")
p(f"  pending_gt_168h        {s168:>+8.1f}/h")
p(f"  pending_total          {spend:>+8.1f}/h")
p(f"  oldest_pending_hours   {sold:>+8.3f}/h")
p("")
p("-- tripwires (blunt thresholds) --")
p(f"  [{mark(t1)}] T1 gt168 still rising  (> FR+{T1_gt168_margin} AND slope > {T1_gt168_slope}/h)")
p(f"  [{mark(t2)}] T2 oldest material     (> {T2_oldest_material}h)")
p(f"  [{mark(t3)}] T3 pending_total rising(> FR+{T3_pending_margin} AND slope > {T3_slope}/h)")
p("")
if t1 or t2 or t3:
    p(">>> TRIPWIRE(S) HIT -- agent should evaluate the dedicated-backlog-lane fix.")
    if t3: p(">>> T3 in particular: capacity may be LOSING, not merely no-surplus.")
else:
    p(">>> No tripwire hit -- consistent with the 3-7d wave having plateaued")
    p(">>> (bounded coverage lag). Agent: confirm against full series, likely leave.")
p("")
p("-- tail of series (last 12) --")
for d in rows[-12:]:
    p(f"  {d['ts_utc']}  pend={d['pending_total']:>7}  old_h={d['oldest_pending_hours']:>7.2f}"
      f"  gt72={d['pending_gt_72h']:>7}  gt168={d['pending_gt_168h']:>7}")
out.close()
PY

echo "second-read artifact written: $OUT"
