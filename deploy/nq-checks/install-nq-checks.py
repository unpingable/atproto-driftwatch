#!/usr/bin/env python3
"""Install/refresh the driftwatch G1 checks into an NQ database. Idempotent.

    python3 install-nq-checks.py --db /opt/notquery/nq.db [--dry-run]

These live only as rows in nq.db on the monitoring host. This script is the
canonical source so they survive loss or rebuild of that database.

NOTE: the NQ aggregator evaluates saved checks itself every generation (~60s)
and emits `check_failed` findings. No cron or timer is required — `nq-monitor
check` is a manual convenience, not the scheduler.
"""
import argparse, datetime, pathlib, sqlite3, sys

HERE = pathlib.Path(__file__).resolve().parent
CHECKS = [
    ("driftwatch volume free space", "driftwatch-volume-free-space.sql",
     "G1: /mnt/zonestorage free bytes below 1.5 GiB. This volume is invisible to "
     "the per-host disk model (v_hosts carries one filesystem per host, the root fs), "
     "which is why it filled to 100% on 2026-08-12 with no finding. Root-view free "
     "bytes, not non-root avail: avail reads 0 permanently due to ext4 reserve."),
    ("driftwatch db slack", "driftwatch-db-slack.sql",
     "G1: labeler.sqlite internal freelist below 5M pages (~19 GiB). auto_vacuum=none, "
     "so retention frees pages inside the file and disk free stays flat until the file "
     "must extend. Disk-free therefore only warns ~6h ahead; this warns ~1.5-2 days "
     "ahead by watching the internal slack that depletes first."),
]

def sql_body(path):
    # strip leading comment block; keep the executable statement
    lines = (HERE / path).read_text().splitlines()
    return "\n".join(l for l in lines if not l.lstrip().startswith("--")).strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(a.db, timeout=30)
    con.execute("PRAGMA busy_timeout=30000")
    cur = con.cursor()
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    existing = {r[0]: r[1] for r in cur.execute("SELECT name, query_id FROM saved_queries")}
    next_id = max((int(r[0]) for r in cur.execute("SELECT query_id FROM saved_queries")), default=0)

    for name, fn, desc in CHECKS:
        body = sql_body(fn)
        if not body:
            print(f"ERROR: {fn} produced empty SQL", file=sys.stderr); return 1
        if name in existing:
            print(f"UPDATE #{existing[name]}: {name}")
            if not a.dry_run:
                cur.execute("UPDATE saved_queries SET sql_text=?, description=?, "
                            "check_mode='non_empty', updated_at=? WHERE name=?",
                            (body, desc, now, name))
        else:
            next_id += 1
            print(f"INSERT #{next_id}: {name}")
            if not a.dry_run:
                cur.execute("INSERT INTO saved_queries (query_id,name,sql_text,description,"
                            "check_mode,check_threshold,check_column,pinned,created_at,updated_at) "
                            "VALUES (?,?,?,?,'non_empty',NULL,NULL,0,?,?)",
                            (str(next_id), name, body, desc, now, now))
    if a.dry_run:
        print("dry-run: no changes written"); con.rollback()
    else:
        con.commit(); print("committed")
    con.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
