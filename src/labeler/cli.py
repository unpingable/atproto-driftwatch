import argparse
import json
import os
import pathlib
import hashlib

from .db import get_conn
from .stability import load_items, compute_stability_report, stability_thresholds_from_env, evaluate_stability
from .driftmetrics import cluster_report


def quarantine_list(limit: int = 50):
    limit = max(1, min(int(limit), 500))
    conn = get_conn()
    rows = conn.execute(
        "SELECT emit_id, created_at, emit_mode, emit_status, emit_reason, payload_json FROM quarantine_emits ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        payload = {}
        try:
            payload = json.loads(r[5] or "{}")
        except Exception:
            payload = {}
        out.append(
            {
                "emit_id": r[0],
                "created_at": r[1],
                "emit_mode": r[2],
                "emit_status": r[3],
                "emit_reason": r[4],
                "payload": payload,
            }
        )
    print(json.dumps(out, indent=2, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(prog="labeler.cli")
    sub = parser.add_subparsers(dest="cmd")

    q = sub.add_parser("quarantine")
    qsub = q.add_subparsers(dest="qcmd")
    qlist = qsub.add_parser("list")
    qlist.add_argument("--limit", type=int, default=50)

    qshow = qsub.add_parser("show")
    qshow.add_argument("emit_id")

    st = sub.add_parser("stability-test")
    st.add_argument("--input", default="fixtures/fingerprint_extended.jsonl")
    st.add_argument("--out", default="out/stability_report.json")
    st.add_argument("--limit", type=int, default=0)

    rel = sub.add_parser("release")
    relsub = rel.add_subparsers(dest="rcmd")
    rq = relsub.add_parser("quarantine")
    rq.add_argument("--report", default="out/stability_report.json")
    rq.add_argument("--out", default="out/release_manifest_quarantine.json")
    rq.add_argument("--force", action="store_true")

    prom = relsub.add_parser("promote")
    prom.add_argument("--in", dest="in_path", default="out/release_manifest_quarantine.json")
    prom.add_argument("--out", default="out/release_manifest_prod.json")

    dw = sub.add_parser("driftwatch")
    dwsub = dw.add_subparsers(dest="dwcmd")
    dwsub.add_parser("preflight", help="run preflight checks")
    dwsub.add_parser("maintenance", help="run maintenance pass (label expiry, disk check, growth stats)")
    dwsub.add_parser("retention", help="run retention pass (strip raw, archive+prune old data)")
    dwsub.add_parser("bake", help="check bake gate status (baselines trustworthy?)")
    dwreport = dwsub.add_parser("report")
    dwreport.add_argument("--top", type=int, default=20, help="top N clusters by burst score")
    dwreport.add_argument("--hours", type=int, default=24, help="lookback window in hours")
    dwreport.add_argument("--bin-hours", type=int, default=1, help="time bin width in hours")
    dwreport.add_argument("--out", default="out/driftwatch_report.json", help="output path")

    args = parser.parse_args()
    if args.cmd == "quarantine" and args.qcmd == "list":
        quarantine_list(args.limit)
    elif args.cmd == "quarantine" and args.qcmd == "show":
        conn = get_conn()
        rows = conn.execute(
            "SELECT emit_id, created_at, emit_mode, emit_status, emit_reason, payload_json FROM quarantine_emits WHERE emit_id = ?",
            (args.emit_id,),
        ).fetchall()
        conn.close()
        if not rows:
            print("{}")
            return
        r = rows[0]
        payload = {}
        try:
            payload = json.loads(r[5] or "{}")
        except Exception:
            payload = {}
        out = {
            "emit_id": r[0],
            "created_at": r[1],
            "emit_mode": r[2],
            "emit_status": r[3],
            "emit_reason": r[4],
            "payload": payload,
        }
        print(json.dumps(out, indent=2, sort_keys=True))
    elif args.cmd == "stability-test":
        items = load_items(args.input, limit=args.limit or None)
        report = compute_stability_report(items)
        thresholds = stability_thresholds_from_env()
        ok, checks = evaluate_stability(report, thresholds)
        report["measurement_window"] = {
            "input_limit": args.limit or 0,
            "bounded_state": False,
            "capacity": None,
            "ttl": None,
        }
        report["thresholds"] = thresholds
        report["checks"] = checks
        report["ok"] = ok
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2, sort_keys=True)
        print(json.dumps({"ok": ok, "out": args.out}, sort_keys=True))
    elif args.cmd == "release" and args.rcmd == "quarantine":
        with open(args.report, "r") as f:
            report = json.load(f)
        thresholds = report.get("thresholds") or stability_thresholds_from_env()
        ok, checks = evaluate_stability(report, thresholds)
        git_commit = os.getenv("GIT_COMMIT", "unknown")
        git_ok = git_commit != "unknown"
        if os.getenv("CI") and not git_ok:
            ok = False
            checks["git_commit_ok"] = False
            if not args.force:
                print(json.dumps({"ok": False, "error": "git_commit_unknown_in_ci"}, sort_keys=True))
                return
        if not ok and not args.force:
            print(json.dumps({"ok": False, "error": "stability thresholds failed"}, sort_keys=True))
            return
        manifest = {
            "channel": "quarantine",
            "generated_at": report.get("generated_at"),
            "fp_version": report.get("fp_version"),
            "config_hash": report.get("config_hash"),
            "thresholds": thresholds,
            "checks": checks,
            "ok": ok,
            "report_path": args.report,
            "git_commit": git_commit,
            "git_commit_ok": git_ok,
        }
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        print(json.dumps({"ok": ok, "out": args.out}, sort_keys=True))
    elif args.cmd == "release" and args.rcmd == "promote":
        with open(args.in_path, "r") as f:
            manifest = json.load(f)
        if manifest.get("channel") != "quarantine":
            print(json.dumps({"ok": False, "error": "manifest channel is not quarantine"}, sort_keys=True))
            return
        if not manifest.get("ok"):
            print(json.dumps({"ok": False, "error": "quarantine manifest failed checks"}, sort_keys=True))
            return
        with open(args.in_path, "rb") as f:
            source_hash = hashlib.sha256(f.read()).hexdigest()
        prod = dict(manifest)
        prod["channel"] = "prod"
        prod["promoted_from"] = args.in_path
        prod["source_ok"] = manifest.get("ok")
        prod["source_manifest_hash"] = source_hash
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(prod, f, indent=2, sort_keys=True)
        print(json.dumps({"ok": True, "out": args.out}, sort_keys=True))
    elif args.cmd == "driftwatch" and args.dwcmd == "preflight":
        from .db import init_db
        from .preflight import preflight
        init_db()
        conn = get_conn()
        result = preflight(conn=conn)
        conn.close()
        # Pretty-print: verdict line + per-check lines
        verdict = result["verdict"]
        print(f"Preflight: {verdict}")
        for check in result["checks"]:
            status = check["status"]
            marker = {"PASS": "+", "WARN": "~", "FAIL": "!"}[status]
            print(f"  [{marker}] {check['name']}: {status} — {check['detail']}")
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.cmd == "driftwatch" and args.dwcmd == "maintenance":
        from .db import init_db
        from .maintenance import run_maintenance_once
        init_db()
        results = run_maintenance_once()
        print(json.dumps(results, indent=2, sort_keys=True))
    elif args.cmd == "driftwatch" and args.dwcmd == "retention":
        import logging as _log
        _log.basicConfig(level=_log.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
        from .db import init_db
        from .retention import run_retention_once
        init_db()
        results = run_retention_once()
        print(json.dumps({k: v for k, v in results.items() if k != "archive_files"},
                         indent=2, sort_keys=True))
        if results.get("archive_files"):
            print(f"\nArchive files: {len(results['archive_files'])}")
            for f in results["archive_files"]:
                print(f"  {f}")
    elif args.cmd == "driftwatch" and args.dwcmd == "bake":
        from .bake_gate import bake_check
        result = bake_check()
        verdict = result["verdict"]
        print(f"Bake: {verdict}")
        for check in result["checks"]:
            status = check["status"]
            marker = {"PASS": "+", "WARN": "~", "FAIL": "!"}[status]
            print(f"  [{marker}] {check['name']}: {status} — {check['detail']}")
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.cmd == "driftwatch" and args.dwcmd == "report":
        from .db import init_db
        init_db()
        conn = get_conn()
        report = cluster_report(conn=conn, top_n=args.top, hours=args.hours, bin_hours=args.bin_hours)
        conn.close()
        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2, sort_keys=True)
        # print summary to stdout
        nc = len(report.get("clusters", []))
        ns = len([s for s in report.get("regime_shifts", []) if s.get("is_shift")])
        print(json.dumps({"ok": True, "clusters": nc, "regime_shifts": ns, "out": args.out}, sort_keys=True))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
