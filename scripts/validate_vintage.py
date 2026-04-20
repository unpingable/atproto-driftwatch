"""Pilot validation: cross-check /export-derived vintage against /log.

After a pilot /export backfill run, sample N DIDs that got vintage written
and verify the timestamp matches what plc.directory/<did>/log reports as the
genesis createdAt. Characterizes mismatch classes so we know whether /export
is faithful enough to stake the admissibility rerun on.

Usage:
    python3 scripts/validate_vintage.py \
        --db /opt/driftwatch/deploy/data/labeler.sqlite \
        --sample 200 \
        [--run-id <run_id>] \
        --out out/admissibility/vintage_validation.json
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT / "src"))

from labeler.vintage import fetch_did_genesis  # noqa: E402

LOG = logging.getLogger("validate_vintage")


def sample_vintaged_dids(db_path: str, sample: int, run_id: str | None) -> list[tuple[str, str]]:
    """Return up to `sample` (did, did_created_at) pairs from DIDs with vintage set.

    If run_id provided, restrict to that run. Otherwise sample across all
    plc_export-sourced vintage.
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        if run_id:
            rows = conn.execute(
                "SELECT did, did_created_at FROM actor_identity_current "
                "WHERE vintage_run_id = ? AND vintage_source = 'plc_export' "
                "AND did LIKE 'did:plc:%'",
                (run_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT did, did_created_at FROM actor_identity_current "
                "WHERE vintage_source = 'plc_export' AND did LIKE 'did:plc:%'"
            ).fetchall()
    finally:
        conn.close()

    if len(rows) <= sample:
        return rows
    return random.sample(rows, sample)


def classify_mismatch(export_ts: str, log_ts: str | None, error: str | None) -> str:
    """Bucket a validation result into a mismatch class.

    Returns:
        'match' — timestamps identical
        'match_ms_rounding' — differ only in subsecond digits
        'drift_<N>s' — differ by roughly N seconds
        'log_missing' — /log call failed
        'log_empty' — /log returned no genesis timestamp
        'fmt_error' — unparseable timestamp
    """
    if error:
        return "log_missing"
    if log_ts is None:
        return "log_empty"
    if export_ts == log_ts:
        return "match"
    try:
        dt_e = datetime.fromisoformat(export_ts.replace("Z", "+00:00"))
        dt_l = datetime.fromisoformat(log_ts.replace("Z", "+00:00"))
    except Exception:
        return "fmt_error"
    delta_s = abs((dt_e - dt_l).total_seconds())
    if delta_s < 0.001:
        return "match_ms_rounding"
    if delta_s < 1:
        return "drift_sub1s"
    if delta_s < 60:
        return "drift_sub60s"
    if delta_s < 3600:
        return "drift_sub1h"
    if delta_s < 86400:
        return "drift_sub1d"
    return "drift_gt1d"


def validate(db_path: str, sample: int, run_id: str | None, sleep_s: float) -> dict:
    pairs = sample_vintaged_dids(db_path, sample, run_id)
    LOG.info("sampled %d pairs (run_id=%s)", len(pairs), run_id or "*")

    if not pairs:
        return {
            "status": "no_data",
            "sampled": 0,
            "run_id": run_id,
            "note": "no actor_identity_current rows with vintage_source=plc_export found",
        }

    results = []
    classes = Counter()
    for i, (did, export_ts) in enumerate(pairs, 1):
        v = fetch_did_genesis(did)
        cls = classify_mismatch(export_ts, v.created_at, v.error)
        classes[cls] += 1
        results.append({
            "did": did,
            "export_createdAt": export_ts,
            "log_createdAt": v.created_at,
            "log_error": v.error,
            "classification": cls,
        })
        if i % 10 == 0:
            LOG.info("progress %d/%d classes=%s", i, len(pairs), dict(classes))
        time.sleep(sleep_s)

    total = sum(classes.values())
    match_like = classes.get("match", 0) + classes.get("match_ms_rounding", 0)
    return {
        "status": "ok",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "sampled": len(pairs),
        "classes": dict(classes),
        "match_rate": round(100.0 * match_like / total, 2) if total else 0,
        "results": results,
    }


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--sample", type=int, default=200)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sleep-s", type=float, default=0.3)
    parser.add_argument("--out", required=True, help="Path for JSON report")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    report = validate(args.db, args.sample, args.run_id, args.sleep_s)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    LOG.info("wrote %s", out_path)

    if report.get("status") == "ok":
        print(f"\nsampled={report['sampled']} match_rate={report['match_rate']}%")
        print(f"classes: {report['classes']}")


if __name__ == "__main__":
    main()
