#!/usr/bin/env python3
"""Run Driftwatch's detect-only fixture and assert a meaningful result."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from labeler import ops_status


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    owner = None
    if args.output is None:
        owner = tempfile.TemporaryDirectory(prefix="driftwatch-demo-")
        root = Path(owner.name)
    else:
        root = args.output.resolve()
        root.mkdir(parents=True, exist_ok=True)
    labels_path = root / "labels.jsonl"
    subprocess.run([
        sys.executable, "-m", "labeler.drift.cli", "run", "--input",
        "fixtures/posts.jsonl", "--out", str(labels_path),
    ], check=True)
    findings = [json.loads(line) for line in labels_path.read_text().splitlines()]
    expected = {"label": "provenance_laundering_possible", "subject_uri": "uri:demo:2"}
    if len(findings) != 9 or not any(all(item.get(k) == v for k, v in expected.items()) for item in findings):
        raise SystemExit(f"unexpected detection result: {len(findings)} findings")
    status = ops_status.build_status(root / "labeler.sqlite", data_dir=root,
                                     now=datetime(2026, 9, 7, tzinfo=timezone.utc))
    result = {
        "schema": "driftwatch.offline-demo/v1",
        "finding_count": len(findings),
        "expected_finding": expected,
        "status_schema": status["schema"],
    }
    print(json.dumps(result, sort_keys=True))
    if owner is not None:
        owner.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
