#!/usr/bin/env python3
"""Exercise stale snapshot cleanup against a retained production-size snapshot.

The restored inputs are never opened for writing.  The script hard-links the
published snapshot into a fresh run directory, creates one full-size disposable
``.tmp`` copy beside it, then invokes the exact cleanup helper under test.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from labeler.facts_export import (  # noqa: E402
    _discard_stale_snapshot,
    _snapshot_preflight,
    _snapshot_space_plan,
)


def _file_record(path):
    stat = path.stat()
    return {
        "path": str(path),
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size_bytes": stat.st_size,
        "allocated_bytes": stat.st_blocks * 512,
        "mtime_ns": stat.st_mtime_ns,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restored-data", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--production-total-bytes", type=int, required=True)
    parser.add_argument("--production-free-before-cleanup-bytes", type=int, required=True)
    parser.add_argument("--production-stale-allocated-bytes", type=int, required=True)
    args = parser.parse_args()

    source_snapshot = args.restored_data / "facts.sqlite"
    source_work = args.restored_data / "facts_work.sqlite"
    if args.run_dir.exists():
        raise SystemExit(f"refusing existing run directory: {args.run_dir}")
    if not source_snapshot.is_file() or not source_work.is_file():
        raise SystemExit("retained facts.sqlite and facts_work.sqlite are required")

    args.run_dir.mkdir(mode=0o700, parents=True)
    published = args.run_dir / "facts.sqlite"
    stale = args.run_dir / "facts.sqlite.tmp"
    os.link(source_snapshot, published)
    if source_snapshot.stat().st_dev != args.run_dir.stat().st_dev:
        raise SystemExit("run directory must share a filesystem with retained input")

    source_before = _file_record(source_snapshot)
    published_before = _file_record(published)

    copy_started = time.monotonic()
    shutil.copyfile(source_snapshot, stale)
    copy_seconds = time.monotonic() - copy_started
    stale_record = _file_record(stale)

    cleanup_started = time.monotonic()
    reclaimed_bytes = _discard_stale_snapshot(str(published))
    cleanup_seconds = time.monotonic() - cleanup_started

    source_after = _file_record(source_snapshot)
    published_after = _file_record(published)
    if stale.exists():
        raise SystemExit("cleanup left the exact stale workspace present")
    if source_before != source_after:
        raise SystemExit("retained source snapshot metadata changed")
    if published_before != published_after:
        raise SystemExit("published snapshot metadata changed")
    if (source_after["device"], source_after["inode"]) != (
        published_after["device"], published_after["inode"]
    ):
        raise SystemExit("published test path no longer identifies retained snapshot")

    work_size = source_work.stat().st_size
    production_free_after_cleanup = (
        args.production_free_before_cleanup_bytes
        + args.production_stale_allocated_bytes
    )
    production_plan = _snapshot_space_plan(
        args.production_total_bytes,
        production_free_after_cleanup,
        work_size,
    )
    local_usage = shutil.disk_usage(args.run_dir)
    local_plan = _snapshot_space_plan(
        local_usage.total,
        local_usage.free,
        work_size,
    )
    work_uri = f"file:{source_work}?mode=ro"
    work_conn = sqlite3.connect(work_uri, uri=True)
    try:
        restored_preflight = _snapshot_preflight(work_conn, str(published))
    finally:
        work_conn.close()

    result = {
        "schema": "driftwatch.snapshot-workspace-qualification.v1",
        "source_snapshot": source_after,
        "source_work": _file_record(source_work),
        "disposable_workspace": stale_record,
        "copy_seconds": copy_seconds,
        "cleanup_seconds": cleanup_seconds,
        "reclaimed_bytes": reclaimed_bytes,
        "published_snapshot_unchanged": True,
        "production_geometry_plan": production_plan,
        "local_geometry_plan": local_plan,
        "restored_work_preflight": restored_preflight,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
