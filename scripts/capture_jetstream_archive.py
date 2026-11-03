#!/usr/bin/env python3
"""Bounded private Jetstream archive capture with atomic NFS publication."""

from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import shutil
import stat
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets


def mount_record(path: Path) -> tuple[str, str, set[str]]:
    proc = subprocess.run(
        ["findmnt", "-J", "-T", str(path), "-o", "SOURCE,FSTYPE,OPTIONS"],
        check=True,
        text=True,
        capture_output=True,
        timeout=5,
    )
    fs = json.loads(proc.stdout)["filesystems"][0]
    return fs["source"], fs["fstype"], set(fs["options"].split(","))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("x", encoding="utf-8") as stream:
        os.chmod(temp, 0o600)
        json.dump(value, stream, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


async def capture(args) -> dict:
    destination = args.destination.resolve()
    scratch = args.scratch.resolve()
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination_parent = destination.parent
    if not destination_parent.is_dir():
        raise SystemExit("refused: destination parent is absent")
    source, fstype, options = mount_record(destination_parent)
    if source != args.expected_source or fstype != args.expected_fstype or "rw" not in options:
        raise SystemExit("refused: destination mount identity/writability mismatch")
    destination.mkdir(mode=0o700, exist_ok=True)
    if mount_record(destination) != (source, fstype, options):
        raise SystemExit("refused: destination filesystem changed during setup")
    if destination.stat().st_dev == scratch.stat().st_dev:
        raise SystemExit("refused: scratch must not share the archive filesystem")

    run_id = args.run_id
    scratch_file = scratch / f".{run_id}.jsonl.gz.partial"
    destination_partial = destination / f".{run_id}.jsonl.gz.partial"
    final = destination / f"{run_id}.jsonl.gz"
    manifest = destination / f"{run_id}.manifest.json"
    for path in (scratch_file, destination_partial, final, manifest):
        if path.exists():
            raise SystemExit(f"refused: existing run path {path.name}")

    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    count = raw_bytes = 0
    first = last = None
    try:
        with gzip.open(scratch_file, "xb", compresslevel=6) as stream:
            os.chmod(scratch_file, 0o600)

            async def accept(raw: bytes) -> bool:
                nonlocal count, raw_bytes, first, last
                if time.monotonic() - started >= args.max_seconds:
                    return False
                if count >= args.max_events or raw_bytes + len(raw) + 1 > args.max_raw_bytes:
                    return False
                value = json.loads(raw)
                stamp = value.get("time_us")
                if not isinstance(stamp, int) or stamp <= 0:
                    raise ValueError("source event missing positive time_us")
                if last is not None and stamp < last:
                    raise ValueError("source cursor became non-monotonic")
                first = stamp if first is None else first
                last = stamp
                stream.write(raw + b"\n")
                count += 1
                raw_bytes += len(raw) + 1
                return True

            if args.fixture:
                with args.fixture.open("rb") as fixture:
                    for raw in fixture:
                        if not await accept(raw.rstrip(b"\n")):
                            break
            else:
                async with websockets.connect(
                    args.url,
                    max_size=10 * 1024 * 1024,
                    ping_interval=30,
                    ping_timeout=10,
                    open_timeout=15,
                ) as socket:
                    while True:
                        remaining = args.max_seconds - (time.monotonic() - started)
                        if remaining <= 0:
                            break
                        try:
                            raw = await asyncio.wait_for(
                                socket.recv(), timeout=min(10, remaining)
                            )
                        except asyncio.TimeoutError:
                            # Expiry of the caller's own capture deadline is a
                            # normal bounded stop, not a source failure. A full
                            # ten-second receive timeout before that deadline
                            # remains a failure.
                            if time.monotonic() - started >= args.max_seconds:
                                break
                            raise
                        encoded = raw.encode() if isinstance(raw, str) else raw
                        if not await accept(encoded):
                            break

        if count < args.min_events:
            raise RuntimeError(
                f"archive capture produced {count} events; minimum is {args.min_events}"
            )

        shutil.copyfile(scratch_file, destination_partial)
        os.chmod(destination_partial, 0o600)
        actual_owner_uid = destination_partial.stat().st_uid
        if actual_owner_uid != args.expected_owner_uid:
            raise RuntimeError(
                "archive destination ownership mismatch after NFS mapping"
            )
        with destination_partial.open("rb") as stream:
            os.fsync(stream.fileno())
        source_hash = sha256(scratch_file)
        destination_hash = sha256(destination_partial)
        if source_hash != destination_hash:
            raise RuntimeError("archive copy checksum mismatch")
        with gzip.open(destination_partial, "rb") as stream:
            verified_count = sum(1 for _ in stream)
        if verified_count != count:
            raise RuntimeError("archive read verification count mismatch")
        os.replace(destination_partial, final)
        directory_fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        result = {
            "schema": "driftwatch.jetstream-private-archive.v1",
            "run_id": run_id,
            "source": args.url,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "events": count,
            "raw_bytes": raw_bytes,
            "compressed_bytes": final.stat().st_size,
            "first_time_us": first,
            "last_time_us": last,
            "sha256": source_hash,
            "destination_source": source,
            "destination_fstype": fstype,
            "owner_uid": actual_owner_uid,
            "limits": {
                "seconds": args.max_seconds,
                "events": args.max_events,
                "raw_bytes": args.max_raw_bytes,
            },
        }
        atomic_json(manifest, result)
        return result
    finally:
        scratch_file.unlink(missing_ok=True)
        destination_partial.unlink(missing_ok=True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--expected-fstype", required=True)
    parser.add_argument("--expected-owner-uid", required=True, type=int)
    parser.add_argument("--url", default="wss://jetstream2.us-east.bsky.network/subscribe")
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--max-seconds", type=float, default=300)
    parser.add_argument("--max-events", type=int, default=250_000)
    parser.add_argument("--max-raw-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--min-events", type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(capture(parse_args())), sort_keys=True))
