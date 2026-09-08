#!/usr/bin/env python3
"""Rehearse synthetic application recovery from a saved Driftwatch image."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "driftwatch.synthetic-recovery/v1"
REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "fixtures" / "posts.jsonl"
FIXED_NOW = "2026-09-07T12:00:00+00:00"
EXPECTED = {"label": "provenance_laundering_possible", "subject_uri": "uri:demo:2"}


def run(argv: list[str], capture: bool = False) -> str:
    proc = subprocess.run(argv, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None)
    return proc.stdout.strip() if capture else ""


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def image_tag(path: Path) -> str:
    with tarfile.open(path) as archive:
        handle = archive.extractfile("manifest.json")
        if handle is None:
            raise ValueError("image archive has no manifest.json")
        manifest = json.load(handle)
    tags = sorted({tag for item in manifest for tag in item.get("RepoTags") or []})
    if len(tags) != 1:
        raise ValueError(f"expected one tagged image in archive, found {tags}")
    return tags[0]


def safe_output(path: Path) -> Path:
    result = path.expanduser().resolve()
    forbidden = {Path("/"), Path.home().resolve(), REPO.resolve(),
                 (REPO / "data").resolve(), (REPO / "deploy" / "data").resolve()}
    if result in forbidden or result.exists():
        raise ValueError("--output must name a new, non-live directory")
    if any((parent / ".driftwatch-live").exists() for parent in (result, *result.parents)):
        raise ValueError("destination is below a .driftwatch-live marker")
    return result


def docker_run(image: str, mounts: list[tuple[Path, str, str]], command: list[str],
               capture: bool = False, detached_name: str | None = None) -> str:
    argv = ["docker", "run"]
    argv += ["-d", "--name", detached_name] if detached_name else ["--rm"]
    for source, target, mode in mounts:
        readonly = ",readonly" if mode == "readonly" else ""
        argv += ["--mount", f"type=bind,source={source},target={target}{readonly}"]
    return run([*argv, image, *command], capture=capture or bool(detached_name))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-archive", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    archive = args.image_archive.expanduser().resolve()
    if not archive.is_file():
        parser.error("--image-archive must be an existing Docker image archive")
    try:
        output = safe_output(args.output)
        image = image_tag(archive)
    except ValueError as exc:
        parser.error(str(exc))

    source_data, source_out = output / "source/data", output / "source/out"
    backup = output / "backup"
    restored_data, restored_out = output / "restored/data", output / "restored/out"
    for path in (source_data, source_out, backup, restored_data, restored_out):
        path.mkdir(parents=True, exist_ok=False)

    started = datetime.now(timezone.utc).isoformat()
    archive_sha = digest(archive)
    run(["docker", "load", "--input", str(archive)])
    image_id = run(["docker", "image", "inspect", "--format", "{{.Id}}", image], True)
    source_mounts = [(source_data, "/app/data", "rw"),
                     (source_out, "/app/out", "rw"),
                     (FIXTURE, "/fixture/posts.jsonl", "readonly")]
    seed = (
        "from labeler.db import init_db,get_conn; init_db(); c=get_conn(); "
        "c.execute(\"INSERT OR REPLACE INTO cursors VALUES (?,?,?)\","
        "('jetstream','cursor-9','2026-09-07T11:59:30+00:00')); c.commit(); c.close()"
    )
    docker_run(image, source_mounts, ["python", "-c", seed])
    docker_run(image, source_mounts, ["python", "-m", "labeler.drift.cli", "run",
                                      "--input", "/fixture/posts.jsonl",
                                      "--out", "/app/out/labels.jsonl"])
    backup_code = (
        "import sqlite3; a=sqlite3.connect('/app/data/labeler.sqlite'); "
        "b=sqlite3.connect('/backup/labeler.sqlite'); a.backup(b); "
        "b.close(); a.close()"
    )
    docker_run(image, [(source_data, "/app/data", "readonly"),
                       (backup, "/backup", "rw")], ["python", "-c", backup_code])
    shutil.copy2(source_out / "labels.jsonl", backup / "labels.jsonl")
    shutil.copy2(backup / "labeler.sqlite", restored_data / "labeler.sqlite")
    shutil.copy2(backup / "labels.jsonl", restored_out / "labels.jsonl")

    restored_mounts = [(restored_data, "/app/data", "rw"),
                       (restored_out, "/app/out", "rw"),
                       (FIXTURE, "/fixture/posts.jsonl", "readonly")]
    verify = (
        "import json,sqlite3; c=sqlite3.connect("
        "'file:/app/data/labeler.sqlite?mode=ro',uri=True); "
        "print(json.dumps({'integrity':c.execute('PRAGMA integrity_check').fetchone()[0],"
        "'cursor':c.execute(\"SELECT cursor FROM cursors WHERE consumer='jetstream'\").fetchone()[0]})); c.close()"
    )
    state = json.loads(docker_run(image, restored_mounts, ["python", "-c", verify], True))
    status = json.loads(docker_run(image, restored_mounts, [
        "python", "-m", "labeler.ops_status", "--format", "json", "--db",
        "/app/data/labeler.sqlite", "--data-dir", "/app/data", "--now", FIXED_NOW], True))
    docker_run(image, restored_mounts, ["python", "-m", "labeler.drift.cli", "run",
                                       "--input", "/fixture/posts.jsonl",
                                       "--out", "/app/out/replayed-labels.jsonl"])
    findings = [json.loads(line) for line in
                (restored_out / "replayed-labels.jsonl").read_text().splitlines()]

    name = f"driftwatch-recovery-{os.getpid()}"
    health = None
    try:
        docker_run(image, [(restored_data, "/app/data", "rw")], [], detached_name=name)
        for _ in range(30):
            try:
                body = run(["docker", "exec", name, "python", "-c",
                            "import urllib.request; print(urllib.request.urlopen("
                            "'http://127.0.0.1:8000/health',timeout=2).read().decode())"], True)
                health = json.loads(body)
                break
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                time.sleep(0.25)
    finally:
        subprocess.run(["docker", "rm", "--force", name], check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    passed = (state == {"integrity": "ok", "cursor": "cursor-9"}
              and status.get("schema") == "project.ops.status/v1"
              and len(findings) == 9
              and any(all(item.get(k) == v for k, v in EXPECTED.items()) for item in findings)
              and health == {"status": "ok"})
    receipt = {
        "schema": SCHEMA, "passed": passed, "started_at": started,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "release_artifact": {"path": str(archive), "sha256": archive_sha,
                             "image": image, "image_id": image_id},
        "fixture": str(FIXTURE.relative_to(REPO)),
        "backup": {"method": "sqlite3.Connection.backup plus exact output copy",
                   "path": str(backup)},
        "restore": {"path": str(output / "restored"), **state},
        "assertions": {"startup_health": health, "status_schema": status.get("schema"),
                       "finding_count": len(findings), "expected_finding": EXPECTED},
        "claims": {"synthetic_application_recovery": passed,
                   "source_worktree_reconstruction": False, "production_recovery": False},
    }
    result = output / "result.json"
    result.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"passed": passed, "result": str(result)}, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
