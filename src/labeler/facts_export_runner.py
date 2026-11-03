"""Fail-closed one-shot entrypoint for the SQLite facts exporter.

This module validates and creates one owned scratch directory, then sets
``SQLITE_TMPDIR`` before importing either sqlite3 or ``facts_export``.
"""

import fcntl
import os
import pathlib
import shutil
import signal
import sys
import uuid


def _mount_for(path):
    resolved = os.path.realpath(path)
    best = None
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        for raw in stream:
            left, sep, right = raw.rstrip("\n").partition(" - ")
            if not sep:
                continue
            fields = left.split()
            after = right.split()
            if len(fields) < 6 or len(after) < 2:
                continue
            mountpoint = fields[4].replace("\\040", " ")
            if resolved != mountpoint and not resolved.startswith(mountpoint.rstrip("/") + "/"):
                continue
            if best is None or len(mountpoint) > len(best["mountpoint"]):
                best = {"mountpoint": mountpoint, "filesystem": after[0], "source": after[1]}
    return best


def _fail(message, code=78):
    print(f"facts export refused: {message}", file=sys.stderr)
    raise SystemExit(code)


def _validate_root(root):
    if not root or not os.path.isabs(root):
        _fail("FACTS_EXPORT_TEMP_ROOT must be an absolute path")
    if os.path.islink(root) or not os.path.isdir(root):
        _fail("temporary-workspace root is missing, not a directory, or symlinked")
    mount = _mount_for(root)
    expected_mount = os.path.realpath(os.environ.get("FACTS_EXPORT_TEMP_MOUNT", ""))
    if not mount or not expected_mount or mount["mountpoint"] != expected_mount:
        _fail(f"temporary-workspace mount mismatch: actual={mount} expected={expected_mount}")
    expected_fstype = os.environ.get("FACTS_EXPORT_TEMP_FSTYPE")
    if expected_fstype and mount["filesystem"] != expected_fstype:
        _fail(
            f"temporary-workspace filesystem mismatch: actual={mount['filesystem']} "
            f"expected={expected_fstype}"
        )
    if mount["filesystem"] in {"nfs", "nfs4"}:
        _fail("live SQLite temporary workspace must not use NFS")
    return mount


def _remove_owned_attempt(attempt, root):
    attempt_path = pathlib.Path(attempt)
    root_path = pathlib.Path(root).resolve()
    if attempt_path.parent.resolve() != root_path:
        _fail("refusing cleanup outside the configured temporary-workspace root", 70)
    if not attempt_path.name.startswith("facts-export-") or attempt_path.is_symlink():
        _fail("refusing cleanup of a non-owned temporary-workspace name", 70)
    shutil.rmtree(attempt_path)


def main():
    root = os.environ.get("FACTS_EXPORT_TEMP_ROOT", "")
    _validate_root(root)

    lock_path = os.environ.get("FACTS_EXPORT_LOCK_PATH", "/app/data/.facts-export.lock")
    lock = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            _fail("another exporter owns the lock", 75)

        attempt = os.path.join(root, "facts-export-" + uuid.uuid4().hex)
        os.mkdir(attempt, 0o700)
        os.environ["SQLITE_TMPDIR"] = attempt
        os.environ["FACTS_EXPORT_TEMP_ATTEMPT_DIR"] = attempt
        os.environ["FACTS_EXPORT_REQUIRE_EXPLICIT_WORKSPACE"] = "1"

        def terminate(_signum, _frame):
            raise SystemExit(143)

        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGINT, terminate)
        try:
            # Deliberately imported only after SQLITE_TMPDIR is final.
            from . import facts_export
            return facts_export.main()
        finally:
            _remove_owned_attempt(attempt, root)
    finally:
        os.close(lock)


if __name__ == "__main__":
    main()
