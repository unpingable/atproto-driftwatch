"""Focused qualification for explicit SQLite export workspace custody."""

import fcntl
import os
import sqlite3
from collections import namedtuple

import pytest

from labeler import facts_export as fe
from labeler import facts_export_runner as runner


Usage = namedtuple("Usage", "total used free")


def _mount(device, mountpoint, filesystem="ext4", source="/dev/test"):
    return {
        "device": device,
        "mountpoint": mountpoint,
        "filesystem": filesystem,
        "source": source,
    }


def test_filesystem_plan_combines_shared_device_by_phase(monkeypatch, tmp_path):
    monkeypatch.setattr(fe, "_mount_for_path", lambda _path: _mount(1, str(tmp_path)))
    monkeypatch.setattr(fe.shutil, "disk_usage", lambda _path: Usage(1000, 0, 1000))
    plan = fe._filesystem_space_plan(
        obligations={
            "aggregation": [(str(tmp_path), 120), (str(tmp_path), 80)],
            "publication": [(str(tmp_path), 300)],
        },
        reserves=[(str(tmp_path), 400), (str(tmp_path), 350)],
        growth_allowance=50,
    )
    item = plan["filesystems"][1]
    assert item["peak_additional_bytes"] == 300
    assert item["required_free_bytes"] == 750
    assert plan["admitted"] is True


def test_filesystem_plan_separates_devices(monkeypatch, tmp_path):
    scratch = tmp_path / "scratch"
    output = tmp_path / "output"
    scratch.mkdir()
    output.mkdir()

    def mounts(path):
        return _mount(1, str(scratch)) if str(path).startswith(str(scratch)) else _mount(2, str(output))

    monkeypatch.setattr(fe, "_mount_for_path", mounts)
    monkeypatch.setattr(fe.shutil, "disk_usage", lambda _path: Usage(1000, 0, 1000))
    plan = fe._filesystem_space_plan(
        obligations={"aggregation": [(str(scratch), 200)], "publication": [(str(output), 300)]},
        reserves=[(str(scratch), 100), (str(output), 400)],
        growth_allowance=50,
    )
    assert plan["filesystems"][1]["required_free_bytes"] == 350
    assert plan["filesystems"][2]["required_free_bytes"] == 750


def test_missing_wrong_nfs_and_unwritable_scratch_refuse(monkeypatch, tmp_path):
    missing = tmp_path / "missing"
    with pytest.raises(fe.WorkspaceRefusal, match="absent"):
        fe._validate_runtime_scratch(str(missing))

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    monkeypatch.setenv("SQLITE_TMPDIR", str(scratch))
    monkeypatch.setenv("FACTS_EXPORT_TEMP_MOUNT", "/wrong")
    monkeypatch.setattr(fe, "_mount_for_path", lambda _path: _mount(1, str(tmp_path)))
    with pytest.raises(fe.WorkspaceRefusal, match="mount mismatch"):
        fe._validate_runtime_scratch(str(scratch))

    monkeypatch.setenv("FACTS_EXPORT_TEMP_MOUNT", str(tmp_path))
    monkeypatch.setattr(fe, "_mount_for_path", lambda _path: _mount(1, str(tmp_path), "nfs4"))
    with pytest.raises(fe.WorkspaceRefusal, match="must not use NFS"):
        fe._validate_runtime_scratch(str(scratch))

    monkeypatch.setattr(fe, "_mount_for_path", lambda _path: _mount(1, str(tmp_path)))
    real_open = fe.os.open
    monkeypatch.setattr(fe.os, "open", lambda *_a, **_k: (_ for _ in ()).throw(PermissionError("no")))
    with pytest.raises(fe.WorkspaceRefusal, match="not safely writable"):
        fe._validate_runtime_scratch(str(scratch))
    monkeypatch.setattr(fe.os, "open", real_open)


def test_sqlite_connection_reports_explicit_temp_directory(tmp_path, monkeypatch):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    conn = sqlite3.connect(tmp_path / "work.sqlite")
    fe._configure_connection_temp(conn, str(scratch))
    assert conn.execute("PRAGMA temp_store").fetchone()[0] == 1
    assert os.path.realpath(conn.execute("PRAGMA temp_store_directory").fetchone()[0]) == str(scratch)
    conn.close()


def test_guard_refuses_runtime_floor_and_owned_budget(monkeypatch, tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    work = tmp_path / "work.sqlite"
    work.write_bytes(b"")
    snap = tmp_path / "facts.sqlite"
    plan = {
        "filesystems": {
            1: {
                "mountpoint": str(tmp_path),
                "reserve_bytes": 500,
                "growth_allowance_bytes": 100,
            }
        }
    }
    guard = fe.WorkspaceGuard(plan, str(scratch), str(work), str(snap))
    monkeypatch.setattr(fe.shutil, "disk_usage", lambda _path: Usage(1000, 500, 499))
    assert guard.progress() == 1
    assert "floor threatened" in guard.abort_reason

    guard.abort_reason = None
    monkeypatch.setattr(fe.shutil, "disk_usage", lambda _path: Usage(1000, 0, 1000))
    monkeypatch.setattr(fe, "_allocated_bytes", lambda path: guard.scratch_budget + 1 if path == str(scratch) else 0)
    assert guard.progress() == 1
    assert "scratch budget exceeded" in guard.abort_reason


def test_sqlite_progress_refuses_space_depletion_during_work(monkeypatch, tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    work = tmp_path / "work.sqlite"
    published = tmp_path / "facts.sqlite"
    plan = {
        "filesystems": {
            1: {
                "mountpoint": str(tmp_path),
                "reserve_bytes": 500,
                "growth_allowance_bytes": 100,
            }
        }
    }
    guard = fe.WorkspaceGuard(plan, str(scratch), str(work), str(published))
    free = iter((1000, 599))
    monkeypatch.setattr(
        fe.shutil,
        "disk_usage",
        lambda _path: Usage(2000, 0, next(free, 599)),
    )
    conn = sqlite3.connect(work)
    conn.execute("CREATE TABLE t (x)")
    conn.executemany("INSERT INTO t VALUES (?)", ((n,) for n in range(2000)))
    conn.set_progress_handler(guard.progress, 1)
    with pytest.raises(sqlite3.OperationalError, match="interrupted") as caught:
        conn.execute("SELECT count(*) FROM t AS a, t AS b").fetchone()
    conn.close()
    with pytest.raises(fe.WorkspaceRefusal, match="floor threatened"):
        guard.raise_if_interrupted(caught.value)


def test_busy_checkpoint_is_not_accepted_for_geometry(tmp_path):
    path = tmp_path / "work.sqlite"
    writer = sqlite3.connect(path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("CREATE TABLE t (x)")
    writer.commit()

    reader = sqlite3.connect(path)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM t").fetchone()
    writer.execute("INSERT INTO t VALUES (1)")
    writer.commit()
    try:
        assert fe._checkpoint_snapshot_wal(writer) is False
    finally:
        reader.rollback()
        reader.close()
        writer.close()


def test_runner_cleanup_is_exact_and_lock_is_exclusive(tmp_path):
    root = tmp_path / "scratch"
    root.mkdir()
    attempt = root / "facts-export-owned"
    attempt.mkdir()
    (attempt / "spill").write_bytes(b"x")
    neighbour = root / "facts-export-neighbour"
    neighbour.mkdir()
    runner._remove_owned_attempt(str(attempt), str(root))
    assert not attempt.exists()
    assert neighbour.exists()

    lock_path = tmp_path / "lock"
    first = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    second = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(first, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            fcntl.flock(second, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        os.close(second)
        os.close(first)


def test_failed_vacuum_preserves_publication_and_removes_only_tmp(tmp_path, monkeypatch):
    work_path = tmp_path / "work.sqlite"
    published = tmp_path / "facts.sqlite"
    published.write_bytes(b"accepted")
    neighbour = tmp_path / "facts.sqlite.other"
    neighbour.write_bytes(b"keep")
    conn = sqlite3.connect(work_path)
    conn.execute("CREATE TABLE t (x)")
    conn.commit()
    monkeypatch.setattr(fe, "_snapshot_preflight", lambda *_a: {
        "admitted": True,
        "free_bytes": 100,
        "required_free_bytes": 1,
        "output_budget_bytes": 1,
        "reserve_bytes": 0,
    })

    class RefusingGuard:
        output_budget = 1
        abort_reason = "simulated depletion"
        def set_phase(self, *_a):
            pass
        def raise_if_interrupted(self, exc):
            raise fe.WorkspaceRefusal(self.abort_reason) from exc

    conn.set_progress_handler(lambda: 1, 1)
    with pytest.raises(fe.WorkspaceRefusal, match="simulated depletion"):
        fe._snapshot(conn, str(published), RefusingGuard())
    conn.close()
    assert published.read_bytes() == b"accepted"
    assert neighbour.read_bytes() == b"keep"
    assert not (tmp_path / "facts.sqlite.tmp").exists()
