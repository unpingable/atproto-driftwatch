import asyncio
import importlib.util
import json
import os
import stat
import subprocess
import sys
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_durable_receipt_is_atomic_and_private(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=None))
    monkeypatch.setenv("LABELER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("EXPERIMENT_RUN_ID", "receipt-test")
    monkeypatch.setenv("EXPERIMENT_EXPECTED_CURSOR", "100")
    monkeypatch.setenv("EXPERIMENT_IMAGE_ID", "sha256:test")
    tool = load("bounded_receipt_tool", ROOT / "scripts/bounded_ingestion_experiment.py")
    path = tmp_path / "receipt.json"
    tool.atomic_json(path, {"generation": 1})
    tool.atomic_json(path, {"generation": 2})
    assert json.loads(path.read_text()) == {"generation": 2}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".*.tmp"))


def test_fixture_stream_forces_disconnect_after_exact_count(monkeypatch):
    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=None))
    monkeypatch.setenv("EXPERIMENT_RUN_ID", "stream-test")
    monkeypatch.setenv("EXPERIMENT_EXPECTED_CURSOR", "100")
    monkeypatch.setenv("EXPERIMENT_IMAGE_ID", "sha256:test")
    tool = load("bounded_stream_tool", ROOT / "scripts/bounded_ingestion_experiment.py")

    async def exercise():
        stream = tool.FixtureStream(3, 1_000)
        values = [json.loads(await stream.recv()) for _ in range(3)]
        assert [value["time_us"] for value in values] == [1_000, 2_000, 3_000]
        try:
            await stream.recv()
        except tool.ForcedStreamTimeout:
            return
        raise AssertionError("fixture stream did not disconnect")

    asyncio.run(exercise())


def test_missing_run_identity_does_not_derive_receipt_path(monkeypatch):
    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=None))
    monkeypatch.delenv("EXPERIMENT_RUN_ID", raising=False)
    monkeypatch.delenv("EXPERIMENT_EXPECTED_CURSOR", raising=False)
    monkeypatch.delenv("EXPERIMENT_IMAGE_ID", raising=False)
    tool = load("bounded_missing_identity_tool", ROOT / "scripts/bounded_ingestion_experiment.py")
    assert tool.RECEIPT_PATH is None
    tool.update_terminal_failure(RuntimeError("fixture"))


def test_deadlined_command_retains_receipt_and_exits(tmp_path):
    receipt = tmp_path / "lifecycle.json"
    env = {
        **os.environ,
        "DIAGNOSTIC_DEADLINE_SECONDS": "1",
        "DIAGNOSTIC_KILL_AFTER_SECONDS": "1",
        "DIAGNOSTIC_RECEIPT": str(receipt),
    }
    result = subprocess.run(
        [
            str(ROOT / "scripts/run_deadlined_command.sh"),
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
        ],
        env=env,
        timeout=10,
        check=False,
    )
    value = json.loads(receipt.read_text())
    assert result.returncode == 124
    assert value["exit_code"] == 124
    assert value["cleanup_verified"] is True
    assert "command" not in value


def test_archive_capture_refuses_wrong_mount_before_writing(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=None))
    tool = load("archive_capture_tool", ROOT / "scripts/capture_jetstream_archive.py")
    destination = tmp_path / "archive"
    scratch = tmp_path / "scratch"
    fixture = tmp_path / "fixture.jsonl"
    fixture.write_text('{"time_us":1}\n')
    monkeypatch.setattr(tool, "mount_record", lambda path: ("actual", "ext4", {"rw"}))
    args = type(
        "Args",
        (),
        {
            "destination": destination,
            "scratch": scratch,
            "expected_source": "expected",
            "expected_fstype": "ext4",
            "expected_owner_uid": os.geteuid(),
            "run_id": "refuse",
            "url": "wss://example.invalid/subscribe",
            "fixture": fixture,
            "max_seconds": 1,
            "max_events": 1,
            "max_raw_bytes": 1024,
            "min_events": 1,
        },
    )()
    try:
        asyncio.run(tool.capture(args))
    except SystemExit as exc:
        assert "mount identity" in str(exc)
    else:
        raise AssertionError("wrong archive mount was accepted")
    assert not destination.exists()


def test_archive_capture_treats_own_deadline_as_bounded_stop(tmp_path, monkeypatch):
    sockets = types.SimpleNamespace(connect=None)
    monkeypatch.setitem(sys.modules, "websockets", sockets)
    tool = load("archive_deadline_tool", ROOT / "scripts/capture_jetstream_archive.py")
    destination_parent = tmp_path / "archive"
    destination_parent.mkdir()
    destination = destination_parent / "capture"
    scratch = tmp_path / "scratch"
    original_stat = Path.stat

    def split_filesystem_stat(path, *args, **kwargs):
        value = original_stat(path, *args, **kwargs)
        if path == scratch:
            fields = list(value)
            fields[2] += 1
            return os.stat_result(fields)
        return value

    monkeypatch.setattr(Path, "stat", split_filesystem_stat)

    class Stream:
        calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *unused):
            return False

        async def recv(self):
            self.calls += 1
            if self.calls == 1:
                return '{"time_us":1}'
            await asyncio.sleep(1)

    sockets.connect = lambda *args, **kwargs: Stream()
    monkeypatch.setattr(
        tool, "mount_record", lambda path: ("expected", "ext4", {"rw"})
    )
    args = type(
        "Args",
        (),
        {
            "destination": destination,
            "scratch": scratch,
            "expected_source": "expected",
            "expected_fstype": "ext4",
            "expected_owner_uid": os.geteuid(),
            "run_id": "deadline",
            "url": "wss://example.invalid/subscribe",
            "fixture": None,
            "max_seconds": 0.02,
            "max_events": 2,
            "max_raw_bytes": 1024,
            "min_events": 1,
        },
    )()
    result = asyncio.run(tool.capture(args))
    assert result["events"] == 1
    assert (destination / "deadline.jsonl.gz").is_file()


def test_archive_capture_refuses_mapped_owner_mismatch(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "websockets", types.SimpleNamespace(connect=None))
    tool = load("archive_owner_tool", ROOT / "scripts/capture_jetstream_archive.py")
    destination_parent = tmp_path / "archive"
    destination_parent.mkdir()
    destination = destination_parent / "capture"
    scratch = tmp_path / "scratch"
    fixture = tmp_path / "fixture.jsonl"
    fixture.write_text('{"time_us":1}\n')
    monkeypatch.setattr(tool, "mount_record", lambda path: ("expected", "ext4", {"rw"}))
    original_stat = Path.stat

    def split_filesystem_stat(path, *args, **kwargs):
        value = original_stat(path, *args, **kwargs)
        if path == scratch:
            fields = list(value)
            fields[2] += 1
            return os.stat_result(fields)
        return value

    monkeypatch.setattr(Path, "stat", split_filesystem_stat)
    args = type(
        "Args",
        (),
        {
            "destination": destination,
            "scratch": scratch,
            "expected_source": "expected",
            "expected_fstype": "ext4",
            "expected_owner_uid": os.geteuid() + 1,
            "run_id": "owner",
            "url": "wss://example.invalid/subscribe",
            "fixture": fixture,
            "max_seconds": 1,
            "max_events": 1,
            "max_raw_bytes": 1024,
            "min_events": 1,
        },
    )()
    with pytest.raises(RuntimeError, match="ownership mismatch"):
        asyncio.run(tool.capture(args))
    assert not list(destination.glob("*.partial"))
    assert not list(destination.glob("*.jsonl.gz"))
