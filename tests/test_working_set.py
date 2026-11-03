import sqlite3

import pytest

from labeler.working_set import WorkingSetGuard, WorkingSetLimits, evaluate, sample


def database(path, rows=1000):
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
    connection.execute("CREATE TABLE t (value BLOB)")
    connection.executemany(
        "INSERT INTO t VALUES (?)", [(b"x" * 2000,) for _ in range(rows)]
    )
    connection.commit()
    return connection


def limits(**changes):
    values = {
        "max_live_bytes": 32 * 1024 * 1024,
        "filesystem_reserve_bytes": 0,
        "shutdown_growth_bytes": 1024 * 1024,
        "wal_allowance_bytes": 64 * 1024 * 1024,
    }
    values.update(changes)
    return WorkingSetLimits(**values)


def test_freelist_is_reusable_capacity_not_new_allocation(tmp_path):
    path = tmp_path / "working.sqlite"
    connection = database(path)
    before = sample(path)
    connection.execute("DELETE FROM t WHERE rowid <= 800")
    connection.commit()
    after = sample(path)
    connection.close()
    assert after["reusable_freelist_bytes"] > before["reusable_freelist_bytes"]
    plan = evaluate(after, limits())
    assert plan["physical_growth_capacity_bytes"] >= after["reusable_freelist_bytes"]


def test_live_cap_refuses_even_when_filesystem_has_space(tmp_path):
    path = tmp_path / "working.sqlite"
    connection = database(path)
    current = sample(path)
    connection.close()
    plan = evaluate(current, limits(max_live_bytes=current["live_bytes"]))
    assert plan["admitted"] is False
    assert "live_cap_reached" in plan["reasons"]


def test_current_wal_is_not_counted_twice(tmp_path):
    path = tmp_path / "working.sqlite"
    connection = database(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("INSERT INTO t VALUES (?)", (b"y" * 2000,))
    connection.commit()
    current = sample(path)
    connection.close()
    plan = evaluate(current, limits(wal_allowance_bytes=current["wal_allocated_bytes"]))
    assert plan["wal_unallocated_allowance_bytes"] == 0


def test_shutdown_growth_is_a_hard_admission_obligation(tmp_path):
    path = tmp_path / "working.sqlite"
    connection = database(path)
    current = sample(path)
    connection.close()
    plan = evaluate(
        current,
        limits(
            max_live_bytes=current["live_bytes"] + 1024,
            shutdown_growth_bytes=2048,
        ),
    )
    assert plan["admitted"] is False
    assert "shutdown_growth_not_preserved" in plan["reasons"]


def test_configured_guard_fails_closed_when_measurement_fails(tmp_path):
    guard = WorkingSetGuard(tmp_path / "absent.sqlite", limits(), 0)
    state = guard.state(force=True)
    assert state["admitted"] is False
    assert state["reasons"] == ["measurement_failed"]
    assert "failure_class" in state


def test_unconfigured_guard_is_inert(tmp_path):
    guard = WorkingSetGuard(tmp_path / "absent.sqlite", None)
    assert guard.pressure() is False
    assert guard.state()["configured"] is False


def test_enabled_guard_requires_every_operator_reserve(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKING_SET_MAX_LIVE_BYTES", "1000")
    monkeypatch.delenv("WORKING_SET_FILESYSTEM_RESERVE_BYTES", raising=False)
    monkeypatch.delenv("WORKING_SET_SHUTDOWN_GROWTH_BYTES", raising=False)
    monkeypatch.delenv("WORKING_SET_WAL_ALLOWANCE_BYTES", raising=False)
    with pytest.raises(ValueError, match="FILESYSTEM_RESERVE"):
        WorkingSetGuard.from_environment(tmp_path / "database.sqlite")
