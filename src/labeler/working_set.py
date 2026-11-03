"""Logical and physical SQLite working-set accounting.

This module is deliberately policy-free about deletion.  It measures the
online database, accounts reusable freelist separately from filesystem space,
and produces a fail-closed admission decision against operator-supplied caps.
Retention remains a separately reviewed semantic choice.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path


def allocated_bytes(path: Path) -> int:
    try:
        return path.stat().st_blocks * 512
    except FileNotFoundError:
        return 0


@dataclass(frozen=True)
class WorkingSetLimits:
    max_live_bytes: int
    filesystem_reserve_bytes: int
    shutdown_growth_bytes: int
    wal_allowance_bytes: int

    def validate(self) -> None:
        values = asdict(self)
        if self.max_live_bytes <= 0:
            raise ValueError("max_live_bytes must be positive")
        if any(value < 0 for key, value in values.items() if key != "max_live_bytes"):
            raise ValueError("working-set limits must not be negative")


def sample(database: Path, filesystem_path: Path | None = None) -> dict:
    """Take one bounded geometry sample without scanning table content."""
    database = database.resolve()
    filesystem_path = (filesystem_path or database.parent).resolve()
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=2)
    try:
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        auto_vacuum = int(connection.execute("PRAGMA auto_vacuum").fetchone()[0])
    finally:
        connection.close()
    statvfs = os.statvfs(filesystem_path)
    wal = Path(str(database) + "-wal")
    shm = Path(str(database) + "-shm")
    return {
        "database": str(database),
        "filesystem_path": str(filesystem_path),
        "filesystem_device": os.stat(filesystem_path).st_dev,
        "filesystem_free_bytes": statvfs.f_bavail * statvfs.f_frsize,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "auto_vacuum": auto_vacuum,
        "database_allocated_bytes": allocated_bytes(database),
        "wal_allocated_bytes": allocated_bytes(wal),
        "shm_allocated_bytes": allocated_bytes(shm),
        "live_bytes": (page_count - freelist_count) * page_size,
        "reusable_freelist_bytes": freelist_count * page_size,
    }


def evaluate(current: dict, limits: WorkingSetLimits) -> dict:
    """Evaluate logical growth and physical reserve without double counting.

    ``live_bytes`` excludes freelist and WAL. Existing database allocation is
    already paid for, so freelist is reusable physical headroom rather than an
    additional filesystem obligation. Only the unallocated remainder of the
    WAL allowance and the bounded shutdown growth are reserved from current
    filesystem free space.
    """
    limits.validate()
    live_remaining = limits.max_live_bytes - current["live_bytes"]
    wal_remaining = max(0, limits.wal_allowance_bytes - current["wal_allocated_bytes"])
    free_floor = (
        limits.filesystem_reserve_bytes
        + limits.shutdown_growth_bytes
        + wal_remaining
    )
    unallocated_physical = current["filesystem_free_bytes"] - free_floor
    physical_capacity = current["reusable_freelist_bytes"] + max(
        0, unallocated_physical
    )
    admitted_growth = min(max(0, live_remaining), physical_capacity)
    reasons = []
    if live_remaining <= 0:
        reasons.append("live_cap_reached")
    if current["wal_allocated_bytes"] > limits.wal_allowance_bytes:
        reasons.append("wal_allowance_exceeded")
    if unallocated_physical < 0:
        reasons.append("filesystem_reserve_insufficient")
    if admitted_growth < limits.shutdown_growth_bytes:
        reasons.append("shutdown_growth_not_preserved")
    return {
        "schema": "driftwatch.working-set-accounting.v1",
        "admitted": not reasons,
        "reasons": reasons,
        "sample": current,
        "limits": asdict(limits),
        "live_headroom_bytes": max(0, live_remaining),
        "wal_unallocated_allowance_bytes": wal_remaining,
        "filesystem_free_after_reserves_bytes": max(0, unallocated_physical),
        "physical_growth_capacity_bytes": physical_capacity,
        "admitted_growth_bytes": admitted_growth,
    }


class WorkingSetGuard:
    """TTL-cached fail-closed intake guard for an explicitly configured cap."""

    def __init__(
        self,
        database: Path,
        limits: WorkingSetLimits | None,
        sample_interval_seconds: float = 5.0,
    ):
        self.database = database
        self.limits = limits
        self.sample_interval_seconds = sample_interval_seconds
        self._sampled_at = 0.0
        self._state = {
            "schema": "driftwatch.working-set-accounting.v1",
            "configured": limits is not None,
            "admitted": limits is None,
            "reasons": [] if limits is None else ["not_yet_sampled"],
        }

    @classmethod
    def from_environment(cls, database: Path) -> "WorkingSetGuard":
        maximum = int(os.getenv("WORKING_SET_MAX_LIVE_BYTES", "0"))
        if maximum <= 0:
            return cls(database, None)

        def required_int(name: str) -> int:
            value = os.getenv(name)
            if value is None:
                raise ValueError(f"{name} is required when the working-set guard is enabled")
            return int(value)

        limits = WorkingSetLimits(
            max_live_bytes=maximum,
            filesystem_reserve_bytes=required_int(
                "WORKING_SET_FILESYSTEM_RESERVE_BYTES"
            ),
            shutdown_growth_bytes=required_int("WORKING_SET_SHUTDOWN_GROWTH_BYTES"),
            wal_allowance_bytes=required_int("WORKING_SET_WAL_ALLOWANCE_BYTES"),
        )
        limits.validate()
        return cls(
            database,
            limits,
            float(os.getenv("WORKING_SET_SAMPLE_INTERVAL_SECONDS", "5")),
        )

    def state(self, *, force: bool = False) -> dict:
        if self.limits is None:
            return dict(self._state)
        now = time.monotonic()
        if not force and now - self._sampled_at < self.sample_interval_seconds:
            return dict(self._state)
        try:
            state = evaluate(sample(self.database), self.limits)
            state["configured"] = True
        except Exception as exc:
            # Class only: paths and upstream exception text aren't suitable for
            # the public operational fact surface.
            state = {
                "schema": "driftwatch.working-set-accounting.v1",
                "configured": True,
                "admitted": False,
                "reasons": ["measurement_failed"],
                "failure_class": type(exc).__name__,
            }
        self._state = state
        self._sampled_at = now
        return dict(state)

    def pressure(self) -> bool:
        return not self.state()["admitted"]
