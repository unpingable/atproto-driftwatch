"""Emergency brake: absolute pressure, hysteresis, no write to the full volume."""

import os
import sqlite3
import time

import pytest

from labeler import maintenance as M
from labeler.retention_scheduler import (
    DISK_RUNWAY_CRITICAL_DAYS,
    DISK_RUNWAY_MIN_FREE_BYTES,
    RetentionScheduler,
)

GIB = 1024 ** 3
DAY = 86400.0


@pytest.fixture(autouse=True)
def _clean_brake():
    M._reset_brake_for_test()
    yield
    M._reset_brake_for_test()


class TestDiskPressureClassification:
    def test_percentage_threshold(self):
        assert M.classify_disk_pressure(0.50, 100 * GIB) == "ok"
        assert M.classify_disk_pressure(0.86, 100 * GIB) == "warn"
        assert M.classify_disk_pressure(0.95, 100 * GIB) == "critical"

    def test_absolute_floor_independent_of_percentage(self):
        """A huge volume at a comfortable percentage can still be nearly full."""
        # 0.5% used but only 1 GiB free -> still critical.
        assert M.classify_disk_pressure(0.005, 1 * GIB) == "critical"

    def test_zero_free_is_critical(self):
        assert M.classify_disk_pressure(1.0, 0) == "critical"


class TestBrakeTransitions:
    def test_engages_on_critical(self):
        assert M.evaluate_brake("critical", 0.99, engaged=False) is True

    def test_does_not_engage_on_warn(self):
        assert M.evaluate_brake("warn", 0.86, engaged=False) is False

    def test_hysteresis_holds_brake_between_release_and_critical(self):
        """Once engaged, dropping below critical is not enough to release."""
        assert M.evaluate_brake("warn", 0.90, engaged=True) is True

    def test_releases_below_release_threshold(self):
        assert M.evaluate_brake("ok", 0.50, engaged=True) is False

    def test_release_is_defined_and_not_sticky(self):
        """Full engage -> recover -> release cycle."""
        engaged = M.evaluate_brake("critical", 0.99, False)
        assert engaged is True
        engaged = M.evaluate_brake("warn", 0.88, engaged)
        assert engaged is True          # hysteresis
        engaged = M.evaluate_brake("ok", 0.10, engaged)
        assert engaged is False         # released

    def test_unknown_level_does_not_drop_an_active_brake(self):
        """A transient statvfs failure must not release the brake."""
        assert M.evaluate_brake("unknown", 0.0, engaged=True) is True
        assert M.evaluate_brake("unknown", 0.0, engaged=False) is False


class TestBrakeDoesNotDependOnMaintenanceLoop:
    def test_brake_arms_without_run_maintenance_once(self, monkeypatch, tmp_path):
        """The incident condition: ENABLE_MAINTENANCE=false.

        run_maintenance_once() never executed in production, so the only code
        path that armed the old file-based brake never ran. is_disk_pressure()
        must now arm itself from its own sample.
        """
        import shutil as _sh

        class _FullDisk:
            total, used, free = 200 * GIB, 200 * GIB, 0

        monkeypatch.setattr(M, "DATA_DIR", tmp_path)
        monkeypatch.setattr(_sh, "disk_usage", lambda p: _FullDisk)

        # No call to run_maintenance_once anywhere in this test.
        assert M.is_disk_pressure() is True
        assert M.brake_state()["engaged"] is True

    def test_brake_writes_nothing_to_the_protected_volume(self, monkeypatch, tmp_path):
        """Phase 2C: arming must not require a write to the full filesystem."""
        import shutil as _sh

        class _FullDisk:
            total, used, free = 200 * GIB, 200 * GIB, 0

        monkeypatch.setattr(M, "DATA_DIR", tmp_path)
        monkeypatch.setattr(_sh, "disk_usage", lambda p: _FullDisk)

        before = set(os.listdir(tmp_path))
        assert M.is_disk_pressure() is True
        after = set(os.listdir(tmp_path))
        assert before == after, "brake must not create files in DATA_DIR"
        assert not (tmp_path / ".disk_pressure").exists()

    def test_health_reports_brake_consistently_with_level(self, monkeypatch, tmp_path):
        """level=critical and emergency_brake=false must not coexist again."""
        import shutil as _sh

        class _FullDisk:
            total, used, free = 200 * GIB, 200 * GIB, 0

        monkeypatch.setattr(M, "DATA_DIR", tmp_path)
        monkeypatch.setattr(_sh, "disk_usage", lambda p: _FullDisk)

        info = M.check_disk_pressure()
        assert info["level"] == "critical"
        assert info["emergency_brake"] is True


# ---------------------------------------------------------------------------
# Phase 3 — auto_vacuum must be explicit policy, not an inherited default.
# ---------------------------------------------------------------------------


class TestBrakeDisarmIsLoud:
    """Disarming is a legitimate operational choice; being silent about it is
    what caused the incident."""

    def _full_disk(self, monkeypatch, tmp_path):
        import shutil as _sh

        class _Full:
            total, used, free = 200 * GIB, 200 * GIB, 0
        monkeypatch.setattr(M, "DATA_DIR", tmp_path)
        monkeypatch.setattr(_sh, "disk_usage", lambda p: _Full)

    def test_disarmed_brake_does_not_pause_ingest(self, monkeypatch, tmp_path):
        self._full_disk(monkeypatch, tmp_path)
        monkeypatch.setenv("DISK_BRAKE_ENABLED", "0")
        assert M.is_disk_pressure() is False

    def test_disarmed_brake_still_reports_it_would_engage(self, monkeypatch, tmp_path):
        self._full_disk(monkeypatch, tmp_path)
        monkeypatch.setenv("DISK_BRAKE_ENABLED", "0")
        info = M.check_disk_pressure()
        assert info["level"] == "critical"
        assert info["emergency_brake"] is False
        assert info["brake_would_engage"] is True
        assert info["brake_armed"] is False
        assert "NOT being paused" in info["brake_disarmed_warning"]

    def test_armed_by_default(self, monkeypatch, tmp_path):
        self._full_disk(monkeypatch, tmp_path)
        monkeypatch.delenv("DISK_BRAKE_ENABLED", raising=False)
        assert M.is_disk_pressure() is True
        assert M.brake_state()["armed"] is True

    def test_brake_state_distinguishes_disarmed_from_not_engaged(
            self, monkeypatch, tmp_path):
        self._full_disk(monkeypatch, tmp_path)
        monkeypatch.setenv("DISK_BRAKE_ENABLED", "0")
        M.is_disk_pressure()
        st = M.brake_state()
        assert st["would_engage"] is True and st["engaged"] is False
