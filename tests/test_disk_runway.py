"""Runway metric must not vanish when the volume saturates (2026-08-12)."""

import time

import pytest

from labeler.retention_scheduler import (
    DISK_RUNWAY_CRITICAL_DAYS,
    DISK_RUNWAY_MIN_FREE_BYTES,
    RetentionScheduler,
)

GIB = 1024 ** 3
DAY = 86400.0


def _sched_with_history(samples):
    """Build a scheduler with a synthetic (ts, db_bytes, free_bytes) history."""
    s = RetentionScheduler(consumer=None, sleep_between_s=0)
    s._disk_history = list(samples)
    return s


class TestDiskRunway:
    def test_healthy_and_flat_reports_no_runway_but_stays_ok(self):
        """Case 1: plenty of space, nothing being consumed.

        Runway genuinely does not apply, and must NOT be a false alarm.
        """
        now = time.time()
        s = _sched_with_history([
            (now - 3600, 100 * GIB, 80 * GIB),
            (now, 100 * GIB, 80 * GIB),
        ])
        assert s.estimate_disk_runway_days() is None
        assert s.health_state()["state"] == "ok"

    def test_healthy_and_declining_reports_positive_runway(self):
        """Case 2: space draining at a measurable rate."""
        now = time.time()
        # 10 GiB consumed in 1 day, 100 GiB left -> ~10 days.
        s = _sched_with_history([
            (now - DAY, 50 * GIB, 110 * GIB),
            (now, 60 * GIB, 100 * GIB),
        ])
        runway = s.estimate_disk_runway_days()
        assert runway == pytest.approx(10.0, rel=0.05)
        assert s.health_state()["state"] == "ok"

    def test_threshold_crossing_consumption_goes_critical(self):
        """Case 3: burning fast enough to cross the critical threshold."""
        now = time.time()
        # 50 GiB consumed in a day with 50 GiB left -> 1 day runway < 2.
        s = _sched_with_history([
            (now - DAY, 50 * GIB, 100 * GIB),
            (now, 100 * GIB, 50 * GIB),
        ])
        runway = s.estimate_disk_runway_days()
        assert runway is not None and runway < DISK_RUNWAY_CRITICAL_DAYS
        st = s.health_state()
        assert st["state"] == "critical"
        assert "disk_runway_days" in st["state_reason"]

    def test_zero_free_bytes_is_zero_runway_not_none(self):
        """Case 4: the exact incident condition.

        Old behaviour: free_delta == 0 -> burn_per_s == 0 -> return None, and
        every threshold branch was gated on `is not None`, so a completely full
        volume produced no alarm at all.
        """
        now = time.time()
        s = _sched_with_history([
            (now - 3600, 190 * GIB, 0),
            (now, 193 * GIB, 0),
        ])
        assert s.estimate_disk_runway_days() == 0.0
        st = s.health_state()
        assert st["state"] == "critical"
        assert "disk_exhausted" in st["state_reason"]

    def test_already_full_and_unchanged_stays_critical(self):
        """Case 5: full and flat — the state production sat in for 13 days."""
        now = time.time()
        s = _sched_with_history([
            (now - 7 * DAY, 193 * GIB, 0),
            (now, 193 * GIB, 0),
        ])
        assert s.estimate_disk_runway_days() == 0.0
        assert s.health_state()["state"] == "critical"

    def test_recovering_free_space_is_not_a_false_burn_alarm(self):
        """Case 6: free space increasing (e.g. after an archive delete)."""
        now = time.time()
        s = _sched_with_history([
            (now - DAY, 100 * GIB, 20 * GIB),
            (now, 80 * GIB, 40 * GIB),
        ])
        assert s.estimate_disk_runway_days() is None
        assert s.health_state()["state"] == "ok"

    def test_absolute_floor_fires_even_while_still_draining_slowly(self):
        """Below the absolute floor is critical regardless of burn rate."""
        now = time.time()
        s = _sched_with_history([
            (now - DAY, 190 * GIB, DISK_RUNWAY_MIN_FREE_BYTES + 1),
            (now, 191 * GIB, DISK_RUNWAY_MIN_FREE_BYTES - 1),
        ])
        assert s.estimate_disk_runway_days() == 0.0
        assert s.health_state()["state"] == "critical"

    def test_single_sample_full_volume_still_alarms(self):
        """One sample is enough when the volume is already exhausted."""
        s = _sched_with_history([(time.time(), 193 * GIB, 0)])
        assert s.estimate_disk_runway_days() == 0.0

    def test_no_history_is_none(self):
        assert _sched_with_history([]).estimate_disk_runway_days() is None


# ---------------------------------------------------------------------------
# Phase 2B/2C — absolute pressure must drive the brake, without writing to
# the volume whose exhaustion it signals.
# ---------------------------------------------------------------------------
