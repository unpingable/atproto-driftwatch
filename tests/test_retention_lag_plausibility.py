"""retention_lag_s must not report epoch-scale nonsense from hostile ctime."""

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


class TestRetentionLag:
    def test_garbage_pre_floor_timestamp_is_rejected(self):
        """The observed production value came from a 2002-04-01 ctime."""
        assert M.compute_retention_lag("2002-04-01T00:00:00+00:00", 259200) is None

    def test_normal_timestamp_computes_lag(self):
        from datetime import datetime, timedelta, timezone
        now = datetime(2026, 8, 25, tzinfo=timezone.utc)
        oldest = (now - timedelta(days=5)).isoformat()
        lag = M.compute_retention_lag(oldest, 3 * 86400, now=now)
        assert lag == pytest.approx(2 * 86400, rel=1e-6)

    def test_inside_window_is_non_positive(self):
        from datetime import datetime, timedelta, timezone
        now = datetime(2026, 8, 25, tzinfo=timezone.utc)
        oldest = (now - timedelta(days=1)).isoformat()
        assert M.compute_retention_lag(oldest, 3 * 86400, now=now) < 0

    def test_none_and_garbage_inputs_are_none(self):
        assert M.compute_retention_lag(None, 259200) is None
        assert M.compute_retention_lag("", 259200) is None
        assert M.compute_retention_lag("not-a-date", 259200) is None

    def test_never_returns_epoch_scale_value(self):
        """Guard against the 11.8-year regression specifically."""
        for bad in ["1970-01-01T00:00:00+00:00", "2002-04-01T00:00:00+00:00",
                    "2014-09-30T00:00:00+00:00"]:
            lag = M.compute_retention_lag(bad, 259200)
            assert lag is None or lag < 365 * 86400
