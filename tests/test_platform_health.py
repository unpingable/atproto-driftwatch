"""Tests for the platform health watermark (stream-health gate)."""

import time
import pytest
from labeler import platform_health
from labeler.platform_health import (
    PlatformHealth,
    WARMING_UP,
    OK,
    DEGRADED,
    CONSECUTIVE_BAD_WINDOWS,
    CONSECUTIVE_GOOD_WINDOWS,
    RECALIBRATION_WINDOWS,
    WARMUP_WINDOWS,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset module-level singleton before each test."""
    platform_health._reset()
    yield
    platform_health._reset()


def _window(ph, events_in=500, window_secs=60.0, backlog=0):
    """Helper: record one STATS window."""
    return ph.record_window(events_in, window_secs, backlog)


class TestWarmup:
    def test_starts_warming_up(self):
        ph = PlatformHealth()
        assert ph.get_health_snapshot()["health_state"] == WARMING_UP

    def test_warmup_transitions_to_ok(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        assert ph.get_health_snapshot()["health_state"] == OK

    def test_warmup_is_not_degraded_normally(self):
        ph = PlatformHealth()
        _window(ph, events_in=500)
        assert ph.is_degraded() is False

    def test_warmup_with_insane_lag_is_degraded(self):
        ph = PlatformHealth()
        _window(ph, events_in=500)
        # Simulate high lag via direct event time (far in past)
        wall_us = int(time.time() * 1_000_000)
        old_time_us = wall_us - 200_000_000  # 200s ago
        ph.record_event_time(old_time_us)
        assert ph.is_degraded() is True


class TestEWMA:
    def test_first_window_sets_baseline(self):
        ph = PlatformHealth()
        snap = _window(ph, events_in=600, window_secs=60.0)
        assert snap["baseline_eps"] == 10.0  # 600/60
        assert snap["current_eps"] == 10.0

    def test_subsequent_windows_smooth(self):
        ph = PlatformHealth()
        _window(ph, events_in=600, window_secs=60.0)  # baseline = 10.0
        snap = _window(ph, events_in=1200, window_secs=60.0)  # current = 20.0
        # EWMA should be between 10 and 20
        assert 10.0 < snap["baseline_eps"] < 20.0

    def test_coverage_clamps_to_one(self):
        ph = PlatformHealth()
        _window(ph, events_in=600, window_secs=60.0)
        # Double the events — coverage should clamp at 1.0
        snap = _window(ph, events_in=1200, window_secs=60.0)
        assert snap["coverage_pct"] <= 1.0


class TestLowEPSTrigger:
    def test_three_bad_windows_trigger_degraded(self):
        ph = PlatformHealth()
        # Build baseline during warmup
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        assert ph.get_health_snapshot()["health_state"] == OK

        # Now send low EPS
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            _window(ph, events_in=10)  # very low
        assert ph.get_health_snapshot()["health_state"] == DEGRADED
        assert "platform_low_eps" in ph.get_gate_reasons()

    def test_good_window_resets_counter(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)

        # Two bad windows, then one good
        for _ in range(CONSECUTIVE_BAD_WINDOWS - 1):
            _window(ph, events_in=10)
        _window(ph, events_in=500)  # reset
        _window(ph, events_in=10)   # only 1 bad now
        assert ph.get_health_snapshot()["health_state"] == OK


class TestLagTrigger:
    def test_high_lag_triggers_degraded(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        assert ph.get_health_snapshot()["health_state"] == OK

        # Simulate sustained high lag
        wall_us = int(time.time() * 1_000_000)
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            ph.record_event_time(wall_us - 200_000_000)  # 200s lag
            _window(ph, events_in=500)

        assert ph.get_health_snapshot()["health_state"] == DEGRADED
        assert "lag_high" in ph.get_gate_reasons()


class TestConsumerBacklogTrigger:
    def test_growing_backlog_triggers_degraded(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500, backlog=100)

        # Simulate lag > recovery threshold
        wall_us = int(time.time() * 1_000_000)
        ph.record_event_time(wall_us - 50_000_000)  # 50s lag (> 30s recover threshold)

        # Growing backlog: each window 200 more than previous
        backlog = 100
        for i in range(CONSECUTIVE_BAD_WINDOWS):
            backlog += 200
            _window(ph, events_in=500, backlog=backlog)

        assert ph.get_health_snapshot()["health_state"] == DEGRADED
        assert "consumer_backlog" in ph.get_gate_reasons()


class TestRecovery:
    def _degrade(self, ph):
        """Helper: get ph into DEGRADED via low EPS."""
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            _window(ph, events_in=10)
        assert ph.get_health_snapshot()["health_state"] == DEGRADED

    def test_recovery_requires_consecutive_good_windows(self):
        ph = PlatformHealth()
        self._degrade(ph)

        # Reset lag to 0 so lag check passes
        ph._stream_lag_s = 0.0
        ph._prev_backlog = 100

        # Not enough good windows
        for _ in range(CONSECUTIVE_GOOD_WINDOWS - 1):
            _window(ph, events_in=500, backlog=100)
        assert ph.get_health_snapshot()["health_state"] == DEGRADED

        # One more pushes it over
        _window(ph, events_in=500, backlog=100)
        assert ph.get_health_snapshot()["health_state"] == OK

    def test_recalibration_gates_rechecks(self):
        ph = PlatformHealth()
        self._degrade(ph)

        ph._stream_lag_s = 0.0
        ph._prev_backlog = 100

        for _ in range(CONSECUTIVE_GOOD_WINDOWS):
            _window(ph, events_in=500, backlog=100)
        assert ph.get_health_snapshot()["health_state"] == OK

        # Recalibration: still degraded for rechecks
        if RECALIBRATION_WINDOWS > 0:
            assert ph.is_degraded() is True

        # Tick through recalibration
        for _ in range(RECALIBRATION_WINDOWS):
            _window(ph, events_in=500, backlog=100)
        assert ph.is_degraded() is False


class TestBaselineFreezing:
    def test_baseline_frozen_during_degraded(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)

        # Go degraded (baseline drifts down during these OK->DEGRADED windows)
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            _window(ph, events_in=10)
        assert ph.get_health_snapshot()["health_state"] == DEGRADED

        # Capture baseline at entry to DEGRADED
        baseline_at_degraded = ph.get_health_snapshot()["baseline_eps"]

        # Send more low EPS windows — baseline should NOT drop further
        for _ in range(5):
            _window(ph, events_in=5)
        baseline_after = ph.get_health_snapshot()["baseline_eps"]
        assert baseline_after == baseline_at_degraded


class TestGateReasons:
    def test_multiple_triggers_stack(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500, backlog=100)

        # Simultaneous: low EPS + high lag
        wall_us = int(time.time() * 1_000_000)
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            ph.record_event_time(wall_us - 200_000_000)
            _window(ph, events_in=10)

        reasons = ph.get_gate_reasons()
        assert "platform_low_eps" in reasons
        assert "lag_high" in reasons

    def test_reasons_clear_on_recovery(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            _window(ph, events_in=10)
        assert len(ph.get_gate_reasons()) > 0

        # Recover
        ph._stream_lag_s = 0.0
        ph._prev_backlog = 100
        for _ in range(CONSECUTIVE_GOOD_WINDOWS):
            _window(ph, events_in=500, backlog=100)
        assert ph.get_gate_reasons() == []


class TestSnapshot:
    def test_snapshot_has_expected_fields(self):
        ph = PlatformHealth()
        _window(ph, events_in=500)
        snap = ph.get_health_snapshot()
        expected = {
            "health_state",
            "coverage_pct",
            "current_eps",
            "baseline_eps",
            "stream_lag_s",
            "drop_frac",
            "reconnect_count",
            "reconnect_gap_s",
            "gate_reasons",
            "windows_seen",
            "recalibration_remaining",
            "baseline_restored",
        }
        assert set(snap.keys()) == expected


class TestModuleSingleton:
    def test_module_functions_delegate_to_singleton(self):
        snap = platform_health.record_window(600, 60.0, 0)
        assert snap["current_eps"] == 10.0
        assert platform_health.is_degraded() is False

        full = platform_health.get_health_snapshot()
        assert full["health_state"] == WARMING_UP

    def test_reconnect_tracking(self):
        platform_health.record_reconnect()
        snap = platform_health.get_health_snapshot()
        assert snap["reconnect_count"] == 1

    def test_event_time_tracking(self):
        wall_us = int(time.time() * 1_000_000)
        platform_health.record_event_time(wall_us - 5_000_000)  # 5s ago
        snap = platform_health.get_health_snapshot()
        assert snap["stream_lag_s"] > 0


class TestCheckpointRestore:
    def test_checkpoint_roundtrip(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        assert ph.get_health_snapshot()["health_state"] == OK

        data = ph.checkpoint()
        assert data["version"] == PlatformHealth.CHECKPOINT_VERSION
        assert data["baseline_eps"] > 0
        assert data["windows_seen"] >= WARMUP_WINDOWS

        # Restore into a fresh instance
        ph2 = PlatformHealth()
        assert ph2.get_health_snapshot()["health_state"] == WARMING_UP
        assert ph2.restore(data) is True
        assert ph2.get_health_snapshot()["health_state"] == OK
        assert abs(ph2.get_health_snapshot()["baseline_eps"] - data["baseline_eps"]) < 0.1
        assert ph2.get_health_snapshot()["baseline_restored"] is True

    def test_restore_rejects_wrong_version(self):
        ph = PlatformHealth()
        assert ph.restore({"version": 999}) is False

    def test_restore_rejects_stale_checkpoint(self):
        ph = PlatformHealth()
        data = {
            "version": PlatformHealth.CHECKPOINT_VERSION,
            "baseline_eps": 100.0,
            "checkpoint_at": time.time() - 7200,  # 2 hours old
        }
        assert ph.restore(data) is False

    def test_restore_rejects_zero_baseline(self):
        ph = PlatformHealth()
        data = {
            "version": PlatformHealth.CHECKPOINT_VERSION,
            "baseline_eps": 0,
            "checkpoint_at": time.time(),
        }
        assert ph.restore(data) is False

    def test_restore_rejects_empty(self):
        ph = PlatformHealth()
        assert ph.restore({}) is False
        assert ph.restore(None) is False

    def test_checkpoint_file_roundtrip(self, tmp_path):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)

        path = tmp_path / "baseline.json"
        assert ph.force_checkpoint(path) is True
        assert path.exists()

        ph2 = PlatformHealth()
        import json
        data = json.loads(path.read_text())
        assert ph2.restore(data) is True
        assert ph2.get_health_snapshot()["health_state"] == OK

    def test_maybe_checkpoint_respects_interval(self, tmp_path):
        ph = PlatformHealth()
        path = tmp_path / "baseline.json"

        # Warmup first
        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)

        # Should checkpoint at interval boundaries
        interval = PlatformHealth.CHECKPOINT_INTERVAL_WINDOWS
        # Record enough windows to hit the next interval
        for i in range(1, interval + 1):
            _window(ph, events_in=500)
            result = ph.maybe_checkpoint(path)
            if ph._windows_seen % interval == 0:
                assert result is True
            else:
                assert result is False

    def test_restored_state_in_snapshot(self):
        ph = PlatformHealth()
        assert ph.get_health_snapshot()["baseline_restored"] is False

        for _ in range(WARMUP_WINDOWS):
            _window(ph, events_in=500)
        data = ph.checkpoint()

        ph2 = PlatformHealth()
        ph2.restore(data)
        assert ph2.get_health_snapshot()["baseline_restored"] is True
