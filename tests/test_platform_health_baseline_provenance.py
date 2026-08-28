"""Baseline provenance — the 2026-08-28 `platform_low_eps` latch.

These tests encode the incident directly. On 2026-08-28 Driftwatch restarted
after a 2h49m outage, resumed from a ~3h old cursor, and learned its EWMA
baseline from five windows of *backlog replay*. During those windows Jetstream
offered 690-1200 events/s while the pipeline could only commit 171-230/s,
shedding the rest — so `events_in` measured the pipeline's own drain ceiling,
not the source's arrival rate. 168.1 eps became "the baseline".

The state machine then latched DEGRADED and froze that baseline. Recovery
required coverage > 0.8, i.e. > 134.5 eps; the true rate never exceeded
127.2 eps in the preceding 96 hours. The alarm could never clear.

The repair is a provenance rule: only *admissible* windows may teach the
baseline, and only a baseline built from enough admissible windows earns the
standing to gate `platform_low_eps` or to be frozen.

Evidence: /data/jbeck/driftwatch-low-eps/20260828/FINAL-REPORT.md
"""

import pytest

from labeler.platform_health import (
    PlatformHealth,
    BASELINE_ESTABLISH_WINDOWS,
    BASELINE_ESTABLISHED,
    BASELINE_PROVISIONAL,
    BASELINE_UNESTABLISHED,
    CONSECUTIVE_BAD_WINDOWS,
    CONSECUTIVE_GOOD_WINDOWS,
    DEGRADED,
    OK,
    WARMUP_WINDOWS,
    LAG_RECOVER_THRESHOLD_S,
)


def _replay_window(ph, committed=200, dropped=900, lag=600.0, backlog=3000):
    """A backlog-drain window, shaped like the real 05:04-05:08 UTC windows:
    high commit rate, heavy shedding, lag pinned at the 600s clamp."""
    ph._stream_lag_s = lag
    return ph.record_window(int(committed * 60), 60.0, backlog, dropped=int(dropped * 60))


def _steady_window(ph, eps=91.0, backlog=10, lag=0.0):
    """A representative steady-state window: no drops, no lag, stable queue."""
    ph._stream_lag_s = lag
    return ph.record_window(int(eps * 60), 60.0, backlog, dropped=0)


class TestReplayCannotSeedBaseline:
    def test_replay_windows_are_inadmissible(self):
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS):
            snap = _replay_window(ph)
        assert snap["last_window_admissible"] is False
        assert snap["admissible_windows"] == 0

    def test_replay_never_establishes_a_baseline(self):
        ph = PlatformHealth()
        for _ in range(20):
            snap = _replay_window(ph)
        assert snap["baseline_standing"] == BASELINE_UNESTABLISHED
        assert snap["baseline_eps"] == 0.0

    def test_replay_cannot_raise_platform_low_eps(self):
        """The specific defect: a replay-derived denominator must never be the
        thing that declares the platform starved."""
        ph = PlatformHealth()
        for _ in range(20):
            snap = _replay_window(ph)
        assert "platform_low_eps" not in snap["gate_reasons"]

    def test_replay_still_degrades_on_its_own_merits(self):
        """The repair must not blind the honest alarms. Replay really is a
        degraded condition — it just isn't a *baseline*."""
        ph = PlatformHealth()
        for _ in range(WARMUP_WINDOWS + CONSECUTIVE_BAD_WINDOWS):
            snap = _replay_window(ph)
        assert snap["health_state"] == DEGRADED
        assert "lag_high" in snap["gate_reasons"]
        assert "high_drop_rate" in snap["gate_reasons"]


class TestTransitionToSteadyState:
    def test_baseline_establishes_from_admissible_windows(self):
        ph = PlatformHealth()
        for _ in range(10):
            _replay_window(ph)
        for _ in range(BASELINE_ESTABLISH_WINDOWS):
            snap = _steady_window(ph, eps=91.0)
        assert snap["baseline_standing"] == BASELINE_ESTABLISHED
        assert snap["baseline_eps"] == pytest.approx(91.0, abs=1.0)

    def test_health_converges_after_replay_drains(self):
        """End-to-end incident shape: replay storm, then ~91 eps steady state.
        The service must reach OK, which on the deployed build it never did."""
        ph = PlatformHealth()
        for _ in range(10):
            _replay_window(ph)
        assert ph.get_health_snapshot()["health_state"] == DEGRADED

        for _ in range(40):
            snap = _steady_window(ph, eps=91.0)

        assert snap["health_state"] == OK
        assert snap["gate_reasons"] == []
        assert snap["baseline_standing"] == BASELINE_ESTABLISHED
        assert snap["baseline_eps"] == pytest.approx(91.0, abs=2.0)
        assert snap["coverage_pct"] > 0.9


class TestNoMathematicallyPermanentPoison:
    def test_poisoned_baseline_has_a_path_to_recovery(self):
        """Reproduce the exact latch: a 168.1 baseline against a source that
        sustains ~91 eps — below the 134.5 eps the old recovery gate demanded."""
        ph = PlatformHealth()
        for _ in range(10):
            _replay_window(ph, committed=168, dropped=900)
        for _ in range(60):
            snap = _steady_window(ph, eps=91.0)

        assert snap["health_state"] == OK, (
            "the 91 eps steady state must be reachable; the deployed build "
            "latched here forever"
        )
        assert snap["baseline_eps"] < 134.5, (
            "baseline must reflect attainable traffic, not the drain ceiling"
        )

    def test_stale_v1_checkpoint_is_not_authoritative(self):
        """The poisoned production checkpoint is v1 and carries no provenance.
        It must not be restored as fact."""
        poisoned = {
            "version": 1,
            "baseline_eps": 168.1288533536345,
            "current_eps": 90.28,
            "stream_lag_s": 0.0127,
            "windows_seen": 700,
            "state": "degraded",
            "checkpoint_at": __import__("time").time(),
        }
        ph = PlatformHealth()
        assert ph.restore(poisoned) is False
        assert ph.get_health_snapshot()["baseline_eps"] == 0.0
        assert ph.get_health_snapshot()["baseline_standing"] == BASELINE_UNESTABLISHED

    def test_recovery_does_not_require_self_lowering_an_established_baseline(self):
        """Recovery must come from the baseline never having been poisoned —
        not from a degraded service quietly redefining normal downward."""
        ph = PlatformHealth()
        for _ in range(BASELINE_ESTABLISH_WINDOWS):
            _steady_window(ph, eps=91.0)
        assert ph.get_health_snapshot()["baseline_standing"] == BASELINE_ESTABLISHED
        established = ph.get_health_snapshot()["baseline_eps"]

        # Genuine sustained collapse to 20 eps, cleanly delivered.
        for _ in range(30):
            snap = _steady_window(ph, eps=20.0)

        assert snap["health_state"] == DEGRADED
        assert "platform_low_eps" in snap["gate_reasons"]
        assert snap["baseline_eps"] == pytest.approx(established, abs=0.5), (
            "an established baseline must stay frozen while degraded"
        )


class TestLegitimateDegradationRemainsLegitimate:
    def test_real_drop_still_latches(self):
        ph = PlatformHealth()
        for _ in range(BASELINE_ESTABLISH_WINDOWS + 5):
            _steady_window(ph, eps=100.0)
        assert ph.get_health_snapshot()["health_state"] == OK

        for _ in range(CONSECUTIVE_BAD_WINDOWS + 2):
            snap = _steady_window(ph, eps=10.0)
        assert snap["health_state"] == DEGRADED
        assert "platform_low_eps" in snap["gate_reasons"]

    def test_recovery_requires_returning_to_the_established_baseline(self):
        """The alarm must not have become self-healing: coming back to 50% of
        a legitimate baseline is not recovery."""
        ph = PlatformHealth()
        for _ in range(BASELINE_ESTABLISH_WINDOWS + 5):
            _steady_window(ph, eps=100.0)
        for _ in range(CONSECUTIVE_BAD_WINDOWS + 2):
            _steady_window(ph, eps=10.0)
        assert ph.get_health_snapshot()["health_state"] == DEGRADED

        for _ in range(CONSECUTIVE_GOOD_WINDOWS + 3):
            snap = _steady_window(ph, eps=50.0)
        assert snap["health_state"] == DEGRADED, (
            "50% of an established baseline must not count as recovered"
        )

        for _ in range(CONSECUTIVE_GOOD_WINDOWS + 3):
            snap = _steady_window(ph, eps=95.0)
        assert snap["health_state"] == OK


class TestAdmissibilityPredicate:
    def test_dropped_events_invalidate_learning(self):
        ph = PlatformHealth()
        for _ in range(10):
            snap = ph.record_window(60 * 91, 60.0, 10, dropped=1)
        assert snap["admissible_windows"] == 0
        assert snap["baseline_standing"] == BASELINE_UNESTABLISHED

    def test_elevated_lag_invalidates_learning(self):
        ph = PlatformHealth()
        for _ in range(10):
            ph._stream_lag_s = LAG_RECOVER_THRESHOLD_S + 1
            snap = ph.record_window(60 * 91, 60.0, 10, dropped=0)
        assert snap["admissible_windows"] == 0
        assert snap["baseline_standing"] == BASELINE_UNESTABLISHED

    def test_growing_backlog_invalidates_learning(self):
        # The first window cannot know backlog direction (no previous sample),
        # so prime with one window and assert nothing accrues after that.
        ph = PlatformHealth()
        ph._stream_lag_s = 0.0
        primed = ph.record_window(60 * 91, 60.0, 0, dropped=0)["admissible_windows"]

        backlog = 0
        for _ in range(10):
            backlog += 500  # far above BACKLOG_GROWTH_THRESHOLD
            ph._stream_lag_s = 0.0
            snap = ph.record_window(60 * 91, 60.0, backlog, dropped=0)
        assert snap["last_window_admissible"] is False
        assert snap["admissible_windows"] == primed

    def test_clean_window_is_admissible(self):
        ph = PlatformHealth()
        snap = _steady_window(ph, eps=91.0)
        assert snap["last_window_admissible"] is True
        assert snap["admissible_windows"] == 1
        assert snap["baseline_standing"] in (
            BASELINE_PROVISIONAL, BASELINE_ESTABLISHED
        )

    def test_provisional_baseline_does_not_gate_low_eps(self):
        """One admissible window is a measurement, not a denominator."""
        ph = PlatformHealth()
        _steady_window(ph, eps=500.0)
        assert ph.get_health_snapshot()["baseline_standing"] == BASELINE_PROVISIONAL
        for _ in range(CONSECUTIVE_BAD_WINDOWS):
            snap = ph.record_window(60 * 1, 60.0, 10, dropped=1)
        assert "platform_low_eps" not in snap["gate_reasons"]


class TestCheckpointProvenance:
    def test_established_checkpoint_restores(self):
        ph = PlatformHealth()
        for _ in range(BASELINE_ESTABLISH_WINDOWS + 2):
            _steady_window(ph, eps=91.0)
        data = ph.checkpoint()
        assert data["version"] == 2
        assert data["baseline_standing"] == BASELINE_ESTABLISHED

        ph2 = PlatformHealth()
        assert ph2.restore(data) is True
        snap = ph2.get_health_snapshot()
        assert snap["health_state"] == OK
        assert snap["baseline_standing"] == BASELINE_ESTABLISHED
        assert snap["baseline_eps"] == pytest.approx(91.0, abs=1.0)

    def test_provisional_checkpoint_does_not_restore(self):
        ph = PlatformHealth()
        _steady_window(ph, eps=91.0)
        data = ph.checkpoint()
        assert data["baseline_standing"] == BASELINE_PROVISIONAL

        ph2 = PlatformHealth()
        assert ph2.restore(data) is False
        assert ph2.get_health_snapshot()["baseline_standing"] == BASELINE_UNESTABLISHED

    def test_restored_baseline_is_freeze_eligible_immediately(self):
        """A restored established baseline must behave like an established one:
        frozen during degradation, not quietly relearned downward."""
        ph = PlatformHealth()
        for _ in range(BASELINE_ESTABLISH_WINDOWS + 2):
            _steady_window(ph, eps=100.0)
        data = ph.checkpoint()

        ph2 = PlatformHealth()
        assert ph2.restore(data) is True
        restored = ph2.get_health_snapshot()["baseline_eps"]
        for _ in range(30):
            snap = _steady_window(ph2, eps=10.0)
        assert snap["health_state"] == DEGRADED
        assert snap["baseline_eps"] == pytest.approx(restored, abs=0.5)
