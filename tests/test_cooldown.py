"""Tests for CooldownFilter — detection dedupe across windows."""

import pytest
from labeler.detection import (
    CooldownFilter,
    DetectionEnvelope,
    SubjectRef,
    build_envelope,
    make_note,
    COOLDOWN_WINDOWS,
    COOLDOWN_SCORE_DELTA,
)


def _make_det(det_id_seed="test", score=1.0, severity="low"):
    """Build a minimal detection envelope for testing cooldown."""
    return build_envelope(
        detector_id=det_id_seed,
        detector_version="v1",
        ts_start="2026-02-28T00:00:00+00:00",
        ts_end="2026-02-28T01:00:00+00:00",
        window="1h",
        subject=SubjectRef("fingerprint", f"fp_{det_id_seed}"),
        detection_type="test_burst",
        score=score,
        severity=severity,
        explain={"test": True},
        evidence=(make_note("test"),),
        window_fingerprint="wfp_test",
        config_hash="cfg_test",
    )


@pytest.fixture
def cf():
    return CooldownFilter()


class TestCooldownFilter:
    def test_first_emission_passes(self, cf):
        """First time seeing a det_id → pass through."""
        det = _make_det("sensor_a")
        result = cf.filter_detections([det])
        assert len(result) == 1

    def test_duplicate_suppressed(self, cf):
        """Same det_id in next window → suppressed."""
        det = _make_det("sensor_a")
        cf.filter_detections([det])
        cf.tick_window()
        result = cf.filter_detections([det])
        assert len(result) == 0

    def test_cooldown_expires(self, cf):
        """After COOLDOWN_WINDOWS, same det_id passes again."""
        det = _make_det("sensor_a")
        cf.filter_detections([det])
        for _ in range(COOLDOWN_WINDOWS):
            cf.tick_window()
        result = cf.filter_detections([det])
        assert len(result) == 1

    def test_severity_escalation_bypasses_cooldown(self, cf):
        """Severity increase → re-emit despite cooldown."""
        det_low = _make_det("sensor_a", score=1.0, severity="low")
        cf.filter_detections([det_low])
        cf.tick_window()

        det_high = _make_det("sensor_a", score=1.0, severity="high")
        result = cf.filter_detections([det_high])
        assert len(result) == 1

    def test_same_severity_suppressed(self, cf):
        """Same severity, same score → suppressed."""
        det = _make_det("sensor_a", score=1.0, severity="low")
        cf.filter_detections([det])
        cf.tick_window()
        result = cf.filter_detections([det])
        assert len(result) == 0

    def test_score_delta_bypasses_cooldown(self, cf):
        """Score increase by >= COOLDOWN_SCORE_DELTA → re-emit."""
        det1 = _make_det("sensor_a", score=1.0, severity="low")
        cf.filter_detections([det1])
        cf.tick_window()

        det2 = _make_det("sensor_a", score=1.0 + COOLDOWN_SCORE_DELTA, severity="low")
        result = cf.filter_detections([det2])
        assert len(result) == 1

    def test_small_score_increase_suppressed(self, cf):
        """Score increase below delta → still suppressed."""
        det1 = _make_det("sensor_a", score=1.0, severity="low")
        cf.filter_detections([det1])
        cf.tick_window()

        det2 = _make_det("sensor_a", score=1.0 + COOLDOWN_SCORE_DELTA - 0.01, severity="low")
        result = cf.filter_detections([det2])
        assert len(result) == 0

    def test_different_det_ids_independent(self, cf):
        """Different det_ids don't share cooldown state."""
        det_a = _make_det("sensor_a")
        det_b = _make_det("sensor_b")
        cf.filter_detections([det_a])
        cf.tick_window()
        result = cf.filter_detections([det_a, det_b])
        # sensor_a suppressed, sensor_b passes
        assert len(result) == 1
        assert result[0].detector_id == "sensor_b"

    def test_reset_clears_state(self, cf):
        """After reset, all det_ids pass again."""
        det = _make_det("sensor_a")
        cf.filter_detections([det])
        cf.tick_window()
        cf.reset()
        result = cf.filter_detections([det])
        assert len(result) == 1

    def test_severity_downgrade_suppressed(self, cf):
        """Severity decrease → still suppressed (only escalation bypasses)."""
        det_high = _make_det("sensor_a", score=1.0, severity="high")
        cf.filter_detections([det_high])
        cf.tick_window()

        det_low = _make_det("sensor_a", score=1.0, severity="low")
        result = cf.filter_detections([det_low])
        assert len(result) == 0
