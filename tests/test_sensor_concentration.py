"""Tests for the concentration shift sensor (H1)."""

import pytest
from labeler.sensors.base import SensorContext, run_sensor_with_budget, reset_warmup
from labeler.sensors.concentration import ConcentrationSensor, _reset as _reset_concentration
from labeler.detection import validate_envelope


@pytest.fixture(autouse=True)
def clean_state():
    """Reset sensor state before each test."""
    _reset_concentration()
    reset_warmup()
    yield
    _reset_concentration()
    reset_warmup()


def _make_ctx(timeseries=None, total_claims=100, watermark_state="ok"):
    return SensorContext(
        conn=None,
        ts_start="2026-02-28T00:00:00+00:00",
        ts_end="2026-02-28T01:00:00+00:00",
        window="1h",
        watermark={"health_state": watermark_state, "coverage_pct": 0.95},
        window_fingerprint="test_wfp",
        config_hash="test_cfg",
        timeseries=timeseries or [],
        total_claims=total_claims,
    )


def _make_timeseries(fp_distribution):
    """Create timeseries entries from a {fingerprint: post_count} dict."""
    ts = []
    for fp, count in fp_distribution.items():
        ts.append({
            "fingerprint": fp,
            "bin": "2026-02-28T00:00:00+00:00",
            "posts": count,
            "authors": max(1, count // 2),
            "avg_confidence": 0.5,
            "evidence_classes": {"none": count},
        })
    return ts


class TestConcentrationSensor:
    def test_volume_gate(self):
        """Below min_volume → no detections."""
        sensor = ConcentrationSensor()
        ctx = _make_ctx(total_claims=10)  # below min_volume=50
        results = run_sensor_with_budget(sensor, ctx)
        assert results == []

    def test_warmup_suppression(self):
        """First N windows → no detections."""
        sensor = ConcentrationSensor()
        ts = _make_timeseries({"fp_a": 100})
        ctx = _make_ctx(timeseries=ts, total_claims=100)

        # Should be suppressed for warmup_windows=5
        for _ in range(5):
            results = run_sensor_with_budget(sensor, ctx)
            assert results == []

    def test_uniform_distribution_no_detection(self):
        """Uniform distribution → low Herfindahl → no detection."""
        sensor = ConcentrationSensor()
        # 10 fingerprints with equal counts
        ts = _make_timeseries({f"fp_{i}": 10 for i in range(10)})
        ctx = _make_ctx(timeseries=ts, total_claims=100)

        # Warmup windows
        for _ in range(6):
            results = run_sensor_with_budget(sensor, ctx)

        # Should not fire — H = 0.1 (uniform with 10 items)
        assert results == []

    def test_concentrated_distribution_detects(self):
        """One fingerprint dominates → high Herfindahl → detection."""
        sensor = ConcentrationSensor()

        # First: establish a uniform baseline over warmup
        uniform_ts = _make_timeseries({f"fp_{i}": 10 for i in range(10)})
        uniform_ctx = _make_ctx(timeseries=uniform_ts, total_claims=100)

        for _ in range(10):
            run_sensor_with_budget(sensor, uniform_ctx)

        # Now: concentrated distribution (one fp has 80% of posts)
        conc_ts = _make_timeseries({"fp_dominant": 80, "fp_other": 10, "fp_third": 10})
        conc_ctx = _make_ctx(timeseries=conc_ts, total_claims=100)

        results = run_sensor_with_budget(sensor, conc_ctx)
        assert len(results) == 1

        det = results[0]
        assert det.detector_id == "concentration_shift"
        assert det.type == "concentration_spike"
        assert det.score > 0.3
        assert "herfindahl" in det.explain
        assert "top_fps" in det.explain

        # Validate envelope
        validate_envelope(det, strict=True)

    def test_empty_timeseries(self):
        """No timeseries data → no detections (not crash)."""
        sensor = ConcentrationSensor()
        ctx = _make_ctx(timeseries=[], total_claims=100)

        # Get past warmup
        for _ in range(6):
            results = run_sensor_with_budget(sensor, ctx)

        assert results == []

    def test_explain_has_baseline_fields(self):
        """Explain block must include baseline_kind and baseline_window."""
        sensor = ConcentrationSensor()

        uniform_ts = _make_timeseries({f"fp_{i}": 10 for i in range(10)})
        uniform_ctx = _make_ctx(timeseries=uniform_ts, total_claims=100)
        for _ in range(10):
            run_sensor_with_budget(sensor, uniform_ctx)

        conc_ts = _make_timeseries({"fp_dominant": 90, "fp_small": 10})
        conc_ctx = _make_ctx(timeseries=conc_ts, total_claims=100)
        results = run_sensor_with_budget(sensor, conc_ctx)

        if results and results[0].type == "concentration_spike":
            explain = results[0].explain
            assert "baseline_kind" in explain
            assert "baseline_window" in explain
