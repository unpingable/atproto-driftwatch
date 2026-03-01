"""Tests for the author velocity anomaly sensor (H2)."""

import pytest
from labeler.sensors.base import SensorContext, run_sensor_with_budget, reset_warmup
from labeler.sensors.author_velocity import AuthorVelocitySensor, _reset as _reset_velocity
from labeler.detection import validate_envelope


@pytest.fixture(autouse=True)
def clean_state():
    """Reset sensor state before each test."""
    _reset_velocity()
    reset_warmup()
    yield
    _reset_velocity()
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


def _make_timeseries(entries):
    """Create timeseries from list of (fingerprint, posts, authors) tuples."""
    ts = []
    for fp, posts, authors in entries:
        ts.append({
            "fingerprint": fp,
            "bin": "2026-02-28T00:00:00+00:00",
            "posts": posts,
            "authors": authors,
            "avg_confidence": 0.5,
            "evidence_classes": {"none": posts},
        })
    return ts


class TestAuthorVelocitySensor:
    def test_volume_gate(self):
        """Below min_volume → no detections."""
        sensor = AuthorVelocitySensor()
        ctx = _make_ctx(total_claims=10)
        results = run_sensor_with_budget(sensor, ctx)
        assert results == []

    def test_warmup_suppression(self):
        """First N windows → no detections."""
        sensor = AuthorVelocitySensor()
        ts = _make_timeseries([("fp_a", 50, 25)])
        ctx = _make_ctx(timeseries=ts, total_claims=100)

        for _ in range(5):
            results = run_sensor_with_budget(sensor, ctx)
            assert results == []

    def test_stable_ratio_no_detection(self):
        """Stable author/post ratio → no detection."""
        sensor = AuthorVelocitySensor()
        ts = _make_timeseries([("fp_a", 100, 50)])
        ctx = _make_ctx(timeseries=ts, total_claims=100)

        # Warmup + establish baseline
        for _ in range(10):
            results = run_sensor_with_budget(sensor, ctx)

        # Should not fire — ratio stable at 0.5
        assert results == []

    def test_spread_detection(self):
        """Sudden increase in author ratio → spread detection."""
        sensor = AuthorVelocitySensor()

        # Establish low-ratio baseline
        low_ratio_ts = _make_timeseries([("fp_a", 100, 10)])  # ratio = 0.1
        low_ctx = _make_ctx(timeseries=low_ratio_ts, total_claims=100)

        for _ in range(10):
            run_sensor_with_budget(sensor, low_ctx)

        # Now: high author ratio (many new authors)
        high_ratio_ts = _make_timeseries([("fp_a", 100, 80)])  # ratio = 0.8
        high_ctx = _make_ctx(timeseries=high_ratio_ts, total_claims=100)

        results = run_sensor_with_budget(sensor, high_ctx)
        spread = [d for d in results if d.type == "author_spread"]
        assert len(spread) >= 1

        det = spread[0]
        assert det.detector_id == "author_velocity"
        assert det.subject.type == "fingerprint"
        assert det.subject.value == "fp_a"
        assert "current_ratio" in det.explain
        assert "baseline_ratio" in det.explain
        validate_envelope(det, strict=True)

    def test_amplification_detection(self):
        """Sudden decrease in author ratio → amplification detection."""
        sensor = AuthorVelocitySensor()

        # Establish high-ratio baseline
        high_ratio_ts = _make_timeseries([("fp_a", 100, 80)])  # ratio = 0.8
        high_ctx = _make_ctx(timeseries=high_ratio_ts, total_claims=100)

        for _ in range(10):
            run_sensor_with_budget(sensor, high_ctx)

        # Now: low author ratio (few authors, lots of posts)
        low_ratio_ts = _make_timeseries([("fp_a", 100, 10)])  # ratio = 0.1
        low_ctx = _make_ctx(timeseries=low_ratio_ts, total_claims=100)

        results = run_sensor_with_budget(sensor, low_ctx)
        amp = [d for d in results if d.type == "author_amplification"]
        assert len(amp) >= 1

        det = amp[0]
        assert det.subject.value == "fp_a"
        assert "delta_pct" in det.explain
        validate_envelope(det, strict=True)

    def test_top_n_limit(self):
        """Only top-N fingerprints by volume are checked."""
        sensor = AuthorVelocitySensor()

        # Create 30 fingerprints — only top 20 should be analyzed
        entries = [(f"fp_{i:02d}", 100 - i, 50) for i in range(30)]
        ts = _make_timeseries(entries)
        ctx = _make_ctx(timeseries=ts, total_claims=3000)

        for _ in range(10):
            run_sensor_with_budget(sensor, ctx)

        # No detection on stable ratios anyway, but verify no crash
        results = run_sensor_with_budget(sensor, ctx)
        # All results should be for top-20 fingerprints
        for det in results:
            if hasattr(det, 'subject') and det.subject.type == "fingerprint":
                fp_num = int(det.subject.value.split("_")[1])
                assert fp_num < 20

    def test_explain_has_baseline_fields(self):
        """Explain block must include baseline_kind and baseline_window."""
        sensor = AuthorVelocitySensor()

        low_ts = _make_timeseries([("fp_a", 100, 10)])
        low_ctx = _make_ctx(timeseries=low_ts, total_claims=100)
        for _ in range(10):
            run_sensor_with_budget(sensor, low_ctx)

        high_ts = _make_timeseries([("fp_a", 100, 80)])
        high_ctx = _make_ctx(timeseries=high_ts, total_claims=100)
        results = run_sensor_with_budget(sensor, high_ctx)

        for det in results:
            if det.type in ("author_spread", "author_amplification"):
                assert "baseline_kind" in det.explain
                assert "baseline_window" in det.explain

    def test_empty_timeseries(self):
        """No timeseries data → no detections."""
        sensor = AuthorVelocitySensor()
        ctx = _make_ctx(timeseries=[], total_claims=100)
        for _ in range(6):
            results = run_sensor_with_budget(sensor, ctx)
        assert results == []
