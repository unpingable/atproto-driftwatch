"""Tests for driftwatch summary formatting."""

from labeler.summary import format_summary


def _base_report(**overrides):
    """Minimal cluster_report-shaped dict."""
    report = {
        "generated_at": "2026-03-16T12:00:00+00:00",
        "window_hours": 24,
        "bin_hours": 1,
        "total_claims_in_window": 500000,
        "distinct_fps_in_window": 12000,
        "cluster_kinds": {
            "entity": {"fingerprints": 5000, "posts": 200000},
            "text": {"fingerprints": 4000, "posts": 150000},
            "domain": {"fingerprints": 2000, "posts": 100000},
            "span": {"fingerprints": 1000, "posts": 50000},
        },
        "platform_health": "ok",
        "coverage_pct": 0.986,
        "stream_lag_s": 0,
        "gate_reasons": [],
        "clusters": [],
        "likely_automation": [],
        "regime_shifts": [],
        "detections": [],
        "detections_suppressed": 0,
    }
    report.update(overrides)
    return report


def _make_cluster(fingerprint="test_fp", burst_score=1.5, total_posts=100,
                  latest_authors=5, fp_kind="entity", half_life_bins=None):
    hl = {
        "fingerprint": fingerprint,
        "peak_bin": "2026-03-16T06:00:00+00:00",
        "peak_posts": 50,
        "half_life_bins": half_life_bins,
        "decayed_to_bin": None,
    }
    return {
        "fingerprint": fingerprint,
        "burst_score": burst_score,
        "synchrony": 0.5,
        "variant_entropy": 1.2,
        "author_growth": 0.3,
        "latest_posts": 20,
        "latest_authors": latest_authors,
        "total_posts": total_posts,
        "total_bins": 24,
        "half_life": hl,
        "fp_kind": fp_kind,
    }


class TestFormatSummary:
    def test_basic_output_has_header(self):
        text = format_summary(_base_report())
        assert "Driftwatch Summary" in text
        assert "24h window" in text

    def test_platform_stable(self):
        text = format_summary(_base_report())
        assert "stable" in text.lower()

    def test_platform_degraded(self):
        text = format_summary(_base_report(platform_health="degraded"))
        assert "degraded" in text.lower()

    def test_platform_warming(self):
        text = format_summary(_base_report(platform_health="warming_up"))
        assert "warming" in text.lower()

    def test_cold_start_banner(self):
        text = format_summary(_base_report(platform_health="warming_up"))
        assert ">>>" in text
        assert "COLD START" in text

    def test_restored_banner(self):
        text = format_summary(_base_report(
            platform_health="warming_up", baseline_restored=True,
        ))
        assert ">>>" in text
        assert "RESTORED" in text
        assert "COLD" not in text

    def test_degraded_banner_loud(self):
        text = format_summary(_base_report(platform_health="degraded"))
        assert ">>>" in text
        assert "DEGRADED" in text

    def test_no_banner_when_ok(self):
        text = format_summary(_base_report(platform_health="ok"))
        assert ">>>" not in text

    def test_no_clusters_shown(self):
        text = format_summary(_base_report())
        assert "Notable clusters: none" in text

    def test_clusters_shown(self):
        clusters = [_make_cluster(f"fp_{i}", burst_score=5.0 - i) for i in range(3)]
        text = format_summary(_base_report(clusters=clusters))
        assert "fp_0" in text
        assert "burst=" in text

    def test_clusters_qualified_when_warming(self):
        clusters = [_make_cluster("hot", burst_score=10.0)]
        text = format_summary(_base_report(clusters=clusters, platform_health="warming_up"))
        assert "scores unreliable" in text.lower()

    def test_clusters_not_qualified_when_ok(self):
        clusters = [_make_cluster("hot", burst_score=10.0)]
        text = format_summary(_base_report(clusters=clusters, platform_health="ok"))
        assert "scores unreliable" not in text.lower()

    def test_high_burst_marker(self):
        clusters = [_make_cluster("hot_fp", burst_score=5.5)]
        text = format_summary(_base_report(clusters=clusters))
        assert "[!]" in text

    def test_automation_mentioned(self):
        auto = [_make_cluster("auto_fp", latest_authors=1, total_posts=50)]
        text = format_summary(_base_report(likely_automation=auto))
        assert "automation" in text.lower()

    def test_regime_shifts_none(self):
        text = format_summary(_base_report())
        assert "none detected" in text

    def test_regime_shifts_shown(self):
        shifts = [{"bin": "2026-03-16T10:00:00", "divergence": 0.45, "is_shift": True,
                   "baseline_total": 1000, "bin_total": 50}]
        text = format_summary(_base_report(regime_shifts=shifts))
        assert "1 detected" in text
        assert "JSD=0.450" in text

    def test_fingerprint_kinds_shown(self):
        text = format_summary(_base_report())
        assert "entity:" in text
        assert "text:" in text

    def test_baseline_maturity_good(self):
        text = format_summary(_base_report(total_claims_in_window=500000))
        assert "good" in text

    def test_baseline_maturity_weak(self):
        text = format_summary(_base_report(
            total_claims_in_window=5000,
            platform_health="warming_up",
        ))
        assert "weak" in text

    def test_interpretation_normal(self):
        text = format_summary(_base_report())
        assert "normal variance" in text.lower()

    def test_interpretation_bursts(self):
        clusters = [_make_cluster("hot", burst_score=4.0)]
        text = format_summary(_base_report(clusters=clusters))
        assert "burst" in text.lower()

    def test_sensor_detections_shown(self):
        detections = [{
            "detector_id": "concentration_shift",
            "severity": "med",
            "score": 0.8,
        }]
        text = format_summary(_base_report(detections=detections))
        assert "concentration_shift" in text

    def test_caveat_always_present(self):
        text = format_summary(_base_report())
        assert "Caveat" in text

    def test_half_life_shown(self):
        clusters = [_make_cluster("decaying", half_life_bins=6)]
        text = format_summary(_base_report(clusters=clusters))
        assert "half-life=6h" in text

    def test_long_fingerprint_truncated(self):
        clusters = [_make_cluster("a" * 60)]
        text = format_summary(_base_report(clusters=clusters))
        assert "..." in text
