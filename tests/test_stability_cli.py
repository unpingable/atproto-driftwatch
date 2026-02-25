import json
import pytest


def test_stability_report_generation(tmp_path, monkeypatch):
    from labeler.stability import compute_stability_report, evaluate_stability, stability_thresholds_from_env

    items = [
        {"text": "200 people were affected.", "group": "g1"},
        {"text": "200 people were affected.", "group": "g1"},
        {"text": "20 people were affected.", "group": "g2"},
    ]
    report = compute_stability_report(items)
    monkeypatch.setenv("STABILITY_COLLISION_RATE_MAX", "1.0")
    thresholds = stability_thresholds_from_env()
    ok, checks = evaluate_stability(report, thresholds)
    assert "collision" in report and "churn" in report and "drift" in report
    assert isinstance(ok, bool)
    assert isinstance(checks, dict)


def test_release_promote_cli(tmp_path):
    from labeler.cli import main
    import sys

    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps({
        "generated_at": "2026-01-30T00:00:00+00:00",
        "fp_version": "v1",
        "config_hash": "cfg",
        "collision": {"collision_rate": 0.0},
        "churn": {"churn_rate_per_anchor": 0.0},
        "drift": {},
        "thresholds": {"collision_rate": 1.0, "churn_rate_per_anchor": 1.0, "drift_max": {"small_edit": 1.0}},
    }))

    out_quarantine = tmp_path / "q.json"
    argv = sys.argv
    try:
        sys.argv = ["labeler.cli", "release", "quarantine", "--report", str(report_path), "--out", str(out_quarantine), "--force"]
        main()
        sys.argv = ["labeler.cli", "release", "promote", "--in", str(out_quarantine), "--out", str(tmp_path / "p.json")]
        main()
    finally:
        sys.argv = argv
