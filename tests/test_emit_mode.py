from labeler.emitter import record_emit_decision


def test_record_emit_decision_uses_audit_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("LABELER_EMIT_AUDIT_DIR", str(tmp_path))
    labels = [{"subject_uri": "uri:2", "label": "y", "score": 0.6}]
    path, status = record_emit_decision(labels, "quarantine")
    assert status == "suppressed"
    assert path.startswith(str(tmp_path))


def test_record_emit_decision_detect_only(tmp_path):
    labels = [{"subject_uri": "uri:1", "label": "x", "score": 0.5}]
    out = tmp_path / "detect.jsonl"
    path, status = record_emit_decision(labels, "detect-only", audit_path=str(out))
    assert status == "suppressed"
    assert path == str(out)
    assert out.read_text().strip() != ""
