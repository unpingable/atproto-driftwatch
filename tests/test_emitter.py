from pathlib import Path
from labeler.emitter import emit_labels_to_audit


def test_emit_writes_audit(tmp_path):
    labels = [
        {"subject_uri": "uri:demo:1", "label": "test_label", "score": 0.5, "reasons": ["r"], "evidence": []}
    ]
    audit = tmp_path / "audit.jsonl"
    path = emit_labels_to_audit(labels, audit_path=str(audit))
    assert Path(path).exists()
    lines = Path(path).read_text().strip().splitlines()
    assert len(lines) == 1
    assert "test_label" in lines[0]
