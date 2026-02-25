import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, get_conn
from labeler.emitter import record_emit_decision


def test_quarantine_stored_on_suppressed_emit(tmp_path):
    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM quarantine_emits")
    conn.commit()
    conn.close()

    labels = [{"subject_uri": "uri:q:1", "label": "x", "score": 0.5, "emit_reason": "test"}]
    record_emit_decision(labels, "quarantine", audit_path=str(tmp_path / "q.jsonl"))

    conn = get_conn()
    rows = conn.execute("SELECT emit_mode, emit_status, emit_reason FROM quarantine_emits").fetchall()
    conn.close()
    assert rows and rows[0][0] == "quarantine"
    assert rows[0][1] == "suppressed"
