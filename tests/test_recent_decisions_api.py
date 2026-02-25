import json
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient


def test_recent_decisions_endpoint(tmp_path, monkeypatch):
    from labeler.main import app
    from labeler.db import init_db, get_conn

    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM label_decisions")
    conn.execute(
        "INSERT INTO label_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "dec-1",
            "2026-01-30T00:00:00+00:00",
            "uri:api:1",
            "root:api:1",
            "provenance_laundering_possible",
            "provenance_laundering",
            "v1",
            json.dumps({"spans": ["x"]}),
            json.dumps(["ehash1"]),
            "trace",
            "cfg",
            "committed",
        ),
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("ADMIN_API_TOKEN", "secret")

    client = TestClient(app)
    res = client.get("/recent-decisions", headers={"X-Admin-Token": "secret"})
    assert res.status_code == 200
    body = res.json()
    assert "decisions" in body
    assert body["decisions"][0]["rule_id"] == "provenance_laundering"
