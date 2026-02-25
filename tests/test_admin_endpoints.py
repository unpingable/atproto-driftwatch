import pytest
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from labeler.main import app


def test_admin_mappings_endpoint(monkeypatch):
    fake = {"labeler.example": ["did:lab:1", "did:lab:2"]}

    async def fake_get_all_mappings():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_mappings", fake_get_all_mappings)

    client = TestClient(app)
    r = client.get("/admin/mappings")
    assert r.status_code == 200
    assert r.json() == {"mappings": fake}


def test_admin_cooldowns_endpoint(monkeypatch):
    fake = {"labeler.example": 120}

    async def fake_get_all_active_cooldowns():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_active_cooldowns", fake_get_all_active_cooldowns)

    client = TestClient(app)
    r = client.get("/admin/cooldowns")
    assert r.status_code == 200
    assert r.json() == {"cooldowns": fake}
