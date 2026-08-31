import pytest
pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from labeler.main import app

# These endpoints are behind `admin_auth`, which fails closed. The token is
# configured here so the tests exercise the handlers rather than the refusal.
TOKEN = "test-admin-token"
AUTH = {"X-Admin-Token": TOKEN}


def test_admin_mappings_endpoint(monkeypatch):
    fake = {"labeler.example": ["did:lab:1", "did:lab:2"]}

    async def fake_get_all_mappings():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_mappings", fake_get_all_mappings)
    monkeypatch.setenv("ADMIN_API_TOKEN", TOKEN)

    client = TestClient(app)
    r = client.get("/admin/mappings", headers=AUTH)
    assert r.status_code == 200
    assert r.json() == {"mappings": fake}


def test_admin_cooldowns_endpoint(monkeypatch):
    fake = {"labeler.example": 120}

    async def fake_get_all_active_cooldowns():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_active_cooldowns", fake_get_all_active_cooldowns)
    monkeypatch.setenv("ADMIN_API_TOKEN", TOKEN)

    client = TestClient(app)
    r = client.get("/admin/cooldowns", headers=AUTH)
    assert r.status_code == 200
    assert r.json() == {"cooldowns": fake}
