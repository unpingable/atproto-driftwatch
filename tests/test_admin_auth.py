import os
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from labeler.main import app


def test_admin_endpoints_require_token(monkeypatch, caplog):
    # set env token
    monkeypatch.setenv("ADMIN_API_TOKEN", "s3cr3t")

    fake = {"labeler.example": ["did:lab:1"]}

    async def fake_get_all_mappings():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_mappings", fake_get_all_mappings)

    client = TestClient(app)

    # no header -> 401
    r = client.get("/admin/mappings")
    assert r.status_code == 401

    # wrong token -> 401 and logs warning
    with caplog.at_level("WARNING"):
        r = client.get("/admin/mappings", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401
        assert any("admin auth failed: invalid token" in rec.getMessage() for rec in caplog.records)

    # correct bearer token
    r = client.get("/admin/mappings", headers={"Authorization": "Bearer s3cr3t"})
    assert r.status_code == 200
    assert r.json() == {"mappings": fake}

    # X-Admin-Token header also works
    r = client.get("/admin/mappings", headers={"X-Admin-Token": "s3cr3t"})
    assert r.status_code == 200


def test_admin_endpoints_unprotected_when_no_token(monkeypatch):
    # ensure env var not set
    monkeypatch.delenv("ADMIN_API_TOKEN", raising=False)

    fake = {"labeler.example": ["did:lab:1"]}

    async def fake_get_all_mappings():
        return fake

    monkeypatch.setattr("labeler.cooldown.get_all_mappings", fake_get_all_mappings)

    client = TestClient(app)
    r = client.get("/admin/mappings")
    assert r.status_code == 200
    assert r.json() == {"mappings": fake}
