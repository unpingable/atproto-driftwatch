"""Tests for preflight checks."""

import os
import sqlite3
import pytest
from unittest import mock

from labeler.preflight import preflight, _check_watermark, _check_indexes, _check_hot_zone, _check_sensors_enabled


@pytest.fixture
def mem_conn():
    """In-memory SQLite with the required tables and indexes."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS claim_history (
            authorDid TEXT,
            claim_fingerprint TEXT,
            createdAt TIMESTAMP,
            confidence REAL,
            provenance TEXT,
            evidence_hash TEXT,
            post_uri TEXT,
            post_cid TEXT,
            fingerprint_version TEXT,
            evidence_class TEXT DEFAULT 'none',
            fp_kind TEXT DEFAULT 'unknown'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_history_fp ON claim_history(claim_fingerprint, createdAt)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_claim_history_created ON claim_history(createdAt)")
    return conn


class TestCheckWatermark:
    def test_ok_state(self):
        snap = {"health_state": "ok", "coverage_pct": 0.95}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = _check_watermark()
        assert result["status"] == "PASS"
        assert result["name"] == "watermark_ok"

    def test_degraded_state(self):
        snap = {"health_state": "degraded", "gate_reasons": ["platform_low_eps"]}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = _check_watermark()
        assert result["status"] == "FAIL"

    def test_warming_up_low_lag(self):
        snap = {"health_state": "warming_up", "stream_lag_s": 5.0}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = _check_watermark()
        assert result["status"] == "WARN"

    def test_warming_up_high_lag(self):
        snap = {"health_state": "warming_up", "stream_lag_s": 200.0}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = _check_watermark()
        assert result["status"] == "FAIL"


class TestCheckIndexes:
    def test_indexes_present(self, mem_conn):
        result = _check_indexes(mem_conn)
        assert result["status"] == "PASS"
        assert result["name"] == "indexes_ok"

    def test_indexes_missing(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE claim_history (authorDid TEXT, createdAt TEXT, claim_fingerprint TEXT)")
        result = _check_indexes(conn)
        assert result["status"] == "FAIL"
        assert "missing" in result["detail"]


class TestCheckHotZone:
    def test_no_data(self, mem_conn):
        result = _check_hot_zone(mem_conn)
        assert result["status"] == "FAIL"
        assert "no claims" in result["detail"]

    def test_thin_data(self, mem_conn):
        from labeler import timeutil
        now = timeutil.now_utc().isoformat()
        for i in range(10):
            mem_conn.execute(
                "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt) VALUES (?, ?, ?)",
                (f"did:plc:test{i}", f"fp_{i}", now),
            )
        mem_conn.commit()
        result = _check_hot_zone(mem_conn)
        assert result["status"] == "WARN"

    def test_healthy_data(self, mem_conn):
        from labeler import timeutil
        now = timeutil.now_utc().isoformat()
        for i in range(200):
            mem_conn.execute(
                "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt) VALUES (?, ?, ?)",
                (f"did:plc:test{i}", f"fp_{i}", now),
            )
        mem_conn.commit()
        result = _check_hot_zone(mem_conn)
        assert result["status"] == "PASS"


class TestCheckSensorsEnabled:
    def test_enabled(self):
        with mock.patch.dict("os.environ", {}, clear=False):
            os.environ.pop("SENSOR_ARRAY_ENABLED", None)
            result = _check_sensors_enabled()
        assert result["status"] == "PASS"

    def test_disabled(self):
        with mock.patch.dict("os.environ", {"SENSOR_ARRAY_ENABLED": "false"}):
            result = _check_sensors_enabled()
        assert result["status"] == "WARN"
        assert "operator disabled" in result["detail"]


class TestPreflight:
    def test_all_pass(self, mem_conn):
        """All checks pass → verdict PASS."""
        from labeler import timeutil
        now = timeutil.now_utc().isoformat()
        for i in range(200):
            mem_conn.execute(
                "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt) VALUES (?, ?, ?)",
                (f"did:plc:test{i}", f"fp_{i}", now),
            )
        mem_conn.commit()

        snap = {"health_state": "ok", "coverage_pct": 0.95}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            with mock.patch.dict("os.environ", {}, clear=False):
                os.environ.pop("SENSOR_ARRAY_ENABLED", None)
                result = preflight(conn=mem_conn)

        assert result["verdict"] == "PASS"
        assert len(result["checks"]) == 4
        assert all(c["status"] == "PASS" for c in result["checks"])

    def test_any_fail_means_fail(self, mem_conn):
        """One FAIL check → verdict FAIL."""
        snap = {"health_state": "degraded", "gate_reasons": ["lag_high"]}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = preflight(conn=mem_conn)

        assert result["verdict"] == "FAIL"

    def test_warn_only_means_warn(self, mem_conn):
        """Only WARN checks (no FAIL) → verdict WARN."""
        from labeler import timeutil
        now = timeutil.now_utc().isoformat()
        for i in range(200):
            mem_conn.execute(
                "INSERT INTO claim_history (authorDid, claim_fingerprint, createdAt) VALUES (?, ?, ?)",
                (f"did:plc:test{i}", f"fp_{i}", now),
            )
        mem_conn.commit()

        snap = {"health_state": "warming_up", "stream_lag_s": 5.0}
        with mock.patch("labeler.preflight._get_health_snapshot", return_value=snap):
            result = preflight(conn=mem_conn)

        assert result["verdict"] == "WARN"
