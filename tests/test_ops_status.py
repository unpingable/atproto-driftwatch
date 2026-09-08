import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from labeler import ops_status


NOW = datetime(2026, 8, 25, 16, 0, tzinfo=timezone.utc)


def _db(tmp_path, cursor_at=None):
    path = tmp_path / "labeler.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cursors (consumer TEXT PRIMARY KEY, cursor TEXT, updated_at TEXT);
        CREATE TABLE IF NOT EXISTS recheck_queue (claim_fingerprint TEXT PRIMARY KEY, scheduled_at TEXT);
        CREATE TABLE IF NOT EXISTS events (event_uri TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS claim_history (id INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS label_decisions (id INTEGER PRIMARY KEY);
        """
    )
    if cursor_at:
        conn.execute("INSERT OR REPLACE INTO cursors VALUES ('driftwatch_consumer', '1000', ?)", (cursor_at,))
    conn.commit()
    conn.close()
    return path


def _fact(tmp_path, producer, **values):
    payload = {
        "schema": "driftwatch.ops.producer_fact/v1",
        "producer": producer,
        "observed_at": values.pop("observed_at", NOW.isoformat()),
        **values,
    }
    (tmp_path / f"ops-{producer}.json").write_text(json.dumps(payload), encoding="utf-8")


def _consumer(tmp_path, **overrides):
    values = {
        "health_state": "ok",
        "connected": True,
        "session_id": "new-session",
        "session_windows_seen": 5,
        "warmup_windows_required": 5,
        "recalibration_remaining": 0,
        "drop_frac": 0,
        "events_dropped_total": 0,
        "rollback_lost_total": 0,
        "parse_failures": 0,
        "last_cursor": "1000",
        "last_event_at": NOW.timestamp(),
    }
    values.update(overrides)
    _fact(tmp_path, "consumer", **values)


def _evaluation(tmp_path, **overrides):
    values = {
        "status": "completed",
        "disposition": "no_drift_observed",
        "input_adequate": True,
        "processed_fingerprints": 2,
        "candidates_loaded": 4,
        "matched_labels": 0,
        "failed_fingerprints": 0,
        "observation_basis": {"health_state": "ok"},
    }
    values.update(overrides)
    _fact(tmp_path, "evaluation", **values)


def _items(status):
    return {item["id"]: item["observation"] for item in status["concerns"]}


def test_clean_state_makes_required_absence_visible(tmp_path):
    items = _items(ops_status.build_status(tmp_path / "missing.sqlite", data_dir=tmp_path, now=NOW))
    assert items["driftwatch.observation.stream_coverage"]["local_state"] == "ABSENT"
    assert items["driftwatch.evaluation.freshness"]["observation_present"] is False
    assert items["driftwatch.drift.bounded_current_state"]["domain_state"] == "NOT_ESTABLISHED"


def test_restart_does_not_manufacture_coverage(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path, session_windows_seen=0, health_state="ok")
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.observation.stream_coverage"]
    assert observed["local_state"] == "UNKNOWN"


def test_live_process_with_disconnected_source_is_not_adequate(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path, connected=False)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.observation.stream_coverage"]
    assert observed["local_state"] == "DEGRADED"


def test_connected_but_dropping_is_degraded(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path, events_dropped_total=7, drop_frac=0.03)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.observation.stream_coverage"]
    assert observed["local_state"] == "DEGRADED"
    assert observed["facts"]["events_dropped_total"] == 7


def test_unresolved_batch_or_admission_is_degraded_without_claiming_loss(tmp_path):
    path = _db(tmp_path, cursor_at=NOW.isoformat())
    for state in ({"batch_retry_pending": True}, {"admission_pending": True}):
        _consumer(tmp_path, **state)
        observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.observation.stream_coverage"]
        assert observed["local_state"] == "DEGRADED"
        assert observed["facts"]["events_dropped_total"] == 0


def test_connected_but_parse_or_rollback_loss_is_degraded(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path, parse_failures=1, rollback_lost_total=2)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))[
        "driftwatch.observation.stream_coverage"
    ]
    assert observed["local_state"] == "DEGRADED"
    assert observed["facts"]["parse_failures"] == 1
    assert observed["facts"]["rollback_lost_total"] == 2


def test_live_cursor_beyond_stale_durable_cursor_is_degraded(tmp_path):
    old = (NOW - timedelta(hours=1)).isoformat()
    path = _db(tmp_path, old)
    _consumer(tmp_path, last_cursor="2000")
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.observation.cursor_continuity"]
    assert observed["local_state"] == "DEGRADED"


def test_unknown_comparison_never_becomes_no_drift(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path)
    _evaluation(tmp_path, disposition="unknown", input_adequate=False)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.drift.bounded_current_state"]
    assert observed["local_state"] == "UNKNOWN"
    assert observed["domain_state"] == "UNKNOWN"


def test_stale_comparison_never_becomes_current_no_drift(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path)
    _evaluation(tmp_path, observed_at=(NOW - timedelta(hours=1)).isoformat())
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.drift.bounded_current_state"]
    assert observed["local_state"] == "STALE"
    assert observed["domain_state"] == "STALE"


def test_missing_comparison_side_is_not_established_not_clean(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path)
    _evaluation(tmp_path, disposition="not_established", processed_fingerprints=0, candidates_loaded=0)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.drift.bounded_current_state"]
    assert observed["local_state"] == "ABSENT"
    assert observed["domain_state"] == "NOT_ESTABLISHED"


def test_failed_evaluator_never_becomes_no_drift(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path)
    _evaluation(tmp_path, status="failed", disposition="failed", failed_fingerprints=1)
    items = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))
    assert items["driftwatch.evaluation.freshness"]["local_state"] == "DEGRADED"
    assert items["driftwatch.drift.bounded_current_state"]["domain_state"] == "FAILED"


def test_adequate_bounded_zero_is_explicitly_limited_no_drift(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    _consumer(tmp_path)
    _evaluation(tmp_path)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.drift.bounded_current_state"]
    assert observed["local_state"] == "PRESENT"
    assert observed["domain_state"] == "NO_DRIFT_OBSERVED"
    assert "bounded evaluated candidate set" in observed["reason"]


def test_hotset_depth_is_not_claimed_as_backlog_convergence(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    conn = sqlite3.connect(path)
    conn.execute("INSERT INTO recheck_queue VALUES ('fp', ?)", (NOW.isoformat(),))
    conn.commit()
    conn.close()
    _consumer(tmp_path)
    _evaluation(tmp_path)
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.evaluation.execution"]
    assert observed["facts"]["rolling_hotset_depth"] == 1
    assert observed["facts"]["depth_is_backlog_claim"] is False


def test_resource_concerns_remain_separate(tmp_path, monkeypatch):
    path = _db(tmp_path, NOW.isoformat())
    monkeypatch.setattr(
        ops_status.shutil,
        "disk_usage",
        lambda _path: type("Usage", (), {"total": 100, "used": 90, "free": 10})(),
    )
    items = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))
    assert items["driftwatch.persistence.sqlite_continuity"]["local_state"] == "PRESENT"
    assert items["driftwatch.persistence.volume_capacity"]["local_state"] == "DEGRADED"
    assert items["driftwatch.persistence.sqlite_slack"]["local_state"] == "DEGRADED"


def test_sqlite_continuity_v2_is_bounded_and_does_not_claim_integrity(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))[
        "driftwatch.persistence.sqlite_continuity"
    ]
    facts = observed["facts"]
    assert observed["local_state"] == "PRESENT"
    assert facts["integrity_check_performed"] is False
    assert facts["integrity_evidence_scope"] == "separate_backup_restore_receipt"
    assert facts["read_probe_succeeded"] is True
    assert facts["write_transaction_acquired"] is True
    assert facts["required_tables_missing"] == []
    assert facts["required_tables_present"] == sorted(ops_status.SQLITE_CONTINUITY_REQUIRED_TABLES)
    assert isinstance(facts["schema_version"], int)
    assert isinstance(facts["user_version"], int)
    assert isinstance(facts["page_size"], int)
    assert isinstance(facts["page_count"], int)
    assert isinstance(facts["freelist_count"], int)
    assert "quick_check" not in facts


def test_sqlite_continuity_v2_executes_no_integrity_pragma(tmp_path, monkeypatch):
    path = _db(tmp_path, NOW.isoformat())
    statements = []
    connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(ops_status.sqlite3, "connect", traced_connect)
    ops_status.build_status(path, data_dir=tmp_path, now=NOW)
    normalized = "\n".join(statements).lower()
    assert "quick_check" not in normalized
    assert "integrity_check" not in normalized


def test_sqlite_continuity_v2_missing_structure_is_degraded_not_healthy(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE label_decisions")
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))[
        "driftwatch.persistence.sqlite_continuity"
    ]
    assert observed["local_state"] == "DEGRADED"
    assert observed["facts"]["required_tables_missing"] == ["label_decisions"]
    assert observed["facts"]["integrity_check_performed"] is False


def test_sqlite_continuity_declaration_is_v2(tmp_path):
    manifest = ops_status.load_manifest()
    concern = next(
        item for item in manifest["concerns"]
        if item["id"] == "driftwatch.persistence.sqlite_continuity"
    )
    assert concern["question"] == "driftwatch.question.sqlite_continuity/v2"


def test_old_facts_snapshot_is_stale_observational_output(tmp_path):
    path = _db(tmp_path, NOW.isoformat())
    facts = tmp_path / "facts.sqlite"
    facts.write_bytes(b"fixture")
    old = (NOW - timedelta(hours=3)).timestamp()
    os.utime(facts, (old, old))
    observed = _items(ops_status.build_status(path, data_dir=tmp_path, now=NOW))["driftwatch.output.facts_snapshot_freshness"]
    assert observed["local_state"] == "STALE"
    assert observed["facts"]["artifact_authority"] == "observational"
