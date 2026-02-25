import datetime
import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event, get_conn
from labeler.longitudinal import recheck_once
from labeler.claims import FP_VERSION


def test_label_decision_ledger_minimal(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)

    prior = {
        "uri": "uri:ld:1",
        "cid": "ld1",
        "text": "According to source X, 100 people were evacuated.",
        "createdAt": now.isoformat(),
        "authorDid": "did:alice",
    }
    later = {
        "uri": "uri:ld:2",
        "cid": "ld2",
        "text": "100 people were evacuated.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:alice",
    }

    insert_event(prior["uri"], now, prior["authorDid"], prior)
    insert_event(later["uri"], now + datetime.timedelta(minutes=10), later["authorDid"], later)

    recheck_once()

    conn = get_conn()
    rows = conn.execute(
        "SELECT rule_id, fingerprint_version, inputs_json, evidence_hashes_json FROM label_decisions WHERE subject_uri = ?",
        ("uri:ld:2",),
    ).fetchall()
    conn.close()

    assert rows, "expected at least one label decision"
    rule_id, fp_ver, inputs_json, evidence_hashes_json = rows[0]
    assert rule_id == "provenance_laundering"
    assert fp_ver == FP_VERSION
    assert inputs_json and ("spans" in inputs_json or "quantities" in inputs_json)
    assert evidence_hashes_json is not None
