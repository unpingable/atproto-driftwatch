import json
from pathlib import Path
from collections import Counter, defaultdict
import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, get_conn, insert_event
from labeler.longitudinal import recheck_once
from labeler import timeutil
from labeler.claims import fingerprint_text, FP_VERSION, fingerprint_config_hash


def _load_jsonl(path):
    lines = Path(path).read_text().strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def _reset_tables():
    conn = get_conn()
    for t in ("claim_history", "events", "labels", "event_versions", "recheck_requests", "label_decisions"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()


def test_fingerprint_regression_suite_extended_fixture():
    items = _load_jsonl("fixtures/fingerprint_extended.jsonl")
    fps_by_group = defaultdict(list)
    for it in items:
        g = it.get("group")
        fps_by_group[g].append(fingerprint_text(it["text"]))

    totals = 0
    majority_sum = 0
    for fps in fps_by_group.values():
        totals += len(fps)
        majority_sum += Counter(fps).most_common(1)[0][1]
    assert totals > 0
    assert majority_sum / totals >= 0.5

    # spot-check: affected vs evacuated should remain distinct
    assert fingerprint_text("200 people were affected.") != fingerprint_text("200 people were evacuated.")


def test_label_decision_regression_golden_fixtures():
    init_db()
    _reset_tables()

    for path in ("fixtures/golden_laundering_posts.jsonl", "fixtures/golden_assertiveness_posts.jsonl"):
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            ctime = timeutil.to_utc_datetime(ev.get("createdAt")) if ev.get("createdAt") else timeutil.now_utc()
            insert_event(ev["uri"], ctime, ev.get("authorDid"), ev)

    recheck_once()

    conn = get_conn()
    rows = conn.execute(
        "SELECT rule_id, fingerprint_version, inputs_json, evidence_hashes_json, config_hash, status FROM label_decisions"
    ).fetchall()
    conn.close()

    assert rows, "expected label_decisions rows from golden fixtures"
    allowed_rules = {
        "provenance_laundering",
        "assertiveness_increase",
        "repeat_claim_no_new_evidence",
        "quote_mismatch",
        "time_inconsistency",
        "external_labeler",
        "unknown",
    }
    expected_cfg_hash = fingerprint_config_hash()
    for r in rows:
        rule_id, fp_ver, inputs_json, evidence_hashes_json, config_hash, status = r
        assert rule_id in allowed_rules
        assert fp_ver == FP_VERSION
        assert inputs_json is not None
        assert evidence_hashes_json is not None
        assert config_hash == expected_cfg_hash
        assert status in ("committed", "expired", "proposed")

        if rule_id in ("provenance_laundering", "assertiveness_increase"):
            try:
                hashes = json.loads(evidence_hashes_json or "[]")
            except Exception:
                hashes = []
            assert hashes, f"expected evidence hashes for rule {rule_id}"
