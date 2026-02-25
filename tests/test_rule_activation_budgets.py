import json
from pathlib import Path
import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, get_conn, insert_event
from labeler.longitudinal import recheck_once
from labeler import timeutil


def _load_jsonl(path):
    lines = Path(path).read_text().strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def _reset_tables(conn):
    for t in ("claim_history", "events", "labels", "event_versions", "recheck_requests", "label_decisions"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    conn.commit()


def test_rule_activation_budgets_golden_fixtures(tmp_path):
    init_db()
    conn = get_conn()
    _reset_tables(conn)
    conn.close()

    for path in ("fixtures/golden_laundering_posts.jsonl", "fixtures/golden_assertiveness_posts.jsonl"):
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            ev = json.loads(line)
            ctime = timeutil.to_utc_datetime(ev.get("createdAt")) if ev.get("createdAt") else timeutil.now_utc()
            insert_event(ev["uri"], ctime, ev.get("authorDid"), ev)

    recheck_once()

    budgets = {
        "provenance_laundering": 1,
        "assertiveness_increase": 1,
    }

    conn = get_conn()
    rows = conn.execute(
        "SELECT rule_id, COUNT(*) FROM label_decisions GROUP BY rule_id"
    ).fetchall()
    conn.close()

    counts = {r[0]: r[1] for r in rows}
    for rule_id, max_allowed in budgets.items():
        assert counts.get(rule_id, 0) <= max_allowed, f"budget exceeded for {rule_id}: {counts.get(rule_id, 0)} > {max_allowed}"
