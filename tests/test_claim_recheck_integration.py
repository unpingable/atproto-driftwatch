import datetime
import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event, get_conn
from labeler.longitudinal import recheck_once
from labeler import timeutil


def test_claim_recheck_runs_when_enabled(monkeypatch):
    init_db()
    conn = get_conn()
    for t in ("claim_history", "events", "labels", "event_versions", "recheck_requests", "label_decisions", "claim_recheck_requests"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()

    now = datetime.datetime.now(datetime.timezone.utc)
    prior = {
        "uri": "uri:cr:1",
        "cid": "cr1",
        "text": "200 people were affected.",
        "createdAt": now.isoformat(),
        "authorDid": "did:alice",
        "externalLinks": [],
        "embeds": [],
        "facets": [],
    }
    later = {
        "uri": "uri:cr:2",
        "cid": "cr2",
        "text": "200 people were affected.",
        "createdAt": (now + datetime.timedelta(minutes=5)).isoformat(),
        "authorDid": "did:alice",
        "externalLinks": [],
        "embeds": [],
        "facets": [],
    }

    insert_event(prior["uri"], now, prior["authorDid"], prior)
    insert_event(later["uri"], now + datetime.timedelta(minutes=5), later["authorDid"], later)

    monkeypatch.setenv("ENABLE_CLAIM_RECHECK", "1")
    monkeypatch.setenv("CLAIM_RECHECK_MAX_PER_RUN", "10")

    recheck_once()

    conn = get_conn()
    rows = conn.execute(
        "SELECT decision_trace FROM label_decisions WHERE subject_uri = ?",
        ("uri:cr:2",),
    ).fetchall()
    conn.close()

    # ensure at least one decision includes scheduler=claim_group
    assert any("claim_group" in (r[0] or "") for r in rows)
