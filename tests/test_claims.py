import pytest
pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event
from labeler.claims import fingerprint_text, get_claim_history
import datetime


def test_claim_fingerprint_and_history(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)

    p1 = {
        "uri": "uri:cl:1",
        "cid": "c1",
        "text": "According to source X, 100 people were evacuated.",
        "createdAt": now.isoformat(),
        "authorDid": "did:alice",
    }
    p2 = {
        "uri": "uri:cl:2",
        "cid": "c2",
        "text": "100 people were evacuated, according to Source X.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:alice",
    }

    inserted, updated = insert_event(p1["uri"], now, p1["authorDid"], p1)
    assert inserted is True
    inserted2, updated2 = insert_event(p2["uri"], now + datetime.timedelta(minutes=10), p2["authorDid"], p2)
    assert inserted2 is True

    # their fingerprints should match
    fp1 = fingerprint_text(p1["text"])
    history = get_claim_history("did:alice", fp1)
    assert len(history) >= 2
    assert history[0]["post_uri"] == "uri:cl:1"
    assert history[1]["post_uri"] == "uri:cl:2"
