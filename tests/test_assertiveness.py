import datetime
import pytest
pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event, get_labels_for_subject
from labeler.longitudinal import recheck_once


def test_assertiveness_increase_triggers_label(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)

    prior = {
        "uri": "uri:ai:1",
        "cid": "c1",
        "text": "I think there might be about 100 cases, possibly.",
        "createdAt": now.isoformat(),
        "authorDid": "did:alice",
    }

    later = {
        "uri": "uri:ai:2",
        "cid": "c2",
        "text": "Confirmed: 100 cases occurred.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:alice",
    }

    inserted, updated = insert_event(prior["uri"], now, prior["authorDid"], prior)
    assert inserted is True
    inserted2, updated2 = insert_event(later["uri"], now + datetime.timedelta(minutes=10), later["authorDid"], later)
    assert inserted2 is True

    # run recheck to process
    processed = recheck_once()
    assert processed >= 1

    labels = get_labels_for_subject("uri:ai:2")
    assert any(l["label"]["label"] == "assertiveness_increase_possible" for l in labels)


def test_assertiveness_does_not_trigger_with_new_evidence(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)

    prior = {
        "uri": "uri:ai:3",
        "cid": "c3",
        "text": "I think there might be about 50 cases, possibly.",
        "createdAt": now.isoformat(),
        "authorDid": "did:bob",
    }

    later = {
        "uri": "uri:ai:4",
        "cid": "c4",
        "text": "Confirmed: 50 cases occurred. See link.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:bob",
        "externalLinks": ["https://example.com/report"]
    }

    inserted, updated = insert_event(prior["uri"], now, prior["authorDid"], prior)
    assert inserted is True
    inserted2, updated2 = insert_event(later["uri"], now + datetime.timedelta(minutes=10), later["authorDid"], later)
    assert inserted2 is True

    processed = recheck_once()
    assert processed >= 1

    labels = get_labels_for_subject("uri:ai:4")
    assert not any(l["label"]["label"] == "assertiveness_increase_possible" for l in labels)


def test_assertiveness_threshold_monkeypatch(tmp_path, monkeypatch):
    # Ensure the threshold is configurable and respected
    monkeypatch.setenv("ASSERTIVENESS_DELTA", "0.25")
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)

    prior = {
        "uri": "uri:ai:5",
        "cid": "c5",
        "text": "I think maybe around 20 cases.",
        "createdAt": now.isoformat(),
        "authorDid": "did:carol",
    }

    later = {
        "uri": "uri:ai:6",
        "cid": "c6",
        "text": "Confirmed: 20 cases occurred.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:carol",
    }

    inserted, updated = insert_event(prior["uri"], now, prior["authorDid"], prior)
    assert inserted is True
    inserted2, updated2 = insert_event(later["uri"], now + datetime.timedelta(minutes=10), later["authorDid"], later)
    assert inserted2 is True

    processed = recheck_once()
    assert processed >= 1

    labels = get_labels_for_subject("uri:ai:6")
    # Delta is ~0.2 which is below 0.25 so should not trigger
    assert not any(l["label"]["label"] == "assertiveness_increase_possible" for l in labels)
