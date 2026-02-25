import datetime
import pytest

# Skip this test on systems that don't have duckdb available
pytest.importorskip("duckdb")

from labeler.db import init_db, insert_event, get_labels_for_subject
from labeler.longitudinal import recheck_once


def test_edit_triggers_recheck_and_labeling(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)
    pytest.importorskip("prometheus_client")
    from labeler import metrics as metrics_module
    before_inserted = metrics_module.RECHECK_LABELS_INSERTED._value.get()
    before_expired = metrics_module.RECHECK_LABELS_EXPIRED._value.get()

    # prior post with explicit attribution
    prior = {
        "uri": "uri:lt:1",
        "cid": "c1",
        "text": "According to source X, 100 people were evacuated.",
        "createdAt": now.isoformat(),
        "authorDid": "did:alice",
    }

    # later post that copies attribution (no laundering initially)
    later = {
        "uri": "uri:lt:2",
        "cid": "c2",
        "text": "According to source X, 100 people were evacuated.",
        "createdAt": (now + datetime.timedelta(minutes=10)).isoformat(),
        "authorDid": "did:alice",
    }

    # insert prior and later
    inserted_prior = insert_event(prior["uri"], now, prior["authorDid"], prior)
    inserted_later = insert_event(later["uri"], now + datetime.timedelta(minutes=10), later["authorDid"], later)

    # initial recheck should process and not emit provenance_laundering (same attribution)
    processed = recheck_once()
    assert processed >= 1
    labels = get_labels_for_subject("uri:lt:2")
    assert all(l["label"]["label"] != "provenance_laundering_possible" for l in labels)

    # Now simulate edit: later post removes attribution
    later_edit = later.copy()
    later_edit["text"] = "100 people were evacuated."  # removed 'According to'
    # call insert_event with same URI but different raw -> should be an update and schedule recheck
    inserted, updated = insert_event(later_edit["uri"], now + datetime.timedelta(minutes=20), later_edit["authorDid"], later_edit)
    assert updated is True

    # process rechecks and expect provenance_laundering label now exists for the later post
    processed2 = recheck_once()
    assert processed2 >= 1
    labels_after = get_labels_for_subject("uri:lt:2")
    assert any(l["label"]["label"] == "provenance_laundering_possible" for l in labels_after)
    assert metrics_module.RECHECK_LABELS_INSERTED._value.get() - before_inserted >= 1

    # Now simulate restoring attribution in the later post (edit to add attribution back)
    later_restore = later_edit.copy()
    later_restore["text"] = "According to source X, 100 people were evacuated."  # restored attribution
    inserted, updated = insert_event(later_restore["uri"], now + datetime.timedelta(minutes=30), later_restore["authorDid"], later_restore)
    assert updated is True

    # process rechecks and expect the previously-created provenance label to be expired
    processed3 = recheck_once()
    assert processed3 >= 1
    labels_final = get_labels_for_subject("uri:lt:2", include_expired=True)
    assert any((l["label"]["label"] == "provenance_laundering_possible" and l["expired_at"] is not None) for l in labels_final)
    assert metrics_module.RECHECK_LABELS_EXPIRED._value.get() - before_expired >= 1
