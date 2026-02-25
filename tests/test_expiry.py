import datetime
import pytest
# Skip if duckdb isn't available in this environment
pytest.importorskip("duckdb")

from labeler.db import init_db, insert_label, get_labels_for_subject
from labeler.expiry import expire_labels_by_ttl


def test_expire_labels_by_ttl(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)
    subj = "uri:exp:1"
    # insert an old label
    old_label = {"label": "old", "score": 0.5}
    insert_label(subj, "did:lab", old_label, ctime=(now - datetime.timedelta(days=60)).isoformat())
    # insert a recent label
    new_label = {"label": "new", "score": 0.6}
    insert_label(subj, "did:lab", new_label, ctime=now.isoformat())

    before = get_labels_for_subject(subj, include_expired=True)
    assert len(before) >= 2

    expire_labels_by_ttl(30)

    after_active = get_labels_for_subject(subj, include_expired=False)
    after_all = get_labels_for_subject(subj, include_expired=True)

    assert any(l["label"]["label"] == "new" for l in after_active)
    assert any(l["label"]["label"] == "old" for l in after_all if l["expired_at"] is not None)


def test_timezone_aware_ttl_and_ordering(tmp_path):
    init_db()
    now = datetime.datetime.now(datetime.timezone.utc)
    subj = "uri:exp:tz"

    old_local = (now - datetime.timedelta(days=31)).astimezone(datetime.timezone(datetime.timedelta(hours=-5)))
    new_local = (now - datetime.timedelta(days=1)).astimezone(datetime.timezone(datetime.timedelta(hours=2)))

    insert_label(subj, "did:lab", {"label": "old"}, ctime=old_local.isoformat())
    insert_label(subj, "did:lab", {"label": "new"}, ctime=new_local.isoformat())

    expire_labels_by_ttl(30)

    active = get_labels_for_subject(subj, include_expired=False)
    all_labels = get_labels_for_subject(subj, include_expired=True)

    assert any(l["label"]["label"] == "new" for l in active)
    assert any(l["label"]["label"] == "old" and l["expired_at"] is not None for l in all_labels)
    assert all_labels[0]["label"]["label"] == "new"
