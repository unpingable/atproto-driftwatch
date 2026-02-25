import json
from labeler.db import init_db, get_conn, insert_label


def test_insert_label_writes_expired_at_and_prevents_duplicates(tmp_path):
    # init and ensure clean DB
    init_db()
    conn = get_conn()
    for t in ("labels", "events", "claim_history", "event_versions", "recheck_requests"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()

    label = {"label": "test_label", "score": 0.5, "reasons": [], "evidence": [], "time": "2026-01-30T00:00:00", "labeler": "did:test"}

    # First insert should succeed
    inserted = insert_label("subject:1", "did:test", label, ctime="2026-01-30T00:00:00")
    assert inserted is True

    # Inspect raw DB row and assert `expired_at` column exists and is NULL
    conn = get_conn()
    rows = conn.execute("SELECT subject_uri, labeler_did, label, ctime, expired_at FROM labels WHERE subject_uri = ? AND labeler_did = ?", ("subject:1", "did:test")).fetchall()
    conn.close()

    assert len(rows) == 1
    subj, did, label_json, ctime, expired = rows[0]
    label_obj = json.loads(label_json)
    assert subj == "subject:1"
    assert did == "did:test"
    assert label_obj["label"] == "test_label"
    assert expired is None

    # Duplicate insert should be a noop and return False
    inserted_again = insert_label("subject:1", "did:test", label, ctime="2026-01-30T00:00:00")
    assert inserted_again is False
