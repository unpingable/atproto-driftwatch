import json
from pathlib import Path
from labeler.db import init_db, insert_event
from labeler.longitudinal import recheck_once


def _load_jsonl(path):
    lines = Path(path).read_text().strip().splitlines()
    return [json.loads(l) for l in lines if l.strip()]


def test_golden_assertiveness(tmp_path):
    init_db()
    # ensure clean DB for deterministic golden runs
    from labeler.db import get_conn as _get_conn
    _c = _get_conn()
    for t in ("claim_history","events","labels","event_versions","recheck_requests"):
        try:
            _c.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    _c.commit()
    _c.close()

    in_path = Path("fixtures/golden_assertiveness_posts.jsonl")
    for line in in_path.read_text().splitlines():
        if not line.strip():
            continue
        ev = json.loads(line)
        from labeler import timeutil
        ctime = timeutil.to_utc_datetime(ev.get("createdAt")) if ev.get("createdAt") else timeutil.now_utc()
        insert_event(ev["uri"], ctime, ev.get("authorDid"), ev)

    recheck_once()

    # gather labels for subject
    from labeler.db import get_labels_for_subject
    labels = get_labels_for_subject("uri:gold:ai:2", include_expired=True)
    out = [{"subject_uri": l["label"]["subject_uri"] if "subject_uri" in l["label"] else l["label"]["label"],
            } for l in labels]
    # Instead, compare serialized label payloads to golden by searching for the expected label
    got = labels
    expected = _load_jsonl(Path("tests/golden/expected_assertiveness.jsonl"))

    # ensure expected label is present
    assert any(e["label"] == got_l["label"]["label"] for e in expected for got_l in got)
    # crude equality check for subject+label presence
    for e in expected:
        assert any((got_l["label"]["label"] == e["label"] and got_l["label"]["reasons"] == e["reasons"]) for got_l in got)
