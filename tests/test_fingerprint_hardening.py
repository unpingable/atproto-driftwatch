import json
from pathlib import Path
from labeler.claims import fingerprint_text, FP_VERSION


def _load_fixture():
    p = Path("fixtures/fingerprint_adversarial.jsonl")
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def test_adversarial_variants_collapse_by_default():
    # Default config aims to collapse variants for the same claim
    items = _load_fixture()
    fps = [fingerprint_text(i["text"]) for i in items]
    # expect most variants to collide (we check that >50% collide with majority fingerprint)
    from collections import Counter
    c = Counter(fps)
    most_common_count = c.most_common(1)[0][1]
    assert most_common_count >= 5, f"expected at least 5 of 10 to collide, got {most_common_count}"


def test_near_miss_separated():
    # make a near-miss that should remain distinct (e.g., different quantity)
    a = "200 people were affected"
    b = "20 people were affected"  # different magnitude -> should be different under bucket mode
    fa = fingerprint_text(a)
    fb = fingerprint_text(b)
    assert fa != fb


def test_fingerprint_version_is_contract():
    assert FP_VERSION == "v1"


def test_known_pairs_fixture_contract():
    p = Path("fixtures/fingerprint_known_pairs.jsonl")
    items = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    for i in items:
        fa = fingerprint_text(i["a"])
        fb = fingerprint_text(i["b"])
        if i["expect"] == "same":
            assert fa == fb, f"expected same for: {i['a']} vs {i['b']}"
        else:
            assert fa != fb, f"expected different for: {i['a']} vs {i['b']}"


def test_fingerprint_version_stored_when_adding_history(tmp_path, monkeypatch):
    # ensure FP_VERSION is used in claim_history rows
    from labeler.db import init_db, get_conn
    from labeler.claims import add_claim_history
    init_db()
    conn = get_conn()
    for t in ("claim_history", "events", "labels", "recheck_requests"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    conn.commit()

    add_claim_history("did:test", "200 people were affected", "2026-01-30T00:00:00", "uri:test:1", "cid1", None, None, "ehash")
    rows = conn.execute("SELECT fingerprint_version FROM claim_history WHERE authorDid = ?", ("did:test",)).fetchall()
    conn.close()
    assert rows and rows[0][0] == FP_VERSION
