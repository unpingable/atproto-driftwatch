from labeler.drift.extract import extract_claim_signals
from labeler.drift.rules import rule_provenance_laundering, rule_quote_mismatch, rule_repeat_claim_no_new_evidence
from labeler.drift.models import Post


def make_post(data):
    return Post(**data)


def test_extract_claims():
    s = "According to source X, 2024-10-01 there were 100 cases."
    cs = extract_claim_signals(s)
    assert cs.dates == ["2024-10-01"]
    assert any("source" in m.lower() or "according" in m.lower() for m in cs.modal)


def test_provenance_laundering():
    p1 = make_post({"uri": "p1", "cid": "c1", "text": "According to source X, it happened", "createdAt": "2024-10-01T10:00:00", "authorDid": "did:a"})
    p2 = make_post({"uri": "p2", "cid": "c2", "text": "It happened for sure.", "createdAt": "2024-10-01T11:00:00", "authorDid": "did:a"})
    labs = rule_provenance_laundering(p2, [p1, p2])
    assert labs and any(l.label == "provenance_laundering_possible" for l in labs)


def test_quote_mismatch():
    p = make_post({"uri": "p4", "cid": "c4", "text": '"Study shows 1000"', "createdAt": "2024-10-01T12:00:00", "authorDid": "did:b", "externalLinks": [], "facets": []})
    labs = rule_quote_mismatch(p, [p])
    assert labs and labs[0].label == "quote_mismatch"


def test_repeat_claim_no_new_evidence():
    p1 = make_post({"uri": "p1", "cid": "c1", "text": "Event H occurred", "createdAt": "2024-10-01T10:00:00", "authorDid": "did:a", "externalLinks": [], "embeds": []})
    p2 = make_post({"uri": "p2", "cid": "c2", "text": "Event H occurred", "createdAt": "2024-10-01T11:00:00", "authorDid": "did:a", "externalLinks": [], "embeds": []})
    labs = rule_repeat_claim_no_new_evidence(p2, [p1, p2])
    assert labs and any(l.label == "repeat_claim_no_new_evidence" for l in labs)
