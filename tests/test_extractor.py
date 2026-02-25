from labeler.extractor import extract_edges_from_event


def test_reply_edge():
    ev = {
        "time": "2024-01-01T00:00:00Z",
        "author": "did:example:alice",
        "record": {
            "reply": {
                "parent": {"author": "did:example:bob"}
            }
        }
    }

    edges = extract_edges_from_event(ev)
    assert len(edges) == 1
    assert edges[0][0] == "did:example:alice"
    assert edges[0][1] == "did:example:bob"
    assert edges[0][2] == "reply"


def test_no_edges():
    ev = {"time": "2024-01-01T00:00:00Z", "author": "did:example:alice", "record": {}}
    edges = extract_edges_from_event(ev)
    assert edges == []
