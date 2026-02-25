import pytest

pytest.importorskip("duckdb")

from labeler.db import init_db, enqueue_claim_recheck, dequeue_claim_rechecks


def test_claim_recheck_queue_roundtrip():
    init_db()
    enqueue_claim_recheck("did:alice", "fp1")
    enqueue_claim_recheck("did:alice", "fp1")
    enqueue_claim_recheck("did:bob", "fp2")
    items = dequeue_claim_rechecks(limit=10)
    assert ("did:alice", "fp1") in items
    assert ("did:bob", "fp2") in items
