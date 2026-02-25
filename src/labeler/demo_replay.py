"""Demo replay script: insert a handful of mock events into the shared DB.

Run as: python -m labeler.demo_replay
It writes several events that ingestion can pick up and label.
"""
import time
import uuid
from .db import init_db, insert_event

MOCK_URIS = [f"uri:demo:{i}" for i in range(1, 6)]


def make_event(uri):
    now = time.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {"uri": uri, "time": now, "author": "did:demo:alice", "record": {"text": "demo event"}}


if __name__ == "__main__":
    init_db()
    for u in MOCK_URIS:
        ev = make_event(u)
        insert_event(u, ev.get("time"), ev.get("author"), ev)
    print(f"Inserted {len(MOCK_URIS)} demo events into DB")
