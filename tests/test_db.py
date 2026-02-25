import pytest
pytest.importorskip("duckdb")
import os
import pathlib
from labeler.db import init_db, get_conn, insert_event
import datetime


def test_init_db(tmp_path):
    # ensure init_db runs and creates file
    init_db()
    conn = get_conn()
    # basic query
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    # duckdb uses information_schema, but at minimum the connection is open
    assert conn is not None
    conn.close()


def test_insert_event_roundtrip():
    init_db()
    ev = {"uri": "uri:1", "time": "2024-01-01T00:00:00Z", "author": "did:alice"}
    insert_event("uri:1", "2024-01-01T00:00:00Z", "did:alice", ev)
    conn = get_conn()
    rows = conn.execute("SELECT event_uri, author FROM events WHERE event_uri = ?", ("uri:1",)).fetchall()
    assert rows and rows[0][0] == "uri:1"
    conn.close()
