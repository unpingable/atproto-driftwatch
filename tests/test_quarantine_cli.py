import json
import pytest

pytest.importorskip("duckdb")


def test_quarantine_show_cli(capsys):
    from labeler.db import init_db, get_conn
    from labeler.cli import main

    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM quarantine_emits")
    conn.execute(
        "INSERT INTO quarantine_emits VALUES (?, ?, ?, ?, ?, ?)",
        (
            "emit-1",
            "2026-01-30T00:00:00+00:00",
            "quarantine",
            "suppressed",
            "test",
            json.dumps({"label": "x"}),
        ),
    )
    conn.commit()
    conn.close()

    import sys
    argv = sys.argv
    try:
        sys.argv = ["labeler.cli", "quarantine", "show", "emit-1"]
        main()
    finally:
        sys.argv = argv

    out = capsys.readouterr().out
    assert "emit-1" in out
