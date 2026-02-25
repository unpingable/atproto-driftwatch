import pytest
pytest.importorskip("duckdb")

from labeler.budgets import parse_rule_budgets, budget_exceeded_in_run
from labeler.db import init_db, get_conn


def test_parse_rule_budgets(monkeypatch):
    monkeypatch.setenv("LABELER_RULE_BUDGETS", "a:1,b:2, bad, c:3")
    budgets = parse_rule_budgets()
    assert budgets == {"a": 1, "b": 2, "c": 3}


def test_budget_exceeded_in_run():
    budgets = {"rule_x": 1}
    exceeded, reason = budget_exceeded_in_run({"rule_x": 2}, budgets)
    assert exceeded is True
    assert "rule_x" in reason


def test_budget_window_query(monkeypatch):
    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM label_decisions")
    conn.execute(
        "INSERT INTO label_decisions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "d1",
            "2026-01-30T00:00:00+00:00",
            "uri:1",
            "root:1",
            "label",
            "rule_a",
            "v1",
            "{}",
            "[]",
            "",
            "",
            "committed",
        ),
    )
    conn.commit()
    monkeypatch.setenv("LABELER_RULE_BUDGET_WINDOW_HOURS", "48")
    from labeler.budgets import budget_exceeded_in_window
    exceeded, _ = budget_exceeded_in_window(conn, {"rule_a": 0})
    conn.close()
    assert exceeded is True
