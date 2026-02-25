import os
import datetime
from typing import Dict, Tuple

from . import timeutil


def parse_rule_budgets() -> Dict[str, int]:
    raw = os.getenv("LABELER_RULE_BUDGETS", "").strip()
    if not raw:
        return {}
    budgets: Dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        rule_id, val = part.split(":", 1)
        rule_id = rule_id.strip()
        try:
            budgets[rule_id] = int(val.strip())
        except Exception:
            continue
    return budgets


def budget_window_hours() -> int:
    try:
        return int(os.getenv("LABELER_RULE_BUDGET_WINDOW_HOURS", "24"))
    except Exception:
        return 24


def budget_exceeded_in_run(run_counts: Dict[str, int], budgets: Dict[str, int]) -> Tuple[bool, str]:
    for rule_id, count in run_counts.items():
        limit = budgets.get(rule_id)
        if limit is None:
            continue
        if count > limit:
            return True, f"run_budget_exceeded:{rule_id}:{count}>{limit}"
    return False, ""


def budget_exceeded_in_window(conn, budgets: Dict[str, int]) -> Tuple[bool, str]:
    if not budgets:
        return False, ""
    window_hours = budget_window_hours()
    cutoff = (timeutil.now_utc() - datetime.timedelta(hours=window_hours)).isoformat()
    rows = conn.execute(
        "SELECT rule_id, COUNT(*) FROM label_decisions WHERE created_at >= ? GROUP BY rule_id",
        (cutoff,),
    ).fetchall()
    counts = {r[0]: r[1] for r in rows}
    for rule_id, limit in budgets.items():
        if counts.get(rule_id, 0) > limit:
            return True, f"window_budget_exceeded:{rule_id}:{counts.get(rule_id, 0)}>{limit}"
    return False, ""
