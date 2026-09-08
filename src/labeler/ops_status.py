"""Repository-declared, producer-local visibility for Driftwatch."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

from . import ops_runtime


REPO_ROOT = Path(__file__).resolve().parents[2]
REPO_MANIFEST_PATH = REPO_ROOT / ".ops" / "concerns.toml"
MANIFEST_PATH = (REPO_MANIFEST_PATH if REPO_MANIFEST_PATH.is_file()
                 else Path(__file__).resolve().parent / "_ops" / "concerns.toml")
STATUS_SCHEMA = "project.ops.status/v1"
CONSUMER_MAX_AGE_S = 120
CURSOR_MAX_AGE_S = 15 * 60
EVALUATION_MAX_AGE_S = 180
FACTS_MAX_AGE_S = 2 * 60 * 60
SLACK_PAGE_FLOOR = 5_000_000
SQLITE_CONTINUITY_REQUIRED_TABLES = (
    "claim_history",
    "cursors",
    "events",
    "label_decisions",
)


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _age(now: datetime, value: Any) -> float | None:
    parsed = _parse_ts(value)
    return max(0.0, (now - parsed).total_seconds()) if parsed else None


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    with path.open("rb") as handle:
        manifest = tomllib.load(handle)
    if manifest.get("schema") != "project.concerns/v1":
        raise ValueError("unsupported concern manifest schema")
    ids = [item.get("id") for item in manifest.get("concerns", [])]
    if not ids or any(not item for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("concern IDs must be present and unique")
    return manifest


def _obs(state: str, observed_at: str | None, validity: int | None, reason: str,
         facts: dict[str, Any] | None = None, domain_state: str | None = None) -> dict[str, Any]:
    return {
        "observation_present": state != "ABSENT",
        "local_state": state,
        "domain_state": domain_state,
        "observed_at": observed_at,
        "valid_for_seconds": validity,
        "reason": reason,
        "facts": facts or {},
    }


def _consumer(fact: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    if not fact:
        return _obs("ABSENT", None, CONSUMER_MAX_AGE_S, "the stream consumer has emitted no current-session fact")
    observed_at = fact.get("observed_at")
    age = _age(now, observed_at)
    if age is None:
        return _obs("UNKNOWN", observed_at, CONSUMER_MAX_AGE_S, "the consumer fact has an invalid timestamp", fact)
    if age > CONSUMER_MAX_AGE_S:
        return _obs("STALE", observed_at, CONSUMER_MAX_AGE_S, "the consumer fact is stale", fact)
    session_windows = int(fact.get("session_windows_seen") or 0)
    required_windows = int(fact.get("warmup_windows_required") or 5)
    if not fact.get("connected"):
        state = "UNKNOWN" if session_windows == 0 else "DEGRADED"
        return _obs(state, observed_at, CONSUMER_MAX_AGE_S, "the current session has not established a live Jetstream connection", fact)
    if session_windows < required_windows:
        return _obs("UNKNOWN", observed_at, CONSUMER_MAX_AGE_S, "the restarted consumer has not re-established a bounded coverage basis", fact)
    impaired = (
        fact.get("health_state") != "ok"
        or bool(fact.get("batch_retry_pending"))
        or bool(fact.get("admission_pending"))
        or float(fact.get("drop_frac") or 0) > 0
        or int(fact.get("events_dropped_total") or 0) > 0
        or int(fact.get("rollback_lost_total") or 0) > 0
        or int(fact.get("parse_failures") or 0) > 0
        or int(fact.get("recalibration_remaining") or 0) > 0
    )
    if impaired:
        return _obs("DEGRADED", observed_at, CONSUMER_MAX_AGE_S, "the live session reports loss, lag, parse failure, rollback loss, or incomplete recalibration", fact)
    return _obs("PRESENT", observed_at, CONSUMER_MAX_AGE_S, "the current session has a live bounded coverage basis without known loss", fact)


def _cursor(db_path: Path, consumer_fact: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    if not db_path.exists():
        return _obs("ABSENT", None, CURSOR_MAX_AGE_S, "the configured SQLite store does not exist", {"path": str(db_path)})
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        row = conn.execute("SELECT consumer, cursor, updated_at FROM cursors ORDER BY updated_at DESC LIMIT 1").fetchone()
        conn.close()
    except sqlite3.Error as exc:
        return _obs("UNKNOWN", None, CURSOR_MAX_AGE_S, "the durable cursor is unreadable", {"error": str(exc)})
    if not row:
        return _obs("ABSENT", None, CURSOR_MAX_AGE_S, "no durable consumer cursor has been produced")
    facts = {"consumer": row[0], "durable_cursor": row[1], "durable_updated_at": row[2]}
    if consumer_fact:
        facts["session_cursor"] = consumer_fact.get("last_cursor")
        facts["session_last_event_at"] = consumer_fact.get("last_event_at")
    age = _age(now, row[2])
    if age is None:
        return _obs("UNKNOWN", row[2], CURSOR_MAX_AGE_S, "the durable cursor timestamp is invalid", facts)
    if age > CURSOR_MAX_AGE_S:
        consumer_age = _age(now, consumer_fact.get("observed_at")) if consumer_fact else None
        if consumer_fact and consumer_fact.get("last_cursor") and consumer_fact.get("last_cursor") != row[1] and consumer_age is not None and consumer_age <= CONSUMER_MAX_AGE_S:
            return _obs("DEGRADED", row[2], CURSOR_MAX_AGE_S, "the live session has advanced beyond a stale durable restart cursor", facts)
        return _obs("STALE", row[2], CURSOR_MAX_AGE_S, "the durable restart cursor is stale; process existence does not renew it", facts)
    return _obs("PRESENT", row[2], CURSOR_MAX_AGE_S, "a recent durable restart cursor is present", facts)


def _evaluation(fact: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    if not fact:
        return _obs("ABSENT", None, EVALUATION_MAX_AGE_S, "the evaluator has emitted no bounded run fact")
    observed_at = fact.get("observed_at")
    age = _age(now, observed_at)
    if age is None:
        return _obs("UNKNOWN", observed_at, EVALUATION_MAX_AGE_S, "the evaluator fact has an invalid timestamp", fact)
    if age > EVALUATION_MAX_AGE_S:
        return _obs("STALE", observed_at, EVALUATION_MAX_AGE_S, "the last evaluator run is stale", fact)
    if fact.get("status") == "failed":
        return _obs("DEGRADED", observed_at, EVALUATION_MAX_AGE_S, "the last evaluator run failed", fact, "FAILED")
    if not fact.get("input_adequate"):
        return _obs("UNKNOWN", observed_at, EVALUATION_MAX_AGE_S, "the evaluator completed without an adequate current observation basis", fact, "UNKNOWN")
    return _obs("PRESENT", observed_at, EVALUATION_MAX_AGE_S, "a recent bounded evaluator pass completed under adequate inputs", fact)


def _drift_state(fact: dict[str, Any] | None, now: datetime) -> dict[str, Any]:
    evaluation = _evaluation(fact, now)
    if evaluation["local_state"] == "STALE":
        return _obs("STALE", evaluation["observed_at"], EVALUATION_MAX_AGE_S, "a historical drift result cannot represent current state", evaluation["facts"], "STALE")
    if not fact:
        return _obs("ABSENT", None, EVALUATION_MAX_AGE_S, "no drift evaluation has been established", domain_state="NOT_ESTABLISHED")
    disposition = str(fact.get("disposition") or "unknown")
    domain = {
        "drift_observed": "DRIFT_OBSERVED",
        "no_drift_observed": "NO_DRIFT_OBSERVED",
        "unknown": "UNKNOWN",
        "failed": "FAILED",
        "not_established": "NOT_ESTABLISHED",
    }.get(disposition, "UNKNOWN")
    if evaluation["local_state"] == "DEGRADED" or domain == "FAILED":
        return _obs("DEGRADED", fact.get("observed_at"), EVALUATION_MAX_AGE_S, "the bounded drift evaluation failed", fact, domain)
    if evaluation["local_state"] == "UNKNOWN" or domain == "UNKNOWN":
        return _obs("UNKNOWN", fact.get("observed_at"), EVALUATION_MAX_AGE_S, "drift status is unknown because the required observation basis was inadequate", fact, "UNKNOWN")
    if domain == "NOT_ESTABLISHED":
        return _obs("ABSENT", fact.get("observed_at"), EVALUATION_MAX_AGE_S, "no candidate comparison was established in the bounded run", fact, domain)
    reason = "drift was observed in the bounded evaluated candidate set" if domain == "DRIFT_OBSERVED" else "no drift was observed in the bounded evaluated candidate set under adequate inputs"
    return _obs("PRESENT", fact.get("observed_at"), EVALUATION_MAX_AGE_S, reason, fact, domain)


def _execution(fact: dict[str, Any] | None, now: datetime, db_path: Path) -> dict[str, Any]:
    evaluation = _evaluation(fact, now)
    facts = dict(evaluation["facts"])
    if db_path.exists():
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            row = conn.execute("SELECT COUNT(*), MIN(scheduled_at) FROM recheck_queue").fetchone()
            conn.close()
            facts.update({"rolling_hotset_depth": int(row[0]), "oldest_scheduled_at": row[1], "depth_is_backlog_claim": False})
        except sqlite3.Error as exc:
            facts["queue_observation_error"] = str(exc)
    if evaluation["local_state"] in {"ABSENT", "STALE", "UNKNOWN", "DEGRADED"}:
        return _obs(evaluation["local_state"], evaluation["observed_at"], EVALUATION_MAX_AGE_S, evaluation["reason"], facts)
    if int(fact.get("failed_fingerprints") or 0):
        return _obs("DEGRADED", fact.get("observed_at"), EVALUATION_MAX_AGE_S, "the bounded evaluation recorded candidate failures", facts)
    return _obs("PRESENT", fact.get("observed_at"), EVALUATION_MAX_AGE_S, "the evaluator completed; rolling-hotset depth is reported without a convergence inference", facts)


def _sqlite(db_path: Path, now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    if not db_path.exists():
        absent = _obs("ABSENT", None, None, "the configured SQLite file does not exist", {"path": str(db_path)})
        return absent, absent
    facts: dict[str, Any] = {
        "path": str(db_path),
        # Full integrity checks scale with the entire database and belong to a
        # quiesced backup/restore rehearsal, not a status endpoint with a
        # bounded consumer timeout.
        "integrity_check_performed": False,
        "integrity_evidence_scope": "separate_backup_restore_receipt",
    }
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=rw", uri=True, timeout=1)
        read_probe = conn.execute("SELECT 1").fetchone()[0] == 1
        schema_version = int(conn.execute("PRAGMA schema_version").fetchone()[0])
        user_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        placeholders = ",".join("?" for _ in SQLITE_CONTINUITY_REQUIRED_TABLES)
        present_tables = {
            str(row[0])
            for row in conn.execute(
                f"SELECT name FROM sqlite_schema WHERE type = 'table' AND name IN ({placeholders})",
                SQLITE_CONTINUITY_REQUIRED_TABLES,
            )
        }
        missing_tables = sorted(set(SQLITE_CONTINUITY_REQUIRED_TABLES) - present_tables)
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        auto_vacuum = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
        conn.execute("BEGIN IMMEDIATE")
        conn.rollback()
        conn.close()
        facts.update({
            "read_probe_succeeded": read_probe,
            "schema_version": schema_version,
            "user_version": user_version,
            "required_tables_checked": list(SQLITE_CONTINUITY_REQUIRED_TABLES),
            "required_tables_present": sorted(present_tables),
            "required_tables_missing": missing_tables,
            "write_transaction_acquired": True,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist,
            "auto_vacuum": auto_vacuum,
        })
        structurally_usable = read_probe and not missing_tables
        continuity = _obs(
            "PRESENT" if structurally_usable else "DEGRADED",
            now.isoformat(),
            None,
            (
                "SQLite passed bounded read, required-table, metadata, and write-intent probes; "
                "full integrity is established separately"
                if structurally_usable
                else "SQLite is readable but required continuity tables are missing"
            ),
            facts,
        )
        slack_facts = {"freelist_count": freelist, "page_size": page_size, "slack_bytes": freelist * page_size, "floor_pages": SLACK_PAGE_FLOOR, "auto_vacuum": auto_vacuum}
        slack = _obs("PRESENT" if freelist >= SLACK_PAGE_FLOOR else "DEGRADED", now.isoformat(), None, "internal SQLite reuse capacity is at or above the production floor" if freelist >= SLACK_PAGE_FLOOR else "internal SQLite reuse capacity is below the production floor", slack_facts)
        return continuity, slack
    except sqlite3.Error as exc:
        if conn is not None:
            conn.close()
        facts.update({"read_probe_succeeded": False, "write_transaction_acquired": False, "error": str(exc)})
        return _obs("DEGRADED", now.isoformat(), None, "bounded SQLite continuity probe failed", facts), _obs("UNKNOWN", now.isoformat(), None, "SQLite internal slack is unobservable", facts)


def _volume(data_dir: Path, now: datetime) -> dict[str, Any]:
    target = data_dir if data_dir.exists() else data_dir.parent
    try:
        usage = shutil.disk_usage(target)
    except OSError as exc:
        return _obs("UNKNOWN", now.isoformat(), None, "the data volume is unobservable", {"path": str(target), "error": str(exc)})
    used = (usage.total - usage.free) / usage.total if usage.total else 0.0
    state = "DEGRADED" if used >= 0.85 else "PRESENT"
    domain = "CRITICAL" if used >= 0.92 else "WARN" if used >= 0.85 else "OK"
    facts = {"path": str(target), "free_bytes": usage.free, "total_bytes": usage.total, "used_fraction": round(used, 6), "warn_threshold": 0.85, "critical_threshold": 0.92}
    return _obs(state, now.isoformat(), None, "the data volume is below the warning bound" if state == "PRESENT" else "the data volume crossed an existing pressure bound", facts, domain)


def _facts_snapshot(data_dir: Path, now: datetime) -> dict[str, Any]:
    path = data_dir / "facts.sqlite"
    if not path.exists():
        return _obs("ABSENT", None, FACTS_MAX_AGE_S, "the facts snapshot has not been produced", {"path": str(path)})
    observed_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    age = _age(now, observed_at) or 0
    facts = {"path": str(path), "size_bytes": path.stat().st_size, "age_seconds": round(age, 3), "artifact_authority": "observational"}
    if age > FACTS_MAX_AGE_S:
        return _obs("STALE", observed_at, FACTS_MAX_AGE_S, "the facts snapshot is stale", facts)
    return _obs("PRESENT", observed_at, FACTS_MAX_AGE_S, "a recent observational facts snapshot is present", facts)


def build_status(db_path: str | os.PathLike[str], *, data_dir: str | os.PathLike[str] | None = None,
                 now: datetime | None = None, manifest_path: Path = MANIFEST_PATH) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifest = load_manifest(manifest_path)
    db_path = Path(db_path).resolve()
    data_path = Path(data_dir).resolve() if data_dir else db_path.parent
    consumer_fact = ops_runtime.read_fact("consumer", data_path)
    evaluation_fact = ops_runtime.read_fact("evaluation", data_path)
    continuity, slack = _sqlite(db_path, now)
    produced = {
        "driftwatch.observation.stream_coverage": _consumer(consumer_fact, now),
        "driftwatch.observation.cursor_continuity": _cursor(db_path, consumer_fact, now),
        "driftwatch.evaluation.freshness": _evaluation(evaluation_fact, now),
        "driftwatch.drift.bounded_current_state": _drift_state(evaluation_fact, now),
        "driftwatch.evaluation.execution": _execution(evaluation_fact, now, db_path),
        "driftwatch.persistence.sqlite_continuity": continuity,
        "driftwatch.persistence.volume_capacity": _volume(data_path, now),
        "driftwatch.persistence.sqlite_slack": slack,
        "driftwatch.output.facts_snapshot_freshness": _facts_snapshot(data_path, now),
    }
    concerns = []
    for declared in manifest["concerns"]:
        observation = produced.get(declared["id"])
        if observation is None:
            observation = {"observation_present": False, "local_state": "ABSENT", "domain_state": None, "observed_at": None, "valid_for_seconds": None, "reason": "required concern has no local observation producer", "facts": {}}
        concerns.append({**declared, "observation": observation})
    return {
        "schema": STATUS_SCHEMA,
        "project": manifest["project"],
        "generated_at": now.isoformat(),
        "manifest": {"schema": manifest["schema"], "path": ".ops/concerns.toml"},
        "producer": {"id": "driftwatch.ops-status"},
        "authority": {"kind": "producer_local_observation", "does_not_establish": ["nq_admission", "pulse_qualification", "nightshift_attention"]},
        "concerns": concerns,
    }


def render_text(status: dict[str, Any]) -> str:
    lines = [f"{status['project']} local visibility ({status['generated_at']})"]
    for item in status["concerns"]:
        obs = item["observation"]
        marker = {"PRESENT": "+", "DEGRADED": "!", "UNKNOWN": "?", "STALE": "~", "ABSENT": "-", "NOT_APPLICABLE": "="}.get(obs["local_state"], "?")
        domain = f" [{obs['domain_state']}]" if obs.get("domain_state") else ""
        lines.append(f"[{marker}] {item['id']}: {obs['local_state']}{domain} — {obs['reason']}")
    lines.append("Local observations only: not NQ-admitted, Pulse-qualified, or Nightshift-escalated.")
    return "\n".join(lines)


def dumps(status: dict[str, Any]) -> str:
    return json.dumps(status, indent=2, sort_keys=True)


def main(argv: list[str] | None = None) -> int:
    """Dependency-light status entry point for the repository binding."""
    parser = argparse.ArgumentParser(prog="python -m labeler.ops_status")
    parser.add_argument("--db", default="src/data/labeler.sqlite")
    parser.add_argument("--data-dir", default="src/data")
    parser.add_argument("--now", default=None)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    args = parser.parse_args(argv)
    observed_now = _parse_ts(args.now) if args.now else datetime.now(timezone.utc)
    if observed_now is None:
        parser.error("--now must be an RFC3339 date-time")
    status = build_status(args.db, data_dir=args.data_dir, now=observed_now)
    print(dumps(status) if args.format == "json" else render_text(status))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
