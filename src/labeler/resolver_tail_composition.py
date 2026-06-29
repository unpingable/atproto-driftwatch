"""Resolver aged-tail composition report (READ-ONLY measurement).

Slice 1 of the backlog-lane work (specs/gaps/gap-spec-resolver-backlog-lane.md):
*measurement before mechanism*. Before any backlog lane is built, decompose the
pending resolver pool so we know whether we are looking at drainable sediment,
poison gravel, or a seed-import wave in a trench coat.

This module ONLY reads. It produces counts. It writes no resolver status, runs no
scheduler, mutates no quarantine state, and changes no behavior. The connection is
opened read-only (`mode=ro`) when given a path.

Honesty contract: where the current schema cannot support a requested dimension,
the bucket is reported as ``unavailable`` (structurally not computable) or
``unknown`` (computable bucket, value not recognized) — never inferred. The schema
gaps are listed explicitly in the report's ``schema_gaps`` field so the backlog-lane
spec's provisional thresholds can be ratified against real numbers, and the gaps
themselves become the next open questions.

Schema reality (src/labeler/db.py actor_identity_current), which drives what is /
isn't supported:
  - first_seen_at TIMESTAMP        -> age buckets: SUPPORTED
  - identity_source TEXT           -> live/labelwatch_seed/both: SUPPORTED;
                                      retry/requeue: UNAVAILABLE (not a value here)
  - resolver_last_attempt_at TS    -> attempted vs never: SUPPORTED;
                                      exact attempt count (1 / 2-3 / 4+): UNAVAILABLE
                                      (NO attempt counter column exists)
  - resolver_status TEXT           -> unresolved/ok/not_found/error
  - resolver_error TEXT            -> error kind via the known emit format
                                      (resolver.py): SUPPORTED (deterministic map)
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Optional

# --- age buckets (hours). Boundaries shared with the SQL-free Python classifier;
#     mirror specs/gaps/gap-spec-resolver-backlog-lane.md. ---
AGE_BUCKETS = ("<24h", "24-72h", "72-168h", "168-240h", "240-336h", ">336h")

SOURCE_KNOWN = ("live", "labelwatch_seed", "both")

# Error kinds requested by the spec. `deleted_or_unavailable` is intentionally NOT
# emitted: the data cannot distinguish a deleted/deactivated account from a missing
# DID doc, so such rows fall under `did_doc_missing` — see schema_gaps.
ERROR_KINDS = (
    "never_attempted",
    "transient_network",
    "resolver_timeout",
    "rate_limited",
    "did_doc_missing",
    "malformed_did",
    "permanentish",
    "unknown",
)


def bucket_age(hours: Optional[float]) -> str:
    """Map an age in hours to a bucket label. None/negative -> '<24h' guard via unparseable handled upstream."""
    if hours is None:
        return "unparseable"
    if hours < 24:
        return "<24h"
    if hours < 72:
        return "24-72h"
    if hours < 168:
        return "72-168h"
    if hours < 240:
        return "168-240h"
    if hours < 336:
        return "240-336h"
    return ">336h"


def classify_source(identity_source: Optional[str]) -> str:
    """live/labelwatch_seed/both are real; anything else -> 'unknown'.

    NOTE: retry/requeue is UNAVAILABLE — identity_source does not encode it.
    """
    if identity_source in SOURCE_KNOWN:
        return identity_source
    return "unknown"


def classify_attempt(last_attempt_at: Optional[str]) -> str:
    """'0' (never attempted) vs '>=1' (attempted at least once).

    Finer buckets (1 / 2-3 / 4+) are UNAVAILABLE: there is no attempt-count column,
    only a last-attempt timestamp.
    """
    return "0" if last_attempt_at in (None, "") else ">=1"


def classify_error_kind(
    resolver_status: Optional[str],
    resolver_error: Optional[str],
    last_attempt_at: Optional[str],
) -> str:
    """Deterministic map from resolver.py's known emit format to an error kind.

    resolver.py writes resolver_error as one of:
      unsupported_method:<m> | http_404 | http_<code> | <ExceptionClassName>
    """
    if last_attempt_at in (None, ""):
        return "never_attempted"
    err = (resolver_error or "").strip()
    if not err:
        # attempted but no error string and not ok -> genuinely unknown
        if resolver_status == "not_found":
            return "did_doc_missing"
        return "unknown"
    low = err.lower()
    if err.startswith("unsupported_method"):
        return "malformed_did"
    if err == "http_404":
        return "did_doc_missing"
    if err == "http_429":
        return "rate_limited"
    if err.startswith("http_4"):
        return "permanentish"          # 4xx client errors other than 404/429
    if err.startswith("http_5"):
        return "transient_network"     # 5xx upstream/server, retryable
    if "timeout" in low:
        return "resolver_timeout"      # TimeoutError, socket.timeout, ReadTimeout...
    if any(k in low for k in ("urlerror", "connection", "gaierror", "ssl", "reset", "refused", "broken")):
        return "transient_network"
    if resolver_status == "not_found":
        return "did_doc_missing"
    return "unknown"


def quarantine_flags(
    age_hours: Optional[float],
    last_attempt_at: Optional[str],
    error_kind: str,
) -> dict:
    """Quarantine-candidate flags, COUNTS ONLY — this never writes status.

    Two of the spec's criteria are structurally UNAVAILABLE and are reported as such
    rather than faked:
      - age_gt_336h_and_attempts_gte_3 : no attempt counter -> proxied by
        age_gt_336h_and_attempted (attempts>=3 portion UNAVAILABLE)
      - repeated_timeout : no per-DID attempt history -> UNAVAILABLE
    """
    attempted = last_attempt_at not in (None, "")
    aged = (age_hours is not None) and (age_hours > 336)
    return {
        "age_gt_336h_and_attempted": bool(aged and attempted),  # proxy; >=3 UNAVAILABLE
        "permanentish_error": error_kind == "permanentish",
        "malformed_input": error_kind == "malformed_did",
        # repeated_timeout: UNAVAILABLE (no counter) -> always False, surfaced in schema_gaps
    }


SCHEMA_GAPS = [
    "attempt_count: no counter column on actor_identity_current; only "
    "resolver_last_attempt_at (timestamp). Buckets 1 / 2-3 / 4+ are UNAVAILABLE; "
    "only 0 (never) vs >=1 (attempted) is computable.",
    "quarantine 'age>336h AND attempts>=3': attempts>=3 portion UNAVAILABLE; "
    "reported as proxy age_gt_336h_and_attempted.",
    "quarantine 'repeated_timeout': UNAVAILABLE — no per-DID attempt history.",
    "source 'retry/requeue': UNAVAILABLE — identity_source only encodes "
    "live/labelwatch_seed/both; unrecognized values bucket as 'unknown'.",
    "error kind 'deleted_or_unavailable': NOT separable from did_doc_missing with "
    "current fields; such rows fall under did_doc_missing.",
]


def _parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    txt = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        # last-ditch: drop fractional seconds / try space form
        try:
            dt = datetime.fromisoformat(txt.split(".")[0])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# Pending pool definition matches the sampler / second read: status NULL or 'unresolved'.
_PENDING_WHERE = "resolver_status IS NULL OR resolver_status = 'unresolved'"


def compose_tail(conn: sqlite3.Connection, now: Optional[datetime] = None) -> dict:
    """Decompose the pending resolver pool. READ-ONLY: a single SELECT, no writes.

    Iterates pending rows and classifies each via the pure functions above (single
    source of truth — no SQL/Python divergence). For ~300k pending rows this is a
    few seconds in-process; intended to run where the DB is local (e.g. the VM),
    read-only.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    by_age = {b: 0 for b in AGE_BUCKETS}
    by_age["unparseable"] = 0
    by_source = {s: 0 for s in SOURCE_KNOWN}
    by_source["unknown"] = 0
    by_attempt = {"0": 0, ">=1 (exact count unavailable)": 0}
    by_error = {k: 0 for k in ERROR_KINDS}
    quar = {
        "age_gt_336h_and_attempted": 0,
        "permanentish_error": 0,
        "malformed_input": 0,
        "repeated_timeout": "unavailable",
        "age_gt_336h_and_attempts_gte_3": "unavailable",
    }
    total = 0

    cur = conn.execute(
        f"SELECT first_seen_at, identity_source, resolver_status, "
        f"resolver_last_attempt_at, resolver_error "
        f"FROM actor_identity_current WHERE {_PENDING_WHERE}"
    )
    for first_seen_at, identity_source, status, last_attempt_at, resolver_error in cur:
        total += 1
        ts = _parse_ts(first_seen_at)
        age_h = None if ts is None else (now - ts).total_seconds() / 3600.0
        by_age[bucket_age(age_h)] += 1
        by_source[classify_source(identity_source)] += 1
        if classify_attempt(last_attempt_at) == "0":
            by_attempt["0"] += 1
        else:
            by_attempt[">=1 (exact count unavailable)"] += 1
        kind = classify_error_kind(status, resolver_error, last_attempt_at)
        by_error[kind] = by_error.get(kind, 0) + 1
        flags = quarantine_flags(age_h, last_attempt_at, kind)
        for k, v in flags.items():
            if v:
                quar[k] += 1

    oldest_h = None
    row = conn.execute(
        f"SELECT MIN(first_seen_at) FROM actor_identity_current WHERE {_PENDING_WHERE}"
    ).fetchone()
    if row and row[0]:
        ts = _parse_ts(row[0])
        if ts:
            oldest_h = round((now - ts).total_seconds() / 3600.0, 2)

    # Adjacent terminal pool (status error/not_found) — NOT pending, but this is
    # where the real poison lives; surfaced for quarantine context, counts only.
    terminal = {}
    for st in ("error", "not_found"):
        r = conn.execute(
            "SELECT COUNT(*) FROM actor_identity_current WHERE resolver_status = ?", (st,)
        ).fetchone()
        terminal[st] = r[0] if r else 0

    return {
        "report": "resolver_tail_composition",
        "report_version": "v1",
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scope": "pending pool (resolver_status IS NULL OR 'unresolved')",
        "totals": {
            "pending_total": total,
            "oldest_pending_hours": oldest_h,
            "pending_gt_72h": by_age["72-168h"] + by_age["168-240h"] + by_age["240-336h"] + by_age[">336h"],
            "pending_gt_168h": by_age["168-240h"] + by_age["240-336h"] + by_age[">336h"],
        },
        "by_age_bucket": by_age,
        "by_source_population": by_source,
        "by_attempt_count": by_attempt,
        "by_last_error_kind": by_error,
        "quarantine_candidate_counts": quar,
        "adjacent_terminal_pool": terminal,
        "schema_gaps": SCHEMA_GAPS,
    }


def open_readonly(db_path: str) -> sqlite3.Connection:
    """Open the DB strictly read-only (mode=ro URI + query_only pragma)."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def main(argv=None) -> int:
    import argparse
    import json
    import os

    ap = argparse.ArgumentParser(description="Read-only resolver aged-tail composition report")
    ap.add_argument(
        "--db",
        default=os.environ.get("DRIFTWATCH_DB", "/mnt/zonestorage/driftwatch/data/labeler.sqlite"),
        help="path to labeler.sqlite (opened read-only)",
    )
    ap.add_argument("--out", default="-", help="output path for JSON ('-' = stdout)")
    args = ap.parse_args(argv)

    conn = open_readonly(args.db)
    try:
        report = compose_tail(conn)
    finally:
        conn.close()

    text = json.dumps(report, indent=2)
    if args.out == "-":
        print(text)
    else:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
