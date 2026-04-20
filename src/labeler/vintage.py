"""DID vintage (creation-time) capture and storage.

Vintage is an immutable covariate per DID: when the DID was created, measured
as the timestamp on its genesis operation in the PLC directory log.

This module is separate from resolver.py because vintage and current-host are
conceptually different observables. pds_host changes over time (mutable). Genesis
timestamp does not (immutable, write-once by policy).

Sources in priority order:
  plc_export       — from paginated /export dump; primary backfill path
  plc_audit_log    — from per-DID /log endpoint; repair and spot-checks
  plc_stream       — from /export/stream live tail (future)
  unsupported_method — did:web and friends; no PLC log exists
  repair_override  — deliberate correction; requires explicit caller opt-in

Exclude rows with NULL or 'unsupported_method' vintage from admissibility
analysis. See docs/VINTAGE-ADMISSIBILITY.md for analytic exclusion rules.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

from . import timeutil

LOG = logging.getLogger("labeler.vintage")

USER_AGENT = "driftwatch-vintage/0.1 (+https://github.com/unpingable/atproto-driftwatch)"
PLC_LOG_TIMEOUT_S = 10


@dataclass(frozen=True)
class DIDVintage:
    did: str
    created_at: Optional[str]  # ISO timestamp of genesis op
    source: str  # one of the vintage_source enum values
    error: Optional[str] = None


def _extract_genesis_created_at(log_entries: list) -> Optional[str]:
    """Given a PLC /log response (list of ops ordered oldest-first),
    return the createdAt of the first (genesis) operation.

    Each entry is shaped like {"createdAt": "...", "operation": {...}, ...}.
    """
    if not isinstance(log_entries, list) or not log_entries:
        return None
    first = log_entries[0]
    if not isinstance(first, dict):
        return None
    ts = first.get("createdAt")
    return ts if isinstance(ts, str) else None


def fetch_did_genesis(did: str) -> DIDVintage:
    """Fetch DID genesis timestamp via plc.directory/<did>/log/audit.

    /log returns bare operations without timestamps; /log/audit wraps each op
    with {did, operation, cid, createdAt, nullified} envelopes — that is the
    endpoint that carries the canonical timestamp. Used for repair,
    spot-checks, and per-DID resolution. Bulk backfill should use the
    /export path (see scripts/plc_export_backfill.py) instead.
    """
    if not did.startswith("did:plc:"):
        return DIDVintage(
            did=did, created_at=None, source="unsupported_method",
            error=f"non_plc_method:{did.split(':')[1] if ':' in did else '?'}",
        )

    url = f"https://plc.directory/{urllib.parse.quote(did, safe=':')}/log/audit"
    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        })
        with urllib.request.urlopen(req, timeout=PLC_LOG_TIMEOUT_S) as resp:
            body = resp.read()
        entries = json.loads(body)
    except urllib.error.HTTPError as e:
        return DIDVintage(
            did=did, created_at=None, source="plc_audit_log",
            error=f"http_{e.code}",
        )
    except Exception as e:
        return DIDVintage(
            did=did, created_at=None, source="plc_audit_log",
            error=type(e).__name__,
        )

    created_at = _extract_genesis_created_at(entries)
    if not created_at:
        return DIDVintage(
            did=did, created_at=None, source="plc_audit_log",
            error="no_genesis_op",
        )
    return DIDVintage(did=did, created_at=created_at, source="plc_audit_log")


def set_vintage(
    conn: sqlite3.Connection,
    did: str,
    created_at: str,
    source: str,
    run_id: Optional[str] = None,
    allow_repair: bool = False,
) -> str:
    """Write vintage for a DID. Write-once by policy.

    Returns one of:
      'written' — row updated, vintage was NULL
      'skipped_existing' — vintage already set, not a repair
      'skipped_missing_row' — no actor_identity_current row for this DID
      'repaired' — allow_repair=True and source='repair_override' overwrote prior value

    Only updates existing rows. Does not insert new ones — vintage enrichment
    is a covariate, not a population source.
    """
    if source not in {
        "plc_export", "plc_audit_log", "plc_stream",
        "unsupported_method", "repair_override",
    }:
        raise ValueError(f"invalid vintage_source: {source}")
    if source == "repair_override" and not allow_repair:
        raise ValueError("repair_override requires allow_repair=True")

    now_iso = timeutil.now_utc().isoformat()

    if allow_repair and source == "repair_override":
        cur = conn.execute(
            "UPDATE actor_identity_current "
            "SET did_created_at = ?, vintage_source = ?, "
            "    vintage_resolved_at = ?, vintage_run_id = ? "
            "WHERE did = ?",
            (created_at, source, now_iso, run_id, did),
        )
        return "repaired" if cur.rowcount > 0 else "skipped_missing_row"

    cur = conn.execute(
        "UPDATE actor_identity_current "
        "SET did_created_at = ?, vintage_source = ?, "
        "    vintage_resolved_at = ?, vintage_run_id = ? "
        "WHERE did = ? AND did_created_at IS NULL",
        (created_at, source, now_iso, run_id, did),
    )
    if cur.rowcount > 0:
        return "written"

    check = conn.execute(
        "SELECT did_created_at FROM actor_identity_current WHERE did = ?",
        (did,),
    ).fetchone()
    if check is None:
        return "skipped_missing_row"
    return "skipped_existing"


def set_vintage_many(
    conn: sqlite3.Connection,
    rows: list[tuple[str, str]],
    source: str,
    run_id: Optional[str] = None,
) -> dict:
    """Bulk set vintage for (did, created_at) pairs. Write-once per DID.

    Returns stats dict: {written, skipped_existing, skipped_missing_row}.
    Uses a single UPDATE per row but within caller's transaction — commit externally.
    """
    if source not in {"plc_export", "plc_audit_log", "plc_stream", "unsupported_method"}:
        raise ValueError(f"set_vintage_many does not accept source={source}")

    now_iso = timeutil.now_utc().isoformat()
    stats = {"written": 0, "skipped_existing": 0, "skipped_missing_row": 0}

    # Fast-path: UPDATE WHERE did_created_at IS NULL. For rows that don't match
    # (either missing or already set), we count by diff.
    dids_in_batch = [d for d, _ in rows]
    if not dids_in_batch:
        return stats

    # Pre-fetch which DIDs exist and which already have vintage to avoid a
    # second COUNT pass per skip class.
    placeholders = ",".join("?" * len(dids_in_batch))
    existing = dict(conn.execute(
        f"SELECT did, did_created_at FROM actor_identity_current "
        f"WHERE did IN ({placeholders})",
        dids_in_batch,
    ).fetchall())

    for did, created_at in rows:
        if did not in existing:
            stats["skipped_missing_row"] += 1
            continue
        if existing[did] is not None:
            stats["skipped_existing"] += 1
            continue
        conn.execute(
            "UPDATE actor_identity_current "
            "SET did_created_at = ?, vintage_source = ?, "
            "    vintage_resolved_at = ?, vintage_run_id = ? "
            "WHERE did = ? AND did_created_at IS NULL",
            (created_at, source, now_iso, run_id, did),
        )
        stats["written"] += 1

    return stats


def start_export_run(conn: sqlite3.Connection, run_id: str, note: Optional[str] = None) -> None:
    """Record the start of a /export backfill run."""
    conn.execute(
        "INSERT INTO plc_export_runs "
        "(run_id, started_at, status, note) VALUES (?, ?, 'running', ?)",
        (run_id, timeutil.now_utc().isoformat(), note),
    )


def finish_export_run(
    conn: sqlite3.Connection,
    run_id: str,
    first_cursor: Optional[str],
    last_cursor: Optional[str],
    ops_processed: int,
    dids_seen: int,
    genesis_ops_written: int,
    status: str = "completed",
    note: Optional[str] = None,
) -> None:
    """Record completion of a /export backfill run.

    first_cursor / last_cursor are createdAt timestamps used by the
    plc.directory /export endpoint for pagination.
    """
    conn.execute(
        "UPDATE plc_export_runs SET "
        "ended_at = ?, first_cursor = ?, last_cursor = ?, "
        "ops_processed = ?, dids_seen = ?, genesis_ops_written = ?, "
        "status = ?, note = COALESCE(?, note) "
        "WHERE run_id = ?",
        (
            timeutil.now_utc().isoformat(),
            first_cursor, last_cursor,
            ops_processed, dids_seen, genesis_ops_written,
            status, note, run_id,
        ),
    )
