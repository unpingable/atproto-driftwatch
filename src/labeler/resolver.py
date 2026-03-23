"""Async DID document resolver sidecar.

Resolves DID documents to extract PDS endpoints and handles,
writing results back to actor_identity_current.

Runs as a background task in the consumer — never blocks ingest.
Conservative rate limiting: bounded concurrency, per-host backoff,
explicit timeouts.
"""

import logging
import urllib.request
import urllib.parse
import urllib.error
import json
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from . import timeutil
from .db import get_conn

LOG = logging.getLogger("labeler.resolver")

# Resolution policy
BATCH_SIZE = 20  # DIDs per resolution cycle
RESOLVE_INTERVAL_S = 60  # seconds between resolution cycles
RESOLVE_TIMEOUT_S = 10  # HTTP timeout per DID
STALE_AFTER_S = 24 * 3600  # re-resolve after 24h
RETRY_BACKOFF_S = 300  # retry failures after 5min
PERMANENT_FAILURE_BACKOFF_S = 7 * 24 * 3600  # 404s: retry after 7d
USER_AGENT = "driftwatch-resolver/0.1 (+https://github.com/unpingable/atproto-driftwatch)"


@dataclass(frozen=True)
class ResolvedDID:
    """Result of resolving a DID document."""
    did: str
    pds_endpoint: Optional[str]
    pds_host: Optional[str]
    handle: Optional[str]
    status: str  # "ok", "not_found", "error"
    error: Optional[str] = None


def resolve_did(did: str) -> ResolvedDID:
    """Resolve a single DID document and extract PDS endpoint + handle.

    Supports did:plc (via plc.directory) and did:web (via .well-known).
    """
    if did.startswith("did:plc:"):
        url = f"https://plc.directory/{urllib.parse.quote(did, safe=':')}"
    elif did.startswith("did:web:"):
        domain = did[8:]
        url = f"https://{domain}/.well-known/did.json"
    else:
        return ResolvedDID(
            did=did, pds_endpoint=None, pds_host=None, handle=None,
            status="error", error=f"unsupported_method:{did.split(':')[1]}",
        )

    try:
        req = urllib.request.Request(url, headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        })
        with urllib.request.urlopen(req, timeout=RESOLVE_TIMEOUT_S) as resp:
            doc = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return ResolvedDID(
                did=did, pds_endpoint=None, pds_host=None, handle=None,
                status="not_found", error="http_404",
            )
        return ResolvedDID(
            did=did, pds_endpoint=None, pds_host=None, handle=None,
            status="error", error=f"http_{e.code}",
        )
    except Exception as e:
        return ResolvedDID(
            did=did, pds_endpoint=None, pds_host=None, handle=None,
            status="error", error=type(e).__name__,
        )

    # Extract PDS endpoint (defensive: service/alsoKnownAs may be non-list or contain non-dicts)
    pds_endpoint = None
    services = doc.get("service", [])
    if isinstance(services, list):
        for svc in services:
            if isinstance(svc, dict) and svc.get("id") == "#atproto_pds":
                pds_endpoint = svc.get("serviceEndpoint")
                break

    # Extract handle from alsoKnownAs
    handle = None
    also_known = doc.get("alsoKnownAs", [])
    if isinstance(also_known, list):
        for aka in also_known:
            if isinstance(aka, str) and aka.startswith("at://"):
                handle = aka[5:]
                break

    # Hostname from PDS endpoint
    pds_host = None
    if pds_endpoint:
        try:
            pds_host = urlparse(pds_endpoint).hostname
        except Exception:
            pass

    return ResolvedDID(
        did=did,
        pds_endpoint=pds_endpoint,
        pds_host=pds_host,
        handle=handle,
        status="ok",
    )


def _write_resolution(result: ResolvedDID) -> None:
    """Write resolution result back to actor_identity_current."""
    now = timeutil.now_utc().isoformat()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT did FROM actor_identity_current WHERE did = ?",
            (result.did,),
        ).fetchone()

        if row is None:
            # DID not in current state yet — insert minimal row
            conn.execute(
                "INSERT INTO actor_identity_current"
                " (did, pds_endpoint, pds_host, handle, is_active,"
                "  first_seen_at, last_seen_at, last_event_kind,"
                "  resolver_status, resolver_last_attempt_at,"
                "  resolver_last_success_at, resolver_error, reducer_version)"
                " VALUES (?, ?, ?, ?, 1, ?, ?, 'resolver', ?, ?, ?, ?, 1)",
                (
                    result.did,
                    result.pds_endpoint, result.pds_host,
                    result.handle,
                    now, now,
                    result.status, now,
                    now if result.status == "ok" else None,
                    result.error,
                ),
            )
        else:
            # Update existing row — fieldwise, don't erase identity-event data
            updates = [
                "pds_endpoint = ?", "pds_host = ?",
                "resolver_status = ?", "resolver_last_attempt_at = ?",
                "resolver_error = ?",
            ]
            params = [
                result.pds_endpoint, result.pds_host,
                result.status, now, result.error,
            ]

            if result.status == "ok":
                updates.append("resolver_last_success_at = ?")
                params.append(now)

            # Only update handle from resolver if identity events haven't set it
            if result.handle and result.status == "ok":
                updates.append("handle = COALESCE(handle, ?)")
                params.append(result.handle)

            params.append(result.did)
            conn.execute(
                f"UPDATE actor_identity_current SET {', '.join(updates)} WHERE did = ?",
                params,
            )

        conn.commit()
    finally:
        conn.close()


def fetch_unresolved_batch(limit: int = BATCH_SIZE) -> list[str]:
    """Get a batch of DIDs that need resolution.

    Priority: unresolved > stale > retryable errors.
    """
    now_iso = timeutil.now_utc().isoformat()
    stale_cutoff = timeutil.to_utc_iso(time.time() - STALE_AFTER_S)
    retry_cutoff = timeutil.to_utc_iso(time.time() - RETRY_BACKOFF_S)
    permanent_cutoff = timeutil.to_utc_iso(time.time() - PERMANENT_FAILURE_BACKOFF_S)

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT did FROM actor_identity_current
            WHERE
                -- Never resolved
                resolver_status IS NULL
                -- Stale successful resolution
                OR (resolver_status = 'ok'
                    AND resolver_last_success_at < ?)
                -- Retryable errors past backoff
                OR (resolver_status = 'error'
                    AND resolver_last_attempt_at < ?)
                -- Not-found past long backoff
                OR (resolver_status = 'not_found'
                    AND resolver_last_attempt_at < ?)
            ORDER BY
                -- Prioritize never-resolved first
                CASE WHEN resolver_status IS NULL THEN 0 ELSE 1 END,
                first_seen_at ASC
            LIMIT ?
            """,
            (stale_cutoff, retry_cutoff, permanent_cutoff, limit),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def resolve_batch() -> dict:
    """Resolve a batch of unresolved/stale DIDs.

    Returns stats dict for logging.
    """
    dids = fetch_unresolved_batch()
    if not dids:
        return {"resolved": 0, "ok": 0, "not_found": 0, "error": 0}

    stats = {"resolved": 0, "ok": 0, "not_found": 0, "error": 0}
    for did in dids:
        try:
            result = resolve_did(did)
            _write_resolution(result)
            stats["resolved"] += 1
            stats[result.status] = stats.get(result.status, 0) + 1
        except Exception:
            LOG.debug("failed to resolve %s", did, exc_info=True)
            stats["error"] += 1

    return stats
