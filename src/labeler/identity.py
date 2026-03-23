"""Identity event capture and current-state reduction.

Captures Jetstream identity/account events into an append-only log
and maintains a reduced current-state table for fast joins.

Core invariants:
  1. No identity event is silently dropped once classified as relevant
  2. identity_events is append-only
  3. actor_identity_current.last_seen_at never moves backward
  4. Missing fields in sparse events do not erase prior known values
  5. Tombstones mark inactive; they do not delete truth
"""

import json
import logging
from dataclasses import dataclass
from typing import Optional, Any

from . import timeutil
from .db import get_conn

LOG = logging.getLogger("labeler.identity")

# Sentinel: distinguishes "field not present in this event" from
# "field explicitly cleared/set to None."
MISSING = object()


@dataclass(frozen=True)
class IdentityDelta:
    """Normalized identity change extracted from a Jetstream event."""

    did: str
    event_kind: str  # "identity" or "account"
    time_us: int  # outer envelope timestamp — ordering key
    observed_at: str  # ISO8601

    # Fields use MISSING sentinel when not present in the event.
    # None means explicitly cleared.
    handle: Any = MISSING  # str | None | MISSING
    active: Any = MISSING  # bool | None | MISSING
    seq: Any = MISSING  # int | None | MISSING

    raw: Optional[dict] = None  # full Jetstream envelope


def parse_identity_event(js: dict) -> Optional[IdentityDelta]:
    """Parse a Jetstream message into an IdentityDelta.

    Returns None for events we don't capture (commits, etc).
    """
    kind = js.get("kind")
    if kind not in ("identity", "account"):
        return None

    did = js.get("did", "")
    if not did:
        return None

    time_us = js.get("time_us")
    if not time_us:
        return None

    observed_at = timeutil.to_utc_iso(time_us / 1_000_000)

    if kind == "identity":
        identity = js.get("identity", {})
        if not isinstance(identity, dict):
            identity = {}
        handle = identity.get("handle", MISSING)
        seq = identity.get("seq", MISSING)
        return IdentityDelta(
            did=did,
            event_kind="identity",
            time_us=time_us,
            observed_at=observed_at,
            handle=handle,
            seq=seq,
            raw=js,
        )

    if kind == "account":
        account = js.get("account", {})
        if not isinstance(account, dict):
            account = {}
        active = account.get("active", MISSING)
        seq = account.get("seq", MISSING)
        return IdentityDelta(
            did=did,
            event_kind="account",
            time_us=time_us,
            observed_at=observed_at,
            active=active,
            seq=seq,
            raw=js,
        )

    return None


def _event_is_newer(delta_time_us: int, existing_time_us: Optional[int]) -> bool:
    """Is this event newer than the last one that updated current state?"""
    if existing_time_us is None:
        return True
    return delta_time_us >= existing_time_us


def apply_identity_event(delta: IdentityDelta) -> None:
    """Append raw event, then reduce to current state.

    Opens and closes its own connection (matches db.py pattern).
    """
    conn = get_conn()
    try:
        # Step 1: append to identity_events (always, unconditionally)
        conn.execute(
            "INSERT INTO identity_events (did, event_kind, time_us, received_at, raw)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                delta.did,
                delta.event_kind,
                delta.time_us,
                delta.observed_at,
                json.dumps(delta.raw) if delta.raw else "{}",
            ),
        )

        # Step 2: reduce to actor_identity_current
        row = conn.execute(
            "SELECT * FROM actor_identity_current WHERE did = ?",
            (delta.did,),
        ).fetchone()

        if row is None:
            # First time seeing this DID — insert new row
            conn.execute(
                "INSERT INTO actor_identity_current"
                " (did, handle, is_active, first_seen_at, last_seen_at,"
                "  last_event_time_us, last_event_kind, reducer_version)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    delta.did,
                    None if delta.handle is MISSING else delta.handle,
                    1 if delta.active is MISSING else int(bool(delta.active)),
                    delta.observed_at,
                    delta.observed_at,
                    delta.time_us,
                    delta.event_kind,
                    1,
                ),
            )
        else:
            # Existing row — fieldwise update
            # Convert sqlite3.Row to dict for easier manipulation
            if hasattr(row, "keys"):
                row = dict(row)
            else:
                # Positional columns from schema
                cols = [
                    "did", "handle", "is_active", "first_seen_at",
                    "last_seen_at", "last_event_time_us", "last_event_kind",
                    "reducer_version",
                ]
                row = dict(zip(cols, row))

            # Time boundaries are order-independent
            first_seen = min(row["first_seen_at"], delta.observed_at)
            last_seen = max(row["last_seen_at"], delta.observed_at)

            # Only newer events change current-state fields
            if _event_is_newer(delta.time_us, row.get("last_event_time_us")):
                handle = row["handle"]
                if delta.handle is not MISSING:
                    handle = delta.handle

                is_active = row["is_active"]
                if delta.active is not MISSING:
                    is_active = int(bool(delta.active))

                conn.execute(
                    "UPDATE actor_identity_current SET"
                    " handle = ?, is_active = ?,"
                    " first_seen_at = ?, last_seen_at = ?,"
                    " last_event_time_us = ?, last_event_kind = ?"
                    " WHERE did = ?",
                    (
                        handle, is_active,
                        first_seen, last_seen,
                        delta.time_us, delta.event_kind,
                        delta.did,
                    ),
                )
            else:
                # Older event: only update time boundaries
                conn.execute(
                    "UPDATE actor_identity_current SET"
                    " first_seen_at = ?, last_seen_at = ?"
                    " WHERE did = ?",
                    (first_seen, last_seen, delta.did),
                )

        conn.commit()
    finally:
        conn.close()
