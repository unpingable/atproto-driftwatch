"""Privacy-safe WebSocket lifecycle diagnostics."""

from __future__ import annotations

import re


_URL = re.compile(r"(?i)\b(?:https?|wss?|at)://\S+")
_DID = re.compile(r"(?i)\bdid:[a-z0-9]+:[a-z0-9._:%-]+")
_CONTROL = re.compile(r"[^\x20-\x7e]")


def sanitize_close_reason(value, limit: int = 160) -> str | None:
    if value is None:
        return None
    text = _CONTROL.sub("?", str(value))
    text = _URL.sub("[url]", text)
    text = _DID.sub("[identity]", text)
    return text[:limit]


def _frame(frame) -> dict | None:
    if frame is None:
        return None
    return {
        "code": getattr(frame, "code", None),
        "reason": sanitize_close_reason(getattr(frame, "reason", None)),
    }


def connection_failure_record(exc, ws=None, **context) -> dict:
    """Return selected close metadata without URLs, bodies, or credentials."""
    received = _frame(getattr(exc, "rcvd", None))
    sent = _frame(getattr(exc, "sent", None))
    order = getattr(exc, "rcvd_then_sent", None)
    if order is True:
        initiator = "peer"
    elif order is False:
        initiator = "client"
    else:
        initiator = "unknown"
    chain = []
    cursor = exc
    while cursor is not None and len(chain) < 6:
        chain.append(type(cursor).__name__)
        cursor = cursor.__cause__ or cursor.__context__
    record = {
        "exception_chain": chain,
        "deadline_expired": "TimeoutError" in chain,
        "close_code": getattr(exc, "code", getattr(ws, "close_code", None)),
        "close_reason": sanitize_close_reason(
            getattr(exc, "reason", getattr(ws, "close_reason", None))
        ),
        "close_received": received,
        "close_sent": sent,
        "initiator": initiator,
    }
    record.update(context)
    return record
