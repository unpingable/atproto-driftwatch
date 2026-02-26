"""Lightweight per-minute counters for queue visibility.

All counters are thread-safe (threading.Lock). Call snapshot_and_reset()
every ~60s to get a dict of counts since the last snapshot and zero them.
"""

import threading
import time

_lock = threading.Lock()

_counters = {
    "events_in": 0,
    "claims_written": 0,
    "enqueue_attempts": 0,
    "enqueue_inserted": 0,
    "enqueue_ignored": 0,
    "enqueue_gated": 0,       # blocked by singleton gate
    "dequeued": 0,
}

_gauges = {}  # point-in-time values (not reset on snapshot)

_last_snapshot_ts = time.time()


def inc(name: str, n: int = 1):
    with _lock:
        _counters[name] = _counters.get(name, 0) + n


def set_gauge(name: str, value: float):
    with _lock:
        _gauges[name] = value


def snapshot_and_reset() -> dict:
    """Return a copy of all counters and reset them to zero.
    Gauges are included but not reset."""
    global _last_snapshot_ts
    with _lock:
        snap = dict(_counters)
        snap.update(_gauges)
        now = time.time()
        snap["window_secs"] = round(now - _last_snapshot_ts, 1)
        _last_snapshot_ts = now
        for k in _counters:
            _counters[k] = 0
    return snap
