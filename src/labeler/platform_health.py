"""Platform health watermark — stream-health gate for rechecks.

Tracks Jetstream throughput (EWMA baseline) and stream lag to detect periods
of degraded ingestion. When degraded, rechecks are gated to avoid running
against incomplete data.

Three states: WARMING_UP -> OK <-> DEGRADED

Ephemeral runtime state (resets on restart — correct, since baseline needs
re-learning). Thread-safe via threading.Lock.

This does NOT detect AppView indexing rot (HTTP read health) — that's a
separate future concern.
"""

import json
import logging
import os
import threading
import time
from pathlib import Path

LOG = logging.getLogger("labeler.platform_health")

# --- Configuration (all overridable via env) ---

WARMUP_WINDOWS = int(os.getenv("HEALTH_WARMUP_WINDOWS", "5"))
EWMA_SPAN = int(os.getenv("HEALTH_EWMA_SPAN", "30"))  # ~30 windows = ~30min
EWMA_ALPHA = 2.0 / (EWMA_SPAN + 1)

# Degradation thresholds
COVERAGE_LOW_THRESHOLD = float(os.getenv("HEALTH_COVERAGE_LOW", "0.6"))
LAG_HIGH_THRESHOLD_S = float(os.getenv("HEALTH_LAG_HIGH_S", "120"))
BACKLOG_GROWTH_THRESHOLD = int(os.getenv("HEALTH_BACKLOG_GROWTH", "100"))
DROP_FRAC_HIGH_THRESHOLD = float(os.getenv("HEALTH_DROP_FRAC_HIGH", "0.02"))
CONSECUTIVE_BAD_WINDOWS = int(os.getenv("HEALTH_BAD_WINDOWS", "3"))

# Recovery thresholds
COVERAGE_RECOVER_THRESHOLD = float(os.getenv("HEALTH_COVERAGE_RECOVER", "0.8"))
LAG_RECOVER_THRESHOLD_S = float(os.getenv("HEALTH_LAG_RECOVER_S", "30"))
CONSECUTIVE_GOOD_WINDOWS = int(os.getenv("HEALTH_GOOD_WINDOWS", "5"))
RECALIBRATION_WINDOWS = int(os.getenv("HEALTH_RECALIBRATION_WINDOWS", "3"))

# Lag EWMA smoothing
LAG_EWMA_ALPHA = float(os.getenv("HEALTH_LAG_EWMA_ALPHA", "0.2"))
LAG_CLAMP_MAX_S = 600.0  # 10 min — clock jump guard

# Absolute lag threshold during warmup
WARMUP_LAG_ABSOLUTE_S = float(os.getenv("HEALTH_WARMUP_LAG_ABSOLUTE_S", "120"))

# Epsilon for baseline division
_EPS = 1e-6

# States
WARMING_UP = "warming_up"
OK = "ok"
DEGRADED = "degraded"


class PlatformHealth:
    """Thread-safe platform health tracker."""

    def __init__(self):
        self._lock = threading.Lock()
        self._reset()

    def _reset(self):
        """Reset all state (for testing or restart)."""
        self._state = WARMING_UP
        self._baseline_restored = False
        self._windows_seen = 0

        # EWMA baseline
        self._baseline_eps = 0.0
        self._current_eps = 0.0

        # Lag tracking
        self._stream_lag_s = 0.0
        self._last_event_time_us = 0

        # Reconnect tracking
        self._reconnect_count = 0
        self._reconnect_gap_s = 0.0
        self._last_disconnect_ts = 0.0

        # Degradation counters (consecutive bad windows)
        self._low_coverage_streak = 0
        self._high_lag_streak = 0
        self._backlog_growing_streak = 0

        # Recovery counters (consecutive good windows)
        self._recovery_streak = 0
        self._recalibration_remaining = 0

        # Backlog tracking
        self._prev_backlog = None

        # Drop tracking
        self._drop_frac = 0.0
        self._high_drop_streak = 0

        # Active gate reasons
        self._gate_reasons: list = []

        # Detection emission tracking
        self._pending_detection: str = ""  # "" | "platform_degraded" | "platform_recovered"
        self._degraded_heartbeat_counter = 0

    def record_event_time(self, time_us: int):
        """Record a Jetstream event's time_us for lag tracking.

        Called per message. Negative lag clamped to 0 (clock skew).
        Spikes > 10min clamped (clock jump guard).
        """
        if not time_us:
            return
        wall_us = time.time() * 1_000_000
        raw_lag_s = max(0, (wall_us - time_us) / 1_000_000)
        raw_lag_s = min(raw_lag_s, LAG_CLAMP_MAX_S)

        with self._lock:
            if self._stream_lag_s == 0.0:
                self._stream_lag_s = raw_lag_s
            else:
                self._stream_lag_s = (
                    LAG_EWMA_ALPHA * raw_lag_s
                    + (1 - LAG_EWMA_ALPHA) * self._stream_lag_s
                )

    def record_reconnect(self):
        """Record a WebSocket reconnect event."""
        now = time.time()
        with self._lock:
            self._reconnect_count += 1
            if self._last_disconnect_ts > 0:
                self._reconnect_gap_s = now - self._last_disconnect_ts
            self._last_disconnect_ts = now

    def record_window(self, events_in: int, window_secs: float, backlog: int,
                       dropped: int = 0) -> dict:
        """Record a STATS window and update health state.

        Called once per ~60s STATS cycle. Returns a snapshot dict.
        """
        with self._lock:
            return self._record_window_locked(events_in, window_secs, backlog, dropped)

    def _record_window_locked(self, events_in: int, window_secs: float, backlog: int,
                              dropped: int = 0) -> dict:
        self._windows_seen += 1
        window_secs = max(window_secs, 1.0)  # avoid div-by-zero
        self._current_eps = events_in / window_secs

        # --- EWMA baseline (frozen during DEGRADED) ---
        if self._state != DEGRADED:
            if self._baseline_eps == 0.0:
                self._baseline_eps = self._current_eps
            else:
                self._baseline_eps = (
                    EWMA_ALPHA * self._current_eps
                    + (1 - EWMA_ALPHA) * self._baseline_eps
                )

        # --- Drop fraction ---
        total_received = events_in + dropped
        self._drop_frac = dropped / max(total_received, 1)

        # --- Coverage ---
        coverage = min(
            self._current_eps / max(self._baseline_eps, _EPS),
            1.0,
        )

        # --- Backlog direction ---
        backlog_growing = False
        backlog_stable = False
        if self._prev_backlog is not None:
            delta = backlog - self._prev_backlog
            if delta > BACKLOG_GROWTH_THRESHOLD:
                backlog_growing = True
            elif abs(delta) < BACKLOG_GROWTH_THRESHOLD:
                backlog_stable = True
        self._prev_backlog = backlog

        # --- Degradation triggers (OR logic, independent counters) ---
        new_reasons = []

        # Trigger 1: low coverage
        if coverage < COVERAGE_LOW_THRESHOLD:
            self._low_coverage_streak += 1
        else:
            self._low_coverage_streak = 0
        if self._low_coverage_streak >= CONSECUTIVE_BAD_WINDOWS:
            new_reasons.append("platform_low_eps")

        # Trigger 2: high lag
        if self._stream_lag_s > LAG_HIGH_THRESHOLD_S:
            self._high_lag_streak += 1
        else:
            self._high_lag_streak = 0
        if self._high_lag_streak >= CONSECUTIVE_BAD_WINDOWS:
            new_reasons.append("lag_high")

        # Trigger 3: high drop rate (losing events at ingest)
        if self._drop_frac > DROP_FRAC_HIGH_THRESHOLD:
            self._high_drop_streak += 1
        else:
            self._high_drop_streak = 0
        if self._high_drop_streak >= CONSECUTIVE_BAD_WINDOWS:
            new_reasons.append("high_drop_rate")

        # Trigger 4: consumer backlog growing AND lag rising
        if backlog_growing and self._stream_lag_s > LAG_RECOVER_THRESHOLD_S:
            self._backlog_growing_streak += 1
        else:
            self._backlog_growing_streak = 0
        if self._backlog_growing_streak >= CONSECUTIVE_BAD_WINDOWS:
            new_reasons.append("consumer_backlog")

        self._gate_reasons = new_reasons

        # --- State machine ---
        if self._state == WARMING_UP:
            if self._windows_seen >= WARMUP_WINDOWS:
                if new_reasons:
                    self._state = DEGRADED
                    self._pending_detection = "platform_degraded"
                    self._degraded_heartbeat_counter = 0
                    LOG.warning(
                        "platform health: WARMING_UP -> DEGRADED reasons=%s",
                        new_reasons,
                    )
                else:
                    self._state = OK
                    LOG.info("platform health: WARMING_UP -> OK")

        elif self._state == OK:
            if new_reasons:
                self._state = DEGRADED
                self._pending_detection = "platform_degraded"
                self._degraded_heartbeat_counter = 0
                LOG.warning(
                    "platform health: OK -> DEGRADED reasons=%s coverage=%.1f%% lag=%.1fs",
                    new_reasons,
                    coverage * 100,
                    self._stream_lag_s,
                )
                self._recovery_streak = 0
                self._recalibration_remaining = 0

        elif self._state == DEGRADED:
            # Periodic heartbeat every 10 windows while degraded
            self._degraded_heartbeat_counter += 1
            if self._degraded_heartbeat_counter >= 10:
                self._pending_detection = "platform_degraded"
                self._degraded_heartbeat_counter = 0

            # Recovery check: all triggers must be clear
            recovery_ok = (
                coverage > COVERAGE_RECOVER_THRESHOLD
                and self._stream_lag_s < LAG_RECOVER_THRESHOLD_S
                and backlog_stable
            )
            if recovery_ok:
                self._recovery_streak += 1
            else:
                self._recovery_streak = 0

            if self._recovery_streak >= CONSECUTIVE_GOOD_WINDOWS:
                self._state = OK
                self._recalibration_remaining = RECALIBRATION_WINDOWS
                self._gate_reasons = []
                self._pending_detection = "platform_recovered"
                LOG.info(
                    "platform health: DEGRADED -> OK (recalibration=%d windows)",
                    RECALIBRATION_WINDOWS,
                )
                self._recovery_streak = 0

        # Tick down recalibration
        if self._recalibration_remaining > 0:
            self._recalibration_remaining -= 1

        return self._snapshot_locked(coverage)

    def is_degraded(self) -> bool:
        """True if rechecks should be gated.

        True during DEGRADED, recalibration, or WARMING_UP with insane lag.
        """
        with self._lock:
            if self._state == DEGRADED:
                return True
            if self._recalibration_remaining > 0:
                return True
            if self._state == WARMING_UP and self._stream_lag_s > WARMUP_LAG_ABSOLUTE_S:
                return True
            return False

    def get_health_snapshot(self) -> dict:
        """Full metrics snapshot for API/annotation."""
        with self._lock:
            coverage = min(
                self._current_eps / max(self._baseline_eps, _EPS),
                1.0,
            )
            return self._snapshot_locked(coverage)

    def get_gate_reasons(self) -> list:
        """All active gate reasons."""
        with self._lock:
            return list(self._gate_reasons)

    def get_detection(self, ts_start: str = "", ts_end: str = "", window: str = "1m"):
        """Return a DetectionEnvelope on state transitions or periodic heartbeats.

        Emits on:
        - State transitions (OK→DEGRADED, DEGRADED→OK)
        - Periodic heartbeat every 10 windows while DEGRADED

        Returns None when no detection is warranted.
        """
        from .detection import (
            SubjectRef, build_envelope,
        )

        with self._lock:
            # Only emit on transitions or periodic heartbeats during degraded
            if not self._pending_detection:
                return None

            snap = self._snapshot_locked(
                min(self._current_eps / max(self._baseline_eps, _EPS), 1.0)
            )
            detection_type = self._pending_detection
            self._pending_detection = ""

        severity = "high" if detection_type == "platform_degraded" else "info"
        score = 1.0 if detection_type == "platform_degraded" else 0.0

        explain = {
            "health_state": snap["health_state"],
            "coverage_pct": snap["coverage_pct"],
            "current_eps": snap["current_eps"],
            "baseline_eps": snap["baseline_eps"],
            "stream_lag_s": snap["stream_lag_s"],
            "gate_reasons": snap["gate_reasons"],
            "baseline_kind": "ewma",
            "baseline_windows_seen": snap["windows_seen"],
        }

        return build_envelope(
            detector_id="platform_health",
            detector_version="v1",
            ts_start=ts_start,
            ts_end=ts_end,
            window=window,
            subject=SubjectRef("global", ""),
            detection_type=detection_type,
            score=score,
            severity=severity,
            explain=explain,
            evidence=(),
            window_fingerprint="",
            config_hash="",
        )

    def _snapshot_locked(self, coverage: float) -> dict:
        return {
            "health_state": self._state,
            "coverage_pct": round(coverage, 4),
            "current_eps": round(self._current_eps, 1),
            "baseline_eps": round(self._baseline_eps, 1),
            "stream_lag_s": round(self._stream_lag_s, 1),
            "drop_frac": round(self._drop_frac, 4),
            "reconnect_count": self._reconnect_count,
            "reconnect_gap_s": round(self._reconnect_gap_s, 1),
            "gate_reasons": list(self._gate_reasons),
            "windows_seen": self._windows_seen,
            "recalibration_remaining": self._recalibration_remaining,
            "baseline_restored": self._baseline_restored,
        }

    # --- Baseline persistence ---

    CHECKPOINT_VERSION = 1
    CHECKPOINT_INTERVAL_WINDOWS = 5  # checkpoint every ~5 minutes
    MAX_CHECKPOINT_AGE_S = 3600  # 1 hour — older than this, cold start

    def checkpoint(self) -> dict:
        """Serialize baseline state for persistence.

        Only the sufficient statistics needed to avoid cold start.
        Not the full runtime state — streaks, gates, and pending
        detections are ephemeral and should re-derive.
        """
        with self._lock:
            return {
                "version": self.CHECKPOINT_VERSION,
                "baseline_eps": self._baseline_eps,
                "current_eps": self._current_eps,
                "stream_lag_s": self._stream_lag_s,
                "windows_seen": self._windows_seen,
                "state": self._state,
                "checkpoint_at": time.time(),
            }

    def restore(self, data: dict) -> bool:
        """Restore baseline state from a checkpoint.

        Returns True if restored, False if checkpoint was rejected
        (incompatible version, too stale, or invalid).
        """
        if not data:
            return False

        if data.get("version") != self.CHECKPOINT_VERSION:
            LOG.info("baseline checkpoint version mismatch, cold start")
            return False

        age = time.time() - data.get("checkpoint_at", 0)
        if age > self.MAX_CHECKPOINT_AGE_S:
            LOG.info("baseline checkpoint too stale (%.0fs old), cold start", age)
            return False

        baseline_eps = data.get("baseline_eps", 0)
        if baseline_eps <= 0:
            LOG.info("baseline checkpoint has no baseline_eps, cold start")
            return False

        with self._lock:
            self._baseline_eps = baseline_eps
            self._current_eps = data.get("current_eps", baseline_eps)
            self._stream_lag_s = data.get("stream_lag_s", 0)
            self._windows_seen = data.get("windows_seen", WARMUP_WINDOWS)
            # Start in OK regardless of prior state — let the state machine
            # re-derive DEGRADED if conditions warrant it. This avoids
            # restoring into a stale degraded state.
            if self._windows_seen >= WARMUP_WINDOWS:
                self._state = OK
            self._baseline_restored = True
            LOG.info(
                "baseline restored: eps=%.1f windows=%d age=%.0fs",
                self._baseline_eps, self._windows_seen, age,
            )
            return True

    def maybe_checkpoint(self, path: Path) -> bool:
        """Checkpoint to file if enough windows have passed since last save.

        Called after record_window(). Writes atomically.
        """
        with self._lock:
            if self._windows_seen % self.CHECKPOINT_INTERVAL_WINDOWS != 0:
                return False
            if self._state == WARMING_UP and self._windows_seen < WARMUP_WINDOWS:
                return False  # don't checkpoint before first warmup

        return self._write_checkpoint(path)

    def _write_checkpoint(self, path: Path) -> bool:
        """Atomic write of checkpoint to disk."""
        data = self.checkpoint()
        tmp = path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(data))
            tmp.rename(path)
            return True
        except OSError as e:
            LOG.warning("checkpoint write failed: %s", e)
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass
            return False

    def force_checkpoint(self, path: Path) -> bool:
        """Force a checkpoint write (for clean shutdown)."""
        return self._write_checkpoint(path)


# --- Module-level singleton ---
_instance = PlatformHealth()

# Default checkpoint path (alongside the DB)
_DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CHECKPOINT_PATH = _DATA_DIR / "baseline_checkpoint.json"


def record_event_time(time_us: int):
    _instance.record_event_time(time_us)


def record_reconnect():
    _instance.record_reconnect()


def record_window(events_in: int, window_secs: float, backlog: int,
                   dropped: int = 0) -> dict:
    return _instance.record_window(events_in, window_secs, backlog, dropped)


def is_degraded() -> bool:
    return _instance.is_degraded()


def get_health_snapshot() -> dict:
    return _instance.get_health_snapshot()


def get_gate_reasons() -> list:
    return _instance.get_gate_reasons()


def get_detection(ts_start: str = "", ts_end: str = "", window: str = "1m"):
    return _instance.get_detection(ts_start, ts_end, window)


def restore_baseline(path: Path | None = None) -> bool:
    """Restore baseline from checkpoint file. Call once at startup."""
    path = path or CHECKPOINT_PATH
    try:
        data = json.loads(path.read_text())
        return _instance.restore(data)
    except FileNotFoundError:
        LOG.info("no baseline checkpoint at %s, cold start", path)
        return False
    except (json.JSONDecodeError, OSError) as e:
        LOG.warning("baseline checkpoint unreadable: %s, cold start", e)
        return False


def maybe_checkpoint(path: Path | None = None) -> bool:
    """Checkpoint baseline if interval has elapsed. Call after record_window."""
    return _instance.maybe_checkpoint(path or CHECKPOINT_PATH)


def force_checkpoint(path: Path | None = None) -> bool:
    """Force checkpoint write (clean shutdown)."""
    return _instance.force_checkpoint(path or CHECKPOINT_PATH)


def _reset():
    """Reset singleton state (for testing)."""
    _instance._reset()
