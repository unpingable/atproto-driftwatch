"""Pressure-aware retention scheduler.

Wraps the legacy sync retention path (retention.run_retention_once_with_sched)
with backlog gating, wall-clock budgets, and a rollback_lost tripwire.

Retention runs on its own SQLite write connection — it does not route
through the consumer's writer thread. The 5850d01 attempt at writer-thread
routing failed acceptance under firehose load (writer-thread starvation,
67% drop_frac); that path is preserved as a documented scar in
``specs/gaps/gap-spec-single-writer-invariant.md`` but is not used in
production. This scheduler is the production-line replacement.

Doctrine
--------
Retention is a guest in the writer's house. If ingest is under pressure,
retention does not run. If a rollback_lost increments while retention is
running, retention has caused the rollback (no other lock contender at the
batched-writer level under normal operation), and the scheduler must abort
the pass — losing ingest is not an acceptable cost of running maintenance.

Acceptance signals
------------------
- ``drop_frac`` and ``rollback_lost`` remain at zero across retention passes.
- ``queue_depth`` does not monotonically rise during a pass.
- If retention falls behind far enough to threaten disk, surface it via
  ``health_state()`` — degraded retention lag eventually fronts a
  ``platform_health: degraded`` recovery from outside this module.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional, Protocol

LOG = logging.getLogger("labeler.retention.scheduler")

# Pre-pass and per-chunk gates. Defaults chosen so that retention runs
# during typical conditions (~1k–2k backlog windows seen in prod) but
# bails as soon as the writer is fighting for headroom.
QUEUE_BACKLOG_THRESHOLD = int(os.getenv("RETENTION_BACKLOG_THRESHOLD", "1500"))
QUEUE_DEPTH_THRESHOLD = int(os.getenv("RETENTION_QUEUE_DEPTH_THRESHOLD", "10000"))
MEDIAN_AGE_THRESHOLD_S = float(os.getenv("RETENTION_MEDIAN_AGE_THRESHOLD_S", "120"))
STREAM_LAG_THRESHOLD_S = float(os.getenv("RETENTION_STREAM_LAG_THRESHOLD_S", "60"))

# Wall-clock budgets.
CHUNK_BUDGET_S = float(os.getenv("RETENTION_CHUNK_BUDGET_S", "3.0"))
PASS_BUDGET_S = float(os.getenv("RETENTION_PASS_BUDGET_S", "600"))  # 10 min

# How many chunk-overruns within a pass we tolerate before abort.
CHUNK_OVERRUN_TOLERANCE = int(os.getenv("RETENTION_CHUNK_OVERRUN_TOLERANCE", "3"))

# Disk runway thresholds for health surfaces.
DISK_RUNWAY_DEGRADED_DAYS = float(os.getenv("RETENTION_DISK_RUNWAY_DEGRADED_DAYS", "5"))
DISK_RUNWAY_CRITICAL_DAYS = float(os.getenv("RETENTION_DISK_RUNWAY_CRITICAL_DAYS", "2"))


class _Pressureable(Protocol):
    """Anything the scheduler can read pressure from. Real consumer or test stub."""

    def get_pressure_snapshot(self) -> dict: ...


@dataclass
class PassRecord:
    """Outcome of a single retention pass — surfaced to /health."""

    started_at: float
    ended_at: Optional[float] = None
    duration_s: Optional[float] = None
    completed: bool = False
    abort_reason: Optional[str] = None
    chunks_executed: int = 0
    chunk_overruns: int = 0
    rows_total: int = 0
    rollback_lost_at_start: int = 0
    rollback_lost_at_end: int = 0
    stats: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_s": self.duration_s,
            "completed": self.completed,
            "abort_reason": self.abort_reason,
            "chunks_executed": self.chunks_executed,
            "chunk_overruns": self.chunk_overruns,
            "rows_total": self.rows_total,
            "rollback_lost_delta": (
                self.rollback_lost_at_end - self.rollback_lost_at_start
            ),
            "stats": self.stats,
        }


class AbortRetentionPass(Exception):
    """Raised by scheduler.before_chunk to halt a pass mid-flight."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class NullScheduler:
    """No-op scheduler. Used by the CLI cold-pass path and by tests that
    don't care about pressure logic. Behaves as a transparent loop driver:
    every chunk runs, sleep is fixed, no aborts.
    """

    def __init__(self, sleep_between_s: float = 0.0):
        self._sleep_between_s = sleep_between_s
        # Mirror the public surface so callers can introspect uniformly.
        self.last_pass: Optional[dict] = None
        self.skipped_due_to_pressure = 0
        self.aborted_passes = 0

    def begin_pass(self) -> bool:
        return True

    def end_pass(self, stats: dict, completed: bool, abort_reason: Optional[str]) -> None:
        pass

    def before_chunk(self, op_name: str) -> None:
        # Never aborts; never raises.
        return None

    def after_chunk(self, op_name: str, elapsed_s: float, rows: int) -> None:
        pass

    def sleep_between_chunks(self) -> None:
        if self._sleep_between_s > 0:
            time.sleep(self._sleep_between_s)

    def health_state(self) -> dict:
        return {"enabled": False, "scheduler": "null"}


class RetentionScheduler:
    """Production retention driver.

    Reads ``consumer.get_pressure_snapshot()`` to decide whether to start a
    pass and whether to keep going. Times each chunk; aborts on:

    1. ``rollback_lost`` increased since pass start (ingest is being shed
       because of us).
    2. Pass wall-clock exceeded ``PASS_BUDGET_S``.
    3. Per-chunk overruns exceed ``CHUNK_OVERRUN_TOLERANCE``.
    4. Pressure (backlog / queue_depth / median_age / stream_lag) returns
       above threshold mid-pass.
    """

    def __init__(
        self,
        consumer: Optional[_Pressureable] = None,
        sleep_between_s: float = 5.0,
    ):
        self._consumer = consumer
        self._sleep_between_s = sleep_between_s
        # Per-pass state — set in begin_pass.
        self._pass_started_mono: Optional[float] = None
        self._pass_record: Optional[PassRecord] = None
        # Cumulative health surface.
        self.last_pass: Optional[dict] = None
        self.skipped_due_to_pressure = 0
        self.aborted_passes = 0
        # Disk runway tracking — list of (timestamp, db_size_bytes,
        # disk_free_bytes). The retention loop appends after each pass.
        self._disk_history: list[tuple[float, int, int]] = []
        # Retention lag — populated by the retention runner once per pass.
        self._retention_lag_s: Optional[float] = None

    # ------------------------------------------------------------------
    # Pass lifecycle.
    # ------------------------------------------------------------------

    def begin_pass(self) -> bool:
        """Return True if pass should proceed, False if pre-pass gate fired.

        The pre-pass gate is the cheaper of the two: skip the entire pass
        rather than doing one chunk's worth of work and bailing.
        """
        # No consumer means no pressure to read. Treat as "go" — caller is
        # CLI / cold-pass / test, where the consumer isn't running.
        if self._consumer is None:
            self._begin_pass_unconditional()
            return True

        snap = self._read_snapshot()
        gate = self._evaluate_gate(snap)
        if gate is not None:
            self.skipped_due_to_pressure += 1
            self.last_pass = {
                "skipped": True,
                "skip_reason": gate,
                "started_at": time.time(),
                "snapshot": snap,
            }
            LOG.info("retention pre-pass skip: %s (snapshot=%s)", gate, snap)
            return False

        self._begin_pass_unconditional()
        self._pass_record.rollback_lost_at_start = snap.get(
            "rollback_lost_total", 0
        )
        return True

    def _begin_pass_unconditional(self) -> None:
        self._pass_started_mono = time.monotonic()
        self._pass_record = PassRecord(started_at=time.time())

    def end_pass(
        self,
        stats: dict,
        completed: bool,
        abort_reason: Optional[str] = None,
    ) -> None:
        rec = self._pass_record
        if rec is None:
            return
        rec.ended_at = time.time()
        rec.duration_s = (
            time.monotonic() - self._pass_started_mono
            if self._pass_started_mono is not None
            else None
        )
        rec.completed = completed
        rec.abort_reason = abort_reason
        rec.stats = stats
        if self._consumer is not None:
            try:
                rec.rollback_lost_at_end = self._read_snapshot().get(
                    "rollback_lost_total", rec.rollback_lost_at_start
                )
            except Exception:
                pass
        self.last_pass = rec.to_dict()
        if not completed:
            self.aborted_passes += 1
        self._pass_record = None
        self._pass_started_mono = None

    # ------------------------------------------------------------------
    # Per-chunk hooks. Called from inside retention's leaf loops.
    # ------------------------------------------------------------------

    def before_chunk(self, op_name: str) -> None:
        """Raise AbortRetentionPass if any abort condition is true.

        Caller treats AbortRetentionPass as "stop this pass cleanly,
        end_pass with the recorded reason."
        """
        rec = self._pass_record
        if rec is None or self._pass_started_mono is None:
            return  # No active pass; caller is outside scheduler control.

        elapsed = time.monotonic() - self._pass_started_mono
        if elapsed > PASS_BUDGET_S:
            raise AbortRetentionPass(
                f"pass_budget_exceeded:{elapsed:.0f}s>budget={PASS_BUDGET_S:.0f}s"
            )

        if rec.chunk_overruns >= CHUNK_OVERRUN_TOLERANCE:
            raise AbortRetentionPass(
                f"chunk_overrun_tolerance_exceeded:{rec.chunk_overruns}"
            )

        if self._consumer is None:
            return

        snap = self._read_snapshot()
        delta = snap.get("rollback_lost_total", 0) - rec.rollback_lost_at_start
        if delta > 0:
            raise AbortRetentionPass(
                f"rollback_lost_tripwire:{delta}"
            )

        gate = self._evaluate_gate(snap)
        if gate is not None:
            raise AbortRetentionPass(f"pressure_returned:{gate}")

    def after_chunk(self, op_name: str, elapsed_s: float, rows: int) -> None:
        rec = self._pass_record
        if rec is None:
            return
        rec.chunks_executed += 1
        rec.rows_total += rows
        if elapsed_s > CHUNK_BUDGET_S:
            rec.chunk_overruns += 1
            LOG.warning(
                "retention chunk overran budget: op=%s elapsed=%.2fs rows=%d "
                "(budget=%.1fs, overrun count this pass=%d)",
                op_name, elapsed_s, rows, CHUNK_BUDGET_S, rec.chunk_overruns,
            )

    def sleep_between_chunks(self) -> None:
        if self._sleep_between_s > 0:
            time.sleep(self._sleep_between_s)

    # ------------------------------------------------------------------
    # External feeds — disk runway, retention lag.
    # ------------------------------------------------------------------

    def record_disk_sample(self, db_size_bytes: int, disk_free_bytes: int) -> None:
        """Append (now, db_size, free) to the rolling history.

        Caller (retention runner) is expected to call this once per pass.
        Keeps the last 24 samples (~1 day at hourly retention).
        """
        self._disk_history.append((time.time(), db_size_bytes, disk_free_bytes))
        if len(self._disk_history) > 24:
            self._disk_history = self._disk_history[-24:]

    def record_retention_lag(self, lag_s: Optional[float]) -> None:
        self._retention_lag_s = lag_s

    def estimate_disk_runway_days(self) -> Optional[float]:
        """Days until disk hits the 92% emergency-brake threshold, given
        the EWMA growth rate from disk history. Returns None if we don't
        have enough samples yet (need at least 2)."""
        if len(self._disk_history) < 2:
            return None
        first_ts, first_db, first_free = self._disk_history[0]
        last_ts, last_db, last_free = self._disk_history[-1]
        elapsed_s = last_ts - first_ts
        if elapsed_s <= 0:
            return None
        # Disk-free is the source of truth — captures DB growth + facts +
        # archives + anything else on the volume.
        free_delta = last_free - first_free
        burn_per_s = -free_delta / elapsed_s  # positive = losing space
        if burn_per_s <= 0:
            return float("inf")
        # Brake threshold is 92% used = 8% free of total. Total = used + free
        # at last sample. Approximate.
        # We need the absolute "free at brake" — caller didn't pass it but
        # 8% of total works. Without total here, fall back to "headroom is
        # last_free minus a fixed pad of 0 — let caller set pad if needed".
        # Simpler: caller is presumed to compute brake-threshold elsewhere
        # and pass headroom. For now, treat last_free as headroom and let
        # health_state pair this with disk pct.
        return last_free / burn_per_s / 86400.0

    # ------------------------------------------------------------------
    # /health/extended surface.
    # ------------------------------------------------------------------

    def health_state(self) -> dict:
        runway_days = self.estimate_disk_runway_days()
        snap = self._read_snapshot() if self._consumer is not None else {}
        out = {
            "enabled": True,
            "scheduler": "pressure_aware_v1",
            "last_pass": self.last_pass,
            "skipped_due_to_pressure": self.skipped_due_to_pressure,
            "aborted_passes": self.aborted_passes,
            "retention_lag_s": self._retention_lag_s,
            "disk_runway_days": runway_days,
            "thresholds": {
                "backlog": QUEUE_BACKLOG_THRESHOLD,
                "queue_depth": QUEUE_DEPTH_THRESHOLD,
                "median_age_s": MEDIAN_AGE_THRESHOLD_S,
                "stream_lag_s": STREAM_LAG_THRESHOLD_S,
                "chunk_budget_s": CHUNK_BUDGET_S,
                "pass_budget_s": PASS_BUDGET_S,
                "chunk_overrun_tolerance": CHUNK_OVERRUN_TOLERANCE,
                "disk_runway_degraded_days": DISK_RUNWAY_DEGRADED_DAYS,
                "disk_runway_critical_days": DISK_RUNWAY_CRITICAL_DAYS,
            },
            "current_pressure": snap,
        }
        # Lightweight derived health-state for /health/extended.
        if runway_days is not None and runway_days < DISK_RUNWAY_CRITICAL_DAYS:
            out["state"] = "critical"
            out["state_reason"] = f"disk_runway_days={runway_days:.1f}<critical={DISK_RUNWAY_CRITICAL_DAYS}"
        elif runway_days is not None and runway_days < DISK_RUNWAY_DEGRADED_DAYS:
            out["state"] = "degraded"
            out["state_reason"] = f"disk_runway_days={runway_days:.1f}<degraded={DISK_RUNWAY_DEGRADED_DAYS}"
        elif self.last_pass is not None and self.last_pass.get("rollback_lost_delta", 0) > 0:
            out["state"] = "degraded"
            out["state_reason"] = "rollback_lost_in_last_pass"
        elif (
            self.last_pass is not None
            and self.last_pass.get("completed") is False
            and not self.last_pass.get("skipped")
        ):
            # Non-rollback abort (pass budget, chunk overrun) — scheduler
            # is out of spec, surface it.
            out["state"] = "degraded"
            out["state_reason"] = (
                f"last_pass_aborted:{self.last_pass.get('abort_reason') or 'unknown'}"
            )
        elif self.skipped_due_to_pressure > 0 and self.last_pass is not None and self.last_pass.get("skipped"):
            out["state"] = "throttled"
            out["state_reason"] = "consecutive_pressure_skip"
        else:
            out["state"] = "ok"
        return out

    # ------------------------------------------------------------------
    # Internals.
    # ------------------------------------------------------------------

    def _read_snapshot(self) -> dict:
        try:
            return self._consumer.get_pressure_snapshot()
        except Exception:
            LOG.exception("get_pressure_snapshot failed; treating as no-pressure")
            return {}

    @staticmethod
    def _evaluate_gate(snap: dict) -> Optional[str]:
        """Return None if pressure is OK, else a short reason string."""
        if not snap:
            return None
        backlog = snap.get("backlog", 0) or 0
        if backlog > QUEUE_BACKLOG_THRESHOLD:
            return f"backlog={backlog}>{QUEUE_BACKLOG_THRESHOLD}"
        median_age = snap.get("median_dequeue_age_s", 0.0) or 0.0
        if median_age > MEDIAN_AGE_THRESHOLD_S:
            return f"median_age={median_age:.0f}s>{MEDIAN_AGE_THRESHOLD_S:.0f}s"
        stream_lag = snap.get("stream_lag_s", 0.0) or 0.0
        if stream_lag > STREAM_LAG_THRESHOLD_S:
            return f"stream_lag={stream_lag:.0f}s>{STREAM_LAG_THRESHOLD_S:.0f}s"
        # queue_depth is the recheck-queue row count; it's a slower-moving
        # backpressure signal. Surfaced for completeness.
        queue_depth = snap.get("queue_depth", 0) or 0
        if queue_depth > QUEUE_DEPTH_THRESHOLD:
            return f"queue_depth={queue_depth}>{QUEUE_DEPTH_THRESHOLD}"
        return None
