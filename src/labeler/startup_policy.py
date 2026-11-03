"""Resolve background-mutation controls before application startup."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


class StartupConfigurationError(RuntimeError):
    """A startup control has an unsupported value."""


# Truth sets deliberately preserve main.py's historical interpretation.
_CONTROLS = {
    "FIREHOSE_AUTO_START": frozenset({"1"}),
    "FIREHOSE_LABEL_INGEST": frozenset({"1"}),
    "ENABLE_LONGITUDINAL_RECHECK": frozenset({"1"}),
    "ENABLE_CLAIM_RECHECK": frozenset({"1"}),
    "ENABLE_FACTS_EXPORT": frozenset({"1", "true"}),
    "ENABLE_MAINTENANCE": frozenset({"1", "true"}),
    "ENABLE_RETENTION": frozenset({"1", "true"}),
}
_FALSE_VALUES = frozenset({"", "0", "false"})


def _parse(name: str, value: str | None, true_values: frozenset[str]) -> bool:
    normalized = "" if value is None else value.strip().lower()
    if normalized in true_values:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise StartupConfigurationError(
        f"{name} must be one of: "
        + ", ".join(sorted(true_values | _FALSE_VALUES))
    )


@dataclass(frozen=True)
class StartupPolicy:
    operator_paused: bool
    configured: dict[str, bool]
    effective: dict[str, bool]
    suppressed: tuple[str, ...]

    def public_record(self) -> dict:
        """Return selected non-secret values for logs and health output."""
        return {
            "operator_paused": self.operator_paused,
            "configured": dict(self.configured),
            "effective": dict(self.effective),
            "suppressed": list(self.suppressed),
        }


def resolve_startup_policy(environ: Mapping[str, str]) -> StartupPolicy:
    paused = _parse(
        "DRIFTWATCH_OPERATOR_PAUSED",
        environ.get("DRIFTWATCH_OPERATOR_PAUSED"),
        frozenset({"1", "true"}),
    )
    configured = {
        name: _parse(name, environ.get(name), true_values)
        for name, true_values in _CONTROLS.items()
    }
    suppressed = tuple(sorted(
        name for name, value in configured.items() if paused and value
    ))
    effective = {
        name: value and not paused for name, value in configured.items()
    }
    return StartupPolicy(paused, configured, effective, suppressed)
