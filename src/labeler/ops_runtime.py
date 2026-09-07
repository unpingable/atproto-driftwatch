"""Small atomic producer-fact files consumed by the local ops status view."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import DATA_DIR


def fact_path(producer: str, data_dir: Path | None = None) -> Path:
    return (data_dir or DATA_DIR) / f"ops-{producer}.json"


def write_fact(producer: str, payload: dict[str, Any], data_dir: Path | None = None) -> dict[str, Any]:
    path = fact_path(producer, data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "schema": "driftwatch.ops.producer_fact/v1",
        "producer": producer,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return value


def read_fact(producer: str, data_dir: Path | None = None) -> dict[str, Any] | None:
    path = fact_path(producer, data_dir)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if value.get("schema") != "driftwatch.ops.producer_fact/v1" or value.get("producer") != producer:
        return None
    return value
