"""Helpers for generating monotonically increasing experiment run IDs."""

from __future__ import annotations

import json
from pathlib import Path

_COUNTER_FILE = Path(__file__).resolve().parent / ".exp_run_ids.json"


def _read_counters() -> dict[str, int]:
    if not _COUNTER_FILE.exists():
        return {}
    try:
        raw = json.loads(_COUNTER_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        counters: dict[str, int] = {}
        for key, value in raw.items():
            if isinstance(key, str) and isinstance(value, int):
                counters[key] = value
        return counters
    except Exception:
        return {}


def next_run_id(experiment_key: str) -> int:
    counters = _read_counters()
    new_id = counters.get(experiment_key, 0) + 1
    counters[experiment_key] = new_id
    _COUNTER_FILE.write_text(
        json.dumps(counters, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return new_id
