"""Typed key-value settings store backed by the ``app_settings`` table.

Replaces the import-time globals (``SSH_TIMEOUT``, ``CACHE_FRESH_SEC``,
``TEAM_NAME``, ``AIHUB_OPENSEARCH_URL``, etc.) that used to live in
``server/config.py``. Every well-known key is registered in
:data:`server.schema.APP_SETTINGS_DEFAULTS` with a default value, type
coercer, and human-readable description.

Reads are cheap thanks to a small in-process cache that is invalidated
on every write. The cache is process-local — gunicorn and the MCP
process each maintain their own copy, refreshed when they perform a
write. Writes from one process are not seen by the other process until
its cache TTL expires (10 s by default), which matches the existing
multi-process semantics for clusters/projects/etc.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Dict, Optional

from .db import db_write, get_db
from .schema import APP_SETTINGS_DEFAULTS

_log = logging.getLogger(__name__)


_CACHE_TTL_SEC = 10.0
_cache: Dict[str, Any] = {}
_cache_loaded_ts: float = 0.0
_cache_lock = threading.Lock()


def _load_cache() -> None:
    """Refresh the in-process cache from the DB.

    Always loads the full table — tiny (single-digit rows in practice)
    so multiple round-trips would cost more than the bulk read. If the
    table doesn't exist yet (very early import — before ``init_db()``
    has run), the cache stays empty and registered defaults take over.
    """
    global _cache_loaded_ts
    fresh: Dict[str, Any] = {}
    try:
        con = get_db()
        rows = con.execute("SELECT key, value_json FROM app_settings").fetchall()
        for r in rows:
            try:
                fresh[r["key"]] = json.loads(r["value_json"])
            except json.JSONDecodeError:
                _log.warning("app_settings.%s holds invalid JSON, treating as missing", r["key"])
    except Exception as exc:
        # Most likely "no such table" because init_db() hasn't run yet.
        # Stay quiet — the registered defaults are the right answer until
        # the schema lands. We refresh again on every TTL window.
        _log.debug("settings cache load failed (will retry): %s", exc)
    with _cache_lock:
        _cache.clear()
        _cache.update(fresh)
        _cache_loaded_ts = time.monotonic()


def _maybe_refresh() -> None:
    # Always refresh after the TTL — using ``not _cache`` as a trigger
    # would cause a fresh DB read on every access when the table is
    # genuinely empty (which is the common case in tests + new installs).
    if (time.monotonic() - _cache_loaded_ts) > _CACHE_TTL_SEC:
        _load_cache()


def invalidate_cache() -> None:
    """Force the next ``get_setting()`` call to re-read from the DB."""
    global _cache_loaded_ts
    with _cache_lock:
        _cache_loaded_ts = 0.0


def _coerce(key: str, value: Any) -> Any:
    """Apply the registered coercer for ``key`` if any.

    Unknown keys are passed through unchanged so callers can persist
    arbitrary JSON without registering it first (useful for plugin /
    experimental settings).
    """
    if key in APP_SETTINGS_DEFAULTS:
        _, coercer, _ = APP_SETTINGS_DEFAULTS[key]
        return coercer(value)
    return value


def get_setting(key: str, default: Any = None) -> Any:
    """Return the current value of ``key``.

    Resolution order:
      1. Stored value (if present in the ``app_settings`` table).
      2. Registered default in :data:`APP_SETTINGS_DEFAULTS`.
      3. Caller-provided ``default`` (only used when the key is unknown
         AND nothing is stored).
    """
    _maybe_refresh()
    with _cache_lock:
        if key in _cache:
            try:
                return _coerce(key, _cache[key])
            except (TypeError, ValueError) as exc:
                _log.warning("app_settings.%s coercion failed (%s); falling back to default", key, exc)
    if key in APP_SETTINGS_DEFAULTS:
        registered_default, coercer, _ = APP_SETTINGS_DEFAULTS[key]
        return coercer(registered_default)
    return default


def set_setting(key: str, value: Any) -> Dict[str, Any]:
    """Persist ``value`` under ``key`` and refresh the cache.

    Coercion runs first so bad input fails loudly with the offending
    key in the error message. The stored value is whatever JSON
    serialises from the coerced result.
    """
    if not key:
        return {"status": "error", "error": "key is required"}
    try:
        coerced = _coerce(key, value)
    except (TypeError, ValueError) as exc:
        return {"status": "error", "error": f"{key}: {exc}"}
    try:
        payload = json.dumps(coerced)
    except (TypeError, ValueError) as exc:
        return {"status": "error", "error": f"{key} value is not JSON serialisable: {exc}"}

    with db_write() as con:
        con.execute(
            """
            INSERT INTO app_settings (key, value_json, updated_at, description)
            VALUES (?, ?, datetime('now'), COALESCE(
                (SELECT description FROM app_settings WHERE key=?),
                ?
            ))
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at
            """,
            (key, payload, key, _description_for(key)),
        )
    invalidate_cache()
    return {"status": "ok", "key": key, "value": coerced}


def _description_for(key: str) -> str:
    if key in APP_SETTINGS_DEFAULTS:
        return APP_SETTINGS_DEFAULTS[key][2]
    return ""


def delete_setting(key: str) -> Dict[str, Any]:
    """Remove a stored override so ``get_setting()`` falls back to the
    registered default again. No-op for unknown keys."""
    with db_write() as con:
        cur = con.execute("DELETE FROM app_settings WHERE key=?", (key,))
    invalidate_cache()
    return {"status": "ok", "deleted": key, "rows": cur.rowcount}


def list_settings(*, include_defaults: bool = True) -> Dict[str, Dict[str, Any]]:
    """Return ``{key: {value, default, description, source}}`` for every
    setting.

    ``source`` is ``"db"`` when the value comes from a stored row and
    ``"default"`` when the registered default is in effect. With
    ``include_defaults=False``, only stored rows are returned (useful
    for export / backup).
    """
    _maybe_refresh()
    out: Dict[str, Dict[str, Any]] = {}
    with _cache_lock:
        stored = dict(_cache)

    keys = set(stored)
    if include_defaults:
        keys |= set(APP_SETTINGS_DEFAULTS)

    for key in sorted(keys):
        if key in stored:
            try:
                value = _coerce(key, stored[key])
                source = "db"
            except (TypeError, ValueError):
                value = stored[key]
                source = "db-raw"
        else:
            registered_default, coercer, _ = APP_SETTINGS_DEFAULTS[key]
            value = coercer(registered_default)
            source = "default"
        if key in APP_SETTINGS_DEFAULTS:
            registered_default, _, description = APP_SETTINGS_DEFAULTS[key]
        else:
            registered_default, description = None, ""
        out[key] = {
            "value": value,
            "default": registered_default,
            "description": description,
            "source": source,
        }
    return out


# ─── Typed accessors for the most-used keys ──────────────────────────────────
#
# These exist so callers don't have to remember the string keys and the
# returned type is unambiguous. Adding more is cheap — just add a tiny
# wrapper here when a key starts being read in many places.

def get_team_name() -> str:
    return get_setting("team_name")


def get_aihub_opensearch_url() -> str:
    return get_setting("aihub_opensearch_url")


def get_dashboard_url() -> str:
    return get_setting("dashboard_url")


def get_aihub_cache_ttl() -> int:
    return get_setting("aihub_cache_ttl_sec")


def get_wds_snapshot_interval() -> int:
    return get_setting("wds_snapshot_interval_sec")


def get_ssh_timeout() -> int:
    return get_setting("ssh_timeout")


def get_cache_fresh_sec() -> int:
    return get_setting("cache_fresh_sec")


def get_stats_interval() -> int:
    return get_setting("stats_interval_sec")


def get_backup_interval_hours() -> int:
    return get_setting("backup_interval_hours")


def get_backup_max_keep() -> int:
    return get_setting("backup_max_keep")


def get_sdk_ingest_token() -> str:
    return get_setting("sdk_ingest_token")
