"""Path-list and process-filter CRUD.

Two tiny tables, both shaped as ``(category, value, position)`` rows:

* ``path_bases`` — replaces ``log_search_bases``, ``nemo_run_bases``, and
  ``mount_lustre_prefixes`` from the legacy ``config.json``. ``kind``
  picks the category.
* ``process_filters`` — replaces ``local_process_filters.{include,exclude}``.
  ``mode`` picks the category.

The CRUD pattern is identical for both, so they live in one module.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

from .db import db_write, get_db


PATH_KINDS = ("log_search", "nemo_run", "mount_lustre_prefix")
FILTER_MODES = ("include", "exclude")


# ─── Path bases ─────────────────────────────────────────────────────────────

def _path_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "kind": row["kind"],
        "path": row["path"],
        "position": int(row["position"]),
    }


def list_path_bases(kind: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return path entries, optionally filtered by ``kind``.

    Ordered by ``(kind, position, path)`` so the UI can present grouped
    lists without re-sorting.
    """
    con = get_db()
    if kind is None:
        rows = con.execute(
            "SELECT * FROM path_bases ORDER BY kind, position, path"
        ).fetchall()
    else:
        if kind not in PATH_KINDS:
            return []
        rows = con.execute(
            "SELECT * FROM path_bases WHERE kind=? ORDER BY position, path",
            (kind,),
        ).fetchall()
    return [_path_row(r) for r in rows]


def list_paths(kind: str) -> List[str]:
    """Return raw path strings for one ``kind`` — convenience wrapper for
    callers that just need the legacy list-of-strings view."""
    return [p["path"] for p in list_path_bases(kind=kind)]


def add_path_base(kind: str, path: str, *, position: Optional[int] = None) -> Dict[str, Any]:
    """Append (or insert at ``position``) a path entry under ``kind``."""
    if kind not in PATH_KINDS:
        return {"status": "error", "error": f"kind must be one of {list(PATH_KINDS)}"}
    path = (path or "").strip()
    if not path:
        return {"status": "error", "error": "path is required"}

    if position is None:
        con = get_db()
        row = con.execute(
            "SELECT COALESCE(MAX(position), -1) AS m FROM path_bases WHERE kind=?",
            (kind,),
        ).fetchone()
        position = (row["m"] if row else -1) + 1

    try:
        with db_write() as con:
            cur = con.execute(
                "INSERT INTO path_bases (kind, path, position) VALUES (?, ?, ?)",
                (kind, path, int(position)),
            )
            new_id = cur.lastrowid
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"path already registered for kind={kind!r}: {path!r}"}

    return {"status": "ok", "path": {"id": new_id, "kind": kind, "path": path, "position": int(position)}}


def remove_path_base(kind: str, path: str) -> Dict[str, Any]:
    """Delete a path entry. ``path`` must match exactly."""
    if kind not in PATH_KINDS:
        return {"status": "error", "error": f"kind must be one of {list(PATH_KINDS)}"}
    with db_write() as con:
        cur = con.execute("DELETE FROM path_bases WHERE kind=? AND path=?", (kind, path))
        if cur.rowcount == 0:
            return {"status": "error", "error": f"no path {path!r} for kind={kind!r}"}
    return {"status": "ok", "removed": {"kind": kind, "path": path}}


def remove_path_base_by_id(entry_id: int) -> Dict[str, Any]:
    """Delete a path entry by row id (used by the Settings UI when paths
    contain characters that would be awkward in a URL/query string)."""
    with db_write() as con:
        cur = con.execute("DELETE FROM path_bases WHERE id=?", (int(entry_id),))
        if cur.rowcount == 0:
            return {"status": "error", "error": f"no path entry id={entry_id}"}
    return {"status": "ok", "removed": int(entry_id)}


def reorder_path_bases(kind: str, paths: List[str]) -> Dict[str, Any]:
    """Persist a new order for one ``kind``. Paths not listed keep their
    current relative order at the end."""
    if kind not in PATH_KINDS:
        return {"status": "error", "error": f"kind must be one of {list(PATH_KINDS)}"}
    if not isinstance(paths, list):
        return {"status": "error", "error": "paths must be a list of strings"}
    existing = [p["path"] for p in list_path_bases(kind=kind)]
    unknown = [p for p in paths if p not in existing]
    if unknown:
        return {"status": "error", "error": f"unknown paths for kind={kind!r}: {unknown}"}
    seen = set(paths)
    tail = [p for p in existing if p not in seen]
    ordered = list(paths) + tail
    with db_write() as con:
        con.executemany(
            "UPDATE path_bases SET position=? WHERE kind=? AND path=?",
            [(i, kind, p) for i, p in enumerate(ordered)],
        )
    return {"status": "ok", "order": ordered}


# ─── Process filters ────────────────────────────────────────────────────────

def _filter_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "mode": row["mode"],
        "pattern": row["pattern"],
        "position": int(row["position"]),
    }


def list_process_filters(mode: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return process filter rows, optionally restricted to one ``mode``."""
    con = get_db()
    if mode is None:
        rows = con.execute(
            "SELECT * FROM process_filters ORDER BY mode, position, pattern"
        ).fetchall()
    else:
        if mode not in FILTER_MODES:
            return []
        rows = con.execute(
            "SELECT * FROM process_filters WHERE mode=? ORDER BY position, pattern",
            (mode,),
        ).fetchall()
    return [_filter_row(r) for r in rows]


def list_patterns(mode: str) -> List[str]:
    """Return raw patterns for one ``mode`` — convenience wrapper."""
    return [r["pattern"] for r in list_process_filters(mode=mode)]


def add_process_filter(mode: str, pattern: str, *, position: Optional[int] = None) -> Dict[str, Any]:
    if mode not in FILTER_MODES:
        return {"status": "error", "error": f"mode must be one of {list(FILTER_MODES)}"}
    if not pattern:
        return {"status": "error", "error": "pattern is required"}

    if position is None:
        con = get_db()
        row = con.execute(
            "SELECT COALESCE(MAX(position), -1) AS m FROM process_filters WHERE mode=?",
            (mode,),
        ).fetchone()
        position = (row["m"] if row else -1) + 1

    try:
        with db_write() as con:
            cur = con.execute(
                "INSERT INTO process_filters (mode, pattern, position) VALUES (?, ?, ?)",
                (mode, pattern, int(position)),
            )
            new_id = cur.lastrowid
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"pattern already registered for mode={mode!r}: {pattern!r}"}

    return {"status": "ok", "filter": {"id": new_id, "mode": mode, "pattern": pattern, "position": int(position)}}


def remove_process_filter(mode: str, pattern: str) -> Dict[str, Any]:
    if mode not in FILTER_MODES:
        return {"status": "error", "error": f"mode must be one of {list(FILTER_MODES)}"}
    with db_write() as con:
        cur = con.execute("DELETE FROM process_filters WHERE mode=? AND pattern=?", (mode, pattern))
        if cur.rowcount == 0:
            return {"status": "error", "error": f"no pattern {pattern!r} for mode={mode!r}"}
    return {"status": "ok", "removed": {"mode": mode, "pattern": pattern}}


def remove_process_filter_by_id(entry_id: int) -> Dict[str, Any]:
    with db_write() as con:
        cur = con.execute("DELETE FROM process_filters WHERE id=?", (int(entry_id),))
        if cur.rowcount == 0:
            return {"status": "error", "error": f"no process filter id={entry_id}"}
    return {"status": "ok", "removed": int(entry_id)}


def reorder_process_filters(mode: str, patterns: List[str]) -> Dict[str, Any]:
    if mode not in FILTER_MODES:
        return {"status": "error", "error": f"mode must be one of {list(FILTER_MODES)}"}
    if not isinstance(patterns, list):
        return {"status": "error", "error": "patterns must be a list of strings"}
    existing = [r["pattern"] for r in list_process_filters(mode=mode)]
    unknown = [p for p in patterns if p not in existing]
    if unknown:
        return {"status": "error", "error": f"unknown patterns for mode={mode!r}: {unknown}"}
    seen = set(patterns)
    tail = [p for p in existing if p not in seen]
    ordered = list(patterns) + tail
    with db_write() as con:
        con.executemany(
            "UPDATE process_filters SET position=? WHERE mode=? AND pattern=?",
            [(i, mode, p) for i, p in enumerate(ordered)],
        )
    return {"status": "ok", "order": ordered}
