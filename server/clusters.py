"""Cluster registry CRUD — DB-backed replacement for ``config.json:clusters``.

Every consumer that used to read ``server.config.CLUSTERS[name]`` now
calls ``get_cluster(name)`` (or relies on the ``CLUSTERS`` proxy defined
in ``server/config.py`` which delegates here). The shape of the returned
dict matches the legacy in-memory shape one-for-one so call sites do
not need to change.

The synthetic ``"local"`` cluster is **never** stored in the DB — it is
injected at read time by :func:`list_clusters` so existing logic that
checks ``cluster == "local"`` keeps working.

Aliases
-------
Each row carries an ``aliases`` list — alternate names that resolve to
the canonical cluster (e.g. NeMo-Skills' ``aws-cmh-science`` YAML name
maps to physical ``aws-cmh``). The resolution logic itself lives in
:func:`resolve_canonical_cluster`; aliases are read alongside the rest
of the cluster record so SDK ingest and the public ``/api/cluster_resolve``
endpoint share the same source of truth.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .db import db_write, get_db


_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


# Shape of the synthetic local cluster, returned alongside DB rows so
# code that iterates CLUSTERS keeps seeing a "local" entry without us
# having to insert one into the table.
LOCAL_CLUSTER: Dict[str, Any] = {
    "name": "local",
    "host": None,
    "data_host": "",
    "user": None,
    "key": None,
    "port": None,
    "gpu_type": "local",
    "gpu_mem_gb": 0,
    "gpus_per_node": 0,
    "account": "",
    "aihub_name": "",
    "mount_paths": [],
    "mount_aliases": {},
    "aliases": [],
    "team_gpu_alloc": "",
    "enabled": 1,
}


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    """Convert a DB row to the legacy ``CLUSTERS[name]`` shape.

    Empty ``ssh_user``/``ssh_key`` mean "use the bootstrap defaults" —
    the fallback happens in :func:`get_cluster` so the on-disk row stays
    portable across users.
    """
    from .bootstrap import get_bootstrap

    boot = get_bootstrap()
    ssh_user = row["ssh_user"] or boot.ssh_user
    ssh_key = row["ssh_key"] or boot.ssh_key

    try:
        mount_paths = json.loads(row["mount_paths_json"] or "[]")
    except json.JSONDecodeError:
        mount_paths = []
    if not isinstance(mount_paths, list):
        mount_paths = []

    try:
        mount_aliases = json.loads(row["mount_aliases_json"] or "{}")
    except json.JSONDecodeError:
        mount_aliases = {}
    if not isinstance(mount_aliases, dict):
        mount_aliases = {}

    try:
        aliases = json.loads(row["aliases_json"] or "[]")
    except (json.JSONDecodeError, IndexError):
        aliases = []
    if not isinstance(aliases, list):
        aliases = []
    aliases = [str(a) for a in aliases if isinstance(a, str) and a]

    return {
        "name": row["name"],
        "host": row["host"],
        "data_host": row["data_host"] or "",
        "user": ssh_user,
        "key": os.path.expanduser(ssh_key) if ssh_key else "",
        "port": row["port"],
        "gpu_type": row["gpu_type"] or "",
        "gpu_mem_gb": row["gpu_mem_gb"] or 0,
        "gpus_per_node": row["gpus_per_node"] or 0,
        "account": row["account"] or "",
        "aihub_name": row["aihub_name"] or "",
        "mount_paths": mount_paths,
        "mount_aliases": mount_aliases,
        "aliases": aliases,
        "team_gpu_alloc": row["team_gpu_alloc"] or "",
        "enabled": int(row["enabled"]),
        "position": int(row["position"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


# ─── Read ────────────────────────────────────────────────────────────────────

def list_clusters(*, include_local: bool = True, only_enabled: bool = True) -> List[Dict[str, Any]]:
    """Return every cluster in registration order.

    ``include_local`` appends the synthetic local cluster at the end
    (legacy behaviour). ``only_enabled`` filters out rows with
    ``enabled=0``; pass ``False`` from the Settings UI so disabled
    clusters still appear in the list (greyed out).
    """
    con = get_db()
    sql = "SELECT * FROM clusters"
    params: tuple = ()
    if only_enabled:
        sql += " WHERE enabled=1"
    sql += " ORDER BY position, name"
    rows = con.execute(sql, params).fetchall()
    out = [_row_to_dict(r) for r in rows]
    if include_local:
        out.append(dict(LOCAL_CLUSTER))
    return out


def list_cluster_names(*, include_local: bool = True, only_enabled: bool = True) -> List[str]:
    """Return cluster names only — cheaper than :func:`list_clusters` when
    the caller doesn't need the full record."""
    con = get_db()
    sql = "SELECT name FROM clusters"
    if only_enabled:
        sql += " WHERE enabled=1"
    sql += " ORDER BY position, name"
    names = [r["name"] for r in con.execute(sql).fetchall()]
    if include_local:
        names.append("local")
    return names


def get_cluster(name: str) -> Optional[Dict[str, Any]]:
    """Return one cluster by name, or ``None`` if missing.

    The synthetic ``"local"`` cluster is returned without a DB hit.
    """
    if not name:
        return None
    if name == "local":
        return dict(LOCAL_CLUSTER)
    con = get_db()
    row = con.execute("SELECT * FROM clusters WHERE name=?", (name,)).fetchone()
    return _row_to_dict(row) if row else None


def cluster_map(*, include_local: bool = True, only_enabled: bool = True) -> Dict[str, Dict[str, Any]]:
    """Return ``{name: cluster_dict}`` matching the legacy ``CLUSTERS`` shape."""
    return {c["name"]: c for c in list_clusters(include_local=include_local, only_enabled=only_enabled)}


# ─── Write ───────────────────────────────────────────────────────────────────

def _validate_name(name: str) -> Optional[str]:
    if not name:
        return "name is required"
    if name == "local":
        return "name 'local' is reserved for the synthetic local cluster"
    if not _NAME_RE.match(name):
        return "name must start with a letter and contain only letters, digits, hyphens, underscores"
    return None


def _normalize_mount_paths(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        raise ValueError("mount_paths must be a list of strings")
    out = []
    for entry in value:
        if not isinstance(entry, str):
            raise ValueError("mount_paths entries must be strings")
        entry = entry.strip()
        if entry:
            out.append(entry)
    return out


def _normalize_mount_aliases(value) -> Dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("mount_aliases must be a dict mapping path -> mount index")
    out: Dict[str, int] = {}
    for path, idx in value.items():
        if not isinstance(path, str) or not path.strip():
            raise ValueError("mount_aliases keys must be non-empty strings")
        try:
            out[path] = int(idx)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"mount_aliases[{path!r}] must be an integer index") from exc
    return out


def _normalize_aliases(value, *, owner: str = "") -> List[str]:
    """Coerce ``value`` to a deduped list of valid alias strings.

    Aliases must be non-empty, must not collide with the canonical name
    of any cluster, and must be unique across the whole registry — that
    invariant is the contract :func:`resolve_canonical_cluster` relies
    on. ``owner`` is the cluster we're writing for; aliases already
    owned by ``owner`` are allowed to round-trip.
    """
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        raise ValueError("aliases must be a list of strings")
    cleaned: List[str] = []
    seen: set = set()
    for entry in value:
        if not isinstance(entry, str):
            raise ValueError("aliases entries must be strings")
        s = entry.strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        cleaned.append(s)

    if not cleaned:
        return []

    con = get_db()
    rows = con.execute(
        "SELECT name, aliases_json FROM clusters WHERE name != ?",
        (owner,),
    ).fetchall()
    taken: Dict[str, str] = {}
    for r in rows:
        taken[r["name"]] = r["name"]
        try:
            row_aliases = json.loads(r["aliases_json"] or "[]")
        except json.JSONDecodeError:
            row_aliases = []
        if isinstance(row_aliases, list):
            for a in row_aliases:
                if isinstance(a, str) and a:
                    taken[a] = r["name"]
    if owner:
        taken[owner] = owner

    for alias in cleaned:
        owner_of = taken.get(alias)
        if owner_of and owner_of != owner:
            raise ValueError(
                f"alias {alias!r} is already used by cluster {owner_of!r}"
            )
    return cleaned


def add_cluster(
    name: str,
    *,
    host: str,
    data_host: str = "",
    port: int = 22,
    ssh_user: str = "",
    ssh_key: str = "",
    account: str = "",
    gpu_type: str = "",
    gpu_mem_gb: int = 0,
    gpus_per_node: int = 0,
    aihub_name: str = "",
    mount_paths=None,
    mount_aliases=None,
    aliases=None,
    team_gpu_alloc: str = "",
    enabled: bool = True,
    position: Optional[int] = None,
) -> Dict[str, Any]:
    """Insert a new cluster row.

    Returns ``{"status": "ok", "cluster": {...}}`` on success, or
    ``{"status": "error", "error": "..."}`` for validation/duplicate errors.
    Callers (CLI, REST, MCP) should propagate the error string verbatim.
    """
    err = _validate_name(name)
    if err:
        return {"status": "error", "error": err}
    if not host:
        return {"status": "error", "error": "host is required"}

    try:
        mp_json = json.dumps(_normalize_mount_paths(mount_paths))
        ma_json = json.dumps(_normalize_mount_aliases(mount_aliases))
        al_json = json.dumps(_normalize_aliases(aliases, owner=name))
    except ValueError as exc:
        return {"status": "error", "error": str(exc)}

    if position is None:
        con = get_db()
        row = con.execute("SELECT COALESCE(MAX(position), -1) AS m FROM clusters").fetchone()
        position = (row["m"] if row else -1) + 1

    try:
        with db_write() as con:
            con.execute(
                """
                INSERT INTO clusters
                    (name, host, data_host, port, ssh_user, ssh_key, account,
                     gpu_type, gpu_mem_gb, gpus_per_node, aihub_name,
                     mount_paths_json, mount_aliases_json, aliases_json,
                     team_gpu_alloc, enabled, position)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    name, host, data_host or "", int(port),
                    ssh_user or "", ssh_key or "", account or "",
                    gpu_type or "", int(gpu_mem_gb or 0), int(gpus_per_node or 0),
                    aihub_name or "", mp_json, ma_json, al_json,
                    team_gpu_alloc or "", 1 if enabled else 0, int(position),
                ),
            )
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"cluster {name!r} already exists"}

    _invalidate_resolver_cache()
    return {"status": "ok", "cluster": get_cluster(name)}


_UPDATABLE_FIELDS = {
    "host": ("host", str),
    "data_host": ("data_host", str),
    "port": ("port", int),
    "ssh_user": ("ssh_user", str),
    "ssh_key": ("ssh_key", str),
    "account": ("account", str),
    "gpu_type": ("gpu_type", str),
    "gpu_mem_gb": ("gpu_mem_gb", int),
    "gpus_per_node": ("gpus_per_node", int),
    "aihub_name": ("aihub_name", str),
    "team_gpu_alloc": ("team_gpu_alloc", str),
    "enabled": ("enabled", lambda v: 1 if bool(v) and v != 0 else 0),
    "position": ("position", int),
}


def update_cluster(name: str, **fields) -> Dict[str, Any]:
    """Update one or more fields on an existing cluster.

    Pass only the fields you want to change. ``mount_paths`` and
    ``mount_aliases`` accept Python lists/dicts and are JSON-encoded
    automatically. Unknown fields are silently ignored so future field
    additions don't break old callers.
    """
    if name == "local":
        return {"status": "error", "error": "cannot modify the synthetic 'local' cluster"}
    existing = get_cluster(name)
    if existing is None:
        return {"status": "error", "error": f"cluster {name!r} not found"}

    cols: List[str] = []
    vals: List[Any] = []

    for key, value in fields.items():
        if value is None:
            continue
        if key in _UPDATABLE_FIELDS:
            col, coercer = _UPDATABLE_FIELDS[key]
            try:
                vals.append(coercer(value))
            except (TypeError, ValueError) as exc:
                return {"status": "error", "error": f"{key}: {exc}"}
            cols.append(f"{col}=?")
        elif key == "mount_paths":
            try:
                vals.append(json.dumps(_normalize_mount_paths(value)))
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
            cols.append("mount_paths_json=?")
        elif key == "mount_aliases":
            try:
                vals.append(json.dumps(_normalize_mount_aliases(value)))
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
            cols.append("mount_aliases_json=?")
        elif key == "aliases":
            try:
                vals.append(json.dumps(_normalize_aliases(value, owner=name)))
            except ValueError as exc:
                return {"status": "error", "error": str(exc)}
            cols.append("aliases_json=?")

    if not cols:
        return {"status": "ok", "cluster": existing}

    cols.append("updated_at=datetime('now')")
    vals.append(name)
    with db_write() as con:
        con.execute(f"UPDATE clusters SET {', '.join(cols)} WHERE name=?", vals)
    _invalidate_resolver_cache()
    return {"status": "ok", "cluster": get_cluster(name)}


def remove_cluster(name: str) -> Dict[str, Any]:
    """Delete a cluster row.

    Does not delete historical job rows associated with the cluster —
    those stay queryable for the history page.
    """
    if name == "local":
        return {"status": "error", "error": "cannot remove the synthetic 'local' cluster"}
    if get_cluster(name) is None:
        return {"status": "error", "error": f"cluster {name!r} not found"}
    with db_write() as con:
        con.execute("DELETE FROM clusters WHERE name=?", (name,))
    _invalidate_resolver_cache()
    return {"status": "ok", "removed": name}


def reorder_clusters(names: List[str]) -> Dict[str, Any]:
    """Persist a new display order. Names not in the list keep their position
    relative to each other but are pushed to the end."""
    if not isinstance(names, list):
        return {"status": "error", "error": "names must be a list of cluster names"}
    existing = {c["name"]: c for c in list_clusters(include_local=False, only_enabled=False)}
    unknown = [n for n in names if n not in existing]
    if unknown:
        return {"status": "error", "error": f"unknown clusters: {unknown}"}

    seen = set(names)
    tail = [n for n in existing if n not in seen]
    ordered = list(names) + tail
    with db_write() as con:
        con.executemany(
            "UPDATE clusters SET position=? WHERE name=?",
            [(i, n) for i, n in enumerate(ordered)],
        )
    return {"status": "ok", "order": ordered}


# ─── Mount helpers (replace server/config.py:_load_mount_map family) ─────────

def build_mount_map() -> Dict[str, List[str]]:
    """Return ``{cluster: [local_mount_root, ...]}`` for every registered cluster.

    Drop-in replacement for the old ``MOUNT_MAP`` global. Honours the
    ``CLAUSIUS_MOUNT_MAP`` env var override exactly like the v3 helper.
    """
    home = os.path.expanduser("~")
    base = os.path.realpath(os.path.join(home, ".clausius", "mounts"))
    raw = os.environ.get("CLAUSIUS_MOUNT_MAP", "").strip()
    cluster_dicts = {c["name"]: c for c in list_clusters(include_local=False, only_enabled=False)}

    if raw:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            out: Dict[str, List[str]] = {}
            for name, roots in parsed.items():
                if name not in cluster_dicts:
                    continue
                if isinstance(roots, str):
                    roots = [roots]
                if not isinstance(roots, list):
                    continue
                norm = [
                    os.path.realpath(os.path.expanduser(r.strip()))
                    for r in roots
                    if isinstance(r, str)
                ]
                if norm:
                    out[name] = norm
            if out:
                return out

    result: Dict[str, List[str]] = {}
    for name, ccfg in cluster_dicts.items():
        cluster_base = os.path.join(base, name)
        mount_paths = ccfg.get("mount_paths") or []
        if mount_paths:
            roots = [os.path.join(cluster_base, str(i)) for i in range(len(mount_paths))]
        else:
            roots = [cluster_base]
        result[name] = roots
    return result


def build_mount_remote_map(default_user: str) -> Dict[str, List[str]]:
    """Return ``{cluster: [remote_path_with_user_substituted, ...]}``.

    Used by ``server/mounts.py`` to convert local mount paths back into
    the original remote path for SSH discovery.
    """
    out: Dict[str, List[str]] = {}
    for c in list_clusters(include_local=False, only_enabled=False):
        out[c["name"]] = [p.replace("$USER", default_user) for p in c.get("mount_paths") or []]
    return out


def build_mount_aliases(default_user: str) -> Dict[str, List[tuple]]:
    """Return ``{cluster: [(alias_prefix, mount_index), ...]}``.

    Drop-in replacement for the v3 ``MOUNT_ALIASES`` global.
    """
    out: Dict[str, List[tuple]] = {}
    for c in list_clusters(include_local=False, only_enabled=False):
        aliases = c.get("mount_aliases") or {}
        if aliases:
            out[c["name"]] = [
                (path.replace("$USER", default_user), int(idx))
                for path, idx in aliases.items()
            ]
    return out


# ─── Canonical-name resolution ──────────────────────────────────────────────

_RESOLVER_CACHE_TTL_SEC = 60.0
_resolver_cache_lock = threading.Lock()
_resolver_cache: Dict[str, Any] = {"loaded_at": 0.0, "by_name": {}, "by_host": {}}


def _invalidate_resolver_cache() -> None:
    """Drop the in-memory alias/host index so the next resolve call rebuilds it.

    Called from every write path on the ``clusters`` table. The resolver
    also has a TTL so a missed invalidation degrades to ``<= 60 s`` of
    stale-read, not permanent drift.
    """
    with _resolver_cache_lock:
        _resolver_cache["loaded_at"] = 0.0
        _resolver_cache["by_name"] = {}
        _resolver_cache["by_host"] = {}


def _resolver_index() -> Tuple[Dict[str, Tuple[str, str, Optional[str]]], Dict[str, str]]:
    """Return ``(by_name, by_host)`` indexes for canonical-name resolution.

    ``by_name`` maps every canonical name AND every alias to a tuple of
    ``(canonical, source, matched_alias_or_None)``. ``by_host`` maps each
    cluster's ``host`` (lowercased) to its canonical name.
    """
    now = time.monotonic()
    with _resolver_cache_lock:
        if now - _resolver_cache["loaded_at"] < _RESOLVER_CACHE_TTL_SEC and _resolver_cache["by_name"]:
            return _resolver_cache["by_name"], _resolver_cache["by_host"]

    con = get_db()
    rows = con.execute("SELECT name, host, aliases_json FROM clusters").fetchall()

    by_name: Dict[str, Tuple[str, str, Optional[str]]] = {}
    by_host: Dict[str, str] = {}
    for r in rows:
        canonical = r["name"]
        by_name[canonical] = (canonical, "canonical", None)
        host = (r["host"] or "").strip().lower()
        if host:
            by_host.setdefault(host, canonical)
        try:
            row_aliases = json.loads(r["aliases_json"] or "[]")
        except json.JSONDecodeError:
            row_aliases = []
        if isinstance(row_aliases, list):
            for alias in row_aliases:
                if isinstance(alias, str) and alias and alias not in by_name:
                    by_name[alias] = (canonical, "alias", alias)

    with _resolver_cache_lock:
        _resolver_cache["loaded_at"] = now
        _resolver_cache["by_name"] = by_name
        _resolver_cache["by_host"] = by_host
    return by_name, by_host


def resolve_canonical_cluster(
    name: str,
    host: str = "",
) -> Optional[Dict[str, Any]]:
    """Look up the canonical cluster name for an input name or host.

    Resolution order:
      1. Exact match on ``clusters.name`` -> ``source="canonical"``.
      2. Exact match on any cluster's alias list -> ``source="alias"``.
      3. (Optional) match on ``clusters.host`` -> ``source="host"``.

    Returns ``{"canonical", "source", "matched_alias"?}`` on success or
    ``None`` when nothing matches. Empty ``name`` plus empty ``host`` is
    treated as a miss. The synthetic ``"local"`` cluster is never
    returned because it is not stored in the registry.
    """
    needle = (name or "").strip()
    host_needle = (host or "").strip().lower()
    if not needle and not host_needle:
        return None

    by_name, by_host = _resolver_index()

    if needle and needle in by_name:
        canonical, source, matched_alias = by_name[needle]
        out: Dict[str, Any] = {"canonical": canonical, "source": source}
        if matched_alias is not None:
            out["matched_alias"] = matched_alias
        return out

    if host_needle and host_needle in by_host:
        return {"canonical": by_host[host_needle], "source": "host"}

    return None


def normalize_cluster_name(name: str, host: str = "") -> str:
    """Internal convenience wrapper: returns the canonical name when
    resolvable, otherwise the input ``name`` unchanged.

    Used by the SDK ingest pipeline so non-canonical cluster names from
    legacy clients still land on the canonical row that real Slurm jobs
    write to.
    """
    if not name and not host:
        return name or ""
    hit = resolve_canonical_cluster(name, host=host)
    if hit:
        return hit["canonical"]
    return name


def build_team_gpu_allocations() -> Dict[str, Any]:
    """Return ``{cluster: alloc}`` matching the legacy ``TEAM_GPU_ALLOC``.

    Allocations are stored as TEXT on the cluster row so both ``"any"``
    and integer counts round-trip cleanly. Empty cells are omitted.
    """
    out: Dict[str, Any] = {}
    for c in list_clusters(include_local=False, only_enabled=False):
        raw = c.get("team_gpu_alloc")
        if raw in (None, ""):
            continue
        if str(raw).lower() == "any":
            out[c["name"]] = "any"
        else:
            try:
                out[c["name"]] = int(raw)
            except (TypeError, ValueError):
                out[c["name"]] = raw
    return out
