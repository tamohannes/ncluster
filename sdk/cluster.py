# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cluster-name resolution helper for SDK clients.

NeMo-Skills cluster YAMLs use names like ``aws-cmh-science`` that encode
an account/variant on top of the underlying physical cluster
(``aws-cmh``). Clausius's registry stores only the canonical physical
name because that's what it polls Slurm against, mounts via SSHFS, and
displays in the UI. This helper asks Clausius for the canonical name so
SDK callers don't need to maintain their own mapping.

Resolution path
---------------
The helper prefers a **direct read of Clausius's SQLite database** over
an HTTP call to ``/api/cluster_resolve``. Direct DB reads:

  * survive gunicorn restarts (the file is fine when the server is down),
  * have no network or HTTP-framing failure modes,
  * are roughly an order of magnitude faster than the HTTP roundtrip.

The HTTP path is kept as a fallback for the (rare) case where the
launcher runs on a different machine from Clausius — the DB file isn't
on disk locally, so we ask the server over the network.

Stdlib-only: the SDK is vendored standalone into NeMo-Skills checkouts
and must not introduce pip dependencies (``sqlite3`` is part of stdlib).

Schema contract
---------------
The SDK reads the columns ``name``, ``host``, and ``aliases_json`` from
the ``clusters`` table. Renaming any of those in Clausius would require
re-vendoring the SDK into NeMo-Skills checkouts via
``tools/integrate-sdk.sh``. The columns are stable v4 API.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sqlite3
from urllib.parse import urlencode
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 5.0
_DEFAULT_DB_PATH = "~/clausius/data/history.db"


def resolve_cluster_name(
    name: str,
    *,
    host: str | None = None,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
) -> str:
    """Return the canonical Clausius cluster name for ``name``.

    Returns the canonical name on success, or ``name`` unchanged on any
    failure: ``CLAUSIUS_URL`` unset, DB inaccessible, HTTP unreachable,
    name not found, etc. Fall-through-on-failure is intentional: this
    helper is called from the launch-time event-emission path, and a
    Clausius outage must not block legitimate Slurm submissions.

    Args:
        name: The cluster name as the caller knows it. May be canonical,
            an alias, or unknown.
        host: Optional SSH login hostname used as a fallback signal when
            ``name`` doesn't match a canonical name or alias.
        timeout_sec: HTTP fallback timeout. Default 5 s; the local DB
            path has no timeout because SQLite reads are bounded.

    Returns:
        The canonical cluster name when resolved, otherwise ``name``.
    """
    if not isinstance(name, str) or not name:
        return name  # type: ignore[return-value]
    host_norm = (host or "").strip()
    return _resolve_cached(name, host_norm, float(timeout_sec))


@functools.lru_cache(maxsize=64)
def _resolve_cached(name: str, host: str, timeout_sec: float) -> str:
    """Cache key: ``(name, host, timeout_sec)``. Alias maps rarely change
    within a single launcher process, so caching saves repeated I/O."""
    # Honour the SDK's "no Clausius wanted" gate: only attempt resolution
    # when CLAUSIUS_URL is set, mirroring the rest of the SDK's no-op
    # behaviour. The variable is the single switch that tells us "yes,
    # this run intends to talk to Clausius".
    if not (os.environ.get("CLAUSIUS_URL", "") or "").strip():
        return name

    canonical = _resolve_via_db(name, host)
    if canonical is not None:
        return canonical

    canonical = _resolve_via_http(name, host, timeout_sec)
    if canonical is not None:
        return canonical

    return name


# ─── Direct DB read (preferred) ─────────────────────────────────────────────

def _candidate_db_paths() -> list[str]:
    """Return the local DB paths to try, in order.

    ``CLAUSIUS_DB_PATH`` is an explicit override (e.g. for tests or
    non-default installs); when set, it is the **only** path consulted —
    no silent fallback to the default location. Otherwise we try the
    bootstrap default Clausius itself uses.
    """
    explicit = (os.environ.get("CLAUSIUS_DB_PATH", "") or "").strip()
    if explicit:
        return [os.path.expanduser(explicit)]
    return [os.path.expanduser(_DEFAULT_DB_PATH)]


def _open_readonly(path: str) -> sqlite3.Connection | None:
    """Open a SQLite file read-only, returning ``None`` if absent.

    Read-only mode avoids any chance of write-lock contention with the
    Clausius gunicorn process. SQLite uses WAL on the live DB, so
    concurrent readers don't block the writer either.
    """
    if not os.path.isfile(path):
        return None
    try:
        uri = f"file:{path}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=1.0)
        con.row_factory = sqlite3.Row
        return con
    except sqlite3.Error as exc:  # pragma: no cover - environment-dependent
        LOG.debug("clausius: sqlite open %s failed: %s", path, exc)
        return None


def _resolve_via_db(name: str, host: str) -> str | None:
    """Resolve ``name`` (with optional ``host`` fallback) via the local DB.

    Returns the canonical name on success, ``None`` on any miss/error so
    the caller can fall back to HTTP.
    """
    for path in _candidate_db_paths():
        con = _open_readonly(path)
        if con is None:
            continue
        try:
            rows = con.execute(
                "SELECT name, host, aliases_json FROM clusters"
            ).fetchall()
        except sqlite3.Error as exc:
            LOG.debug("clausius: sqlite query failed on %s: %s", path, exc)
            con.close()
            continue
        con.close()

        canonical_names: dict[str, str] = {}
        alias_index: dict[str, str] = {}
        host_index: dict[str, str] = {}
        for r in rows:
            canonical = r["name"]
            canonical_names[canonical] = canonical
            h = (r["host"] or "").strip().lower()
            if h:
                host_index.setdefault(h, canonical)
            try:
                row_aliases = json.loads(r["aliases_json"] or "[]")
            except (TypeError, json.JSONDecodeError):
                row_aliases = []
            if isinstance(row_aliases, list):
                for alias in row_aliases:
                    if isinstance(alias, str) and alias:
                        alias_index.setdefault(alias, canonical)

        if name in canonical_names:
            return canonical_names[name]
        if name in alias_index:
            return alias_index[name]
        host_needle = host.lower()
        if host_needle and host_needle in host_index:
            return host_index[host_needle]
        return None

    return None


# ─── HTTP fallback (cross-machine) ──────────────────────────────────────────

def _resolve_via_http(name: str, host: str, timeout_sec: float) -> str | None:
    """Resolve ``name`` against a remote Clausius via HTTP.

    Used when the local DB file isn't present (e.g. the launcher runs on
    a different host from Clausius). Returns the canonical name on
    success, ``None`` on any miss/error.
    """
    base = (os.environ.get("CLAUSIUS_URL", "") or "").strip().rstrip("/")
    if not base:
        return None

    params: dict[str, str] = {"name": name}
    if host:
        params["host"] = host
    url = f"{base}/api/cluster_resolve?{urlencode(params)}"

    req = Request(url, method="GET")
    token = (os.environ.get("CLAUSIUS_TOKEN", "") or "").strip()
    if token:
        req.add_header("Authorization", f"Bearer {token}")

    try:
        with urlopen(req, timeout=timeout_sec) as resp:  # nosec: trusted local URL
            if resp.status >= 400:
                return None
            body = json.loads(resp.read().decode("utf-8") or "{}")
    except Exception as exc:  # pragma: no cover - swallowed on every fail path
        LOG.debug("clausius: cluster_resolve HTTP lookup failed for %r: %s", name, exc)
        return None

    if not isinstance(body, dict):
        return None
    canonical = body.get("canonical")
    if isinstance(canonical, str) and canonical:
        return canonical
    return None


def _clear_cache_for_tests() -> None:
    """Drop the resolution cache. Test-only helper; never call in prod."""
    _resolve_cached.cache_clear()
