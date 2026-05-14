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

"""Run-uuid resolution helper for SDK clients.

NeMo-Skills' post-summarize telemetry needs to attach metrics to the
canonical SDK run row when the launcher-side env var
``CLAUSIUS_RUN_UUID`` isn't available (manual ``ns summarize_results``
runs, in-container hooks that lost the env, etc.). Without this helper
the only way to look up the canonical uuid is to construct an HTTP
request to ``/api/sdk/resolve_run`` by hand.

Resolution path
---------------
This module mirrors :mod:`clausius_sdk.cluster`: prefer a **direct read
of Clausius's SQLite database** over an HTTP call. Direct DB reads:

  * survive gunicorn restarts (the DB file is fine when the server is down),
  * have no network failure surface,
  * are roughly an order of magnitude faster than the HTTP roundtrip.

The HTTP path remains as a fallback for cases where the DB file isn't
on disk locally — typically inside a remote Slurm container where only
the network is shared with the launcher box.

Stdlib-only: the SDK is vendored standalone into NeMo-Skills checkouts
and must not introduce pip dependencies (``sqlite3`` is part of stdlib).

Schema contract
---------------
The SDK reads ``runs.run_uuid``, ``runs.primary_output_dir``,
``runs.source``, ``runs.cluster``, ``runs.run_name``, and
``sdk_run_aliases.alias_uuid`` / ``sdk_run_aliases.canonical_uuid`` from
the live DB. Renaming any of those columns in Clausius would require
re-vendoring the SDK into NeMo-Skills checkouts via
``tools/integrate-sdk.sh``. These columns are stable v4 API.
"""

from __future__ import annotations

import functools
import json
import logging
import os
import sqlite3
import urllib.request
from urllib.parse import urlencode

LOG = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 2.5
_DEFAULT_DB_PATH = "~/clausius/data/history.db"


def resolve_run_uuid(
    output_dir: str,
    *,
    run_name: str = "",
    cluster: str = "",
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
) -> str:
    """Return the canonical SDK run_uuid for ``output_dir``, or ``""`` on miss.

    The strongest signal is ``primary_output_dir`` because the fresh-run-IDs
    protocol guarantees distinct output_dirs for distinct experiments. A
    matching output_dir therefore means "this is the same logical run" —
    typically a resume (``ns eval ++skip_filled=True``) or a post-hoc
    ``ns summarize_results`` invocation against a previously-completed
    pipeline.

    Returns ``""`` on every failure path (URL unset, DB inaccessible,
    HTTP unreachable, no match). The empty-string contract matches the
    NeMo-Skills caller's expectation: empty means "skip emission" rather
    than "raise".

    Args:
        output_dir: The run's primary output directory. Trailing slashes
            are normalised so callers can pass either form.
        run_name: Optional expname for tiebreaking (e.g. when multiple
            SDK runs share an output_dir, which shouldn't happen but is
            cheap to guard against).
        cluster: Optional cluster name for tiebreaking; combined with
            ``run_name`` gives the strongest match.
        timeout_sec: HTTP fallback timeout. Default 2.5 s — the SDK is
            on the launch-time critical path and shouldn't wait long.

    Returns:
        Canonical SDK ``run_uuid`` string on hit, ``""`` on miss/error.
    """
    if not isinstance(output_dir, str) or not output_dir:
        return ""
    return _resolve_cached(
        _norm_output_dir(output_dir),
        run_name or "",
        cluster or "",
        float(timeout_sec),
    )


@functools.lru_cache(maxsize=64)
def _resolve_cached(
    output_dir: str,
    run_name: str,
    cluster: str,
    timeout_sec: float,
) -> str:
    """Cache key: ``(output_dir, run_name, cluster, timeout_sec)``.

    Run identity rarely changes within a single launcher process, so
    caching saves repeated I/O when the same output_dir is queried
    multiple times (e.g. one resolution per benchmark when several
    ``metrics.json`` files are summarised in a row).
    """
    # Honour the SDK's "no Clausius wanted" gate: only attempt resolution
    # when CLAUSIUS_URL is set, mirroring the rest of the SDK's no-op
    # behaviour. The variable is the single switch that tells us "yes,
    # this run intends to talk to Clausius".
    if not (os.environ.get("CLAUSIUS_URL", "") or "").strip():
        return ""

    canonical = _resolve_via_db(output_dir, run_name, cluster)
    if canonical:
        return canonical

    canonical = _resolve_via_http(output_dir, run_name, cluster, timeout_sec)
    if canonical:
        return canonical

    return ""


def _norm_output_dir(path: str) -> str:
    """Match the server-side normalisation in ``server.db._norm_output_dir``."""
    return str(path or "").rstrip("/")


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

    Read-only mode avoids any write-lock contention with the Clausius
    gunicorn process. SQLite uses WAL on the live DB, so concurrent
    readers don't block the writer either.
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


def _resolve_alias(con: sqlite3.Connection, run_uuid: str) -> str:
    """Collapse a resume alias to its canonical uuid via ``sdk_run_aliases``."""
    if not run_uuid:
        return run_uuid
    try:
        row = con.execute(
            "SELECT canonical_uuid FROM sdk_run_aliases WHERE alias_uuid=?",
            (run_uuid,),
        ).fetchone()
    except sqlite3.Error:
        return run_uuid
    if row and row["canonical_uuid"]:
        return row["canonical_uuid"]
    return run_uuid


def _resolve_via_db(output_dir: str, run_name: str, cluster: str) -> str:
    """Look up the canonical run_uuid for ``output_dir`` via the local DB.

    Precedence mirrors ``server.db.find_sdk_run_uuid_by_output_dir``:
    same-cluster+name+dir wins, then name+dir (covers cluster drift),
    then dir-only (covers expname changes between resumes). Returns
    ``""`` on any miss or DB error so the caller can fall back to HTTP.
    """
    if not output_dir:
        return ""

    for path in _candidate_db_paths():
        con = _open_readonly(path)
        if con is None:
            continue
        try:
            row = None
            if cluster and run_name:
                row = con.execute(
                    """SELECT run_uuid FROM runs
                       WHERE source='sdk' AND run_uuid != ''
                         AND cluster=? AND run_name=?
                         AND rtrim(primary_output_dir, '/') = ?
                       ORDER BY id DESC LIMIT 1""",
                    (cluster, run_name, output_dir),
                ).fetchone()
            if not row and run_name:
                row = con.execute(
                    """SELECT run_uuid FROM runs
                       WHERE source='sdk' AND run_uuid != ''
                         AND run_name=?
                         AND rtrim(primary_output_dir, '/') = ?
                       ORDER BY id DESC LIMIT 1""",
                    (run_name, output_dir),
                ).fetchone()
            if not row:
                row = con.execute(
                    """SELECT run_uuid FROM runs
                       WHERE source='sdk' AND run_uuid != ''
                         AND rtrim(primary_output_dir, '/') = ?
                       ORDER BY id DESC LIMIT 1""",
                    (output_dir,),
                ).fetchone()
            if not row:
                con.close()
                return ""
            canonical = _resolve_alias(con, str(row["run_uuid"] or ""))
            con.close()
            return canonical or ""
        except sqlite3.Error as exc:
            LOG.debug("clausius: sqlite query failed on %s: %s", path, exc)
            con.close()
            continue

    return ""


# ─── HTTP fallback (cross-machine / in-container) ───────────────────────────

def _resolve_via_http(
    output_dir: str,
    run_name: str,
    cluster: str,
    timeout_sec: float,
) -> str:
    """Resolve via ``/api/sdk/resolve_run`` when the local DB isn't available.

    Used inside Slurm containers and on remote launcher hosts where the
    SQLite file isn't on disk locally but a Clausius HTTP server is
    reachable over the network.
    """
    base = (os.environ.get("CLAUSIUS_URL", "") or "").strip().rstrip("/")
    if not base:
        return ""

    params: dict[str, str] = {"output_dir": output_dir}
    if run_name:
        params["run_name"] = run_name
    if cluster:
        params["cluster"] = cluster
    url = f"{base}/api/sdk/resolve_run?{urlencode(params)}"

    req = urllib.request.Request(url, method="GET")
    token = (os.environ.get("CLAUSIUS_TOKEN", "") or "").strip()
    if token:
        req.add_header("Authorization", f"Bearer {token}")

    try:
        # Resolve ``urllib.request.urlopen`` at call time so tests that
        # monkeypatch the canonical attribute (``urllib.request.urlopen``)
        # still intercept the call.
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:  # nosec: trusted local URL
            if getattr(resp, "status", 200) >= 400:
                return ""
            body = json.loads(resp.read().decode("utf-8") or "{}")
    except Exception as exc:  # pragma: no cover - swallowed on every fail path
        LOG.debug("clausius: resolve_run HTTP lookup failed for %r: %s", output_dir, exc)
        return ""

    if not isinstance(body, dict) or not body.get("exists"):
        return ""
    uuid = body.get("run_uuid")
    if isinstance(uuid, str) and uuid.strip():
        return uuid.strip()
    return ""


def _clear_cache_for_tests() -> None:
    """Drop the resolution cache. Test-only helper; never call in prod."""
    _resolve_cached.cache_clear()
