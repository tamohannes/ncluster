"""Fetch and cache cluster utilization data from the external Science dashboard.

The dashboard URL is configured via `dashboard_url` in config.json. It exposes:
  /api/config  — cluster order, GPU per node, team allocations, team membership
  /api/status  — per-cluster per-user running/pending/total node counts

We cache the merged result in memory with a configurable TTL (default 120s)
and expose a single function `get_cluster_utilization()` for consumption by
the routes layer.
"""

import json
import logging
import threading
import time
import urllib.request
import urllib.error

from .config import DASHBOARD_URL

log = logging.getLogger(__name__)

CACHE_TTL_SEC = 120

_lock = threading.Lock()
_cached_data = None
_cached_at = 0.0
_refreshing = False


def _fetch_json(path, timeout=10):
    if not DASHBOARD_URL:
        return None
    url = f"{DASHBOARD_URL}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError) as exc:
        log.warning("cluster_dashboard: failed to fetch %s: %s", url, exc)
        return None


def _build_utilization():
    """Fetch /api/config and /api/status in parallel, merge into a compact structure."""
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_config = pool.submit(_fetch_json, "/api/config")
        f_status = pool.submit(_fetch_json, "/api/status")
    config = f_config.result()
    status = f_status.result()
    if not config or not status:
        return None

    teams = status.get("_teams", {})
    team_alloc = config.get("team_alloc", {})
    gpu_per_node = config.get("gpu_per_node", {})

    user_team = {}
    for team_name, members in teams.items():
        for u in members:
            user_team[u] = team_name

    clusters = {}
    for cluster_name in config.get("cluster_order", []):
        cdata = status.get(cluster_name)
        if not cdata or not isinstance(cdata, dict) or "total_nodes" not in cdata:
            continue

        users_raw = cdata.get("users", {})
        total_nodes = cdata.get("total_nodes", 0)
        gpus_per_node = gpu_per_node.get(cluster_name, 8)

        active_users = []
        total_running = 0
        total_pending = 0
        for uname, udata in users_raw.items():
            r = udata.get("running", 0)
            p = udata.get("pending", 0)
            if r > 0 or p > 0:
                active_users.append({
                    "user": uname,
                    "running": r,
                    "pending": p,
                    "total": udata.get("total", 0),
                    "team": user_team.get(uname, ""),
                })
            total_running += r
            total_pending += p

        active_users.sort(key=lambda u: (-u["running"], -u["pending"]))

        allocs = {}
        for team_name, alloc_map in team_alloc.items():
            a = alloc_map.get(cluster_name, 0)
            if a > 0:
                allocs[team_name] = a

        clusters[cluster_name] = {
            "total_nodes": total_nodes,
            "gpus_per_node": gpus_per_node,
            "running_nodes": total_running,
            "pending_nodes": total_pending,
            "users": active_users,
            "team_alloc_gpus": allocs,
            "status": cdata.get("status", "ok"),
            "updated_at": cdata.get("updated_at", ""),
        }

    return {
        "clusters": clusters,
        "collected_at": status.get("_collected_at", ""),
    }


def get_cluster_utilization(force=False):
    """Return cached cluster utilization or refresh if stale.

    Returns None if the external dashboard is unreachable.
    Uses single-flight: only one thread fetches at a time; others
    serve stale data rather than stampeding the upstream.
    """
    global _cached_data, _cached_at, _refreshing

    now = time.monotonic()
    with _lock:
        if not force and _cached_data and (now - _cached_at) < CACHE_TTL_SEC:
            return _cached_data
        if _refreshing:
            return _cached_data
        _refreshing = True

    try:
        result = _build_utilization()
        if result:
            with _lock:
                _cached_data = result
                _cached_at = time.monotonic()
            return result
        with _lock:
            return _cached_data
    finally:
        with _lock:
            _refreshing = False
