"""Fetch Lustre storage quotas from clusters via SSH.

Runs `lfs quota` for:
  - User quota (personal usage vs limit)
  - Project quotas for the configured team PPPs

Clusters with Lustre use different FS paths (see CLUSTER_FS_MAP).
Clusters without lfs (e.g. NFS-backed) will return an error.
"""

import logging
import re
import threading
import time

from .ssh import ssh_run_with_timeout
from .config import CLUSTERS, DEFAULT_USER, PPPS

log = logging.getLogger(__name__)

QUOTA_CACHE_TTL_SEC = 3600

_lock = threading.Lock()
_cache = {}
_refreshing_clusters = set()

CLUSTER_FS_MAP = {
    # Add your cluster-to-Lustre-path mappings here.
    # Example: "my-cluster": "/lustre/fsw",
}


def _parse_size(val):
    """Parse lfs quota size strings like '1.093T', '451T', '460.9G', '0k' into bytes."""
    val = val.strip().rstrip("*")
    if val in ("-", "none", "0"):
        return 0
    m = re.match(r"^([\d.]+)\s*([KMGTP]?)$", val, re.IGNORECASE)
    if not m:
        try:
            return int(val)
        except ValueError:
            return 0
    num = float(m.group(1))
    suffix = m.group(2).upper()
    multipliers = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    return int(num * multipliers.get(suffix, 1))


def _fmt_size(b):
    """Format bytes into human-readable string."""
    if b <= 0:
        return "0"
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}" if b != int(b) else f"{int(b)} {unit}"
        b /= 1024
    return f"{b:.1f} EB"


def _parse_quota_line(line):
    """Parse a single lfs quota data line into a dict.

    Expected format:
        /lustre/fsw  1.093T     50T     50T       - 1115013  25000000 25000000       -
    """
    parts = line.split()
    if len(parts) < 8:
        return None
    try:
        return {
            "filesystem": parts[0],
            "space_used": _parse_size(parts[1]),
            "space_quota": _parse_size(parts[2]),
            "space_limit": _parse_size(parts[3]),
            "space_grace": parts[4] if parts[4] != "-" else None,
            "files_used": int(parts[5].rstrip("*")),
            "files_quota": int(parts[6]),
            "files_limit": int(parts[7]),
            "files_grace": parts[8] if len(parts) > 8 and parts[8] != "-" else None,
        }
    except (ValueError, IndexError):
        return None


def _run_quota_cmd(cluster, cmd):
    """Run a quota command on the cluster and return stdout."""
    try:
        stdout, _ = ssh_run_with_timeout(cluster, cmd, timeout_sec=15)
        return stdout
    except Exception as exc:
        log.warning("storage_quota: %s failed on %s: %s", cmd[:40], cluster, exc)
        return None


def _add_pct(q):
    """Add usage percentage fields to a parsed quota dict."""
    if not q:
        return q
    if q["space_quota"] > 0:
        q["space_used_pct"] = round(q["space_used"] / q["space_quota"] * 100, 1)
    else:
        q["space_used_pct"] = 0
    if q["files_quota"] > 0:
        q["files_used_pct"] = round(q["files_used"] / q["files_quota"] * 100, 1)
    else:
        q["files_used_pct"] = 0
    q["space_used_human"] = _fmt_size(q["space_used"])
    q["space_quota_human"] = _fmt_size(q["space_quota"])
    return q


def _fetch_one_quota(cluster, fs_path, flag, identifier):
    """Fetch a single lfs quota (user or project) and parse it."""
    out = _run_quota_cmd(cluster, f"lfs quota -h {flag} {identifier} {fs_path} 2>&1")
    if not out:
        return None
    for line in out.splitlines():
        stripped = line.strip()
        if stripped.startswith(fs_path) or stripped.startswith("/lustre"):
            q = _parse_quota_line(stripped)
            if q:
                _add_pct(q)
            return q
    return None


def fetch_storage_quota(cluster):
    """Fetch user and project quotas for a cluster. Returns structured dict."""
    if cluster not in CLUSTERS or cluster == "local":
        return {"status": "error", "error": f"Cluster '{cluster}' not supported"}

    fs_path = CLUSTER_FS_MAP.get(cluster)
    if not fs_path:
        return {"status": "error", "error": f"No Lustre filesystem mapped for '{cluster}' (may use NFS)"}

    with _lock:
        cached = _cache.get(cluster)
        if cached and (time.monotonic() - cached["_ts"]) < QUOTA_CACHE_TTL_SEC:
            return {k: v for k, v in cached.items() if k != "_ts"}
        if cluster in _refreshing_clusters:
            return {k: v for k, v in cached.items() if k != "_ts"} if cached else {"status": "error", "error": "Refresh in progress"}
        _refreshing_clusters.add(cluster)

    try:
        return _fetch_quota_uncached(cluster)
    finally:
        with _lock:
            _refreshing_clusters.discard(cluster)


def _fetch_quota_uncached(cluster):
    user = CLUSTERS[cluster].get("user", DEFAULT_USER)
    fs_path = CLUSTER_FS_MAP.get(cluster)

    from concurrent.futures import ThreadPoolExecutor

    futures = {}
    with ThreadPoolExecutor(max_workers=max(1, 1 + len(PPPS))) as pool:
        futures["__user__"] = pool.submit(_fetch_one_quota, cluster, fs_path, "-u", user)
        for ppp_name, pid in PPPS.items():
            futures[ppp_name] = pool.submit(_fetch_one_quota, cluster, fs_path, "-p", str(pid))

    user_quota = futures.pop("__user__").result()

    projects = {}
    for ppp_name, fut in futures.items():
        pq = fut.result()
        if pq:
            pq["project_name"] = ppp_name
            pq["project_id"] = PPPS[ppp_name]
            projects[ppp_name] = pq

    result = {
        "status": "ok",
        "cluster": cluster,
        "filesystem": fs_path,
        "user": user,
        "user_quota": user_quota,
        "project_quotas": projects,
    }

    with _lock:
        result["_ts"] = time.monotonic()
        _cache[cluster] = result

    out = {k: v for k, v in result.items() if k != "_ts"}
    return out
