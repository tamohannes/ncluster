"""MCP server for clausius — in-process Flask architecture.

The MCP server runs the same Flask app as gunicorn but inside its own
process. Tool calls go through Werkzeug's WSGI loop via `app.test_client()`,
so there is no HTTP dependency on the gunicorn UI service: when gunicorn
crashes or restarts, the MCP server keeps serving every tool against the
same SQLite database.

Both processes share:
  - SQLite (WAL mode, multi-process safe)
  - The `server.ssh` module (each process has its own bounded semaphores)
  - The on-disk config and mounts

Single-writer responsibilities (backups, mount remounts, WDS snapshots,
the progress scraper) stay in gunicorn — see `_run_init` in app.py.

Follower poller
---------------
A small daemon thread probes `http://localhost:7272/api/health` every
``_FOLLOWER_INTERVAL_SEC``. After ``_FOLLOWER_FAIL_THRESHOLD`` consecutive
failures we assume gunicorn is down and start the cluster poller in this
process so MCP keeps reporting fresh data. The first successful probe
hands polling back to gunicorn.
"""

import logging
import os
import threading
import time
from typing import Optional

from mcp.server.fastmcp import FastMCP

from app import app, mcp_init
from server.poller import poller_running, start_poller, stop_poller

mcp_init()
_client = app.test_client()

mcp = FastMCP("clausius")

log = logging.getLogger("server.mcp")


# ── In-process API helpers ───────────────────────────────────────────────────

def _api(method, path, **kwargs):
    """Invoke a Flask route through the test client and return parsed JSON.

    Mirrors the previous httpx-based signature so tool implementations stay
    untouched. Errors are surfaced as `{"status": "error", "error": "..."}`
    so callers don't need exception handling.
    """
    try:
        resp = _client.open(path=path, method=method, **kwargs)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
    if resp.status_code >= 500:
        # Mirror the legacy proxy's behaviour: still try to surface the
        # JSON error body if the route produced one.
        if resp.is_json:
            return resp.get_json()
        return {"status": "error", "error": f"HTTP {resp.status_code}: {resp.get_data(as_text=True)[:200]}"}
    if resp.is_json:
        return resp.get_json()
    return resp.get_data(as_text=True)


def _api_text(method, path, **kwargs):
    """Like `_api` but returns the raw response body as text."""
    try:
        resp = _client.open(path=path, method=method, **kwargs)
    except Exception as exc:
        return f"Error: {exc}"
    return resp.get_data(as_text=True)


# ── Follower poller ──────────────────────────────────────────────────────────

_FOLLOWER_URL = os.environ.get("CLAUSIUS_LEADER_URL", "http://localhost:7272") + "/api/health"
_FOLLOWER_INTERVAL_SEC = float(os.environ.get("CLAUSIUS_FOLLOWER_INTERVAL_SEC", "10"))
_FOLLOWER_FAIL_THRESHOLD = int(os.environ.get("CLAUSIUS_FOLLOWER_FAIL_THRESHOLD", "3"))
_FOLLOWER_PROBE_TIMEOUT_SEC = float(os.environ.get("CLAUSIUS_FOLLOWER_PROBE_TIMEOUT_SEC", "2"))


def _probe_leader():
    """Return True iff the gunicorn leader's /api/health responds with 2xx."""
    import urllib.request

    try:
        req = urllib.request.Request(_FOLLOWER_URL, method="GET")
        with urllib.request.urlopen(req, timeout=_FOLLOWER_PROBE_TIMEOUT_SEC) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def _follower_step(consecutive_failures):
    """Apply one follower decision based on a single liveness probe.

    Returns the updated `consecutive_failures` counter. Pulled out of the
    loop so the leadership transition can be unit-tested without timers
    or threads.
    """
    healthy = _probe_leader()
    if healthy:
        if poller_running():
            log.info("follower: leader healthy, stopping local poller")
            stop_poller()
        return 0

    consecutive_failures += 1
    if (
        consecutive_failures >= _FOLLOWER_FAIL_THRESHOLD
        and not poller_running()
    ):
        log.warning(
            "follower: leader unreachable for %d probes, taking over polling",
            consecutive_failures,
        )
        start_poller()
    return consecutive_failures


def _follower_loop():
    """Watch gunicorn liveness and start/stop our poller accordingly.

    We only take over polling once the leader has been silent for
    ``_FOLLOWER_FAIL_THRESHOLD`` consecutive probes (~30 s by default) so
    a transient gunicorn restart doesn't trigger a costly handover. As soon
    as the leader answers we step back — duplicate polling during the
    transition is harmless because every poll write is idempotent.
    """
    consecutive_failures = 0
    while True:
        try:
            consecutive_failures = _follower_step(consecutive_failures)
        except Exception:
            log.exception("follower tick failed")
        time.sleep(_FOLLOWER_INTERVAL_SEC)


def _start_follower():
    """Spawn the follower-poller daemon.

    Started from the ``__main__`` block below so that importing this module
    (e.g. from tests) is side-effect-free — no probe thread, no real socket.
    """
    t = threading.Thread(target=_follower_loop, daemon=True, name="mcp-follower")
    t.start()
    return t


# ── helpers ───────────────────────────────────────────────────────────────────

_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted", "account",
    "started_local", "ended_local",
    "progress", "depends_on", "dependents", "dep_details",
    "project", "project_color", "project_emoji", "campaign",
    "_pinned", "exit_code", "crash_detected", "est_start",
]


def _slim_job(cluster: str, job: dict) -> dict:
    out = {"cluster": cluster}
    for k in _JOB_FIELDS:
        v = job.get(k)
        if v is not None and v != "" and v != []:
            out[k] = v
    return out


# ── tools ─────────────────────────────────────────────────────────────────────

@mcp.tool()
def health_check() -> dict:
    """Quick health check. Returns ok if the MCP server is running."""
    svc = _api("GET", "/api/health")
    if isinstance(svc, dict) and svc.get("status") == "ok":
        return {
            "status": "ok",
            "service": "in-process",
            "board_version": svc.get("board_version"),
            "follower_active": poller_running(),
        }
    return {"status": "ok", "service": "degraded", "note": "in-process API responded with an error"}


@mcp.tool()
def list_jobs(cluster: Optional[str] = None, project: Optional[str] = None) -> list[dict]:
    """List active jobs across all clusters, or filtered by cluster/project.

    Returns compact job records with state, progress, dependencies, and est_start.
    Includes both live squeue jobs and board-pinned terminal jobs.
    """
    if cluster:
        data = _api("GET", f"/api/jobs/{cluster}")
        if data.get("status") == "error":
            return [{"error": data.get("error", "Unknown error")}]
        jobs = [_slim_job(cluster, j) for j in data.get("jobs", [])]
    else:
        snapshot = _api("GET", "/api/jobs")
        jobs = []
        if isinstance(snapshot, dict):
            for cname, cdata in snapshot.items():
                if not isinstance(cdata, dict):
                    continue
                for j in cdata.get("jobs", []):
                    jobs.append(_slim_job(cname, j))

    if project:
        jobs = [j for j in jobs if j.get("project") == project]
    return jobs


@mcp.tool()
def list_log_files(cluster: str, job_id: str) -> dict:
    """Discover available log and result files for a job.

    Returns lists of direct log files and explorable directories.
    """
    return _api("GET", f"/api/log_files/{cluster}/{job_id}", query_string={"force": "1"})


@mcp.tool()
def get_job_log(
    cluster: str,
    job_id: str,
    path: Optional[str] = None,
    lines: int = 150,
) -> str:
    """Read the tail of a log file for a job.

    If path is omitted, the best file is auto-selected. Returns raw log text.
    """
    params = {"lines": str(lines)}
    if path:
        params["path"] = path
    data = _api("GET", f"/api/log/{cluster}/{job_id}", query_string=params)
    if isinstance(data, dict):
        if data.get("status") == "error":
            return f"Error: {data.get('error', 'Unknown error')}"
        return data.get("content", "(empty)")
    return str(data)


@mcp.tool()
def get_job_stats(cluster: str, job_id: str) -> dict:
    """Get resource stats for a running job (CPU, memory, GPU utilisation)."""
    return _api("GET", f"/api/stats/{cluster}/{job_id}")


@mcp.tool()
def get_run_info(cluster: str, root_job_id: str) -> dict:
    """Get detailed run info: batch script, scontrol, env vars, conda state, and associated jobs."""
    return _api("GET", f"/api/run_info/{cluster}/{root_job_id}")


@mcp.tool()
def get_history(
    cluster: Optional[str] = None,
    project: Optional[str] = None,
    campaign: Optional[str] = None,
    state: Optional[str] = None,
    partition: Optional[str] = None,
    account: Optional[str] = None,
    search: Optional[str] = None,
    days: Optional[int] = None,
    limit: int = 50,
) -> list[dict]:
    """Get past job history, filterable by cluster, project, campaign, state, partition, account, search, and recent days.

    String filters accept a single value. ``state`` and ``campaign`` also accept comma-separated values.
    """
    params = {"limit": str(limit)}
    if cluster:
        params["cluster"] = cluster
    if project:
        params["project"] = project
    if campaign:
        params["campaign"] = campaign
    if state:
        params["state"] = state
    if partition:
        params["partition"] = partition
    if account:
        params["account"] = account
    if search:
        params["q"] = search
    if days is not None:
        params["days"] = str(days)
    data = _api("GET", "/api/history", query_string=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job. Destructive — only when user explicitly asks."""
    return _api("POST", f"/api/cancel/{cluster}/{job_id}")


@mcp.tool()
def cancel_jobs(cluster: str, job_ids: list[str]) -> dict:
    """Cancel multiple jobs on a cluster. Destructive — only when user explicitly asks."""
    return _api("POST", f"/api/cancel_jobs/{cluster}", json={"job_ids": job_ids})


@mcp.tool()
def run_script(
    cluster: str,
    script: str,
    interpreter: str = "python3",
    timeout: int = 120,
) -> dict:
    """Run a script on a cluster via SSH and return its output.

    Args:
        cluster: Target cluster name.
        script: Full source code.
        interpreter: "python3" (default), "bash", or "sh".
        timeout: Max seconds (1-300, default 120).
    """
    return _api("POST", f"/api/run_script/{cluster}", json={
        "script": script,
        "interpreter": interpreter,
        "timeout": timeout,
    })


# ── cluster info ──────────────────────────────────────────────────────────────

@mcp.tool()
def get_partitions(cluster: Optional[str] = None) -> dict:
    """Get Slurm partition details: state, time limits, priority, nodes, GPUs, queue depth.

    Returns per-partition data including idle_nodes, pending_jobs, gpus_per_node,
    priority_tier, preempt_mode, and access restrictions.
    """
    if cluster:
        return _api("GET", f"/api/partitions/{cluster}")
    return _api("GET", "/api/partitions")


@mcp.tool()
def where_to_submit(
    nodes: int = 1,
    gpus_per_node: int = 8,
    gpu_type: str = "",
) -> dict:
    """Rank clusters by WDS score (0-100) for job submission.

    Combines PPP allocations, fairshare, team usage, queue pressure, and
    cluster occupancy. Higher WDS = better. >=75 good, 50-74 moderate, <50 unlikely.

    Args:
        nodes: GPU nodes needed (default 1).
        gpus_per_node: GPUs per node (default 8).
        gpu_type: Prefer clusters with this GPU (e.g. "H100", "B200").
    """
    return _api("POST", "/api/where_to_submit", json={
        "nodes": nodes,
        "gpus_per_node": gpus_per_node,
        "gpu_type": gpu_type,
    })


# ── mount & board tools ──────────────────────────────────────────────────────

@mcp.tool()
def get_mounts() -> dict:
    """Get SSHFS mount status for all clusters."""
    return _api("GET", "/api/mounts")


@mcp.tool()
def mount_cluster(cluster: str, action: str = "mount") -> dict:
    """Mount or unmount a cluster's remote filesystem via SSHFS."""
    if action not in ("mount", "unmount"):
        return {"status": "error", "error": "action must be 'mount' or 'unmount'"}
    return _api("POST", f"/api/mount/{action}/{cluster}")


@mcp.tool()
def clear_failed(cluster: str) -> dict:
    """Dismiss all failed/cancelled/timeout job pins from a cluster's board."""
    return _api("POST", f"/api/clear_failed/{cluster}")


@mcp.tool()
def clear_completed(cluster: str) -> dict:
    """Dismiss all completed job pins from a cluster's board."""
    return _api("POST", f"/api/clear_completed/{cluster}")


# ── project tools ─────────────────────────────────────────────────────────────

@mcp.tool()
def list_projects() -> list[dict]:
    """List every registered project with its color, emoji, prefixes, and metadata.

    Each entry contains: name, color, emoji, prefixes (list of {prefix,
    default_campaign?}), campaign_delimiter, description, created_at, updated_at.
    """
    data = _api("GET", "/api/projects/all")
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
def create_project(
    name: str,
    prefixes: Optional[list] = None,
    color: Optional[str] = None,
    emoji: Optional[str] = None,
    default_campaign: Optional[str] = None,
    campaign_delimiter: str = "_",
    description: str = "",
) -> dict:
    """Create a new project so jobs whose names start with one of its prefixes
    are auto-assigned to it.

    Args:
      name: lowercase project key (letters, digits, hyphens). Becomes the
        sidebar label and the value stored in ``job_history.project``.
      prefixes: list of prefix specs. Each item is either a string like
        ``"artsiv_"`` or a dict like ``{"prefix": "hle_chem", "default_campaign": "chem"}``.
        Pass an empty list / omit to create a "manual" project that doesn't
        auto-route any jobs.
      color: hex color (e.g. ``"#9effbb"``). Auto-picked from the palette if omitted.
      emoji: single emoji char. Auto-picked if omitted.
      default_campaign: shortcut applied to a single-prefix project.
      campaign_delimiter: char used to split the run-details remainder when
        deriving the campaign (default ``"_"``).
      description: free-form note about the project.
    """
    payload = {
        "name": name,
        "prefixes": prefixes if prefixes is not None else [],
        "campaign_delimiter": campaign_delimiter or "_",
        "description": description or "",
    }
    if color:
        payload["color"] = color
    if emoji:
        payload["emoji"] = emoji
    if default_campaign:
        payload["default_campaign"] = default_campaign
    return _api("POST", "/api/projects", json=payload)


@mcp.tool()
def update_project(
    name: str,
    color: Optional[str] = None,
    emoji: Optional[str] = None,
    prefixes: Optional[list] = None,
    default_campaign: Optional[str] = None,
    campaign_delimiter: Optional[str] = None,
    description: Optional[str] = None,
) -> dict:
    """Update a registered project's metadata. Pass only the fields you want
    to change; ``None`` is treated as "leave unchanged".

    Note: replacing ``prefixes`` does not retroactively re-extract the
    ``project`` field on existing job rows. Run a manual SQL update or call
    a future re-extract helper if you need that.
    """
    payload = {}
    if color is not None:
        payload["color"] = color
    if emoji is not None:
        payload["emoji"] = emoji
    if prefixes is not None:
        payload["prefixes"] = prefixes
    if default_campaign is not None:
        payload["default_campaign"] = default_campaign
    if campaign_delimiter is not None:
        payload["campaign_delimiter"] = campaign_delimiter
    if description is not None:
        payload["description"] = description
    return _api("PUT", f"/api/projects/{name}", json=payload)


@mcp.tool()
def delete_project(name: str) -> dict:
    """Delete a registered project. Destructive — does not touch job history,
    but jobs that referenced this project name will stop appearing in the
    project sidebar (their stored ``project`` string is left as-is)."""
    return _api("DELETE", f"/api/projects/{name}")


# ── logbook tools ─────────────────────────────────────────────────────────────

@mcp.tool()
def list_logbook_entries(
    project: str,
    query: Optional[str] = None,
    sort: str = "edited_at",
    limit: int = 50,
    entry_type: Optional[str] = None,
) -> list[dict]:
    """List logbook entries for a project, optionally filtered by BM25 search.

    Returns: id, project, title, body_preview, entry_type, created_at, edited_at.
    Sort: "edited_at" (default), "created_at", "title".
    """
    params = {"sort": sort, "limit": str(limit)}
    if query:
        params["q"] = query
    if entry_type:
        params["type"] = entry_type
    data = _api("GET", f"/api/logbook/{project}/entries", query_string=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
def read_logbook_entry(project: str, entry_id: int) -> dict:
    """Read a single logbook entry with full markdown body."""
    return _api("GET", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
def bulk_read_logbooks(
    project: Optional[str] = None,
    entry_type: Optional[str] = None,
    sort: str = "created_at",
    limit_per_project: int = 200,
    max_entries: int = 1000,
) -> dict:
    """Bulk-read full logbook entries for one or all projects in a single call.

    Returns full entries with markdown bodies. Use for comprehensive context gathering.
    """
    body = {"sort": sort, "limit_per_project": limit_per_project, "max_entries": max_entries}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return _api("POST", "/api/logbook/bulk_read", json=body)


@mcp.tool()
def find_logbook_entries(
    pattern: str,
    project: Optional[str] = None,
    field: str = "title",
    regex: bool = False,
    entry_type: Optional[str] = None,
    full_body: bool = True,
    limit: int = 50,
) -> dict:
    """Find logbook entries by substring or regex match on title or body.

    Args:
        pattern: Search string (substring by default, regex if regex=True).
        field: "title" (default), "body", or "both".
        regex: Treat pattern as Python regex.
        full_body: Return full body (default True) or preview only.
    """
    body = {"pattern": pattern, "field": field, "regex": regex, "full_body": full_body, "limit": limit}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return _api("POST", "/api/logbook/find", json=body)


@mcp.tool()
def create_logbook_entry(project: str, title: str, body: str = "", entry_type: str = "note") -> dict:
    """Create a new logbook entry. Supports markdown, #N cross-refs, @run-name refs, images.

    See the project-logbook workspace rule for full formatting guidelines.
    entry_type: "note" (results/findings) or "plan" (plans/designs).
    """
    return _api("POST", f"/api/logbook/{project}/entries", json={
        "title": title,
        "body": body,
        "entry_type": entry_type,
    })


@mcp.tool()
def update_logbook_entry(
    project: str,
    entry_id: int,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> dict:
    """Update a logbook entry's title and/or body. Bumps edited_at."""
    payload = {}
    if title is not None:
        payload["title"] = title
    if body is not None:
        payload["body"] = body
    return _api("PUT", f"/api/logbook/{project}/entries/{entry_id}", json=payload)


@mcp.tool()
def delete_logbook_entry(project: str, entry_id: int) -> dict:
    """Delete a logbook entry. Destructive."""
    return _api("DELETE", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
def upload_logbook_image(project: str, image_path: str) -> dict:
    """Upload a local image/HTML file to a project's logbook image store.

    Supported: .png, .jpg, .jpeg, .gif, .webp, .svg, .html, .htm
    See project-logbook workspace rule for embedding and HTML figure requirements.
    """
    if not os.path.isfile(image_path):
        return {"status": "error", "error": f"File not found: {image_path}"}
    filename = os.path.basename(image_path)
    with open(image_path, "rb") as f:
        data = f.read()
    try:
        # Werkzeug's test client accepts a `(BytesIO, filename)` tuple under
        # the multipart field name; this matches what httpx's `files=` did
        # against the live HTTP endpoint.
        from io import BytesIO

        resp = _client.post(
            f"/api/logbook/{project}/images",
            data={"file": (BytesIO(data), filename)},
            content_type="multipart/form-data",
        )
        if resp.is_json:
            return resp.get_json()
        return {"status": "error", "error": f"HTTP {resp.status_code}: {resp.get_data(as_text=True)[:200]}"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ── resources ────────────────────────────────────────────────────────────────

@mcp.resource("jobs://summary")
def jobs_summary() -> str:
    """Quick overview of all clusters: running/pending/failed counts."""
    data = _api("GET", "/api/jobs_summary")
    if isinstance(data, dict) and data.get("status") == "ok":
        return data.get("summary", "")
    return f"Error: {data.get('error', 'Unknown error')}" if isinstance(data, dict) else str(data)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _start_follower()
    mcp.run()
