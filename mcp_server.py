"""MCP server for the job-monitor app.

Exposes cluster job status, log reading, stats, and history as MCP tools
so AI agents can inspect experiment runs without SSH or manual curl.

Requires the job-monitor Flask app to be running at http://localhost:7272.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("job-monitor")

API_BASE = "http://localhost:7272"


# ── helpers ──────────────────────────────────────────────────────────────────

def _api_get(path: str) -> dict:
    url = f"{API_BASE}{path}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"job-monitor unreachable ({exc.reason}). Is the service running?"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_post(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"job-monitor unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted",
    "started_local", "ended_local",
    "progress", "depends_on", "dependents", "dep_details",
    "_pinned",
]


def _slim_job(cluster: str, job: dict) -> dict:
    out = {"cluster": cluster}
    for k in _JOB_FIELDS:
        v = job.get(k)
        if v is not None and v != "" and v != []:
            out[k] = v
    return out


# ── tools ────────────────────────────────────────────────────────────────────

@mcp.tool()
def list_jobs(cluster: Optional[str] = None) -> list[dict]:
    """List active jobs across all clusters, or for a specific cluster.

    Returns a compact list with: cluster, jobid, name, state, elapsed,
    partition, GPUs, dependency info, and progress percentage.
    Includes both live squeue jobs and board-pinned terminal jobs.
    """
    if cluster:
        data = _api_get(f"/api/jobs/{urllib.parse.quote(cluster)}")
        if data.get("status") == "error":
            return [{"error": data.get("error", "Unknown error")}]
        return [_slim_job(cluster, j) for j in data.get("jobs", [])]

    data = _api_get("/api/jobs")
    if isinstance(data, dict) and data.get("status") == "error":
        return [{"error": data.get("error", "Unknown error")}]

    result = []
    for cname, cdata in data.items():
        for j in cdata.get("jobs", []):
            result.append(_slim_job(cname, j))
    return result


@mcp.tool()
def list_log_files(cluster: str, job_id: str) -> dict:
    """Discover available log and result files for a job.

    Returns lists of direct log files and explorable directories
    (eval-logs, eval-results, output dirs).
    """
    return _api_get(f"/api/log_files/{urllib.parse.quote(cluster)}/{urllib.parse.quote(job_id)}")


@mcp.tool()
def get_job_log(
    cluster: str,
    job_id: str,
    path: Optional[str] = None,
    lines: int = 150,
) -> str:
    """Read a log file for a job.

    If path is omitted the best file is auto-selected (prefers main srun
    output over sbatch stdout). Returns the raw log text so you can read
    it directly.
    """
    params = {"lines": str(lines)}
    if path:
        params["path"] = path
    qs = urllib.parse.urlencode(params)
    data = _api_get(f"/api/log/{urllib.parse.quote(cluster)}/{urllib.parse.quote(job_id)}?{qs}")
    if data.get("status") == "ok":
        return data.get("content", "(empty)")
    return f"Error: {data.get('error', 'unknown')}"


@mcp.tool()
def get_job_stats(cluster: str, job_id: str) -> dict:
    """Get resource stats for a running job (CPU, memory, GPU utilisation).

    Works best for running Slurm jobs with GPU allocations.
    """
    return _api_get(f"/api/stats/{urllib.parse.quote(cluster)}/{urllib.parse.quote(job_id)}")


@mcp.tool()
def get_history(cluster: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Get past job history, optionally filtered by cluster.

    Returns recent completed/failed/cancelled jobs with state, elapsed
    time, start/end timestamps, and partition.
    """
    params = {"limit": str(limit)}
    if cluster:
        params["cluster"] = cluster
    qs = urllib.parse.urlencode(params)
    data = _api_get(f"/api/history?{qs}")
    if isinstance(data, list):
        return data
    return [data]


@mcp.tool()
def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job on a cluster.

    This is destructive — only use when the user explicitly asks to
    cancel a job.
    """
    return _api_post(f"/api/cancel/{urllib.parse.quote(cluster)}/{urllib.parse.quote(job_id)}")


@mcp.tool()
def cleanup_history(days: int = 30, dry_run: bool = False) -> dict:
    """Delete history records older than N days and remove their local log files.

    Destructive — only use when the user explicitly asks to clean up old runs.
    Set dry_run=True to preview what would be deleted without actually removing anything.
    """
    payload = json.dumps({"days": days, "dry_run": dry_run}).encode()
    url = f"{API_BASE}/api/cleanup"
    req = urllib.request.Request(url, method="POST", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


# ── resources ────────────────────────────────────────────────────────────────

@mcp.resource("jobs://summary")
def jobs_summary() -> str:
    """Quick overview of all clusters: running/pending/failed counts."""
    data = _api_get("/api/jobs")
    if isinstance(data, dict) and data.get("status") == "error":
        return f"Error: {data.get('error')}"

    lines = []
    total_r = total_p = total_f = 0
    for cname, cdata in data.items():
        if cdata.get("status") == "error":
            lines.append(f"{cname}: unreachable")
            continue
        jobs = cdata.get("jobs", [])
        r = sum(1 for j in jobs if j.get("state", "").upper() == "RUNNING")
        p = sum(1 for j in jobs if j.get("state", "").upper() == "PENDING")
        f = sum(1 for j in jobs if "FAIL" in j.get("state", "").upper())
        total_r += r
        total_p += p
        total_f += f
        parts = []
        if r: parts.append(f"{r} running")
        if p: parts.append(f"{p} pending")
        if f: parts.append(f"{f} failed")
        status = ", ".join(parts) if parts else "idle"
        lines.append(f"{cname}: {status}")

    header = f"Total: {total_r} running, {total_p} pending, {total_f} failed"
    return header + "\n" + "\n".join(lines)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
