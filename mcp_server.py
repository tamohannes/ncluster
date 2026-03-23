"""MCP server for ncluster.

Exposes cluster job status, log reading, stats, and history as MCP tools
so AI agents can inspect experiment runs without SSH or manual curl.

Requires the ncluster Flask app to be running at http://localhost:7272.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("ncluster")

API_BASE = "http://localhost:7272"


# ── helpers ──────────────────────────────────────────────────────────────────

def _api_get(path: str) -> dict:
    url = f"{API_BASE}{path}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"ncluster unreachable ({exc.reason}). Is the service running?"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_post(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"ncluster unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted",
    "started_local", "ended_local",
    "progress", "depends_on", "dependents", "dep_details",
    "project", "project_color", "project_emoji",
    "_pinned", "exit_code", "crash_detected", "est_start",
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
def get_history(cluster: Optional[str] = None, project: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Get past job history, optionally filtered by cluster and/or project.

    Returns recent completed/failed/cancelled jobs with state, elapsed
    time, start/end timestamps, partition, and project info.
    """
    params = {"limit": str(limit)}
    if cluster:
        params["cluster"] = cluster
    if project:
        params["project"] = project
    qs = urllib.parse.urlencode(params)
    data = _api_get(f"/api/history?{qs}")
    if isinstance(data, list):
        return data
    return [data]


@mcp.tool()
def list_projects() -> list[dict]:
    """List all known projects with job counts and colors.

    Returns projects derived from job name prefixes configured in
    Settings > Projects. Each entry has: project name, job_count,
    last_active timestamp, and assigned color.
    """
    data = _api_get("/api/projects")
    if isinstance(data, list):
        return data
    return [data]


@mcp.tool()
def get_project_jobs(project: str, cluster: Optional[str] = None, limit: int = 100) -> list[dict]:
    """Get all jobs for a specific project.

    Combines live running/pending jobs and historical completed/failed
    jobs for the given project. Optionally filter by cluster.
    """
    params = {"limit": str(limit), "project": project}
    if cluster:
        params["cluster"] = cluster
    qs = urllib.parse.urlencode(params)
    history = _api_get(f"/api/history?{qs}")
    if not isinstance(history, list):
        history = [history]

    live_data = _api_get("/api/jobs")
    live_jobs = []
    if isinstance(live_data, dict) and live_data.get("status") != "error":
        for cname, cdata in live_data.items():
            if cluster and cname != cluster:
                continue
            for j in cdata.get("jobs", []):
                if j.get("project") == project and not j.get("_pinned"):
                    live_jobs.append(_slim_job(cname, j))

    return live_jobs + [_slim_job(r.get("cluster", ""), r) for r in history]


@mcp.tool()
def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job on a cluster.

    This is destructive — only use when the user explicitly asks to
    cancel a job.
    """
    return _api_post(f"/api/cancel/{urllib.parse.quote(cluster)}/{urllib.parse.quote(job_id)}")


@mcp.tool()
def cancel_jobs(cluster: str, job_ids: list[str]) -> dict:
    """Cancel multiple jobs on a cluster in one call.

    This is destructive — only use when the user explicitly asks to
    cancel specific jobs. Pass each job ID as a separate list element.
    """
    return _api_post_json(
        f"/api/cancel_jobs/{urllib.parse.quote(cluster)}",
        {"job_ids": job_ids},
    )


@mcp.tool()
def cancel_all_cluster_jobs(cluster: str) -> dict:
    """Cancel ALL of your running and pending jobs on a cluster.

    Runs `scancel -u $USER` on the cluster. This is very destructive —
    it kills every job you own on that cluster, not just jobs from one
    project. Only use when the user explicitly asks to cancel everything
    on a cluster.

    Does not work for the 'local' cluster.

    Returns {"status": "ok"} on success.
    """
    return _api_post(f"/api/cancel_all/{urllib.parse.quote(cluster)}")


@mcp.tool()
def cancel_project_jobs(project: str, cluster: Optional[str] = None) -> dict:
    """Cancel all running and pending jobs belonging to a project.

    Fetches live jobs across all clusters (or a specific cluster),
    filters to those matching the project, and cancels them in batch.
    Pinned terminal jobs (already finished) are skipped.

    This is destructive — only use when the user explicitly asks to
    cancel all jobs for a project.

    Args:
        project:  Project name (e.g. "artsiv", "hle").
        cluster:  Optional — restrict to a single cluster.

    Returns a summary with cancelled count per cluster and any errors.
    """
    data = _api_get("/api/jobs")
    if isinstance(data, dict) and data.get("status") == "error":
        return {"status": "error", "error": data.get("error", "Failed to fetch jobs")}

    to_cancel: dict[str, list[str]] = {}
    for cname, cdata in data.items():
        if cluster and cname != cluster:
            continue
        if not isinstance(cdata, dict) or cdata.get("status") == "error":
            continue
        for j in cdata.get("jobs", []):
            if j.get("_pinned"):
                continue
            st = (j.get("state") or "").upper()
            if st not in ("RUNNING", "COMPLETING", "PENDING"):
                continue
            if j.get("project") == project:
                to_cancel.setdefault(cname, []).append(str(j["jobid"]))

    if not to_cancel:
        return {"status": "ok", "cancelled": 0, "detail": f"No active jobs found for project '{project}'."}

    results = {}
    total = 0
    errors = []
    for cname, ids in to_cancel.items():
        resp = _api_post_json(
            f"/api/cancel_jobs/{urllib.parse.quote(cname)}",
            {"job_ids": ids},
        )
        if resp.get("status") == "ok":
            results[cname] = len(ids)
            total += len(ids)
        else:
            errors.append(f"{cname}: {resp.get('error', 'unknown')}")

    out: dict = {"status": "ok", "cancelled": total, "per_cluster": results}
    if errors:
        out["errors"] = errors
    return out


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


@mcp.tool()
def get_run_info(cluster: str, root_job_id: str) -> dict:
    """Get detailed run information including metadata captured from Slurm.

    Returns the run record with batch script, scontrol output, environment
    variables, conda/pip state, and list of associated jobs. The metadata
    is auto-captured via SSH when jobs are first detected.

    The root_job_id is the job ID of the first job in the dependency chain
    (the one with no parent dependencies).
    """
    return _api_get(f"/api/run_info/{urllib.parse.quote(cluster)}/{urllib.parse.quote(root_job_id)}")


@mcp.tool()
def run_script(
    cluster: str,
    script: str,
    interpreter: str = "python3",
    timeout: int = 120,
) -> dict:
    """Run a script on a cluster via SSH and return its output.

    Use this to analyse result files, JSONL outputs, or any data on the
    cluster's filesystem without needing raw SSH access.

    Args:
        cluster:     Target cluster name (e.g. "ord", "dfw").
        script:      Full source code of the script to run.
        interpreter: "python3" (default), "bash", or "sh".
        timeout:     Max seconds to wait (1-300, default 120).

    Returns:
        {"status": "ok", "stdout": "...", "stderr": "...", ...}

    Example — analyse an eval-results JSONL:
        run_script(
            cluster="ord",
            script='''
import json
path = "/lustre/.../output-rs0.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip()]
correct = sum(1 for r in rows if r.get("judgement"))
print(f"Accuracy: {correct}/{len(rows)} = {correct/len(rows)*100:.1f}%")
''',
        )
    """
    return _api_post_json(
        f"/api/run_script/{urllib.parse.quote(cluster)}",
        {"script": script, "interpreter": interpreter, "timeout": timeout},
    )


# ── cluster availability ─────────────────────────────────────────────────────

@mcp.tool()
def get_storage_quota(cluster: str) -> dict:
    """Get Lustre storage quota for a cluster: your personal usage and team project quotas.

    Returns:
      user_quota — your disk usage vs quota (space + inodes)
      project_quotas — PPP quotas for llmservice_nemo_reasoning and
                       llmservice_nemo_robustness (space + inodes with % used)

    Works on clusters with Lustre (dfw, eos, iad, ord).
    Returns an error for clusters using NFS (lax) or without lfs.

    Use this alongside get_cluster_availability() to make submission
    recommendations that consider both compute AND storage constraints.
    If a project quota is near its limit (>90% space or inodes), the
    cluster may reject new jobs that write large outputs.
    """
    return _api_get(f"/api/storage_quota/{urllib.parse.quote(cluster)}")


@mcp.tool()
def get_cluster_availability() -> dict:
    """Get real-time cluster utilization from the Science dashboard.

    Returns per-cluster data: total nodes, running/pending nodes,
    active users with their node counts, and team GPU allocations.
    Use this to recommend which cluster will start jobs fastest.

    Key fields per cluster:
      total_nodes      — total nodes in use on the cluster
      running_nodes    — nodes currently running jobs (all users)
      pending_nodes    — nodes queued pending (all users)
      gpus_per_node    — GPUs per node (usually 8, 4 for hsg)
      users            — list of {user, running, pending, total, team}
      team_alloc_gpus  — team -> allocated GPU count
      status           — "ok" or "error"

    The returned "collected_at" timestamp shows when the dashboard
    last polled the clusters (typically every ~15 minutes).
    """
    return _api_get("/api/cluster_utilization")


# ── mount & board tools ──────────────────────────────────────────────────────

@mcp.tool()
def get_mounts() -> dict:
    """Get SSHFS mount status for all clusters.

    Returns a dict of cluster -> {mounted, root} showing whether each
    cluster's remote filesystem is mounted locally for fast log reads.
    """
    return _api_get("/api/mounts")


@mcp.tool()
def mount_cluster(cluster: str, action: str = "mount") -> dict:
    """Mount or unmount a cluster's remote filesystem via SSHFS.

    action must be 'mount' or 'unmount'. Mounting enables fast local
    reads of log files instead of SSH fallback.
    """
    if action not in ("mount", "unmount"):
        return {"status": "error", "error": "action must be 'mount' or 'unmount'"}
    return _api_post(f"/api/mount/{urllib.parse.quote(action)}/{urllib.parse.quote(cluster)}")


@mcp.tool()
def clear_failed(cluster: str) -> dict:
    """Dismiss all failed/cancelled/timeout job pins from a cluster's board.

    These are terminal-state jobs that stay visible on the dashboard
    until explicitly cleared. This does not affect job history.
    """
    return _api_post(f"/api/clear_failed/{urllib.parse.quote(cluster)}")


@mcp.tool()
def clear_completed(cluster: str) -> dict:
    """Dismiss all completed job pins from a cluster's board.

    Completed jobs stay pinned on the dashboard until cleared.
    This does not affect job history.
    """
    return _api_post(f"/api/clear_completed/{urllib.parse.quote(cluster)}")


# ── logbook tools ────────────────────────────────────────────────────────────


def _api_post_json(path: str, data: dict) -> dict:
    url = f"{API_BASE}{path}"
    payload = json.dumps(data).encode()
    req = urllib.request.Request(url, method="POST", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"ncluster unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_put_json(path: str, data: dict) -> dict:
    url = f"{API_BASE}{path}"
    payload = json.dumps(data).encode()
    req = urllib.request.Request(url, method="PUT", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"ncluster unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_delete(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"ncluster unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def list_logbooks(project: str) -> list[dict]:
    """List all logbooks for a project.

    Returns name, entry count, and last modified timestamp for each.
    """
    data = _api_get(f"/api/logbooks/{urllib.parse.quote(project)}")
    if isinstance(data, list):
        return data
    return [data]


@mcp.tool()
def read_logbook(project: str, name: str) -> dict:
    """Read a logbook's full content and entries.

    Returns the raw markdown content and a list of individual entries
    (split by --- separators). Use @run-name to reference jobs.
    """
    return _api_get(f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(name)}")


@mcp.tool()
def add_logbook_entry(project: str, name: str, content: str) -> dict:
    """Add a new entry to a project logbook.

    The entry is prepended (newest first). Creates the logbook if it
    doesn't exist. Use @run-name to reference specific jobs.

    Example content:
      "## 14 Mar 2026\\n\\nRan @artsiv_eval-math with 8 GPUs, accuracy: **82.3%**."
    """
    return _api_post_json(
        f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(name)}",
        {"content": content},
    )


@mcp.tool()
def update_logbook_entry(project: str, name: str, index: int, content: str) -> dict:
    """Update an existing logbook entry by index (0 = newest).

    Replaces the full entry content at the given position.
    """
    return _api_put_json(
        f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(name)}/{index}",
        {"content": content},
    )


@mcp.tool()
def delete_logbook_entry(project: str, name: str, index: int) -> dict:
    """Delete a logbook entry by index (0 = newest). This is destructive."""
    return _api_delete(
        f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(name)}/{index}"
    )


@mcp.tool()
def rename_logbook(project: str, old_name: str, new_name: str) -> dict:
    """Rename a logbook."""
    return _api_post_json(
        f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(old_name)}/rename",
        {"new_name": new_name},
    )


@mcp.tool()
def create_logbook(project: str, name: str) -> dict:
    """Create a new empty logbook for a project.

    Common logbook names: experiments, bugs, ideas, eval-notes.
    """
    return _api_post_json(
        f"/api/logbook/{urllib.parse.quote(project)}",
        {"name": name},
    )


@mcp.tool()
def delete_logbook(project: str, name: str) -> dict:
    """Delete a logbook. This is destructive and cannot be undone."""
    return _api_delete(
        f"/api/logbook/{urllib.parse.quote(project)}/{urllib.parse.quote(name)}"
    )


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
