"""MCP server for clausius.

Exposes cluster job status, log reading, stats, and history as MCP tools
so AI agents can inspect experiment runs without SSH or manual curl.

Requires the clausius Flask app to be running at http://localhost:7272.
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("clausius")

API_BASE = "http://localhost:7272"


# ── helpers ──────────────────────────────────────────────────────────────────

def _api_get(path: str) -> dict:
    url = f"{API_BASE}{path}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason}). Is the service running?"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_post(path: str) -> dict:
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, method="POST", data=b"")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def _api_post_json(path: str, data: dict) -> dict:
    url = f"{API_BASE}{path}"
    payload = json.dumps(data).encode()
    req = urllib.request.Request(url, method="POST", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason})"}
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
        cluster:     Target cluster name (as configured in config.json).
        script:      Full source code of the script to run.
        interpreter: "python3" (default), "bash", or "sh".
        timeout:     Max seconds to wait (1-300, default 120).

    Returns:
        {"status": "ok", "stdout": "...", "stderr": "...", ...}

    Example — analyse an eval-results JSONL:
        run_script(
            cluster="my-cluster",
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
      project_quotas — PPP quotas for configured team projects
                       (space + inodes with % used)

    Works on clusters with Lustre filesystems.
    Returns an error for clusters using NFS or without lfs.

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
      gpus_per_node    — GPUs per node (typically 8, varies by cluster)
      users            — list of {user, running, pending, total, team}
      team_alloc_gpus  — team -> allocated GPU count
      status           — "ok" or "error"

    The returned "collected_at" timestamp shows when the dashboard
    last polled the clusters (typically every ~15 minutes).
    """
    return _api_get("/api/cluster_utilization")


# ── partition & recommendation tools ─────────────────────────────────────────

@mcp.tool()
def get_partitions(cluster: Optional[str] = None) -> dict:
    """Get Slurm partition details from clusters via sinfo/scontrol.

    Returns per-partition data: state, time limits, priority tier,
    preemption mode, node counts (allocated/idle/other/total),
    GPUs per node, running and pending job counts, and access restrictions.

    Use this to understand partition structures and make informed
    decisions about which partition to submit jobs to. Each cluster
    has different partitions with different priority tiers, time
    limits, and preemption policies.

    Key fields per partition:
      name           — partition name (e.g. "batch", "batch_short")
      state          — "UP" or "DOWN"
      is_default     — whether this is the default partition
      max_time       — time limit string (e.g. "4:00:00")
      priority_tier  — scheduler priority (higher = scheduled sooner)
      preempt_mode   — "OFF" or "REQUEUE" (preemptable)
      total_nodes    — total nodes in partition
      idle_nodes     — available idle nodes
      other_nodes    — down/drained nodes (not schedulable)
      gpus_per_node  — GPUs per node (from GRES, e.g. 8 for H100, 4 for B200)
      pending_jobs   — jobs waiting in queue
      running_jobs   — jobs currently running
      allow_accounts — "ALL" or comma-separated account names
      user_accessible — whether the current user can submit to this partition

    Note: idle_nodes does NOT mean a job will start instantly. Fair-share
    priority and QOS limits (MaxJobsPerUser, GrpNodeLimit) may still
    cause delays even when idle nodes exist.

    Args:
        cluster: Optional cluster name. If omitted, returns all clusters.
    """
    if cluster:
        return _api_get(f"/api/partitions/{urllib.parse.quote(cluster)}")
    return _api_get("/api/partitions")


@mcp.tool()
def recommend_submission(
    nodes: int = 1,
    time_limit: str = "4:00:00",
    account: str = "",
    can_preempt: bool = False,
    gpu_type: str = "",
    clusters: Optional[list[str]] = None,
) -> dict:
    """Recommend the best cluster, partition, and PPP account for a job.

    Uses real-time partition data (queue depth, idle nodes, priority tiers,
    occupancy) combined with AI Hub fairshare data to rank all eligible
    (cluster, partition) pairs. Each recommendation includes the PPP account
    with the best scheduling priority (highest level_fs).

    Key fairshare fields in each recommendation:
    - recommended_account: which PPP to submit under
    - level_fs: fairshare level (>1.0 = scheduling credit, <1.0 = overdrawn)
    - fairshare_avail_gpus: effective GPU availability for that account
    - allocation_headroom: fairshare_avail - consumed

    Args:
        nodes:       Number of GPU nodes needed (default 1).
        time_limit:  Job time limit, e.g. "4:00:00" or "2:00:00".
        account:     Slurm account for access filtering (optional).
        can_preempt: If True, include preemptable partitions (backfill).
        gpu_type:    Filter by GPU type, e.g. "h100" (optional).
        clusters:    List of cluster names to consider (optional).

    Returns:
        Ranked recommendations with score, estimated wait, fairshare data,
        and recommended account. Lower score = better.
    """
    payload = {
        "nodes": nodes,
        "time_limit": time_limit,
        "account": account,
        "can_preempt": can_preempt,
        "gpu_type": gpu_type,
    }
    if clusters:
        payload["clusters"] = clusters
    return _api_post_json("/api/recommend", payload)


@mcp.tool()
def get_partition_summary() -> dict:
    """Get a compact cross-cluster partition overview with wait estimates.

    BETA: Wait time estimates are heuristic-based and may be inaccurate.
    They do not account for per-user QOS limits, fair-share priority,
    or reservation policies. Treat them as rough guidance, not guarantees.

    Returns per-cluster: GPU type, total/idle nodes, pending job count,
    and a list of user-accessible GPU partitions with:
      name, max_time, priority_tier, total_nodes, idle_nodes,
      gpus_per_node, pending_jobs, preemptable, est_wait, est_wait_cls.

    est_wait is a human-readable wait estimate (e.g. "now", "~5-15 min",
    "~1-2h", "4h+"). est_wait_cls is "fast", "moderate", "slow", or "long".
    These account for drained/down nodes and queue depth, but not per-user
    QOS limits.

    Only user-accessible GPU partitions are included. Admin-only, CPU-only,
    and system partitions (defq, fake) are filtered out.

    This is the same data source as the Cluster Availability popup in the UI.
    Use this for a quick scan before diving into detailed partition data
    with get_partitions(), or use recommend_submission() for job-specific
    recommendations.
    """
    return _api_get("/api/partition_summary")


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

    Each entry has: id, project, title, body_preview, entry_type, created_at, edited_at.
    When query is set, results are ranked by BM25 relevance.
    Sort options: "edited_at" (default), "created_at", "title".
    entry_type: "note" for experiment logs/findings, "plan" for implementation plans,
                or omit for all types.
    """
    params = {"limit": str(limit), "sort": sort}
    if query:
        params["q"] = query
    if entry_type:
        params["type"] = entry_type
    qs = urllib.parse.urlencode(params)
    data = _api_get(f"/api/logbook/{urllib.parse.quote(project)}/entries?{qs}")
    if isinstance(data, list):
        return data
    return [data]


@mcp.tool()
def read_logbook_entry(project: str, entry_id: int) -> dict:
    """Read a single logbook entry with full markdown body.

    Returns: id, project, title, body (full markdown), created_at, edited_at.
    Use @run-name in the body to reference jobs.
    """
    return _api_get(f"/api/logbook/{urllib.parse.quote(project)}/entries/{entry_id}")


@mcp.tool()
def bulk_read_logbooks(
    project: Optional[str] = None,
    entry_type: Optional[str] = None,
    sort: str = "created_at",
    limit_per_project: int = 200,
    max_entries: int = 1000,
) -> dict:
    """Bulk-read full logbook entries for one project or all projects.

    This is a convenience tool for fetching many entries in one MCP call.
    It returns full entries (including markdown body), not previews.

    Args:
        project: Optional project name. If omitted, reads all projects.
        entry_type: Optional filter: "note" or "plan".
        sort: "edited_at", "created_at", or "title" (default: "created_at").
        limit_per_project: Max entries to scan per project (default: 200).
        max_entries: Hard cap across all projects for safety (default: 1000).

    Returns:
        {
          "status": "ok" | "error",
          "count": int,
          "truncated": bool,
          "projects": [project_name, ...],
          "entries": [{id, project, title, body, entry_type, created_at, edited_at}, ...],
          "errors": {project_name: "error", ...}
        }
    """
    if limit_per_project < 1:
        return {"status": "error", "error": "limit_per_project must be >= 1"}
    if max_entries < 1:
        return {"status": "error", "error": "max_entries must be >= 1"}
    if sort not in ("edited_at", "created_at", "title"):
        return {"status": "error", "error": "sort must be one of: edited_at, created_at, title"}
    if entry_type not in (None, "", "note", "plan"):
        return {"status": "error", "error": "entry_type must be 'note', 'plan', or omitted"}

    if project:
        projects = [project]
    else:
        proj_data = _api_get("/api/projects")
        if isinstance(proj_data, dict) and proj_data.get("status") == "error":
            return proj_data
        projects = [p.get("project") for p in proj_data if isinstance(p, dict) and p.get("project")]
        if not projects:
            return {
                "status": "ok",
                "count": 0,
                "truncated": False,
                "projects": [],
                "entries": [],
                "errors": {},
            }

    entries = []
    errors = {}
    truncated = False

    for p in projects:
        params = {"limit": str(limit_per_project), "sort": sort}
        if entry_type:
            params["type"] = entry_type
        qs = urllib.parse.urlencode(params)
        listed = _api_get(f"/api/logbook/{urllib.parse.quote(p)}/entries?{qs}")
        if isinstance(listed, dict) and listed.get("status") == "error":
            errors[p] = listed.get("error", "Failed to list entries")
            continue
        if not isinstance(listed, list):
            errors[p] = "Unexpected list response format"
            continue

        for item in listed:
            entry_id = item.get("id")
            if entry_id is None:
                continue
            full = _api_get(f"/api/logbook/{urllib.parse.quote(p)}/entries/{entry_id}")
            if isinstance(full, dict) and full.get("status") == "error":
                errors[f"{p}:{entry_id}"] = full.get("error", "Failed to read entry")
                continue
            if isinstance(full, dict):
                entries.append(full)
            if len(entries) >= max_entries:
                truncated = True
                break

        if truncated:
            break

    return {
        "status": "ok",
        "count": len(entries),
        "truncated": truncated,
        "projects": projects,
        "entries": entries,
        "errors": errors,
    }


@mcp.tool()
def create_logbook_entry(project: str, title: str, body: str = "", entry_type: str = "note") -> dict:
    """Create a new logbook entry for a project.

    The body supports full markdown including tables, code blocks,
    @run-name references, #entry_id cross-references, and images.
    created_at and edited_at are set automatically.

    Rich content:
    - Images: upload with upload_logbook_image, embed with ![caption](url)
    - HTML figures: upload .html files with upload_logbook_image, then paste
      the returned URL on its own line to embed as an interactive iframe
    - Cross-references: use #<entry_id> to link to another logbook entry
      (e.g. #42 links to entry 42). These show in the semantic map.
    - Code blocks, blockquotes, tables, links all supported

    entry_type: "note" (default) for experiment results, debugging sessions,
    findings. "plan" for implementation plans, research plans, experiment designs.

    Returns: {status, id, created_at}.
    """
    return _api_post_json(
        f"/api/logbook/{urllib.parse.quote(project)}/entries",
        {"title": title, "body": body, "entry_type": entry_type},
    )


@mcp.tool()
def update_logbook_entry(
    project: str,
    entry_id: int,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> dict:
    """Update a logbook entry's title and/or body. Bumps edited_at.

    Pass only the fields you want to change — omitted fields stay unchanged.
    """
    data = {}
    if title is not None:
        data["title"] = title
    if body is not None:
        data["body"] = body
    url = f"{API_BASE}/api/logbook/{urllib.parse.quote(project)}/entries/{entry_id}"
    payload = json.dumps(data).encode()
    req = urllib.request.Request(url, method="PUT", data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def delete_logbook_entry(project: str, entry_id: int) -> dict:
    """Delete a logbook entry. This is destructive."""
    url = f"{API_BASE}/api/logbook/{urllib.parse.quote(project)}/entries/{entry_id}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def search_logbook(
    query: str,
    project: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
) -> list[dict]:
    """Search logbook entries across all projects using BM25 full-text search.

    Searches both titles and bodies. Results are ranked by relevance.
    Optionally filter by project and/or date range (ISO 8601 dates).

    Returns: [{id, project, title, body_preview, created_at, edited_at}, ...]
    """
    params = {"q": query, "limit": str(limit)}
    if project:
        params["project"] = project
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to
    qs = urllib.parse.urlencode(params)
    data = _api_get(f"/api/logbook/search?{qs}")
    if isinstance(data, list):
        return data
    return [data]


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

    More flexible than search_logbook (BM25): supports exact substring
    matching and Python regex patterns. Returns full entries by default.

    Args:
        pattern:    String to search for (substring match by default).
        project:    Optional project name. If omitted, searches all projects.
        field:      "title" (default), "body", or "both".
        regex:      If true, pattern is treated as a Python regex.
        entry_type: Optional filter: "note" or "plan".
        full_body:  If true (default), returns full body. If false, body_preview only.
        limit:      Max entries to return (default: 50).

    Returns:
        {
          "status": "ok" | "error",
          "count": int,
          "entries": [{id, project, title, body/body_preview, entry_type, created_at, edited_at}, ...],
        }

    Examples:
        find_logbook_entries("GPQA Diamond")                   # title substring
        find_logbook_entries("accuracy.*8[0-9]%", regex=True, field="body")  # regex in body
        find_logbook_entries("sandbox", field="both", project="hle")
    """
    import re as _re

    if field not in ("title", "body", "both"):
        return {"status": "error", "error": "field must be 'title', 'body', or 'both'"}
    if entry_type and entry_type not in ("note", "plan"):
        return {"status": "error", "error": "entry_type must be 'note', 'plan', or omitted"}
    if limit < 1:
        return {"status": "error", "error": "limit must be >= 1"}

    if regex:
        try:
            compiled = _re.compile(pattern, _re.IGNORECASE)
        except _re.error as e:
            return {"status": "error", "error": f"Invalid regex: {e}"}
        test = lambda text: bool(compiled.search(text or ""))
    else:
        pat_lower = pattern.lower()
        test = lambda text: pat_lower in (text or "").lower()

    if project:
        projects = [project]
    else:
        proj_data = _api_get("/api/projects")
        if isinstance(proj_data, dict) and proj_data.get("status") == "error":
            return proj_data
        projects = [p.get("project") for p in proj_data if isinstance(p, dict) and p.get("project")]

    results = []
    for p in projects:
        params = {"limit": "500", "sort": "edited_at"}
        if entry_type:
            params["type"] = entry_type
        qs = urllib.parse.urlencode(params)
        listed = _api_get(f"/api/logbook/{urllib.parse.quote(p)}/entries?{qs}")
        if not isinstance(listed, list):
            continue

        for item in listed:
            entry_id = item.get("id")
            if entry_id is None:
                continue
            title = item.get("title", "")

            title_match = test(title) if field in ("title", "both") else False

            if field == "title" and not title_match:
                continue

            if field in ("body", "both") and not title_match:
                full_entry = _api_get(f"/api/logbook/{urllib.parse.quote(p)}/entries/{entry_id}")
                if not isinstance(full_entry, dict) or full_entry.get("status") == "error":
                    continue
                body_text = full_entry.get("body", "")
                if not test(body_text):
                    continue
                results.append(full_entry)
            elif full_body:
                full_entry = _api_get(f"/api/logbook/{urllib.parse.quote(p)}/entries/{entry_id}")
                if isinstance(full_entry, dict) and full_entry.get("status") != "error":
                    results.append(full_entry)
                else:
                    results.append(item)
            else:
                results.append(item)

            if len(results) >= limit:
                break
        if len(results) >= limit:
            break

    return {
        "status": "ok",
        "count": len(results),
        "entries": results,
    }


@mcp.tool()
def upload_logbook_image(project: str, image_path: str) -> dict:
    """Upload a local image or HTML file to a project's logbook store.

    Use this to attach plots, figures, screenshots, diagrams, or interactive
    HTML visualizations (plotly, matplotlib, bokeh exports) to logbook entries.

    For images: insert the returned URL using ![description](url)
    For HTML files: paste the returned URL on its own line in the entry body
    to embed it as an interactive iframe.

    Supported formats: .png, .jpg, .jpeg, .gif, .webp, .svg, .html, .htm

    Args:
        project:    Project name.
        image_path: Absolute path to the file on disk.

    Returns: {status, url, filename} — use the url in markdown image syntax.
    """
    import os
    if not os.path.isfile(image_path):
        return {"status": "error", "error": f"File not found: {image_path}"}

    filename = os.path.basename(image_path)
    with open(image_path, "rb") as f:
        data = f.read()

    boundary = "----clausius_upload_boundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()

    url = f"{API_BASE}/api/logbook/{urllib.parse.quote(project)}/images"
    req = urllib.request.Request(
        url, method="POST", data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as exc:
        return {"status": "error", "error": f"clausius unreachable ({exc.reason})"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@mcp.tool()
def get_team_gpu_status(cluster: Optional[str] = None) -> dict:
    """Get your team's GPU usage, weekly allocations, and per-member breakdown.

    Combines three data sources:
    1. Weekly GPU allocations (set in Settings > Profile) — the informal
       quota your manager communicates (e.g., "you get 1000 GPUs on DFW").
    2. Live team usage from Slurm (squeue by account) — who on your team
       is actually using how many GPUs right now.
    3. Science dashboard team allocation — the formal cluster-level alloc.

    Use this to answer questions like:
    - "Why are my jobs pending?" → teammate using all the quota
    - "How much of our allocation is free?" → alloc - running
    - "Who is using our GPUs?" → per-user breakdown

    Args:
        cluster: Optional cluster name. If omitted, returns all clusters.

    Returns:
        {
          status: "ok",
          clusters: {
            "<cluster>": {
              weekly_alloc_gpus: 1000,    # from Settings (0 if not set)
              running_gpus: 720,          # team total from Slurm
              pending_gpus: 128,          # team total pending
              free_gpus: 280,             # weekly_alloc - running (if alloc set)
              account: "llmservice_...",  # auto-detected Slurm account
              users: {                    # per-member breakdown
                "alice": {running_gpus: 512, pending_gpus: 0},
                "bob": {running_gpus: 208, pending_gpus: 128},
              }
            }
          }
        }
    """
    settings = _api_get("/api/settings")
    allocs = settings.get("team_gpu_allocations", {}) if isinstance(settings, dict) else {}

    progress_payload = {"jobs": []}
    if cluster:
        progress_payload["jobs"] = [{"cluster": cluster, "job_id": "0"}]
    else:
        for c in _api_get("/api/jobs") or {}:
            if c != "local":
                progress_payload["jobs"].append({"cluster": c, "job_id": "0"})

    progress = _api_post_json("/api/progress", progress_payload)
    team_usage = progress.get("team_usage", {}) if isinstance(progress, dict) else {}

    clusters_out = {}
    target_clusters = [cluster] if cluster else list(set(list(allocs.keys()) + list(team_usage.keys())))

    for c in target_clusters:
        if c == "local":
            continue
        tu = team_usage.get(c, {})
        weekly = allocs.get(c, 0)
        running = tu.get("total_running_gpus", 0)
        pending = tu.get("total_pending_gpus", 0)
        clusters_out[c] = {
            "weekly_alloc_gpus": weekly,
            "running_gpus": running,
            "pending_gpus": pending,
            "free_gpus": max(0, weekly - running) if weekly else None,
            "account": tu.get("account", ""),
            "users": tu.get("users", {}),
        }

    ppp = _api_get("/api/aihub/allocations")
    if isinstance(ppp, dict) and ppp.get("status") == "ok":
        for c in clusters_out:
            ppp_cluster = ppp.get("clusters", {}).get(c, {})
            if ppp_cluster:
                clusters_out[c]["ppp_allocations"] = ppp_cluster.get("accounts", {})
                bp = ppp_cluster.get("best_priority", {})
                bc = ppp_cluster.get("best_capacity", {})
                if bp:
                    clusters_out[c]["best_priority_account"] = bp.get("account", "")
                    clusters_out[c]["best_priority_level_fs"] = bp.get("level_fs", 0)
                if bc:
                    clusters_out[c]["best_capacity_account"] = bc.get("account", "")
                    clusters_out[c]["best_capacity_headroom"] = bc.get("headroom", 0)

    return {"status": "ok", "clusters": clusters_out}


@mcp.tool()
def get_ppp_allocations(cluster: Optional[str] = None) -> dict:
    """Get formal PPP GPU allocations from AI Hub across all clusters.

    Returns real-time allocation, consumption, and fairshare data for each
    PPP account (configured in ppp_accounts) on each cluster. This is the
    authoritative source for GPU allocations — not the informal weekly
    team numbers.

    Key fields per account per cluster:
    - gpus_allocated: formal allocation from the Slurm operator
    - gpus_consumed: currently consuming (running jobs)
    - fairshare_avail_gpus: effective availability after fairshare math
    - level_fs: fairshare level (>1.0 = underutilizing, <1.0 = overdrawn)
    - headroom: fairshare_avail - consumed (positive = room to grow)
    - best_priority: account with highest scheduling priority (level_fs)
    - best_capacity: account with most GPU headroom

    Args:
        cluster: Optional cluster name. If omitted, returns all clusters.

    Returns:
        {"status": "ok", "clusters": {"eos": {"accounts": {...}, "best_priority": {...}, ...}}}
    """
    params = f"?cluster={cluster}" if cluster else ""
    return _api_get(f"/api/aihub/allocations{params}")


@mcp.tool()
def where_to_submit(
    nodes: int = 1,
    gpus_per_node: int = 8,
    gpu_type: str = "",
) -> dict:
    """Answer "where should I submit my job?" with a ranked list of clusters.

    This is the primary tool for deciding where to run a job. It combines
    formal PPP allocations (from AI Hub), team quota, team member usage,
    cluster occupancy, and fairshare scheduling priority into a single
    WDS (Where Do I Submit) score from 0-100.

    WDS formula: resource_gate * priority_blend * machine_match
    - resource_gate: can the cluster fit the requested compute?
    - priority_blend: 55% your FS + 20% PPP FS + 25% queue pressure
    - machine_match: 1.0 if GPU type matches, 0.85 if different

    Higher WDS = better. >= 75 is good, 50-74 moderate, < 50 unlikely.

    For each recommended cluster, returns:
    - wds: 0-100 composite score
    - cluster name, GPU type
    - free_for_team: GPUs available considering both PPP headroom and team quota
    - idle_nodes: real-time idle GPU nodes from sinfo (updates every 2min)
    - pending_queue: total pending jobs on the cluster
    - best_account: which PPP account to use (e.g. "llmservice_nemo_reasoning")
    - ppp_level_fs: PPP-wide fairshare priority (>1.0 = credit, <1.0 = overdrawn)
    - my_level_fs: YOUR personal fairshare on that account (determines queue position)
    - resource_gate, queue_score, machine_score: WDS factors for transparency
    - cluster_occupancy: total cluster GPU utilization percentage
    - team_running / team_pending: your team's current GPU usage
    - my_running / my_pending: your personal GPU usage

    Args:
        nodes:         Number of GPU nodes needed. Default 1.
        gpus_per_node: GPUs per node (default 8). Total GPUs = nodes * gpus_per_node.
        gpu_type:      Prefer clusters with this GPU type (e.g. "H100", "B200").
                       Same-type clusters rank first, others still included.

    Returns:
        {"status": "ok", "recommendations": [...], "my_total_running": N, "my_total_pending": N}

    Example use cases:
    - "Where should I submit a 4-node H100 job?" → where_to_submit(nodes=4, gpu_type="H100")
    - "Which cluster has the most room?" → where_to_submit()
    - "Why are my jobs pending on eos?" → Use get_ppp_allocations("eos") for details
    """
    alloc = _api_get("/api/aihub/allocations")
    if not isinstance(alloc, dict) or alloc.get("status") != "ok":
        return {"status": "error", "error": "Could not fetch allocation data"}

    my_fs = _api_get("/api/aihub/my_fairshare")
    my_fs_clusters = my_fs.get("clusters", {}) if isinstance(my_fs, dict) else {}

    partitions = _api_get("/api/partition_summary")
    part_clusters = partitions.get("clusters", {}) if isinstance(partitions, dict) else {}

    team_jobs = _api_get("/api/team_jobs")
    tj_clusters = team_jobs.get("clusters", {}) if isinstance(team_jobs, dict) else {}

    settings = _api_get("/api/settings")
    team_allocs = settings.get("team_gpu_allocations", {}) if isinstance(settings, dict) else {}

    job_gpus = nodes * gpus_per_node
    pref_gpu = gpu_type.lower() if gpu_type else ""

    recommendations = []
    my_total_running = 0
    my_total_pending = 0

    for cn, cd in alloc.get("clusters", {}).items():
        bp = cd.get("best_priority", {})
        bc = cd.get("best_capacity", {})
        ppp_headroom = bc.get("headroom", 0)
        level_fs = bp.get("level_fs", 0)
        best_acct = bc.get("account", "") if ppp_headroom > 50 else bp.get("account", "")
        cluster_gpu = (cd.get("gpu_type") or "").lower()

        ta = team_allocs.get(cn)
        team_num = None
        if ta == "any":
            team_num = None
        elif isinstance(ta, (int, float)) and ta > 0:
            team_num = int(ta)

        tj = tj_clusters.get(cn, {}).get("summary", {})
        tj_users = tj.get("by_user", {})

        team_running = tj.get("total_running", 0)
        team_pending = tj.get("total_pending", 0) + tj.get("total_dependent", 0)

        import os
        me = os.environ.get("USER", "")
        my_data = tj_users.get(me, {})
        my_r = my_data.get("running", 0)
        my_p = my_data.get("pending", 0) + my_data.get("dependent", 0)
        my_total_running += my_r
        my_total_pending += my_p

        if team_num is not None:
            free = min(ppp_headroom, max(0, team_num - team_running))
        else:
            free = ppp_headroom

        same_gpu = (pref_gpu == cluster_gpu) if pref_gpu else True

        occ = cd.get("cluster_occupied_gpus", 0)
        tot = cd.get("cluster_total_gpus", 0)
        occ_pct = round(occ / tot * 100) if tot > 0 else 0

        ps = part_clusters.get(cn, {})
        idle_nodes = ps.get("idle_nodes", 0)
        pending_queue = ps.get("pending_jobs", 0)

        my_acct_fs = my_fs_clusters.get(cn, {}).get(best_acct, {})
        my_level_fs = round(my_acct_fs.get("level_fs", 0), 2) if my_acct_fs else 0

        import math
        hard_capacity = max(ppp_headroom, free)
        resource_gate = min(1, hard_capacity / max(job_gpus, 1), idle_nodes / max(nodes, 1))
        team_penalty = 0.7 if (team_num is not None and free <= 0) else 1.0
        effective_my_fs = my_level_fs if my_level_fs > 0 else level_fs
        my_fs_score = min(effective_my_fs / 1.5, 1)
        ppp_fs_score = min(level_fs / 1.5, 1)
        queue_score = 1 - min(math.log1p(pending_queue / max(idle_nodes, 1)) / math.log1p(50), 1)
        occupancy_factor = 1.15 - 0.30 * min(occ_pct / 100, 1)
        machine_score = 1.0 if same_gpu else 0.85
        priority_blend = 0.55 * my_fs_score + 0.20 * ppp_fs_score + 0.25 * queue_score
        wds = max(0, min(100, round(100 * resource_gate * priority_blend * machine_score * team_penalty * occupancy_factor)))

        if free >= job_gpus or wds > 0:
            recommendations.append({
                "cluster": cn,
                "gpu_type": cd.get("gpu_type", ""),
                "same_gpu_type": same_gpu,
                "wds": wds,
                "free_for_team": free,
                "idle_nodes": idle_nodes,
                "pending_queue": pending_queue,
                "best_account": best_acct,
                "ppp_level_fs": round(level_fs, 2),
                "my_level_fs": my_level_fs,
                "resource_gate": round(resource_gate, 2),
                "queue_score": round(queue_score, 2),
                "machine_score": machine_score,
                "occupancy_factor": round(occupancy_factor, 3),
                "cluster_occupancy_pct": occ_pct,
                "team_running": team_running,
                "team_pending": team_pending,
                "team_alloc": ta if ta is not None else "not set",
                "my_running": my_r,
                "my_pending": my_p,
            })

    recommendations.sort(key=lambda r: -r["wds"])

    return {
        "status": "ok",
        "recommendations": recommendations,
        "my_total_running": my_total_running,
        "my_total_pending": my_total_pending,
        "job_gpus_requested": job_gpus,
    }


@mcp.tool()
def get_gpu_usage_history(
    cluster: Optional[str] = None,
    days: int = 14,
) -> dict:
    """Get GPU allocation vs consumption history over time.

    Shows daily time-series data per PPP account per cluster, useful for
    understanding usage trends, identifying underutilized allocations,
    and planning capacity.

    Each data point includes:
    - gpus_allocated: formal allocation for the day
    - gpus_consumed: actual GPU consumption
    - fairshare_avail: effective availability after fairshare
    - gpus_consumed_normal: consumed under normal QOS
    - gpus_consumed_free: consumed under free/backfill QOS

    Args:
        cluster: Optional cluster name (e.g. "eos"). If omitted, returns all.
        days:    Number of days of history (default 14, max 90).

    Returns:
        {"status": "ok", "clusters": {"eos": {"account_name": [{"date": "...", ...}]}}}
    """
    params = f"?days={min(days, 90)}"
    if cluster:
        params += f"&cluster={cluster}"
    return _api_get(f"/api/aihub/history{params}")


@mcp.tool()
def get_wds_history(
    cluster: Optional[str] = None,
    account: Optional[str] = None,
    days: int = 30,
) -> dict:
    """Get historical WDS scores and their component factors over time.

    WDS (Where Do I Submit) scores are snapshotted every 15 minutes and
    stored with all component factors. Use this to analyze trends:
    - How does my personal fairshare change when I overuse quota?
    - Which clusters have the best scores at which times of day?
    - Does queue pressure correlate with team usage?

    Each row includes: ts, cluster, account, wds, resource_gate,
    my_level_fs, ppp_level_fs, queue_score, idle_nodes, pending_queue,
    ppp_headroom, free_for_team, gpus_consumed, gpus_allocated,
    team_running, my_running, my_pending.

    Args:
        cluster: Filter by cluster name (optional).
        account: Filter by account name or short name like "reasoning" (optional).
        days:    Number of days of history (default 30).

    Returns:
        {"status": "ok", "rows": [...], "count": N}
    """
    params = f"?days={days}"
    if cluster:
        params += f"&cluster={cluster}"
    if account:
        params += f"&account={account}"
    return _api_get(f"/api/wds_history{params}")


@mcp.tool()
def get_cluster_status(cluster: Optional[str] = None) -> dict:
    """Get a unified snapshot of cluster health, capacity, and team position.

    Combines partition data, team usage, weekly allocations, and AI Hub
    fairshare data into a single response. This is a detailed diagnostic
    tool — for quick "where to submit" answers, use where_to_submit().

    Args:
        cluster: Optional cluster name. If omitted, returns all clusters.

    Returns per cluster:
        - gpu_type, total_nodes, idle_nodes, pending_jobs
        - team: weekly_alloc, running_gpus, pending_gpus, free_gpus, status
        - ppp_allocations: per-account formal allocations and fairshare
        - best_priority_account / best_capacity_account: recommended PPP accounts
        - top_partition: name, idle_nodes, priority_tier, est_wait
    """
    result = {}

    avail = _api_get("/api/partition_summary")
    if isinstance(avail, dict) and avail.get("status") == "error":
        avail = {}

    team_data = get_team_gpu_status(cluster)
    team_clusters = team_data.get("clusters", {}) if isinstance(team_data, dict) else {}

    settings = _api_get("/api/settings")
    allocs = settings.get("team_gpu_allocations", {}) if isinstance(settings, dict) else {}

    clusters_to_check = [cluster] if cluster else [
        c for c in (avail if isinstance(avail, dict) else {}) if c not in ("status",)
    ]

    for c in clusters_to_check:
        cdata = avail.get(c, {}) if isinstance(avail, dict) else {}
        if not isinstance(cdata, dict):
            continue

        parts = cdata.get("partitions", [])
        gpu_parts = [p for p in parts if p.get("gpus_per_node", 0) > 0]
        best = sorted(gpu_parts, key=lambda p: (-p.get("idle_nodes", 0), -p.get("priority_tier", 0)))

        tc = team_clusters.get(c, {})
        weekly = allocs.get(c, 0)
        running = tc.get("running_gpus", 0)
        free = tc.get("free_gpus")

        team_status = "unknown"
        if weekly > 0:
            ratio = running / weekly
            if ratio >= 1.0:
                team_status = "over_quota"
            elif ratio >= 0.7:
                team_status = "near_quota"
            else:
                team_status = "under_quota"

        entry = {
            "gpu_type": cdata.get("gpu_type", ""),
            "total_nodes": cdata.get("total_nodes", 0),
            "idle_nodes": cdata.get("idle_nodes", 0),
            "pending_jobs": cdata.get("pending_jobs", 0),
            "team": {
                "weekly_alloc": weekly,
                "running_gpus": running,
                "pending_gpus": tc.get("pending_gpus", 0),
                "free_gpus": free,
                "status": team_status,
                "top_users": sorted(
                    [{"user": u, **d} for u, d in tc.get("users", {}).items()],
                    key=lambda x: -x.get("running_gpus", 0),
                )[:5],
            },
        }

        if best:
            b = best[0]
            entry["top_partition"] = {
                "name": b.get("name", ""),
                "idle_nodes": b.get("idle_nodes", 0),
                "priority_tier": b.get("priority_tier", 0),
                "est_wait": b.get("est_wait", ""),
                "est_wait_cls": b.get("est_wait_cls", ""),
            }

        result[c] = entry

    return {"status": "ok", "clusters": result}


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
