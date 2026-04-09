"""MCP server for clausius — direct-import architecture.

Imports server modules directly and calls Python functions in-process.
No HTTP loopback to localhost:7272 — the MCP process is fully independent
of the Flask/gunicorn UI server.  This means:
  - MCP never goes down during `systemctl restart clausius.service`
  - No HTTP serialization overhead
  - Logbook tools work even when clusters are unreachable
"""

import base64
import math
import os
import re
import sys
import threading
from typing import Optional

sys.path.insert(0, os.path.dirname(__file__))

from mcp.server.fastmcp import FastMCP

from server.db import init_db, get_db, get_history, get_projects, normalize_job_times_local
from server.db import get_board_pinned, dismiss_job, dismiss_by_state_prefix, get_run_with_jobs
from server.config import (
    CLUSTERS, DEFAULT_USER, DB_PATH, TEAM_GPU_ALLOC,
    _cache_lock, _cache, _cache_get,
    _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    PROGRESS_TTL_SEC, CRASH_TTL_SEC, EST_START_TTL_SEC,
    extract_project, get_project_color, get_project_emoji, settings_response,
)
from server.logbooks import (
    list_entries as _lb_list,
    get_entry as _lb_get,
    create_entry as _lb_create,
    update_entry as _lb_update,
    delete_entry as _lb_delete,
    search_entries as _lb_search,
    save_image as _lb_save_image,
    list_logbook_projects,
)
from server.jobs import (
    refresh_all_clusters, refresh_cluster, _is_cache_fresh,
    get_job_stats_cached, fetch_run_metadata_sync, create_run_on_demand,
    schedule_prefetch, fetch_team_jobs,
)
from server.logs import fetch_log_tail, tail_local_file, get_job_log_files_cached
from server.mounts import (
    all_mount_status, cluster_mount_status, run_mount_script,
    resolve_mounted_path,
)
from server.partitions import get_partitions as _get_partitions, get_all_partitions_cached, get_partition_summary
from server.ssh import ssh_run_with_timeout

init_db()

_ssh_initialized = False

def _ensure_ssh():
    global _ssh_initialized
    if _ssh_initialized:
        return
    from server.ssh import ssh_pool_gc_loop
    threading.Thread(target=ssh_pool_gc_loop, daemon=True).start()
    _ssh_initialized = True


mcp = FastMCP("clausius")


# ── helpers ──────────────────────────────────────────────────────────────────

_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted", "account",
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


def _get_all_jobs_snapshot():
    """Return enriched job data for all clusters from the in-memory cache."""
    _ensure_ssh()
    refresh_all_clusters()
    with _cache_lock:
        snapshot = {k: dict(v) for k, v in _cache.items()}
    for name in CLUSTERS:
        if name not in snapshot:
            snapshot[name] = {"status": "ok", "jobs": [], "updated": None}
        data = snapshot[name]
        if data.get("status") != "ok":
            continue
        for j in data.get("jobs", []):
            j = normalize_job_times_local(j)
            if not j.get("project"):
                j["project"] = extract_project(j.get("name") or j.get("job_name") or "")
            proj = j.get("project", "")
            if proj:
                j["project_color"] = get_project_color(proj)
                j["project_emoji"] = get_project_emoji(proj)
    return snapshot


def _get_cluster_jobs(cluster):
    """Return enriched job data for one cluster."""
    _ensure_ssh()
    refresh_cluster(cluster)
    with _cache_lock:
        data = dict(_cache.get(cluster, {"status": "ok", "jobs": [], "updated": None}))
    if data.get("status") == "ok":
        pinned = get_board_pinned(cluster)
        live_ids = {j["jobid"] for j in data.get("jobs", [])}
        for p in pinned:
            if p["job_id"] not in live_ids:
                data["jobs"] = data.get("jobs", []) + [{
                    **p, "_pinned": True, "jobid": p["job_id"], "name": p["job_name"],
                }]
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]
        for j in data.get("jobs", []):
            if not j.get("project"):
                j["project"] = extract_project(j.get("name") or j.get("job_name") or "")
            proj = j.get("project", "")
            if proj:
                j["project_color"] = get_project_color(proj)
                j["project_emoji"] = get_project_emoji(proj)
    return data


# ── tools ────────────────────────────────────────────────────────────────────

@mcp.tool()
def health_check() -> dict:
    """Quick health check. Returns ok if the MCP server is running."""
    return {"status": "ok", "db": os.path.exists(DB_PATH), "clusters": list(CLUSTERS.keys())}


@mcp.tool()
def list_jobs(cluster: Optional[str] = None, project: Optional[str] = None) -> list[dict]:
    """List active jobs across all clusters, or filtered by cluster/project.

    Returns compact job records with state, progress, dependencies, and est_start.
    Includes both live squeue jobs and board-pinned terminal jobs.
    """
    if cluster:
        if cluster not in CLUSTERS:
            return [{"error": f"Unknown cluster: {cluster}"}]
        data = _get_cluster_jobs(cluster)
        if data.get("status") == "error":
            return [{"error": data.get("error", "Unknown error")}]
        jobs = [_slim_job(cluster, j) for j in data.get("jobs", [])]
    else:
        snapshot = _get_all_jobs_snapshot()
        jobs = []
        for cname, cdata in snapshot.items():
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
    _ensure_ssh()
    return get_job_log_files_cached(cluster, job_id)


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
    _ensure_ssh()
    if not path:
        result = get_job_log_files_cached(cluster, job_id)
        files = result.get("files", [])
        if not files:
            return "Error: No log files found for this job."
        preferred = next((f for f in files if "main" in f.get("label", "")), None)
        path = (preferred or files[0])["path"]

    if not path:
        return "Error: No log path available."

    if cluster != "local":
        mounted = resolve_mounted_path(cluster, path, want_dir=False)
        if mounted:
            return tail_local_file(mounted, lines) or "(empty)"
    return fetch_log_tail(cluster, path, lines) or "(empty)"


@mcp.tool()
def get_job_stats(cluster: str, job_id: str) -> dict:
    """Get resource stats for a running job (CPU, memory, GPU utilisation)."""
    _ensure_ssh()
    return get_job_stats_cached(cluster, job_id)


@mcp.tool()
def get_run_info(cluster: str, root_job_id: str) -> dict:
    """Get detailed run info: batch script, scontrol, env vars, conda state, and associated jobs."""
    _ensure_ssh()
    run = get_run_with_jobs(cluster, root_job_id)
    if not run:
        actual_root = create_run_on_demand(cluster, root_job_id)
        if actual_root:
            run = get_run_with_jobs(cluster, actual_root)
        if not run:
            return {"status": "error", "error": "Run not found"}
    if not run.get("meta_fetched"):
        threading.Thread(target=fetch_run_metadata_sync, args=(cluster, run["root_job_id"]), daemon=True).start()
    return {"status": "ok", "run": run}


@mcp.tool()
def get_history(cluster: Optional[str] = None, project: Optional[str] = None, limit: int = 50) -> list[dict]:
    """Get past job history, optionally filtered by cluster and/or project."""
    from server.db import get_history as _db_history
    rows = _db_history(cluster or "all", limit, project=project or "")
    for r in rows:
        if not r.get("project"):
            r["project"] = extract_project(r.get("job_name") or r.get("name") or "")
        proj = r.get("project", "")
        if proj:
            r["project_color"] = get_project_color(proj)
            r["project_emoji"] = get_project_emoji(proj)
    return rows


@mcp.tool()
def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job. Destructive — only when user explicitly asks."""
    _ensure_ssh()
    if cluster not in CLUSTERS:
        return {"status": "error", "error": "Unknown cluster"}
    if cluster == "local":
        try:
            os.kill(int(job_id), 15)
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)}
    try:
        ssh_run_with_timeout(cluster, f"scancel {job_id}", timeout_sec=10)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@mcp.tool()
def cancel_jobs(cluster: str, job_ids: list[str]) -> dict:
    """Cancel multiple jobs on a cluster. Destructive — only when user explicitly asks."""
    _ensure_ssh()
    if cluster not in CLUSTERS:
        return {"status": "error", "error": "Unknown cluster"}
    sanitized = [str(jid).strip() for jid in job_ids if str(jid).strip().isdigit()]
    if not sanitized:
        return {"status": "error", "error": "No valid job IDs"}
    try:
        ssh_run_with_timeout(cluster, f"scancel {','.join(sanitized)}", timeout_sec=10)
        return {"status": "ok", "cancelled": len(sanitized)}
    except Exception as e:
        return {"status": "error", "error": str(e)}


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
    _ensure_ssh()
    if cluster not in CLUSTERS:
        return {"status": "error", "error": "Unknown cluster"}
    if cluster == "local":
        return {"status": "error", "error": "run_script not supported for local"}
    allowed = {"python3", "python", "bash", "sh"}
    if interpreter not in allowed:
        return {"status": "error", "error": f"interpreter must be one of: {', '.join(sorted(allowed))}"}
    timeout = max(1, min(timeout, 300))
    encoded = base64.b64encode(script.encode()).decode()
    cmd = f"echo '{encoded}' | base64 -d | {interpreter}"
    try:
        stdout, stderr = ssh_run_with_timeout(cluster, cmd, timeout_sec=timeout)
        return {"status": "ok", "stdout": stdout, "stderr": stderr, "interpreter": interpreter, "cluster": cluster}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── cluster info ──────────────────────────────────────────────────────────────

@mcp.tool()
def get_partitions(cluster: Optional[str] = None) -> dict:
    """Get Slurm partition details: state, time limits, priority, nodes, GPUs, queue depth.

    Returns per-partition data including idle_nodes, pending_jobs, gpus_per_node,
    priority_tier, preempt_mode, and access restrictions.
    """
    _ensure_ssh()
    if cluster:
        data = _get_partitions(cluster)
        if data is None:
            return {"status": "error", "error": f"Could not fetch partitions from {cluster}"}
        return {"status": "ok", "cluster": cluster, "partitions": data}
    data = get_all_partitions_cached()
    return {"status": "ok", "clusters": data}


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
    _ensure_ssh()
    from concurrent.futures import ThreadPoolExecutor
    from server.aihub import get_ppp_allocations as _aihub_alloc, get_my_fairshare as _aihub_fs

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_alloc = pool.submit(_aihub_alloc)
        f_fs = pool.submit(_aihub_fs)
        f_parts = pool.submit(get_partition_summary)
        f_tj = pool.submit(lambda: {c: fetch_team_jobs(c) for c in CLUSTERS if c != "local"})

    try:
        alloc = f_alloc.result()
    except Exception:
        return {"status": "error", "error": "Could not fetch allocation data"}
    my_fs_clusters = (f_fs.result() or {}).get("clusters", {})
    part_clusters = (f_parts.result() or {})
    tj_clusters = f_tj.result() or {}
    team_allocs = settings_response().get("team_gpu_allocations", {})

    job_gpus = nodes * gpus_per_node
    pref_gpu = gpu_type.lower() if gpu_type else ""
    recommendations = []
    my_total_running = 0
    my_total_pending = 0
    me = os.environ.get("USER", "")

    all_cluster_names = set(alloc.get("clusters", {}).keys())
    for cn in CLUSTERS:
        if cn != "local":
            all_cluster_names.add(cn)

    for cn in all_cluster_names:
        cd = alloc.get("clusters", {}).get(cn, {})
        has_ppp = bool(cd.get("accounts"))
        cluster_gpu = (cd.get("gpu_type") or CLUSTERS.get(cn, {}).get("gpu_type") or "").lower()
        ta = team_allocs.get(cn)
        team_num = int(ta) if isinstance(ta, (int, float)) and ta > 0 else (None if ta == "any" else None)
        tj = (tj_clusters.get(cn) or {}).get("summary", {})
        tj_users = tj.get("by_user", {})
        team_running = tj.get("total_running", 0)
        team_pending = tj.get("total_pending", 0) + tj.get("total_dependent", 0)
        my_data = tj_users.get(me, {})
        my_r = my_data.get("running", 0)
        my_p = my_data.get("pending", 0) + my_data.get("dependent", 0)
        my_total_running += my_r
        my_total_pending += my_p
        same_gpu = (pref_gpu == cluster_gpu) if pref_gpu else True
        ps = part_clusters.get(cn, {})
        idle_nodes = ps.get("idle_nodes", 0)
        pending_queue = ps.get("pending_jobs", 0)
        gpn = ps.get("partitions", [{}])[0].get("gpus_per_node", 0) if ps.get("partitions") else CLUSTERS.get(cn, {}).get("gpus_per_node", 8) or 8
        idle_gpus = idle_nodes * gpn

        notes = []
        if not has_ppp:
            notes.append("No PPP allocation data — fairshare unknown")
        if ta is None or ta == 0:
            notes.append(f"No informal team allocation set for {cn}")
        elif team_num is not None and team_running >= team_num:
            notes.append(f"Team over informal quota ({team_running}/{team_num} GPUs)")

        accounts = []
        if has_ppp:
            for acct_name, ad in cd.get("accounts", {}).items():
                ppp_headroom = ad.get("headroom", 0)
                level_fs = ad.get("level_fs", 0)
                my_acct_fs = my_fs_clusters.get(cn, {}).get(acct_name, {})
                my_level_fs = round(my_acct_fs.get("level_fs", 0), 2) if my_acct_fs else 0
                free = min(ppp_headroom, max(0, team_num - team_running)) if team_num is not None else ppp_headroom
                hard_capacity = max(ppp_headroom, free)
                resource_gate = min(1, hard_capacity / max(job_gpus, 1), idle_nodes / max(nodes, 1))
                team_penalty = 0.7 if (team_num is not None and free <= 0) else 1.0
                effective_my_fs = my_level_fs if my_level_fs > 0 else level_fs
                my_fs_score = min(effective_my_fs / 1.5, 1)
                ppp_fs_score = min(level_fs / 1.5, 1)
                queue_score = 1 - min(math.log1p(pending_queue / max(idle_nodes, 1)) / math.log1p(50), 1)
                occ = cd.get("cluster_occupied_gpus", 0)
                tot = cd.get("cluster_total_gpus", 0)
                occ_pct = round(occ / tot * 100) if tot > 0 else 0
                occupancy_factor = 1.15 - 0.30 * min(occ_pct / 100, 1)
                machine_score = 1.0 if same_gpu else 0.85
                priority_blend = 0.55 * my_fs_score + 0.20 * ppp_fs_score + 0.25 * queue_score
                wds = max(0, min(100, round(100 * resource_gate * priority_blend * machine_score * team_penalty * occupancy_factor)))
                accounts.append({
                    "account": acct_name, "account_short": acct_name.split("_")[-1] if "_" in acct_name else acct_name,
                    "wds": wds, "ppp_level_fs": round(level_fs, 2), "my_level_fs": my_level_fs,
                    "headroom": ppp_headroom, "free_for_team": free,
                    "gpus_consumed": ad.get("gpus_consumed", 0), "gpus_allocated": ad.get("gpus_allocated", 0),
                })
            accounts.sort(key=lambda a: -a["wds"])
        else:
            resource_gate = min(1, idle_nodes / max(nodes, 1))
            queue_score = 1 - min(math.log1p(pending_queue / max(idle_nodes, 1)) / math.log1p(50), 1)
            machine_score = 1.0 if same_gpu else 0.85
            wds = max(0, min(100, round(100 * resource_gate * 0.5 * machine_score)))

        best_wds = accounts[0]["wds"] if accounts else wds
        best_acct = accounts[0]["account"] if accounts else ""

        recommendations.append({
            "cluster": cn,
            "gpu_type": (cd.get("gpu_type") or CLUSTERS.get(cn, {}).get("gpu_type") or "").upper(),
            "same_gpu_type": same_gpu,
            "wds": best_wds,
            "best_account": best_acct,
            "accounts": accounts,
            "idle_gpus": idle_gpus,
            "idle_nodes": idle_nodes,
            "pending_queue": pending_queue,
            "cluster_occupancy_pct": cd.get("cluster_occupied_gpus", 0) * 100 // max(cd.get("cluster_total_gpus", 1), 1) if cd else 0,
            "team_running": team_running,
            "team_pending": team_pending,
            "team_alloc": ta if ta is not None else "not set",
            "my_running": my_r,
            "my_pending": my_p,
            "notes": notes,
        })

    recommendations.sort(key=lambda r: -r["wds"])
    return {"status": "ok", "recommendations": recommendations,
            "my_total_running": my_total_running, "my_total_pending": my_total_pending,
            "job_gpus_requested": job_gpus}


# ── mount & board tools ──────────────────────────────────────────────────────

@mcp.tool()
def get_mounts() -> dict:
    """Get SSHFS mount status for all clusters."""
    return {"status": "ok", "mounts": all_mount_status()}


@mcp.tool()
def mount_cluster(cluster: str, action: str = "mount") -> dict:
    """Mount or unmount a cluster's remote filesystem via SSHFS."""
    if action not in ("mount", "unmount"):
        return {"status": "error", "error": "action must be 'mount' or 'unmount'"}
    ok, msg = run_mount_script(action, cluster)
    if not ok:
        return {"status": "error", "error": msg}
    return {"status": "ok", "message": msg, "mounts": all_mount_status()}


@mcp.tool()
def clear_failed(cluster: str) -> dict:
    """Dismiss all failed/cancelled/timeout job pins from a cluster's board."""
    from server.config import TERMINAL_STATES
    dismiss_by_state_prefix(cluster, list(TERMINAL_STATES))
    return {"status": "ok"}


@mcp.tool()
def clear_completed(cluster: str) -> dict:
    """Dismiss all completed job pins from a cluster's board."""
    dismiss_by_state_prefix(cluster, ["COMPLETED"])
    return {"status": "ok"}


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
    return _lb_list(project, query=query or None, sort=sort, limit=limit, entry_type=entry_type or None)


@mcp.tool()
def read_logbook_entry(project: str, entry_id: int) -> dict:
    """Read a single logbook entry with full markdown body."""
    return _lb_get(project, entry_id)


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
    if sort not in ("edited_at", "created_at", "title"):
        return {"status": "error", "error": "sort must be one of: edited_at, created_at, title"}
    if entry_type and entry_type not in ("note", "plan"):
        return {"status": "error", "error": "entry_type must be 'note', 'plan', or omitted"}

    if project:
        projects = [project]
    else:
        projects = list_logbook_projects()
        if not projects:
            return {"status": "ok", "count": 0, "truncated": False, "projects": [], "entries": [], "errors": {}}

    entries = []
    errors = {}
    truncated = False
    for p in projects:
        listed = _lb_list(p, sort=sort, limit=limit_per_project, entry_type=entry_type or None)
        if isinstance(listed, dict) and listed.get("status") == "error":
            errors[p] = listed.get("error", "Failed")
            continue
        for item in (listed if isinstance(listed, list) else []):
            eid = item.get("id")
            if eid is None:
                continue
            full = _lb_get(p, eid)
            if isinstance(full, dict) and full.get("status") != "error":
                entries.append(full)
            if len(entries) >= max_entries:
                truncated = True
                break
        if truncated:
            break
    return {"status": "ok", "count": len(entries), "truncated": truncated, "projects": projects, "entries": entries, "errors": errors}


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
    if field not in ("title", "body", "both"):
        return {"status": "error", "error": "field must be 'title', 'body', or 'both'"}

    if regex:
        try:
            compiled = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            return {"status": "error", "error": f"Invalid regex: {e}"}
        test = lambda text: bool(compiled.search(text or ""))
    else:
        pat_lower = pattern.lower()
        test = lambda text: pat_lower in (text or "").lower()

    projects = [project] if project else list_logbook_projects()
    results = []
    for p in projects:
        listed = _lb_list(p, sort="edited_at", limit=500, entry_type=entry_type or None)
        if not isinstance(listed, list):
            continue
        for item in listed:
            eid = item.get("id")
            if eid is None:
                continue
            title_text = item.get("title", "")
            title_match = test(title_text) if field in ("title", "both") else False
            if field == "title" and not title_match:
                continue
            if field in ("body", "both") and not title_match:
                full_entry = _lb_get(p, eid)
                if not isinstance(full_entry, dict) or full_entry.get("status") == "error":
                    continue
                if not test(full_entry.get("body", "")):
                    continue
                results.append(full_entry)
            elif full_body:
                full_entry = _lb_get(p, eid)
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
    return {"status": "ok", "count": len(results), "entries": results}


@mcp.tool()
def create_logbook_entry(project: str, title: str, body: str = "", entry_type: str = "note") -> dict:
    """Create a new logbook entry. Supports markdown, #N cross-refs, @run-name refs, images.

    See the project-logbook workspace rule for full formatting guidelines.
    entry_type: "note" (results/findings) or "plan" (plans/designs).
    """
    return _lb_create(project, title, body, entry_type=entry_type)


@mcp.tool()
def update_logbook_entry(
    project: str,
    entry_id: int,
    title: Optional[str] = None,
    body: Optional[str] = None,
) -> dict:
    """Update a logbook entry's title and/or body. Bumps edited_at."""
    return _lb_update(project, entry_id, title=title, body=body)


@mcp.tool()
def delete_logbook_entry(project: str, entry_id: int) -> dict:
    """Delete a logbook entry. Destructive."""
    return _lb_delete(project, entry_id)


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
    return _lb_save_image(project, filename, data)


# ── resources ────────────────────────────────────────────────────────────────

@mcp.resource("jobs://summary")
def jobs_summary() -> str:
    """Quick overview of all clusters: running/pending/failed counts."""
    snapshot = _get_all_jobs_snapshot()
    lines = []
    total_r = total_p = total_f = 0
    for cname, cdata in snapshot.items():
        if cdata.get("status") == "error":
            lines.append(f"{cname}: unreachable")
            continue
        jobs = cdata.get("jobs", [])
        r = sum(1 for j in jobs if j.get("state", "").upper() == "RUNNING")
        p = sum(1 for j in jobs if j.get("state", "").upper() == "PENDING")
        f = sum(1 for j in jobs if "FAIL" in j.get("state", "").upper())
        total_r += r; total_p += p; total_f += f
        parts = []
        if r: parts.append(f"{r} running")
        if p: parts.append(f"{p} pending")
        if f: parts.append(f"{f} failed")
        lines.append(f"{cname}: {', '.join(parts) if parts else 'idle'}")
    return f"Total: {total_r} running, {total_p} pending, {total_f} failed\n" + "\n".join(lines)


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
