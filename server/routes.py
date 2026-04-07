"""Flask route handlers as a Blueprint."""

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from datetime import datetime, timedelta

from flask import Blueprint, g, jsonify, request, render_template, make_response

_log = logging.getLogger(__name__)
_SLOW_REQUEST_MS = 2000

from .config import (
    CLUSTERS, DEFAULT_USER, TEAM_NAME, TERMINAL_STATES, RESULT_DIR_NAMES,
    _CONFIG, _cache_lock, _cache,
    _cache_get, _cache_set,
    _log_content_cache, _dir_list_cache, _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    _team_usage_cache,
    LOG_CONTENT_TTL_SEC, DIR_LIST_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC, EST_START_TTL_SEC,
    TEAM_USAGE_TTL_SEC,
    reload_config, settings_response,
    get_project_color, get_project_emoji, extract_project,
)
from .db import (
    normalize_job_times_local, get_board_pinned,
    dismiss_job, dismiss_by_state_prefix,
    get_history, get_projects, get_db,
    _restore_dependency_fields,
)
from .ssh import ssh_run, ssh_run_with_timeout, ssh_run_data, ssh_run_data_with_timeout
from .mounts import (
    resolve_mounted_path, resolve_file_path,
    list_local_dir, prefetch_nested_dir_cache_local,
    cluster_mount_status, all_mount_status, run_mount_script,
)
from .logs import (
    fetch_log_tail, tail_local_file, extract_progress,
    get_job_log_files_cached,
    read_jsonl_index, read_jsonl_record,
)
from .jobs import (
    refresh_all_clusters, refresh_cluster, _is_cache_fresh,
    schedule_prefetch, prefetch_cluster_bulk, fetch_est_start_bulk,
    fetch_team_usage, fetch_team_jobs,
    get_job_stats_cached, fetch_run_metadata_sync,
    create_run_on_demand,
    _last_polled,
)
from .db import get_run_with_jobs

api = Blueprint("api", __name__)


@api.before_request
def _start_timer():
    g._req_start = time.monotonic()


@api.after_request
def _log_slow(response):
    start = getattr(g, '_req_start', None)
    if start is not None:
        ms = (time.monotonic() - start) * 1000
        if ms > _SLOW_REQUEST_MS:
            _log.warning("slow request: %s %s — %.0fms", request.method, request.path, ms)
    return response


def _rebuild_cross_deps(jobs):
    """Rebuild depends_on/dependents across the full merged set of jobs.

    After merging live and pinned jobs, their dependency arrays only reference
    IDs within their original sets.  This rebuilds them so cross-references
    (e.g. a running child pointing to a completed parent) are restored.
    Uses both explicit dependency parsing and name-based inference.
    """
    from .jobs import parse_dependency
    from .db import _infer_parent_from_name
    id_set = {j.get("jobid") or j.get("job_id", "") for j in jobs}
    by_name = {}
    for j in jobs:
        name = j.get("name") or j.get("job_name") or ""
        if name:
            by_name[name] = j.get("jobid") or j.get("job_id", "")

    for j in jobs:
        dep_details = j.get("dep_details", [])
        if not dep_details:
            raw = j.get("dependency", "")
            if raw and raw not in ("(null)", "None"):
                dep_details = parse_dependency(raw)
                j["dep_details"] = dep_details
        j["depends_on"] = [d["job_id"] for d in dep_details if d["job_id"] in id_set]

        if not j["depends_on"]:
            name = j.get("name") or j.get("job_name") or ""
            inferred = _infer_parent_from_name(name, by_name, id_set, j)
            if inferred:
                j["depends_on"] = [inferred]
                j["dep_details"] = [{"type": "afterany", "job_id": inferred}]

    children_map = {}
    for j in jobs:
        jid = j.get("jobid") or j.get("job_id", "")
        for pid in j.get("depends_on", []):
            children_map.setdefault(pid, []).append(jid)
    for j in jobs:
        jid = j.get("jobid") or j.get("job_id", "")
        j["dependents"] = children_map.get(jid, [])


def _fill_run_ids(cluster, jobs):
    """Look up run_id from DB for live jobs that are missing it."""
    need = [j for j in jobs if not j.get("run_id") and not j.get("_pinned")]
    if not need:
        return
    con = get_db()
    for j in need:
        jid = j.get("jobid") or j.get("job_id", "")
        row = con.execute(
            "SELECT run_id FROM job_history WHERE cluster=? AND job_id=?",
            (cluster, jid),
        ).fetchone()
        if row and row["run_id"]:
            j["run_id"] = row["run_id"]
    con.close()


@api.route("/")
def index():
    resp = make_response(render_template("index.html", clusters=CLUSTERS, username=DEFAULT_USER, team=TEAM_NAME))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@api.route("/api/jobs")
def api_jobs():
    if request.args.get("refresh", "0") == "1":
        refresh_all_clusters()
    else:
        stale = [n for n in CLUSTERS if not _is_cache_fresh(n)]
        if stale:
            threading.Thread(target=refresh_all_clusters, daemon=True).start()

    with _cache_lock:
        snapshot = {k: dict(v) for k, v in _cache.items()}

    all_pinned = get_board_pinned()
    pinned_by_cluster = {}
    for row in all_pinned:
        c = row["cluster"]
        pinned_by_cluster.setdefault(c, []).append(row)

    for name in list(CLUSTERS.keys()):
        if name not in snapshot:
            snapshot[name] = {"status": "ok", "jobs": [], "updated": None}
        data = snapshot[name]
        if data.get("status") != "ok":
            continue
        live_ids = {j["jobid"] for j in data.get("jobs", [])}
        pinned = [
            {**p, "_pinned": True, "jobid": p["job_id"], "name": p["job_name"]}
            for p in pinned_by_cluster.get(name, [])
            if p["job_id"] not in live_ids
        ]
        if pinned:
            data["jobs"] = data.get("jobs", []) + pinned
            _rebuild_cross_deps(data["jobs"])
        _fill_run_ids(name, data.get("jobs", []))
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]
        for j in data.get("jobs", []):
            st = j.get("state", "").upper()
            jid = j.get("jobid")
            if st in ("RUNNING", "COMPLETING"):
                pct = _cache_get(_progress_cache, (name, jid), PROGRESS_TTL_SEC)
                if pct is not None:
                    j["progress"] = pct
                    src = _cache_get(_progress_source_cache, (name, jid), PROGRESS_TTL_SEC)
                    if src:
                        j["progress_source"] = src
                crash = _cache_get(_crash_cache, (name, jid), CRASH_TTL_SEC)
                if crash:
                    j["crash_detected"] = crash
            if st == "PENDING":
                est = _cache_get(_est_start_cache, (name, jid), EST_START_TTL_SEC)
                if est:
                    j["est_start"] = est
            if not j.get("project"):
                j["project"] = extract_project(j.get("name") or j.get("job_name") or "")
            proj = j.get("project", "")
            if proj:
                j["project_color"] = get_project_color(proj)
                j["project_emoji"] = get_project_emoji(proj)

    def cluster_sort_key(item):
        name, data = item
        jobs = data.get("jobs", [])
        has_running = any(j.get("state") in ("RUNNING", "COMPLETING") for j in jobs if not j.get("_pinned"))
        has_pending = any(j.get("state") == "PENDING" for j in jobs if not j.get("_pinned"))
        has_live = any(not j.get("_pinned") for j in jobs)
        return (not has_running, not has_pending, not has_live, name)

    ordered = dict(sorted(snapshot.items(), key=cluster_sort_key))

    for c, d in ordered.items():
        if d.get("status") != "ok":
            continue
        active_jobs = [
            j for j in d.get("jobs", [])
            if str(j.get("state", "")).upper() in {"RUNNING", "COMPLETING"}
            and not j.get("_pinned")
        ][:3]
        for j in active_jobs:
            schedule_prefetch(c, j.get("jobid"))

    mounts = all_mount_status()
    for c, d in ordered.items():
        if c != "local":
            d["mount"] = mounts.get(c, {"mounted": False, "root": ""})
    return jsonify(ordered)


@api.route("/api/mounts")
def api_mounts():
    cluster = request.args.get("cluster", "all")
    if cluster != "all":
        if cluster not in CLUSTERS or cluster == "local":
            return jsonify({"status": "error", "error": "Unknown cluster"}), 404
        return jsonify({"status": "ok", "mounts": {cluster: cluster_mount_status(cluster)}})
    return jsonify({"status": "ok", "mounts": all_mount_status()})


@api.route("/api/mount/<action>/<cluster>", methods=["POST"])
def api_mount_action(action, cluster):
    ok, msg = run_mount_script(action, cluster)
    if not ok:
        return jsonify({"status": "error", "error": msg}), 400
    return jsonify({"status": "ok", "message": msg, "mounts": all_mount_status()})


@api.route("/api/mount/<action>", methods=["POST"])
def api_mount_action_all(action):
    ok, msg = run_mount_script(action, "all")
    if not ok:
        return jsonify({"status": "error", "error": msg}), 400
    return jsonify({"status": "ok", "message": msg, "mounts": all_mount_status()})


@api.route("/api/clear_failed/<cluster>", methods=["POST"])
def api_clear_failed(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, list(TERMINAL_STATES))
    return jsonify({"status": "ok"})


@api.route("/api/clear_cancelled/<cluster>", methods=["POST"])
def api_clear_cancelled(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, ["CANCELLED"])
    return jsonify({"status": "ok"})


@api.route("/api/clear_completed/<cluster>", methods=["POST"])
def api_clear_completed(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, ["COMPLETED"])
    return jsonify({"status": "ok"})


@api.route("/api/clear_failed_job/<cluster>/<job_id>", methods=["POST"])
def api_clear_failed_job(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_job(cluster, job_id)
    return jsonify({"status": "ok"})


@api.route("/api/cleanup", methods=["POST"])
def api_cleanup():
    payload = request.get_json(silent=True) or {}
    days = int(payload.get("days", 30))
    dry_run = bool(payload.get("dry_run", False))
    if days < 1:
        return jsonify({"status": "error", "error": "days must be >= 1"}), 400

    con = get_db()
    try:
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = con.execute("""
            SELECT cluster, job_id, job_name, log_path, ended_at
            FROM job_history WHERE ended_at < ? AND cluster != 'local' ORDER BY ended_at
        """, (cutoff,)).fetchall()

        if not rows:
            return jsonify({"status": "ok", "deleted_records": 0, "cleaned_dirs": 0, "message": f"No records older than {days} days."})

        cleaned_dirs = []
        deleted_ids = []
        for row in rows:
            r = dict(row)
            deleted_ids.append((r["cluster"], r["job_id"]))
            if not dry_run:
                _cleanup_mounted_logs(r["cluster"], r["job_id"], r.get("log_path", ""), cleaned_dirs)

        if not dry_run:
            con.execute("DELETE FROM job_history WHERE ended_at < ? AND cluster != 'local'", (cutoff,))
            con.execute("""DELETE FROM job_stats_snapshots WHERE job_id IN (
                SELECT job_id FROM job_stats_snapshots s
                WHERE s.ts < ? AND NOT EXISTS (
                    SELECT 1 FROM job_history h WHERE h.cluster = s.cluster AND h.job_id = s.job_id
                )
            )""", (cutoff,))
            con.commit()
        return jsonify({"status": "ok", "deleted_records": len(deleted_ids), "cleaned_dirs": len(cleaned_dirs),
                         "dry_run": dry_run, "days": days, "cleaned_paths": cleaned_dirs[:20]})
    finally:
        con.close()


def _cleanup_mounted_logs(cluster_name, job_id, log_path, cleaned_list):
    if not log_path:
        return
    remote_log_dir = os.path.dirname(log_path)
    remote_output_dir = os.path.dirname(remote_log_dir)
    for dname in RESULT_DIR_NAMES:
        remote_dir = remote_output_dir.rstrip("/") + "/" + dname
        local_dir = resolve_mounted_path(cluster_name, remote_dir, want_dir=True)
        if local_dir and os.path.isdir(local_dir):
            for fname in os.listdir(local_dir):
                if job_id in fname:
                    fpath = os.path.join(local_dir, fname)
                    try:
                        if os.path.isfile(fpath):
                            os.remove(fpath)
                            cleaned_list.append(fpath)
                        elif os.path.isdir(fpath):
                            shutil.rmtree(fpath, ignore_errors=True)
                            cleaned_list.append(fpath)
                    except Exception:
                        pass


@api.route("/api/jobs/<cluster>")
def api_jobs_cluster(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if request.args.get("force") == "1":
        _last_polled[cluster] = 0.0
        refresh_cluster(cluster)
    elif not _is_cache_fresh(cluster):
        threading.Thread(target=refresh_cluster, args=(cluster,), daemon=True).start()
    with _cache_lock:
        data = dict(_cache.get(cluster, {"status": "ok", "jobs": [], "updated": None}))
    if data.get("status") == "ok":
        live_ids = {j["jobid"] for j in data.get("jobs", [])}
        pinned = [
            {**p, "_pinned": True, "jobid": p["job_id"], "name": p["job_name"]}
            for p in get_board_pinned(cluster) if p["job_id"] not in live_ids
        ]
        if pinned:
            data = dict(data)
            data["jobs"] = data.get("jobs", []) + pinned
            _rebuild_cross_deps(data["jobs"])
        _fill_run_ids(cluster, data.get("jobs", []))
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]
        for j in data.get("jobs", []):
            s = str(j.get("state", "")).upper()
            if s in {"RUNNING", "COMPLETING"} and not j.get("_pinned"):
                schedule_prefetch(cluster, j.get("jobid"))
            if s in ("RUNNING", "COMPLETING"):
                jid = j.get("jobid")
                pct = _cache_get(_progress_cache, (cluster, jid), PROGRESS_TTL_SEC)
                if pct is not None:
                    j["progress"] = pct
                    src = _cache_get(_progress_source_cache, (cluster, jid), PROGRESS_TTL_SEC)
                    if src:
                        j["progress_source"] = src
                crash = _cache_get(_crash_cache, (cluster, jid), CRASH_TTL_SEC)
                if crash:
                    j["crash_detected"] = crash
            if s == "PENDING":
                jid = j.get("jobid")
                est = _cache_get(_est_start_cache, (cluster, jid), EST_START_TTL_SEC)
                if est:
                    j["est_start"] = est
            if not j.get("project"):
                j["project"] = extract_project(j.get("name") or j.get("job_name") or "")
            proj = j.get("project", "")
            if proj:
                j["project_color"] = get_project_color(proj)
                j["project_emoji"] = get_project_emoji(proj)
    if cluster != "local":
        data["mount"] = cluster_mount_status(cluster)
    return jsonify(data)


@api.route("/api/prefetch_visible", methods=["POST"])
def api_prefetch_visible():
    payload = request.get_json(silent=True) or {}
    jobs = payload.get("jobs", [])
    by_cluster = {}
    pending_by_cluster = {}
    for item in jobs:
        c = item.get("cluster")
        jid = str(item.get("job_id", "")).strip()
        if not c or not jid or c not in CLUSTERS:
            continue
        if item.get("state", "").upper() == "PENDING":
            pending_by_cluster.setdefault(c, []).append(jid)
        else:
            by_cluster.setdefault(c, []).append(jid)

    def _run():
        threads = []
        for c, ids in by_cluster.items():
            t = threading.Thread(target=prefetch_cluster_bulk, args=(c, ids), daemon=True)
            threads.append(t)
            t.start()
        for c, ids in pending_by_cluster.items():
            t = threading.Thread(target=fetch_est_start_bulk, args=(c, ids), daemon=True)
            threads.append(t)
            t.start()
        team_clusters = set(pending_by_cluster.keys())
        for c in team_clusters:
            t = threading.Thread(target=fetch_team_usage, args=(c,), daemon=True)
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=25)

    threading.Thread(target=_run, daemon=True).start()
    total = sum(len(v) for v in by_cluster.values()) + sum(len(v) for v in pending_by_cluster.values())
    return jsonify({"status": "ok", "clusters": list(set(list(by_cluster.keys()) + list(pending_by_cluster.keys()))), "jobs": total})


@api.route("/api/progress", methods=["POST"])
def api_progress():
    """Return cached progress percentages and estimated start times."""
    payload = request.get_json(silent=True) or {}
    jobs = payload.get("jobs", [])
    progress = {}
    progress_sources = {}
    est_starts = {}
    for item in jobs:
        c = item.get("cluster")
        jid = str(item.get("job_id", "")).strip()
        if not c or not jid:
            continue
        pct = _cache_get(_progress_cache, (c, jid), PROGRESS_TTL_SEC)
        if pct is not None:
            progress[f"{c}:{jid}"] = pct
            src = _cache_get(_progress_source_cache, (c, jid), PROGRESS_TTL_SEC)
            if src:
                progress_sources[f"{c}:{jid}"] = src
        est = _cache_get(_est_start_cache, (c, jid), EST_START_TTL_SEC)
        if est:
            est_starts[f"{c}:{jid}"] = est

    team_usage = {}
    seen_clusters = {item.get("cluster") for item in jobs if item.get("cluster")}
    for c in seen_clusters:
        tu = _cache_get(_team_usage_cache, c, TEAM_USAGE_TTL_SEC)
        if tu:
            team_usage[c] = tu

    from .config import TEAM_GPU_ALLOC
    return jsonify({"progress": progress, "progress_sources": progress_sources, "est_starts": est_starts, "team_usage": team_usage, "team_gpu_allocations": dict(TEAM_GPU_ALLOC)})


@api.route("/api/team_usage", methods=["POST"])
def api_team_usage():
    """Fetch fresh team GPU usage for specified clusters (triggers SSH)."""
    payload = request.get_json(silent=True) or {}
    cluster_list = payload.get("clusters", [])
    if not cluster_list:
        cluster_list = [c for c in CLUSTERS if c != "local"]

    results = {}
    threads = []
    import threading as _th
    def _fetch(c):
        try:
            r = fetch_team_usage(c)
            if r:
                results[c] = r
        except Exception:
            pass
    for c in cluster_list:
        if c not in CLUSTERS or c == "local":
            continue
        t = _th.Thread(target=_fetch, args=(c,), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=25)

    from .config import TEAM_GPU_ALLOC
    return jsonify({"status": "ok", "team_usage": results, "team_gpu_allocations": dict(TEAM_GPU_ALLOC)})


@api.route("/api/team_jobs")
def api_team_jobs():
    """Fetch per-job breakdown for team members across all PPP accounts.

    Returns cached data immediately for clusters that have it, and kicks off
    background refreshes for stale clusters. Never blocks on SSH.
    """
    from .jobs import _team_jobs_cache, TEAM_JOBS_TTL_SEC
    cluster_filter = request.args.get("cluster", "")
    if cluster_filter:
        cluster_list = [c.strip() for c in cluster_filter.split(",") if c.strip()]
    else:
        cluster_list = [c for c in CLUSTERS if c != "local"]

    results = {}
    stale = []
    for c in cluster_list:
        if c not in CLUSTERS or c == "local":
            continue
        cached = _cache_get(_team_jobs_cache, c, TEAM_JOBS_TTL_SEC)
        if cached is not None:
            results[c] = cached
        else:
            stale.append(c)

    if stale:
        def _bg():
            for c in stale:
                try:
                    r = fetch_team_jobs(c)
                    if r:
                        results[c] = r
                except Exception:
                    pass
        threading.Thread(target=_bg, daemon=True).start()

    return jsonify({"status": "ok", "clusters": results})


@api.route("/api/cancel/<cluster>/<job_id>", methods=["POST"])
def api_cancel(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    try:
        if cluster == "local":
            os.kill(int(job_id), 15)
            return jsonify({"status": "ok"})
        ssh_run(cluster, f"scancel {job_id}")
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/cancel_jobs/<cluster>", methods=["POST"])
def api_cancel_jobs(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    payload = request.get_json(silent=True) or {}
    job_ids = payload.get("job_ids", [])
    if not job_ids or not isinstance(job_ids, list):
        return jsonify({"status": "error", "error": "job_ids list required"}), 400
    sanitized = [str(jid).strip() for jid in job_ids if str(jid).strip().isdigit()]
    if not sanitized:
        return jsonify({"status": "error", "error": "No valid job IDs"}), 400
    try:
        if cluster == "local":
            errors = []
            for jid in sanitized:
                try:
                    os.kill(int(jid), 15)
                except Exception as e:
                    errors.append(f"{jid}: {e}")
            if errors:
                return jsonify({"status": "partial", "cancelled": len(sanitized) - len(errors), "errors": errors})
            return jsonify({"status": "ok", "cancelled": len(sanitized)})
        ssh_run(cluster, f"scancel {','.join(sanitized)}")
        return jsonify({"status": "ok", "cancelled": len(sanitized)})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})



@api.route("/api/run_script/<cluster>", methods=["POST"])
def api_run_script(cluster):
    """Run an arbitrary script on a cluster via SSH and return the output.

    Body JSON:
      script      — the script source code (required)
      interpreter — "python3" | "bash" | "sh" (default: "python3")
      timeout     — seconds, 1-300 (default: 120)
    """
    import base64
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if cluster == "local":
        return jsonify({"status": "error", "error": "run_script is not supported for the local cluster"}), 400

    payload = request.get_json(silent=True) or {}
    script = payload.get("script", "").strip()
    if not script:
        return jsonify({"status": "error", "error": "No script provided"}), 400

    interpreter = payload.get("interpreter", "python3").strip()
    allowed_interpreters = {"python3", "python", "bash", "sh"}
    if interpreter not in allowed_interpreters:
        return jsonify({"status": "error", "error": f"interpreter must be one of: {', '.join(sorted(allowed_interpreters))}"}), 400

    timeout = int(payload.get("timeout", 120))
    timeout = max(1, min(timeout, 300))

    # Base64-encode the script to safely pass any content through SSH
    encoded = base64.b64encode(script.encode()).decode()
    cmd = f"echo '{encoded}' | base64 -d | {interpreter}"
    try:
        stdout, stderr = ssh_run_with_timeout(cluster, cmd, timeout_sec=timeout)
        return jsonify({
            "status": "ok",
            "stdout": stdout,
            "stderr": stderr,
            "interpreter": interpreter,
            "cluster": cluster,
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/stats/<cluster>/<job_id>")
def api_stats(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    from .jobs import get_stats_snapshots
    result = get_job_stats_cached(cluster, job_id)
    snapshots = get_stats_snapshots(cluster, job_id)
    if isinstance(result, dict):
        result["snapshots"] = snapshots
    return jsonify(result)


def _expand_slurm_nodelist(nodelist_str):
    """Expand compact Slurm node notation into a set of individual hostnames.

    Handles formats like:
      gpu-b200-001                    → {gpu-b200-001}
      gpu-b200-[001-004]              → {gpu-b200-001, ..., gpu-b200-004}
      gpu-b200-[001-003,005,007-009]  → 6 nodes
      gpu-b200-[001-004],gpu-a100-[001-002]  → 6 nodes
    """
    if not nodelist_str or nodelist_str in ("(null)", "None", "N/A", "", "—"):
        return set()

    nodes = set()
    # Split on commas that are NOT inside brackets
    # e.g. "gpu-b200-[001-003],gpu-a100-001" → ["gpu-b200-[001-003]", "gpu-a100-001"]
    parts = re.split(r',(?![^\[]*\])', nodelist_str)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        m = re.match(r'^(.+)\[([^\]]+)\](.*)$', part)
        if not m:
            nodes.add(part)
            continue
        prefix, ranges, suffix = m.group(1), m.group(2), m.group(3)
        for rng in ranges.split(","):
            if "-" in rng:
                lo, hi = rng.split("-", 1)
                width = len(lo)
                for i in range(int(lo), int(hi) + 1):
                    nodes.add(f"{prefix}{str(i).zfill(width)}{suffix}")
            else:
                nodes.add(f"{prefix}{rng}{suffix}")
    return nodes


def _parse_gres_gpu_count(gres_str):
    """Extract per-node GPU count from a GRES string.

    Handles: 'gpu:8', 'gpu:a100:4', 'gpu:b200:4(S:0-1)', 'gpu:4(S:0)'
    """
    count, _ = _parse_gres_gpu_count_with_presence(gres_str)
    return count


def _parse_gres_gpu_count_with_presence(gres_str):
    """Return (gpu_count, has_gpu_spec) for a GRES-like string."""
    if not gres_str or gres_str in ("N/A", "(null)"):
        return 0, False
    m = re.search(r'gpu[^:]*:(?:[a-zA-Z]\w*:)?(\d+)', gres_str)
    if not m:
        return 0, False
    try:
        return int(m.group(1)), True
    except ValueError:
        return 0, True


def _parse_run_metadata_gpus_per_node(scontrol_raw="", batch_script=""):
    """Extract GPUs-per-node hints from run metadata fields."""
    gpn = 0

    if scontrol_raw:
        for m in re.finditer(r'(?:^|\s)(?:Gres|TresPerNode)=([^\s]+)', scontrol_raw):
            g, _ = _parse_gres_gpu_count_with_presence(m.group(1))
            gpn = max(gpn, g)

        for m in re.finditer(r'\bReqTRES=([^\n]+)', scontrol_raw):
            req_tres = m.group(1)
            for gm in re.finditer(r'gres/gpu(?:[:/][a-zA-Z]\w*)?=(\d+)', req_tres):
                try:
                    gpn = max(gpn, int(gm.group(1)))
                except ValueError:
                    continue

    if batch_script:
        for line in batch_script.splitlines():
            l = line.strip()
            if not l.startswith("#SBATCH"):
                continue
            m = re.search(r'--gpus-per-node(?:=|\s+)(\d+)', l)
            if m:
                gpn = max(gpn, int(m.group(1)))
            m = re.search(r'--gres(?:=|\s+)gpu(?::[a-zA-Z]\w*)?:(\d+)', l)
            if m:
                gpn = max(gpn, int(m.group(1)))

    return gpn


def _infer_run_gpus_per_node(cluster, jobs, scontrol_raw="", batch_script=""):
    """Infer GPUs per node when job GRES is absent.

    Fallback order:
      1) Run metadata hints (`scontrol_raw` / `batch_script`)
      2) Partition summary for the run's partitions
      3) Cluster config (`CLUSTERS[cluster].gpus_per_node`)
      4) Conservative default (8) for non-local, non-CPU runs
    """
    if cluster == "local":
        return 0

    partitions = {
        (j.get("partition") or "").strip()
        for j in jobs
        if (j.get("partition") or "").strip()
    }
    non_cpu_parts = [
        p for p in partitions
        if not p.lower().startswith("cpu") and p.lower() not in ("defq", "fake")
    ]

    # If partitions are known and all CPU, do not force GPU fallback.
    if partitions and not non_cpu_parts:
        return 0

    meta_gpn = _parse_run_metadata_gpus_per_node(scontrol_raw, batch_script)
    if meta_gpn > 0:
        return meta_gpn

    part_gpn = 0
    try:
        from .partitions import _cache as _part_cache, _lock as _part_lock
        with _part_lock:
            rec = _part_cache.get(cluster)
        parts = rec["data"] if rec else []
        part_map = {
            (p.get("name") or ""): int(p.get("gpus_per_node") or 0)
            for p in parts
        }
        if non_cpu_parts:
            part_gpn = max((part_map.get(p, 0) for p in non_cpu_parts), default=0)
        if part_gpn <= 0:
            part_gpn = max(part_map.values(), default=0)
    except Exception:
        part_gpn = 0

    if part_gpn > 0:
        return part_gpn

    cfg_gpn = int(CLUSTERS.get(cluster, {}).get("gpus_per_node", 0) or 0)
    if cfg_gpn > 0:
        return cfg_gpn

    return 8


def _compute_run_resources(jobs, cluster="", run_scontrol_raw="", run_batch_script=""):
    """Compute unique node count and total GPU count for a run.

    Total GPUs = unique_nodes × gpus_per_node (not summed across array jobs).
    """
    all_nodes = set()
    gpus_per_node = 0
    per_job_nodes = 0
    saw_gpu_spec = False
    saw_explicit_zero_gpu = False

    for j in jobs:
        nl = j.get("node_list", "")
        expanded = _expand_slurm_nodelist(nl)
        all_nodes |= expanded

        g, has_gpu_spec = _parse_gres_gpu_count_with_presence(j.get("gres", ""))
        if has_gpu_spec:
            saw_gpu_spec = True
            if g == 0:
                saw_explicit_zero_gpu = True
        if g > gpus_per_node:
            gpus_per_node = g

        n = int(j.get("nodes") or 0)
        if n > per_job_nodes:
            per_job_nodes = n

    if gpus_per_node <= 0:
        # Do not apply GPU fallback when jobs explicitly report gpu:0.
        if saw_gpu_spec and saw_explicit_zero_gpu:
            gpus_per_node = 0
        else:
            gpus_per_node = _infer_run_gpus_per_node(
                cluster,
                jobs,
                scontrol_raw=run_scontrol_raw,
                batch_script=run_batch_script,
            )

    unique_node_count = len(all_nodes) if all_nodes else per_job_nodes
    total_gpus = unique_node_count * gpus_per_node
    return unique_node_count, total_gpus, gpus_per_node


@api.route("/api/run_info/<cluster>/<root_job_id>")
def api_run_info(cluster, root_job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run = get_run_with_jobs(cluster, root_job_id)
    if not run:
        actual_root = create_run_on_demand(cluster, root_job_id)
        if actual_root:
            run = get_run_with_jobs(cluster, actual_root)
        if not run:
            return jsonify({"status": "error", "error": "Run not found"}), 404
    if not run.get("meta_fetched"):
        threading.Thread(
            target=fetch_run_metadata_sync,
            args=(cluster, run["root_job_id"]),
            daemon=True,
        ).start()
    for j in run.get("jobs", []):
        if not j.get("project"):
            j["project"] = extract_project(j.get("job_name") or j.get("name") or "")
        proj = j.get("project", "")
        if proj:
            j["project_color"] = get_project_color(proj)
            j["project_emoji"] = get_project_emoji(proj)
    unique_nodes, total_gpus, gpus_per_node = _compute_run_resources(
        run.get("jobs", []),
        cluster=cluster,
        run_scontrol_raw=run.get("scontrol_raw", ""),
        run_batch_script=run.get("batch_script", ""),
    )
    run["unique_nodes"] = unique_nodes
    run["total_gpus"] = total_gpus
    run["gpus_per_node"] = gpus_per_node
    return jsonify({"status": "ok", "run": run})


@api.route("/api/run_info/<cluster>/<root_job_id>/retry_meta", methods=["POST"])
def api_retry_run_meta(cluster, root_job_id):
    """Force retry metadata capture for a run."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    from .jobs import _run_meta_fetched, _capture_run_metadata
    from .db import get_run
    key = (cluster, str(root_job_id))
    _run_meta_fetched.pop(key, None)
    run = get_run(cluster, str(root_job_id))
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404
    db = get_db()
    db.execute("UPDATE runs SET meta_fetched=0 WHERE cluster=? AND root_job_id=?", (cluster, str(root_job_id)))
    db.commit()
    _capture_run_metadata(cluster, str(root_job_id), run["id"])
    return api_run_info(cluster, root_job_id)


@api.route("/api/history")
def api_history():
    cluster = request.args.get("cluster", "all")
    limit = int(request.args.get("limit", 200))
    project = request.args.get("project", "")
    rows = get_history(cluster, limit, project=project)
    for r in rows:
        if not r.get("project"):
            r["project"] = extract_project(r.get("job_name") or r.get("name") or "")
        proj = r.get("project", "")
        if proj:
            r["project_color"] = get_project_color(proj)
            r["project_emoji"] = get_project_emoji(proj)
    return jsonify(rows)


@api.route("/api/projects")
def api_projects():
    from .config import get_project_color as _color, get_project_emoji as _emoji
    projects = get_projects()
    for p in projects:
        p["color"] = _color(p["project"])
        p["emoji"] = _emoji(p["project"])
    return jsonify(projects)


@api.route("/api/logbook_projects")
def api_logbook_projects():
    from .logbooks import list_logbook_projects
    return jsonify(list_logbook_projects())


@api.route("/api/log_files/<cluster>/<job_id>")
def api_log_files(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "files": [], "dirs": [], "error": "Unknown cluster"}), 404
    force = request.args.get("force", "0") == "1"
    include_first = request.args.get("include_first", "0") == "1"
    result = get_job_log_files_cached(cluster, job_id, force=force)
    files = []
    for f in result.get("files", []):
        p = f.get("path", "")
        mounted = resolve_mounted_path(cluster, p, want_dir=False) if (p and cluster != "local") else ""
        source_hint = "local" if cluster == "local" else ("mount" if mounted else "ssh")
        files.append({**f, "source_hint": source_hint, "mounted_path": mounted})
    dirs = []
    for d in result.get("dirs", []):
        p = d.get("path", "")
        mounted = resolve_mounted_path(cluster, p, want_dir=True) if (p and cluster != "local") else ""
        source_hint = "local" if cluster == "local" else ("mount" if mounted else "ssh")
        dirs.append({**d, "source_hint": source_hint, "mounted_path": mounted})
    resp = {"status": "ok", "files": files, "dirs": dirs, "error": result.get("error", "")}

    if include_first and files and not files[0].get("path", "").endswith((".jsonl", ".jsonl-async")):
        first_path = files[0]["path"]
        cache_key = (cluster, str(job_id), first_path)
        cached = _cache_get(_log_content_cache, cache_key, LOG_CONTENT_TTL_SEC)
        if cached is not None:
            resp["first_content"] = cached
            resp["first_source"] = "cache"
            resp["first_resolved_path"] = first_path
        else:
            source = "ssh"
            resolved = first_path
            if cluster != "local":
                mounted = resolve_mounted_path(cluster, first_path, want_dir=False)
                if mounted:
                    content = tail_local_file(mounted, 300)
                    source = "mount"
                    resolved = mounted
                else:
                    content = fetch_log_tail(cluster, first_path, 300)
            else:
                content = fetch_log_tail(cluster, first_path, 300)
                source = "local"
            _cache_set(_log_content_cache, cache_key, content)
            resp["first_content"] = content
            resp["first_source"] = source
            resp["first_resolved_path"] = resolved
        import hashlib
        fc = resp.get("first_content", "")
        if fc:
            resp["first_hash"] = hashlib.md5(fc.encode()).hexdigest()[:12]

    return jsonify(resp)


@api.route("/api/ls/<cluster>")
def api_ls(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    path = request.args.get("path", "")
    force = request.args.get("force", "0") == "1"
    if not path:
        return jsonify({"status": "error", "error": "No path provided"}), 400
    cache_key = (cluster, path)
    if not force:
        cached = _cache_get(_dir_list_cache, cache_key, DIR_LIST_TTL_SEC)
        if cached is not None:
            return jsonify(cached)
    try:
        if cluster == "local":
            entries = list_local_dir(path)
            payload = {"status": "ok", "path": path, "entries": entries, "source": "local", "resolved_path": path}
            _cache_set(_dir_list_cache, cache_key, payload)
            prefetch_nested_dir_cache_local(cluster, path, path, entries)
            return jsonify(payload)
        mounted_dir = resolve_mounted_path(cluster, path, want_dir=True)
        if mounted_dir:
            entries = list_local_dir(mounted_dir)
            for e in entries:
                e["path"] = path.rstrip("/") + "/" + e["name"]
            payload = {"status": "ok", "path": path, "entries": entries, "source": "mount", "resolved_path": mounted_dir}
            _cache_set(_dir_list_cache, cache_key, payload)
            prefetch_nested_dir_cache_local(cluster, path, mounted_dir, entries)
            return jsonify(payload)
        cmd = f"""ls -la '{path}' 2>/dev/null | tail -n +2 | awk '{{
  type = ($1 ~ /^d/) ? "d" : "f"
  size = $5
  name = $NF
  if (name != "." && name != "..") print type "|" size "|" name
}}'"""
        out, _ = ssh_run_data(cluster, cmd)
        entries = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) != 3:
                continue
            ftype, size, name = parts
            entries.append({"name": name, "path": path.rstrip("/") + "/" + name, "is_dir": ftype == "d",
                            "size": int(size) if size.isdigit() else None})
        payload = {"status": "ok", "path": path, "entries": entries, "source": "ssh", "resolved_path": path}
        _cache_set(_dir_list_cache, cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/log/<cluster>/<job_id>")
def api_log(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    lines = int(request.args.get("lines", 150))
    log_path = request.args.get("path", "")
    force = request.args.get("force", "0") == "1"

    if not log_path:
        result = get_job_log_files_cached(cluster, job_id)
        files = result["files"]
        if not files:
            return jsonify({"status": "error", "error": "No log files found for this job."})
        preferred = next((f for f in files if "main" in f["label"]), None)
        log_path = (preferred or files[0])["path"]

    if not log_path:
        return jsonify({"status": "error", "error": "No log path available."})

    cache_key = (cluster, str(job_id), log_path)
    cached = None if force else _cache_get(_log_content_cache, cache_key, LOG_CONTENT_TTL_SEC)
    source = "cache"
    resolved_path = log_path
    if cached is not None:
        content = cached
    else:
        if cluster != "local":
            mounted = resolve_mounted_path(cluster, log_path, want_dir=False)
            if mounted:
                content = tail_local_file(mounted, lines)
                source = "mount"
                resolved_path = mounted
            else:
                content = fetch_log_tail(cluster, log_path, lines)
                source = "ssh"
        else:
            content = fetch_log_tail(cluster, log_path, lines)
            source = "local"
        _cache_set(_log_content_cache, cache_key, content)
        pct = extract_progress(content)
        if pct is not None:
            _cache_set(_progress_cache, (cluster, str(job_id)), pct)
            from .logs import label_log
            _cache_set(_progress_source_cache, (cluster, str(job_id)),
                       label_log(os.path.basename(log_path)))
        from .logs import detect_crash
        crash = detect_crash(content)
        if crash is not None:
            _cache_set(_crash_cache, (cluster, str(job_id)), crash)
    resp = {"status": "ok", "log_path": log_path, "content": content, "source": source, "resolved_path": resolved_path}
    if force and content:
        import hashlib
        h = hashlib.md5(content.encode()).hexdigest()[:12]
        resp["hash"] = h
        if_hash = request.args.get("if_hash", "")
        if if_hash == h:
            return jsonify({"status": "ok", "unchanged": True, "hash": h})
    return jsonify(resp)


@api.route("/api/log_full/<cluster>/<job_id>")
def api_log_full(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    log_path = request.args.get("path", "")
    page = int(request.args.get("page", 0))
    page_size = int(request.args.get("page_size", 500))
    if not log_path:
        return jsonify({"status": "error", "error": "No path provided"}), 400

    local_path = None
    source = "ssh"
    if cluster == "local":
        local_path = log_path if os.path.isfile(log_path) else None
        source = "local"
    else:
        mounted = resolve_mounted_path(cluster, log_path, want_dir=False)
        if mounted:
            local_path = mounted
            source = "mount"

    if local_path:
        try:
            result = subprocess.run(["wc", "-l", local_path], capture_output=True, text=True, timeout=10)
            total_lines = int(result.stdout.strip().split()[0]) if result.stdout.strip() else 0
            total_pages = max(1, -(-total_lines // page_size))
            page = max(0, min(page, total_pages - 1))
            start = page * page_size + 1
            end = start + page_size - 1
            result = subprocess.run(["sed", "-n", f"{start},{end}p", local_path], capture_output=True, text=True, timeout=15)
            content = result.stdout or "(empty)"
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)})
    else:
        try:
            wc_out, _ = ssh_run_data_with_timeout(cluster, f"wc -l '{log_path}' 2>/dev/null", timeout_sec=10)
            total_lines = int(wc_out.strip().split()[0]) if wc_out.strip() else 0
            total_pages = max(1, -(-total_lines // page_size))
            page = max(0, min(page, total_pages - 1))
            start = page * page_size + 1
            end = start + page_size - 1
            content, _ = ssh_run_data_with_timeout(cluster, f"sed -n '{start},{end}p' '{log_path}' 2>/dev/null", timeout_sec=15)
            content = content or "(empty)"
        except Exception as e:
            return jsonify({"status": "error", "error": str(e)})

    return jsonify({"status": "ok", "content": content, "page": page, "page_size": page_size,
                     "total_pages": total_pages, "total_lines": total_lines, "source": source, "log_path": log_path})


@api.route("/api/jsonl_index/<cluster>/<job_id>")
def api_jsonl_index(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    path = request.args.get("path", "")
    mode = request.args.get("mode", "last")
    limit = int(request.args.get("limit", 100))
    if mode not in ("first", "last", "all"):
        mode = "last"
    if not path:
        return jsonify({"status": "error", "error": "No path provided"}), 400

    local_path, source = resolve_file_path(cluster, path)
    if local_path:
        result = read_jsonl_index(local_path, limit=limit, mode=mode)
        result["source"] = source
        return jsonify(result)

    preview_chars = 150
    if mode == "first" and limit > 0:
        cmd = f"head -n {limit} '{path}' 2>/dev/null | awk '{{printf \"%d|%d|%s\\n\", NR-1, length($0), substr($0, 1, {preview_chars})}}'"
    else:
        cmd = f"awk '{{printf \"%d|%d|%s\\n\", NR-1, length($0), substr($0, 1, {preview_chars})}}' '{path}' 2>/dev/null"
    try:
        out, _ = ssh_run_data_with_timeout(cluster, cmd, timeout_sec=15)
        all_records = []
        for line in out.splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            ln, sz = int(parts[0]), int(parts[1])
            prev = parts[2]
            all_records.append({"line": ln, "preview": prev, "valid": len(prev) < sz, "size": sz})
        total = len(all_records)
        if mode == "all" or limit <= 0:
            records = all_records
        elif mode == "first":
            records = all_records
        else:
            records = all_records[-limit:]
        return jsonify({"status": "ok", "total": total, "count": len(records),
                        "mode": mode, "limit": limit, "records": records, "source": "ssh"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/jsonl_record/<cluster>/<job_id>")
def api_jsonl_record(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    path = request.args.get("path", "")
    line_num = int(request.args.get("line", 0))
    if not path:
        return jsonify({"status": "error", "error": "No path provided"}), 400

    local_path, source = resolve_file_path(cluster, path)
    if local_path:
        result = read_jsonl_record(local_path, line_num)
        result["source"] = source
        return jsonify(result)

    sed_line = line_num + 1
    cmd = f"sed -n '{sed_line}p' '{path}' 2>/dev/null"
    try:
        out, _ = ssh_run_data_with_timeout(cluster, cmd, timeout_sec=10)
        return jsonify({"status": "ok", "line": line_num, "content": out.strip(), "source": "ssh"})
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/settings")
def api_settings_get():
    return jsonify(settings_response())


@api.route("/api/settings", methods=["POST"])
def api_settings_post():
    patch = request.get_json(silent=True)
    if not patch or not isinstance(patch, dict):
        return jsonify({"status": "error", "error": "Invalid JSON body"}), 400

    from .config import _sync_config
    _sync_config()
    merged = dict(_CONFIG)
    for key in ("port", "ssh_timeout", "cache_fresh_sec", "stats_interval_sec",
                "backup_interval_hours", "backup_max_keep",
                "log_search_bases", "nemo_run_bases", "mount_lustre_prefixes",
                "local_process_filters"):
        if key in patch:
            merged[key] = patch[key]
    if "clusters" in patch:
        existing = merged.get("clusters", {})
        for cname, cpatch in patch["clusters"].items():
            if cname in existing:
                existing[cname].update(cpatch)
            else:
                existing[cname] = cpatch
        for gone in set(existing) - set(patch["clusters"]):
            del existing[gone]
        merged["clusters"] = existing
    if "projects" in patch:
        merged["projects"] = patch["projects"]
    if "team" in patch:
        merged["team"] = patch["team"]
    if "team_gpu_allocations" in patch:
        merged["team_gpu_allocations"] = patch["team_gpu_allocations"]
    if "ppps" in patch:
        merged["ppps"] = patch["ppps"]

    try:
        reload_config(merged)
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500

    return jsonify({"status": "ok", "settings": settings_response()})


# ─── Logbook routes ──────────────────────────────────────────────────────────

from .cluster_dashboard import get_cluster_utilization
from .config import DASHBOARD_URL as _DASHBOARD_URL
from .storage_quota import fetch_storage_quota


@api.route("/api/user_avatar")
def api_user_avatar():
    """Proxy the user's avatar image from the Science dashboard."""
    import urllib.request as _ur
    user = request.args.get("user", DEFAULT_USER)
    url = f"{_DASHBOARD_URL}/images/{user}.png"
    try:
        with _ur.urlopen(url, timeout=5) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "image/png")
            r = make_response(data)
            r.headers["Content-Type"] = ct
            r.headers["Cache-Control"] = "public, max-age=86400"
            return r
    except Exception:
        url_jpg = f"{_DASHBOARD_URL}/images/{user}.jpeg"
        try:
            with _ur.urlopen(url_jpg, timeout=5) as resp:
                data = resp.read()
                ct = resp.headers.get("Content-Type", "image/jpeg")
                r = make_response(data)
                r.headers["Content-Type"] = ct
                r.headers["Cache-Control"] = "public, max-age=86400"
                return r
        except Exception:
            return "", 404


@api.route("/api/storage_quota/<cluster>")
def api_storage_quota(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    data = fetch_storage_quota(cluster)
    return jsonify(data)


@api.route("/api/cluster_utilization")
def api_cluster_utilization():
    force = request.args.get("force", "0") == "1"
    data = get_cluster_utilization(force=force)
    if not data:
        return jsonify({"status": "error", "error": "External dashboard unreachable"}), 502
    return jsonify({"status": "ok", **data})


# ─── Partition & recommendation routes ───────────────────────────────────────

from .partitions import get_partitions as _get_partitions, get_all_partitions, get_all_partitions_cached, get_partition_summary


@api.route("/api/partitions")
def api_partitions_all():
    force = request.args.get("force", "0") == "1"
    if force:
        data = get_all_partitions(force=True)
    else:
        data = get_all_partitions_cached()
    return jsonify({"status": "ok", "clusters": data})


@api.route("/api/partitions/<cluster>")
def api_partitions_cluster(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if cluster == "local":
        return jsonify({"status": "error", "error": "No partitions for local"}), 400
    force = request.args.get("force", "0") == "1"
    data = _get_partitions(cluster, force=force)
    if data is None:
        return jsonify({"status": "error", "error": f"Could not fetch partitions from {cluster}"}), 502
    return jsonify({"status": "ok", "cluster": cluster, "partitions": data})


@api.route("/api/partition_summary")
def api_partition_summary():
    data = get_partition_summary()
    return jsonify({"status": "ok", "clusters": data})


@api.route("/api/recommend", methods=["POST"])
def api_recommend():
    from .recommendations import recommend
    payload = request.get_json(silent=True) or {}
    nodes = payload.get("nodes", 1)
    time_limit = payload.get("time_limit", "4:00:00")
    account = payload.get("account", "")
    can_preempt = payload.get("can_preempt", False)
    gpu_type = payload.get("gpu_type", "")
    clusters = payload.get("clusters", None)
    accounts = payload.get("accounts", None)

    try:
        results = recommend(
            nodes=nodes, time_limit=time_limit, account=account,
            can_preempt=can_preempt, gpu_type=gpu_type, clusters=clusters,
            accounts=accounts,
        )
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500

    return jsonify({"status": "ok", "recommendations": results})


# ─── AI Hub routes ────────────────────────────────────────────────────────────

from .aihub import (
    get_ppp_allocations as _aihub_alloc,
    get_usage_history as _aihub_history,
    get_user_breakdown as _aihub_users,
    get_cluster_occupancy as _aihub_occupancy,
    get_team_overlay as _aihub_team_overlay,
    get_my_fairshare as _aihub_my_fairshare,
)


@api.route("/api/aihub/allocations")
def api_aihub_allocations():
    accounts = request.args.get("accounts", "")
    acct_list = [a.strip() for a in accounts.split(",") if a.strip()] or None
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_alloc(accounts=acct_list, force=force)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/history")
def api_aihub_history():
    days = int(request.args.get("days", 14))
    cluster = request.args.get("cluster", "")
    interval = request.args.get("interval", "1d")
    clusters = [c.strip() for c in cluster.split(",") if c.strip()] or None
    try:
        data = _aihub_history(clusters=clusters, days=days, interval=interval)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/users")
def api_aihub_users():
    account = request.args.get("account", "")
    cluster = request.args.get("cluster", "")
    days = int(request.args.get("days", 7))
    if not account or not cluster:
        return jsonify({"status": "error", "error": "account and cluster required"}), 400
    try:
        data = _aihub_users(account, cluster, days=days)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/occupancy")
def api_aihub_occupancy():
    days = int(request.args.get("days", 7))
    cluster = request.args.get("cluster", "")
    clusters = [c.strip() for c in cluster.split(",") if c.strip()] or None
    try:
        data = _aihub_occupancy(clusters=clusters, days=days)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/team_overlay")
def api_aihub_team_overlay():
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_team_overlay(force=force)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/my_fairshare")
def api_aihub_my_fairshare():
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_my_fairshare(force=force)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/wds_history")
def api_wds_history():
    from .wds import get_wds_history
    cluster = request.args.get("cluster", "")
    account = request.args.get("account", "")
    days = int(request.args.get("days", 30))
    try:
        rows = get_wds_history(
            cluster=cluster or None,
            account=account or None,
            days=days,
        )
        return jsonify({"status": "ok", "rows": rows, "count": len(rows)})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


# ── Logbook routes ────────────────────────────────────────────────────────────

from .logbooks import (
    list_entries as _lb_list,
    get_entry as _lb_get,
    create_entry as _lb_create,
    update_entry as _lb_update,
    delete_entry as _lb_delete,
    search_entries as _lb_search,
    save_image as _lb_save_image,
    get_image_path as _lb_get_image_path,
    resolve_entry_refs as _lb_resolve_refs,
)


@api.route("/api/logbook/<project>/entries")
def api_logbook_list(project):
    q = request.args.get("q", "")
    sort = request.args.get("sort", "edited_at")
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    entry_type = request.args.get("type", "")
    return jsonify(_lb_list(project, query=q or None, sort=sort, limit=limit, offset=offset, entry_type=entry_type or None))


@api.route("/api/logbook/<project>/entries", methods=["POST"])
def api_logbook_create(project):
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    if not title:
        return jsonify({"status": "error", "error": "Title is required"}), 400
    body = (payload.get("body") or "").strip()
    entry_type = (payload.get("entry_type") or "note").strip()
    return jsonify(_lb_create(project, title, body, entry_type=entry_type))


@api.route("/api/logbook/<project>/entries/<int:entry_id>")
def api_logbook_read(project, entry_id):
    result = _lb_get(project, entry_id)
    if result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result)


@api.route("/api/logbook/resolve_refs")
def api_logbook_resolve_refs():
    """Resolve entry IDs to {id, project, title} across all projects."""
    raw = request.args.get("ids", "")
    try:
        ids = [int(x) for x in raw.split(",") if x.strip()]
    except ValueError:
        return jsonify([])
    if not ids:
        return jsonify([])
    return jsonify(_lb_resolve_refs(ids))


@api.route("/api/logbook/<project>/entries/<int:entry_id>", methods=["PUT"])
def api_logbook_update(project, entry_id):
    payload = request.get_json(silent=True) or {}
    title = payload.get("title")
    body = payload.get("body")
    if title is not None:
        title = title.strip()
    if body is not None:
        body = body.strip()
    entry_type = payload.get("entry_type")
    result = _lb_update(project, entry_id, title=title, body=body, entry_type=entry_type)
    if result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result)


@api.route("/api/logbook/<project>/entries/<int:entry_id>/pin", methods=["POST"])
def api_logbook_pin(project, entry_id):
    pinned = (request.get_json(silent=True) or {}).get("pinned", True)
    con = get_db()
    con.execute("UPDATE logbook_entries SET pinned=? WHERE id=? AND project=?",
                (1 if pinned else 0, entry_id, project))
    con.commit()
    con.close()
    return jsonify({"status": "ok"})


@api.route("/api/logbook/<project>/entries/<int:entry_id>", methods=["DELETE"])
def api_logbook_delete(project, entry_id):
    result = _lb_delete(project, entry_id)
    if result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result)


@api.route("/api/logbook/search")
def api_logbook_search():
    q = request.args.get("q", "")
    if not q.strip():
        return jsonify([])
    project = request.args.get("project", "")
    date_from = request.args.get("from", "")
    date_to = request.args.get("to", "")
    limit = int(request.args.get("limit", 50))
    return jsonify(_lb_search(q, project=project or None, date_from=date_from or None, date_to=date_to or None, limit=limit))


@api.route("/api/logbook/<project>/images", methods=["POST"])
def api_logbook_upload_image(project):
    if "file" not in request.files:
        return jsonify({"status": "error", "error": "No file uploaded"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"status": "error", "error": "No filename"}), 400
    result = _lb_save_image(project, f.filename, f.read())
    if result.get("status") == "error":
        return jsonify(result), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/images/<filename>")
def api_logbook_serve_image(project, filename):
    from flask import send_file
    path = _lb_get_image_path(project, filename)
    if not path:
        return jsonify({"status": "error", "error": "Image not found"}), 404
    return send_file(path)


_export_store = {}

@api.route("/api/logbook/export", methods=["POST"])
def api_logbook_export_create():
    import uuid, time
    payload = request.get_json(silent=True) or {}
    content = payload.get("content", "")
    filename = payload.get("filename", "export.html")
    mime = payload.get("mime", "text/html")
    if not content:
        return jsonify({"status": "error", "error": "No content"}), 400
    token = uuid.uuid4().hex[:16]
    _export_store[token] = {"content": content, "filename": filename, "mime": mime, "ts": time.time()}
    for k in list(_export_store):
        if time.time() - _export_store[k]["ts"] > 120:
            del _export_store[k]
    return jsonify({"status": "ok", "token": token})


@api.route("/api/logbook/export/<token>")
def api_logbook_export_download(token):
    from flask import Response
    entry = _export_store.pop(token, None)
    if not entry:
        return jsonify({"status": "error", "error": "Export expired or not found"}), 404
    return Response(
        entry["content"],
        mimetype="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{entry["filename"]}"',
            "X-Content-Type-Options": "nosniff",
        },
    )


@api.route("/api/logbook/<project>/map")
def api_logbook_map(project):
    con = get_db()
    rows = con.execute(
        "SELECT id, title, entry_type, created_at, edited_at "
        "FROM logbook_entries WHERE project=? ORDER BY edited_at DESC",
        (project,),
    ).fetchall()
    links = con.execute(
        """SELECT l.source_id, l.target_id FROM logbook_links l
           JOIN logbook_entries e ON l.source_id = e.id
           WHERE e.project = ?""",
        (project,),
    ).fetchall()
    con.close()

    nodes = [{"id": r["id"], "title": r["title"], "entry_type": r["entry_type"],
              "created_at": r["created_at"], "edited_at": r["edited_at"]}
             for r in rows]
    explicit_links = [{"source_id": l["source_id"], "target_id": l["target_id"]}
                      for l in links]
    return jsonify({"nodes": nodes, "links": explicit_links})


@api.route("/api/spotlight")
def api_spotlight():
    from .config import get_project_color, get_project_emoji
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"projects": [], "logbook": [], "history": []})

    ql = q.lower()
    all_projects = get_projects()
    projects = [
        {"project": p["project"], "emoji": get_project_emoji(p["project"]),
         "color": get_project_color(p["project"]), "job_count": p["job_count"]}
        for p in all_projects if ql in p["project"].lower()
    ][:8]

    logbook = []
    try:
        logbook = _lb_search(q, limit=8)
    except Exception:
        pass

    history_rows = get_history(limit=8, search=q)
    history = [
        {"cluster": r["cluster"], "job_id": r.get("job_id") or r.get("jobid", ""),
         "job_name": r.get("job_name") or r.get("name", ""),
         "state": r.get("state", ""), "project": r.get("project", ""),
         "started": r.get("started", "")}
        for r in history_rows
    ]

    return jsonify({"projects": projects, "logbook": logbook, "history": history})
