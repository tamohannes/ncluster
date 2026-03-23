"""Flask route handlers as a Blueprint."""

import json
import os
import shutil
import subprocess
import threading
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, render_template, make_response

from .config import (
    CLUSTERS, DEFAULT_USER, TEAM_NAME, TERMINAL_STATES, RESULT_DIR_NAMES,
    _CONFIG, _cache_lock, _cache,
    _cache_get, _cache_set,
    _log_content_cache, _dir_list_cache, _progress_cache, _crash_cache, _est_start_cache,
    LOG_CONTENT_TTL_SEC, DIR_LIST_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC, EST_START_TTL_SEC,
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
    refresh_all_clusters, refresh_cluster,
    schedule_prefetch, prefetch_cluster_bulk, fetch_est_start_bulk,
    get_job_stats_cached, fetch_run_metadata_sync,
    create_run_on_demand,
    _last_polled,
)
from .db import get_run_with_jobs

api = Blueprint("api", __name__)


def _rebuild_cross_deps(jobs):
    """Rebuild depends_on/dependents across the full merged set of jobs.

    After merging live and pinned jobs, their dependency arrays only reference
    IDs within their original sets.  This rebuilds them so cross-references
    (e.g. a running child pointing to a completed parent) are restored.
    """
    from .jobs import parse_dependency
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

    children_map = {}
    for j in jobs:
        jid = j.get("jobid") or j.get("job_id", "")
        for pid in j.get("depends_on", []):
            children_map.setdefault(pid, []).append(jid)
    for j in jobs:
        jid = j.get("jobid") or j.get("job_id", "")
        j["dependents"] = children_map.get(jid, [])


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
        data["jobs"] = [normalize_job_times_local(j) for j in data.get("jobs", [])]
        for j in data.get("jobs", []):
            st = j.get("state", "").upper()
            jid = j.get("jobid")
            if st in ("RUNNING", "COMPLETING"):
                pct = _cache_get(_progress_cache, (name, jid), PROGRESS_TTL_SEC)
                if pct is not None:
                    j["progress"] = pct
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
    _last_polled[cluster] = 0.0
    refresh_cluster(cluster)
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
    est_starts = {}
    for item in jobs:
        c = item.get("cluster")
        jid = str(item.get("job_id", "")).strip()
        if not c or not jid:
            continue
        pct = _cache_get(_progress_cache, (c, jid), PROGRESS_TTL_SEC)
        if pct is not None:
            progress[f"{c}:{jid}"] = pct
        est = _cache_get(_est_start_cache, (c, jid), EST_START_TTL_SEC)
        if est:
            est_starts[f"{c}:{jid}"] = est
    return jsonify({"progress": progress, "est_starts": est_starts})


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


@api.route("/api/cancel_all/<cluster>", methods=["POST"])
def api_cancel_all(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    try:
        if cluster == "local":
            return jsonify({"status": "error", "error": "Not supported for local"})
        ssh_run(cluster, "scancel -u $USER")
        return jsonify({"status": "ok"})
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
    return jsonify(get_job_stats_cached(cluster, job_id))


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
        fetch_run_metadata_sync(cluster, run["root_job_id"])
        run = get_run_with_jobs(cluster, run["root_job_id"])
        if not run:
            return jsonify({"status": "error", "error": "Run not found after fetch"}), 404
    for j in run.get("jobs", []):
        if not j.get("project"):
            j["project"] = extract_project(j.get("job_name") or j.get("name") or "")
        proj = j.get("project", "")
        if proj:
            j["project_color"] = get_project_color(proj)
            j["project_emoji"] = get_project_emoji(proj)
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


@api.route("/api/log_files/<cluster>/<job_id>")
def api_log_files(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "files": [], "dirs": [], "error": "Unknown cluster"}), 404
    force = request.args.get("force", "0") == "1"
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
    return jsonify({"status": "ok", "files": files, "dirs": dirs, "error": result.get("error", "")})


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
        from .logs import detect_crash
        crash = detect_crash(content)
        if crash is not None:
            _cache_set(_crash_cache, (cluster, str(job_id)), crash)
    return jsonify({"status": "ok", "log_path": log_path, "content": content, "source": source, "resolved_path": resolved_path})


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

    merged = dict(_CONFIG)
    for key in ("port", "ssh_timeout", "cache_fresh_sec", "log_search_bases",
                "nemo_run_bases", "mount_lustre_prefixes", "local_process_filters"):
        if key in patch:
            merged[key] = patch[key]
    if "clusters" in patch:
        merged["clusters"] = patch["clusters"]
    if "projects" in patch:
        merged["projects"] = patch["projects"]
    if "team" in patch:
        merged["team"] = patch["team"]
    if "ppps" in patch:
        merged["ppps"] = patch["ppps"]

    try:
        reload_config(merged)
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500

    return jsonify({"status": "ok", "settings": settings_response()})


# ─── Logbook routes ──────────────────────────────────────────────────────────

from .cluster_dashboard import get_cluster_utilization, DASHBOARD_BASE_URL
from .storage_quota import fetch_storage_quota


@api.route("/api/user_avatar")
def api_user_avatar():
    """Proxy the user's avatar image from the Science dashboard."""
    import urllib.request as _ur
    user = request.args.get("user", DEFAULT_USER)
    url = f"{DASHBOARD_BASE_URL}/images/{user}.png"
    try:
        with _ur.urlopen(url, timeout=5) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "image/png")
            r = make_response(data)
            r.headers["Content-Type"] = ct
            r.headers["Cache-Control"] = "public, max-age=86400"
            return r
    except Exception:
        url_jpg = f"{DASHBOARD_BASE_URL}/images/{user}.jpeg"
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
    if data.get("status") == "error":
        return jsonify(data), 400
    return jsonify(data)


@api.route("/api/cluster_utilization")
def api_cluster_utilization():
    force = request.args.get("force", "0") == "1"
    data = get_cluster_utilization(force=force)
    if not data:
        return jsonify({"status": "error", "error": "External dashboard unreachable"}), 502
    return jsonify({"status": "ok", **data})


from .logbooks import (
    list_logbooks as _list_logbooks,
    read_logbook as _read_logbook,
    add_entry as _add_entry,
    update_entry as _update_entry,
    delete_entry as _delete_entry,
    rename_logbook as _rename_logbook,
    create_logbook as _create_logbook,
    delete_logbook as _delete_logbook,
)


@api.route("/api/logbooks/<project>")
def api_logbooks_list(project):
    return jsonify(_list_logbooks(project))


@api.route("/api/logbook/<project>/<name>")
def api_logbook_read(project, name):
    return jsonify(_read_logbook(project, name))


@api.route("/api/logbook/<project>/<name>", methods=["POST"])
def api_logbook_add_entry(project, name):
    payload = request.get_json(silent=True) or {}
    content = payload.get("content", "").strip()
    if not content:
        return jsonify({"status": "error", "error": "No content provided"}), 400
    return jsonify(_add_entry(project, name, content))


@api.route("/api/logbook/<project>/<name>/<int:index>", methods=["PUT"])
def api_logbook_update_entry(project, name, index):
    payload = request.get_json(silent=True) or {}
    content = payload.get("content", "").strip()
    if not content:
        return jsonify({"status": "error", "error": "No content provided"}), 400
    return jsonify(_update_entry(project, name, index, content))


@api.route("/api/logbook/<project>/<name>", methods=["DELETE"])
def api_logbook_delete(project, name):
    return jsonify(_delete_logbook(project, name))


@api.route("/api/logbook/<project>/<name>/<int:index>", methods=["DELETE"])
def api_logbook_delete_entry(project, name, index):
    return jsonify(_delete_entry(project, name, index))


@api.route("/api/logbook/<project>/<name>/rename", methods=["POST"])
def api_logbook_rename(project, name):
    payload = request.get_json(silent=True) or {}
    new_name = payload.get("new_name", "").strip()
    if not new_name:
        return jsonify({"status": "error", "error": "No new_name provided"}), 400
    return jsonify(_rename_logbook(project, name, new_name))


@api.route("/api/logbook/<project>", methods=["POST"])
def api_logbook_create(project):
    payload = request.get_json(silent=True) or {}
    name = payload.get("name", "").strip()
    if not name:
        return jsonify({"status": "error", "error": "No name provided"}), 400
    return jsonify(_create_logbook(project, name))
