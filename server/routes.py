"""Flask route handlers as a Blueprint."""

import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from flask import Blueprint, g, jsonify, request, render_template, make_response

_log = logging.getLogger(__name__)
_SLOW_REQUEST_MS = 2000
_shared_pool = ThreadPoolExecutor(max_workers=4)
_DEBUG_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".cursor", "debug-41bcda.log")
_DEBUG_SESSION_ID = "41bcda"

from .config import (
    CLUSTERS, DEFAULT_USER, TEAM_NAME, TERMINAL_STATES, RESULT_DIR_NAMES,
    _CONFIG, _cache_lock, _cache,
    _cache_get, _cache_set,
    _log_content_cache, _dir_list_cache, _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    _team_usage_cache,
    LOG_CONTENT_TTL_SEC, DIR_LIST_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC, EST_START_TTL_SEC,
    TEAM_USAGE_TTL_SEC,
    reload_config, settings_response,
    get_project_color, get_project_emoji, extract_project, extract_campaign,
)
from .db import (
    dismiss_job, dismiss_by_state_prefix,
    get_history, get_projects, get_db, db_write,
    cache_db_get, cache_db_get_stale, cache_db_get_all, cache_db_get_all_multi,
    cache_db_put,
)
from .ssh import (
    ssh_run, ssh_run_with_timeout, ssh_run_data, ssh_run_data_with_timeout,
    get_circuit_breaker_status, cancel_jobs_with_report,
)
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
    schedule_prefetch,
    get_job_stats_cached,
    create_run_on_demand,
    fetch_team_jobs,
    fetch_team_usage,
)
from .poller import get_poller, get_version, touch_demand
from .db import get_run_with_jobs, update_run_fields
from .board import build_board_snapshot, build_cluster_board_entry, _fill_output_dirs

api = Blueprint("api", __name__)


@api.app_errorhandler(Exception)
def _handle_unhandled(exc):
    _log.exception("Unhandled exception on %s %s", request.method, request.path)
    return jsonify({"status": "error", "error": str(exc)}), 500

_active_threads = set()
_active_requests_meta = {}
_active_lock = threading.Lock()
_MAX_ACTIVE = 20

# Entries older than this are presumed leaked or truly stuck. Matches the
# gunicorn worker timeout (120s in gunicorn.conf.py) — anything older than
# the worker timeout cannot be a healthy in-flight request.
# Without this safety net, a single leaked entry on a `gthread` worker is
# permanent for the worker's lifetime — the pool never reclaims threads,
# so `threading.enumerate()`-based cleanup cannot help us.
_ACTIVE_TTL_SEC = 120

_HEAVY_PREFIXES = (
    "/api/aihub/", "/api/team_jobs", "/api/team_usage",
    "/api/partition_summary", "/api/cluster_utilization",
    "/api/log_files/", "/api/log/", "/api/log_full/",
    "/api/jsonl_index/", "/api/jsonl_record/", "/api/ls/",
    "/api/cancel_jobs/", "/api/cancel/",
    "/api/force_poll/", "/api/run_script/", "/api/stats/",
    "/api/mount/", "/api/run_info/", "/api/run/", "/api/cleanup"
)


def _purge_stale_locked(now_ms=None):
    """Evict bookkeeping entries that cannot correspond to live requests.

    Caller must hold ``_active_lock``. Two eviction reasons:
      * ``dead-thread``: tid no longer in ``threading.enumerate()`` (sync
        workers only — ``gthread`` recycles threads).
      * ``ttl-expired``: entry older than ``_ACTIVE_TTL_SEC``. This is the
        only safety net under ``gthread``, because the thread for a stuck
        or leaked request stays alive in the pool indefinitely.

    Returns the number of entries evicted.
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (_ACTIVE_TTL_SEC * 1000)
    alive = {t.ident for t in threading.enumerate()}

    evictions = []
    for tid in list(_active_threads):
        if tid not in alive:
            evictions.append(("dead-thread", tid, _active_requests_meta.get(tid, {})))
            continue
        meta = _active_requests_meta.get(tid, {})
        started_ms = meta.get("started_ms", now_ms)
        if started_ms < cutoff_ms:
            evictions.append(("ttl-expired", tid, meta))

    for reason, tid, meta in evictions:
        _active_threads.discard(tid)
        _active_requests_meta.pop(tid, None)
        if reason == "ttl-expired" and meta:
            age_s = (now_ms - meta.get("started_ms", now_ms)) / 1000.0
            _log.warning(
                "evicting stale active request: %s %s tid=%s age=%.0fs",
                meta.get("method") or "?",
                meta.get("path") or "?",
                tid,
                age_s,
            )
    return len(evictions)


def _active_request_count():
    """Thread-safe active request count.

    Self-heals via two mechanisms (both required under ``gthread``):
      * ``threading.enumerate()`` filtering for dead threads (sync workers).
      * TTL eviction for entries older than ``_ACTIVE_TTL_SEC`` — the only
        safety net when threads stay alive in the pool but their counter
        entry leaked.
    """
    with _active_lock:
        _purge_stale_locked()
        return len(_active_threads)


def _active_request_snapshot(limit=8):
    now_ms = int(time.time() * 1000)
    with _active_lock:
        _purge_stale_locked(now_ms)
        items = []
        for tid in _active_threads:
            meta = _active_requests_meta.get(tid, {})
            started_ms = meta.get("started_ms", now_ms)
            items.append({
                "thread_id": tid,
                "method": meta.get("method"),
                "path": meta.get("path"),
                "run_id": meta.get("run_id"),
                "age_ms": max(0, now_ms - started_ms),
            })
    items.sort(key=lambda item: item["age_ms"], reverse=True)
    return items[:limit]


def _debug_log(run_id, hypothesis_id, location, message, data):
    pass


_SHED_EXEMPT = (
    "/api/health",
    "/api/sdk/events",
    "/api/_diag/active",
    "/api/_diag/dump_stacks",
)

@api.before_request
def _start_timer():
    g._req_start = time.monotonic()
    tid = threading.current_thread().ident
    path = request.path
    exempt = path in _SHED_EXEMPT or not path.startswith("/api/")
    with _active_lock:
        # Purge stale entries before deciding on shedding so a leaked
        # counter cannot wedge the worker. Without this, a single leak
        # under `gthread` is permanent until the worker dies.
        _purge_stale_locked()
        count = len(_active_threads)
        if not exempt and count >= _MAX_ACTIVE:
            shed = True
        else:
            _active_threads.add(tid)
            _active_requests_meta[tid] = {
                "method": request.method,
                "path": request.path,
                "run_id": request.headers.get("X-Debug-Run-Id"),
                "started_ms": int(time.time() * 1000),
            }
            shed = False
    if shed:
        _log.warning("load shedding: %d active, rejecting %s", count, path)
        return jsonify({"status": "error", "error": "server busy"}), 503


@api.teardown_request
def _release_load(exc):
    # Always discard the current tid, regardless of any per-request flag.
    # Under `gthread` only one request runs per thread at a time, so the
    # current tid maps unambiguously to the request being torn down.
    # This also opportunistically cleans up any prior leak on the same tid.
    tid = threading.current_thread().ident
    with _active_lock:
        _active_threads.discard(tid)
        _active_requests_meta.pop(tid, None)


@api.route("/api/_diag/active")
def api_diag_active():
    """Snapshot of in-flight request threads. Exempt from load shedding so
    it remains reachable when the worker is wedged."""
    return jsonify({
        "active_requests": _active_request_count(),
        "max_active": _MAX_ACTIVE,
        "snapshot": _active_request_snapshot(limit=32),
    })


@api.route("/api/_diag/dump_stacks", methods=["GET", "POST"])
def api_diag_dump_stacks():
    """Force a thread-stack dump on demand. Useful when the dashboard
    appears slow but the watchdog hasn't tripped yet — captures evidence
    while the suspect threads are still stuck.

    Exempt from load shedding so it works even during a wedge.
    """
    from .ssh import _dump_all_thread_stacks
    reason = (request.args.get("reason") or "manual").strip()[:80]
    _dump_all_thread_stacks(reason=f"manual: {reason}")
    return jsonify({
        "status": "ok",
        "message": "stack dump written to log and data/watchdog-dumps/",
        "active_requests": _active_request_count(),
    })


@api.after_request
def _log_slow(response):
    start = getattr(g, '_req_start', None)
    if start is not None:
        ms = (time.monotonic() - start) * 1000
        if ms > _SLOW_REQUEST_MS:
            _log.warning("slow request: %s %s — %.0fms", request.method, request.path, ms)
    return response


@api.route("/")
def index():
    resp = make_response(render_template("index.html", clusters=CLUSTERS, username=DEFAULT_USER, team=TEAM_NAME))
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@api.route("/api/jobs")
def api_jobs():
    from flask import Response

    touch_demand()

    version = get_version()
    etag = f'"{version}"'
    if request.headers.get("If-None-Match") == etag:
        return Response(status=304)
    try:
        snapshot = build_board_snapshot(schedule_prefetch_active=True)

        def cluster_sort_key(item):
            name, data = item
            jobs = data.get("jobs", [])
            has_running = any(j.get("state") in ("RUNNING", "COMPLETING") for j in jobs if not j.get("_pinned"))
            has_pending = any(j.get("state") == "PENDING" for j in jobs if not j.get("_pinned"))
            has_live = any(not j.get("_pinned") for j in jobs)
            return (name == "local", not has_running, not has_pending, not has_live, name)

        ordered = dict(sorted(snapshot.items(), key=cluster_sort_key))

        mounts = all_mount_status()
        poller_status = get_poller().get_status()
        for c, d in ordered.items():
            if c != "local":
                d["mount"] = mounts.get(c, {"mounted": False, "root": ""})
            d["poller"] = poller_status.get(c, {})
        resp = jsonify(ordered)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "no-cache"
        return resp
    except Exception:
        raise


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
    dismiss_by_state_prefix(cluster, ["CANCELLED", "COMPLETING"])
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


@api.route("/api/jobs_summary")
def api_jobs_summary():
    """One-line-per-cluster overview: running/pending/failed counts (MCP resource proxy)."""
    touch_demand()
    snapshot = build_board_snapshot(schedule_prefetch_active=False)
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
    summary = f"Total: {total_r} running, {total_p} pending, {total_f} failed\n" + "\n".join(lines)
    return jsonify({"status": "ok", "summary": summary})


@api.route("/api/jobs/<cluster>")
def api_jobs_cluster(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if request.args.get("force") == "1":
        get_poller().poll_now(cluster)
    touch_demand()
    data = build_cluster_board_entry(cluster, schedule_prefetch_active=True)
    if cluster != "local":
        data["mount"] = cluster_mount_status(cluster)
    data["poller"] = get_poller().get_status().get(cluster, {})
    return jsonify(data)


@api.route("/api/prefetch_visible", methods=["POST"])
def api_prefetch_visible():
    """Accept prefetch hints — the background poller handles actual SSH work."""
    payload = request.get_json(silent=True) or {}
    jobs = payload.get("jobs", [])
    clusters = set()
    valid = 0
    for item in jobs:
        c = item.get("cluster")
        if c and c in CLUSTERS:
            clusters.add(c)
            valid += 1
            jid = str(item.get("job_id", "")).strip()
            if jid:
                schedule_prefetch(c, jid)
    return jsonify({"status": "ok", "clusters": list(clusters), "jobs": valid})


@api.route("/api/progress", methods=["POST"])
def api_progress():
    """Return cached progress percentages and estimated start times."""
    payload = request.get_json(silent=True) or {}
    jobs = payload.get("jobs", [])
    progress = {}
    progress_sources = {}
    est_starts = {}

    _ov = cache_db_get_all_multi(["progress", "progress_source", "est_start"])
    db_progress = _ov["progress"]
    db_progress_src = _ov["progress_source"]
    db_est_start = _ov["est_start"]

    for item in jobs:
        c = item.get("cluster")
        jid = str(item.get("job_id", "")).strip()
        if not c or not jid:
            continue
        ck = f"{c}:{jid}"
        
        pct = _cache_get(_progress_cache, (c, jid), PROGRESS_TTL_SEC)
        if pct is None:
            pct = db_progress.get(ck)
            
        if pct is not None:
            progress[ck] = pct
            src = _cache_get(_progress_source_cache, (c, jid), PROGRESS_TTL_SEC)
            if src is None:
                src = db_progress_src.get(ck)
            if src:
                progress_sources[ck] = src
        est = _cache_get(_est_start_cache, (c, jid), EST_START_TTL_SEC)
        if est is None:
            est = db_est_start.get(ck)
        if est:
            est_starts[ck] = est

    db_team = cache_db_get_all("team_usage")
    team_usage = {}
    seen_clusters = {item.get("cluster") for item in jobs if item.get("cluster")}
    for c in seen_clusters:
        tu = _cache_get(_team_usage_cache, c, TEAM_USAGE_TTL_SEC) or db_team.get(c)
        if tu:
            team_usage[c] = tu

    from .config import TEAM_GPU_ALLOC
    return jsonify({
        "board_version": get_version(),
        "progress": progress,
        "progress_sources": progress_sources,
        "est_starts": est_starts,
        "team_usage": team_usage,
        "team_gpu_allocations": dict(TEAM_GPU_ALLOC),
    })


@api.route("/api/team_usage", methods=["POST"])
def api_team_usage():
    """Return cached team GPU usage; hydrate only on explicit force."""
    payload = request.get_json(silent=True) or {}
    cluster_list = payload.get("clusters", [])
    force = bool(payload.get("force")) or request.args.get("force") == "1"
    if not cluster_list:
        cluster_list = [c for c in CLUSTERS if c != "local"]

    results = {}
    if force:
        for c in cluster_list:
            if c not in CLUSTERS or c == "local":
                continue
            try:
                tu = fetch_team_usage(c)
            except Exception:
                _log.exception("team_usage fetch failed for %s", c)
                tu = None
            if tu:
                results[c] = tu
    else:
        db_team = cache_db_get_all("team_usage")
        for c in cluster_list:
            if c not in CLUSTERS or c == "local":
                continue
            tu = _cache_get(_team_usage_cache, c, TEAM_USAGE_TTL_SEC) or db_team.get(c)
            if tu is None:
                tu, _ = cache_db_get_stale("team_usage", c)
            if tu:
                results[c] = tu

    from .config import TEAM_GPU_ALLOC
    return jsonify({"status": "ok", "team_usage": results, "team_gpu_allocations": dict(TEAM_GPU_ALLOC)})


@api.route("/api/team_jobs")
def api_team_jobs():
    """Return cached team job data; hydrate only on explicit force."""
    from .jobs import _team_jobs_cache, TEAM_JOBS_TTL_SEC
    cluster_filter = request.args.get("cluster", "")
    force = request.args.get("force") == "1"
    if cluster_filter:
        cluster_list = [c.strip() for c in cluster_filter.split(",") if c.strip()]
    else:
        cluster_list = [c for c in CLUSTERS if c != "local"]

    results = {}
    if force:
        for c in cluster_list:
            if c not in CLUSTERS or c == "local":
                continue
            try:
                fetched = fetch_team_jobs(c)
            except Exception:
                _log.exception("team_jobs fetch failed for %s", c)
                fetched = None
            if fetched is not None:
                results[c] = fetched
    else:
        for c in cluster_list:
            if c not in CLUSTERS or c == "local":
                continue
            cached = _cache_get(_team_jobs_cache, c, TEAM_JOBS_TTL_SEC)
            if cached is not None:
                results[c] = cached

    return jsonify({"status": "ok", "clusters": results})


@api.route("/api/cancel/<cluster>/<job_id>", methods=["POST"])
def api_cancel(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if str(job_id).startswith("sdk-"):
        from .db import cancel_sdk_job
        cancel_sdk_job(str(job_id))
        from .poller import bump_version
        bump_version()
        return jsonify({"status": "ok", "note": "SDK run cancelled"})
    try:
        if cluster == "local":
            os.kill(int(job_id), 15)
            return jsonify({"status": "ok"})
        result = cancel_jobs_with_report(cluster, [job_id], timeout_sec=10, chunk_size=1)
        if result["cancelled_ids"]:
            return jsonify({"status": "ok"})
        error = result["errors"][0]["error"] if result["errors"] else "Cancel failed"
        return jsonify({"status": "error", "error": error})
    except Exception as e:
        _log.exception("cancel %s/%s failed", cluster, job_id)
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/cancel_jobs/<cluster>", methods=["POST"])
def api_cancel_jobs(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    payload = request.get_json(silent=True) or {}
    job_ids = payload.get("job_ids", [])
    if not job_ids or not isinstance(job_ids, list):
        return jsonify({"status": "error", "error": "job_ids list required"}), 400
    sdk_ids = [str(jid).strip() for jid in job_ids if str(jid).strip().startswith("sdk-")]
    slurm_ids = [str(jid).strip() for jid in job_ids
                 if str(jid).strip() and not str(jid).strip().startswith("sdk-")
                 and any(c.isdigit() for c in str(jid))]

    sdk_cancelled = 0
    if sdk_ids:
        from .db import cancel_sdk_job
        from .poller import bump_version as _bv
        for sid in sdk_ids:
            cancel_sdk_job(sid)
            sdk_cancelled += 1
        _bv()

    if not slurm_ids and not sdk_ids:
        return jsonify({"status": "error", "error": "No valid job IDs"}), 400
    if not slurm_ids:
        return jsonify({"status": "ok", "cancelled": sdk_cancelled})

    sanitized = slurm_ids
    try:
        if cluster == "local":
            errors = []
            for jid in sanitized:
                try:
                    os.kill(int(jid), 15)
                except Exception as e:
                    errors.append(f"{jid}: {e}")
            if errors:
                return jsonify({"status": "partial", "cancelled": len(sanitized) + sdk_cancelled - len(errors), "errors": errors})
            return jsonify({"status": "ok", "cancelled": len(sanitized) + sdk_cancelled})

        result = cancel_jobs_with_report(cluster, sanitized, timeout_sec=20, chunk_size=25)
        cancelled = len(result["cancelled_ids"]) + sdk_cancelled
        errors = [
            f'{err["job_id"]}: {err["error"]}'
            for err in result["errors"]
        ]
        if errors:
            return jsonify({
                "status": "partial",
                "cancelled": cancelled,
                "cancelled_ids": result["cancelled_ids"] + sdk_ids,
                "failed_ids": [err["job_id"] for err in result["errors"]],
                "errors": errors,
            })
        return jsonify({"status": "ok", "cancelled": cancelled, "cancelled_ids": result["cancelled_ids"] + sdk_ids})
    except Exception as e:
        _log.exception("cancel_jobs %s failed", cluster)
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
        _log.exception("run_script on %s failed", cluster)
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/stats/<cluster>/<job_id>")
def api_stats(cluster, job_id):
    """Return cached stats — poller refreshes running job stats periodically."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    from .jobs import get_stats_snapshots

    db_val, is_fresh = cache_db_get_stale("stats", f"{cluster}:{job_id}")
    if db_val:
        result = dict(db_val)
        if not is_fresh:
            result["_stale"] = True
    else:
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


def _resolve_run_via_job(cluster, job_id):
    """Look up a run by finding the job's run_id, then loading that run with all jobs."""
    try:
        from .db import get_db
        con = get_db()
        row = con.execute(
            "SELECT run_id FROM job_history WHERE cluster=? AND job_id=? AND run_id IS NOT NULL",
            (cluster, str(job_id)),
        ).fetchone()
        con.close()
        if row and row["run_id"]:
            from .db import get_db as _gdb
            c2 = _gdb()
            run_row = c2.execute("SELECT root_job_id FROM runs WHERE id=?", (row["run_id"],)).fetchone()
            c2.close()
            if run_row:
                return get_run_with_jobs(cluster, run_row["root_job_id"])
    except Exception:
        pass
    return None


def _inherit_sdk_provenance(run, cluster):
    """If a legacy run shares a name with an SDK run, copy provenance fields.

    The legacy run_name may have a doubled job_name_prefix (e.g. hle_hle_test_...)
    so we try both the full name and the name with the first prefix stripped.
    """
    run_name = run.get("run_name", "")
    if not run_name:
        return
    try:
        from .db import get_db
        con = get_db()
        candidates = [run_name]
        parts = run_name.split("_", 1)
        if len(parts) == 2:
            candidates.append(parts[1])

        sdk_run = None
        for name in candidates:
            sdk_run = con.execute(
                """SELECT submit_command, submit_cwd, git_commit, launcher_hostname, primary_output_dir, params_json, run_uuid
                   FROM runs WHERE cluster=? AND source='sdk' AND (run_name=? OR run_name LIKE ?) AND submit_command != ''
                   ORDER BY id DESC LIMIT 1""",
                (cluster, name, f"%{name}%"),
            ).fetchone()
            if sdk_run:
                break
        con.close()
        if sdk_run:
            for field in (
                "submit_command", "submit_cwd", "git_commit",
                "launcher_hostname", "primary_output_dir", "params_json",
            ):
                if not run.get(field) and sdk_run[field]:
                    run[field] = sdk_run[field]
            if not run.get("source") or run["source"] == "legacy":
                run["source"] = "sdk+legacy"
    except Exception:
        pass


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
        run = _resolve_run_via_job(cluster, root_job_id)
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404
    if not run.get("meta_fetched"):
        pass
    for j in run.get("jobs", []):
        if not j.get("project"):
            j["project"] = extract_project(j.get("job_name") or j.get("name") or "")
        proj = j.get("project", "")
        if proj:
            j["project_color"] = get_project_color(proj)
            j["project_emoji"] = get_project_emoji(proj)
            _jname = j.get("job_name") or j.get("name") or ""
            j["campaign"] = extract_campaign(_jname, proj)
    unique_nodes, total_gpus, gpus_per_node = _compute_run_resources(
        run.get("jobs", []),
        cluster=cluster,
        run_scontrol_raw=run.get("scontrol_raw", ""),
        run_batch_script=run.get("batch_script", ""),
    )
    run["unique_nodes"] = unique_nodes
    run["total_gpus"] = total_gpus
    run["gpus_per_node"] = gpus_per_node

    if not run.get("submit_command") and run.get("source") != "sdk":
        _inherit_sdk_provenance(run, cluster)

    # Hydrate structured pipeline params (model, benchmarks, …) captured at
    # the SDK hook. Stored as JSON to survive schema-less evolution; parsed
    # here so the frontend can render the Run Parameters block directly.
    raw_params = run.pop("params_json", "") or ""
    if raw_params:
        try:
            run["params"] = json.loads(raw_params)
        except (ValueError, TypeError):
            run["params"] = {}
    else:
        run["params"] = {}

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
    with db_write() as db:
        db.execute("UPDATE runs SET meta_fetched=0 WHERE cluster=? AND root_job_id=?", (cluster, str(root_job_id)))
    _capture_run_metadata(cluster, str(root_job_id), run["id"])
    return api_run_info(cluster, root_job_id)


@api.route("/api/run/<int:run_id>", methods=["PATCH"])
def api_update_run(run_id):
    """Partial update of user-editable run fields (starred, notes)."""
    data = request.get_json(force=True, silent=True) or {}
    starred = data.get("starred")
    notes = data.get("notes")
    if starred is None and notes is None:
        return jsonify({"status": "error", "error": "No fields to update"}), 400
    update_run_fields(run_id, starred=starred, notes=notes)
    return jsonify({"status": "ok"})


@api.route("/api/history")
def api_history():
    def _csv_arg(name):
        values = []
        for raw in request.args.getlist(name):
            values.extend(part.strip() for part in raw.split(",") if part.strip())
        return ",".join(values)

    cluster = request.args.get("cluster", "all")
    limit = int(request.args.get("limit", 200))
    project = request.args.get("project", "")
    campaign = request.args.get("campaign", "")
    partition = request.args.get("partition", "")
    account = request.args.get("account", "")
    search = request.args.get("q", "").strip() or request.args.get("search", "").strip()
    state = _csv_arg("state")
    days = request.args.get("days", "")
    rows = get_history(
        cluster,
        limit,
        project=project,
        search=search,
        state=state,
        campaign=campaign,
        partition=partition,
        account=account,
        days=days,
    )
    by_cluster = {}
    for row in rows:
        by_cluster.setdefault(row.get("cluster") or cluster, []).append(row)
    for cluster_name, cluster_rows in by_cluster.items():
        if cluster_name and cluster_name != "all":
            _fill_output_dirs(cluster_name, cluster_rows)
    for r in rows:
        if not r.get("project"):
            r["project"] = extract_project(r.get("job_name") or r.get("name") or "")
        proj = r.get("project", "")
        if proj:
            r["project_color"] = get_project_color(proj)
            r["project_emoji"] = get_project_emoji(proj)
            _jn = r.get("job_name") or r.get("name") or ""
            r["campaign"] = extract_campaign(_jn, proj)
    return jsonify(rows)


@api.route("/api/projects")
def api_projects():
    from .config import get_project_color as _color, get_project_emoji as _emoji, PROJECTS
    # Only registered settings projects — history may still list removed keys.
    projects = [p for p in get_projects() if p.get("project") in PROJECTS]
    for p in projects:
        p["color"] = _color(p["project"])
        p["emoji"] = _emoji(p["project"])
    return jsonify(projects)


@api.route("/api/projects/all")
def api_projects_all():
    """Return every registered project, regardless of whether it has any jobs.

    Includes the full record (color, emoji, prefixes, campaign delimiter,
    description, timestamps). The sidebar endpoint (``GET /api/projects``)
    only returns projects with cluster activity; this one is the canonical
    list for project-management UIs and the ``list_projects`` MCP tool.
    """
    from .db import db_list_projects
    return jsonify(db_list_projects())


@api.route("/api/projects", methods=["POST"])
def api_project_create():
    from .db import db_create_project, re_extract_unmatched_projects
    payload = request.get_json(silent=True) or {}
    result = db_create_project(
        name=payload.get("name", ""),
        color=payload.get("color"),
        emoji=payload.get("emoji"),
        prefixes=payload.get("prefixes"),
        default_campaign=payload.get("default_campaign"),
        campaign_delimiter=payload.get("campaign_delimiter") or "_",
        description=payload.get("description") or "",
    )
    if result.get("status") == "error":
        return jsonify(result), 400
    result["reassigned"] = re_extract_unmatched_projects()
    return jsonify(result)


@api.route("/api/projects/<name>", methods=["PUT"])
def api_project_update(name):
    from .db import db_update_project, re_extract_unmatched_projects
    payload = request.get_json(silent=True) or {}
    fields = {k: payload.get(k) for k in (
        "color", "emoji", "prefixes", "default_campaign",
        "campaign_delimiter", "description",
    ) if k in payload}
    result = db_update_project(name, **fields)
    if result.get("status") == "error":
        status = 404 if "not found" in result.get("error", "") else 400
        return jsonify(result), status
    if "prefixes" in fields or "default_campaign" in fields:
        result["reassigned"] = re_extract_unmatched_projects()
    return jsonify(result)


@api.route("/api/projects/<name>", methods=["DELETE"])
def api_project_delete(name):
    from .db import db_delete_project
    result = db_delete_project(name)
    if result.get("status") == "error":
        status = 404 if "not found" in result.get("error", "") else 400
        return jsonify(result), status
    return jsonify(result)


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
            if not any(content.startswith(p) for p in ("Could not read log:", "File not found on cluster:", "Invalid local process")):
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
        entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
        payload = {"status": "ok", "path": path, "entries": entries, "source": "ssh", "resolved_path": path}
        _cache_set(_dir_list_cache, cache_key, payload)
        return jsonify(payload)
    except Exception as e:
        _log.exception("ls %s:%s failed", cluster, path)
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
        if not any(content.startswith(p) for p in ("Could not read log:", "File not found on cluster:", "Invalid local process")):
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
            _log.exception("log_full local read %s/%s failed", cluster, job_id)
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
            _log.exception("log_full SSH read %s/%s failed", cluster, job_id)
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
        _log.exception("jsonl_index %s/%s failed", cluster, job_id)
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
        _log.exception("jsonl_record %s/%s line %d failed", cluster, job_id, line_num)
        return jsonify({"status": "error", "error": str(e)})


@api.route("/api/force_poll/<cluster>", methods=["POST"])
def api_force_poll(cluster):
    """Queue one explicit live poll now without tying up the request thread."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    poller = get_poller()
    poller_state = poller.get_status().get(cluster, {})
    touch_demand()
    poller.request_priority(cluster)
    return jsonify({
        "status": "queued",
        "cluster": cluster,
        "queued": True,
        "inflight": poller_state.get("inflight", False),
    }), 202


@api.route("/api/health")
def api_health():
    """Lightweight health check with circuit breaker, poller, and load status."""
    return jsonify({
        "status": "ok",
        "active_requests": _active_request_count(),
        "max_active": _MAX_ACTIVE,
        "circuit_breakers": get_circuit_breaker_status(),
        "poller": get_poller().get_status(),
        "board_version": get_version(),
    })


@api.route("/api/settings")
def api_settings_get():
    return jsonify(settings_response())


@api.route("/api/settings", methods=["POST"])
def api_settings_post():
    patch = request.get_json(silent=True)
    if not patch or not isinstance(patch, dict):
        return jsonify({"status": "error", "error": "Invalid JSON body"}), 400

    import copy
    from . import config as _cfgmod
    _cfgmod._sync_config()
    merged = copy.deepcopy(_cfgmod._CONFIG)
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
        # Projects moved to the SQLite ``projects`` table — manage them via
        # ``/api/projects`` (POST/PUT/DELETE) or the matching MCP tools.
        # Silently drop the field here so older clients don't blow up, but
        # log it so we can spot any stale callers during the transition.
        _log.warning(
            "POST /api/settings included a 'projects' field — ignored. "
            "Use /api/projects endpoints or the MCP project tools instead."
        )
    if "team" in patch:
        merged["team"] = patch["team"]
    if "team_gpu_allocations" in patch:
        merged["team_gpu_allocations"] = patch["team_gpu_allocations"]
    if "ppps" in patch:
        merged["ppps"] = patch["ppps"]

    try:
        reload_config(merged)
    except Exception as exc:
        _log.exception("settings reload failed")
        return jsonify({"status": "error", "error": str(exc)}), 500

    # If any WDS input changed, kick off a fresh snapshot in the background
    # so the next read of wds_history reflects the new value without waiting
    # for the next periodic tick (default 15 min).
    if any(k in patch for k in ("team_gpu_allocations", "ppps", "team")):
        import threading
        from .wds import compute_wds_snapshot

        def _refresh_wds():
            try:
                compute_wds_snapshot()
            except Exception:
                _log.exception("WDS snapshot after settings change failed")

        threading.Thread(target=_refresh_wds, daemon=True,
                         name="wds-after-settings").start()

    return jsonify({"status": "ok", "settings": settings_response()})


# ─── Logbook routes ──────────────────────────────────────────────────────────

from .cluster_dashboard import get_cluster_utilization
from .config import DASHBOARD_URL as _DASHBOARD_URL
from .storage_quota import fetch_storage_quota  # noqa: used only as SSH fallback if needed


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
    """Return cached storage quota — poller refreshes periodically."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    db_val, is_fresh = cache_db_get_stale("storage_quota", cluster)
    if db_val:
        return jsonify(db_val)
    return jsonify({"status": "ok", "quotas": [], "cluster": cluster})


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
    """Return cached partition data — poller refreshes periodically."""
    data = get_all_partitions_cached()
    return jsonify({"status": "ok", "clusters": data})


@api.route("/api/partitions/<cluster>")
def api_partitions_cluster(cluster):
    """Return cached partition data for a cluster."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if cluster == "local":
        return jsonify({"status": "error", "error": "No partitions for local"}), 400
    data = _get_partitions(cluster, force=False)
    if data is None:
        db_data = cache_db_get("partitions", cluster)
        if db_data:
            return jsonify({"status": "ok", "cluster": cluster, "partitions": db_data})
        return jsonify({"status": "ok", "cluster": cluster, "partitions": []})
    return jsonify({"status": "ok", "cluster": cluster, "partitions": data})


def _partition_summary_for_cluster(cluster_name, parts):
    accessible = [p for p in parts if p.get("user_accessible", True) and p.get("state") == "UP"]
    gpu_parts = [
        p for p in accessible
        if not p["name"].startswith("cpu") and p["name"] not in ("defq", "fake")
    ]
    total_nodes = max((p.get("total_nodes", 0) for p in gpu_parts), default=0)
    cluster_gpus_fallback = CLUSTERS.get(cluster_name, {}).get("gpus_per_node", 0)

    part_list = []
    for p in gpu_parts:
        gpn = p.get("gpus_per_node", 0) or cluster_gpus_fallback
        part_list.append({
            "name": p["name"],
            "max_time": p["max_time"],
            "priority_tier": p.get("priority_tier", 0),
            "total_nodes": p.get("total_nodes", 0),
            "idle_nodes": p.get("idle_nodes", 0),
            "pending_jobs": p.get("pending_jobs", 0),
            "gpus_per_node": gpn,
            "preemptable": p.get("preempt_mode", "OFF") != "OFF",
        })

    return {
        "gpu_partitions": len(gpu_parts),
        "total_nodes": total_nodes,
        "idle_nodes": max((p.get("idle_nodes", 0) for p in gpu_parts), default=0),
        "pending_jobs": sum(p.get("pending_jobs", 0) for p in gpu_parts),
        "gpu_type": CLUSTERS.get(cluster_name, {}).get("gpu_type", ""),
        "partitions": part_list,
    }


@api.route("/api/partition_summary")
def api_partition_summary():
    cluster = request.args.get("cluster", "").strip()
    force = request.args.get("force", "0") == "1"
    if cluster:
        if cluster not in CLUSTERS:
            return jsonify({"status": "error", "error": "Unknown cluster"}), 404
        if cluster == "local":
            return jsonify({"status": "error", "error": "No partitions for local"}), 400
        parts = _get_partitions(cluster, force=force) or []
        data = {cluster: _partition_summary_for_cluster(cluster, parts)}
    else:
        data = get_partition_summary()
    return jsonify({"status": "ok", "clusters": data})


@api.route("/api/where_to_submit", methods=["POST"])
def api_where_to_submit():
    """Rank clusters by WDS score for job submission (MCP proxy target)."""
    from .aihub import get_ppp_allocations as _wts_alloc, get_my_fairshare as _wts_fs
    from .partitions import get_partition_summary as _wts_ps
    from .jobs import fetch_team_jobs as _wts_tj

    payload = request.get_json(silent=True) or {}
    nodes = int(payload.get("nodes", 1))
    gpus_per_node = int(payload.get("gpus_per_node", 8))
    gpu_type = payload.get("gpu_type", "")

    job_gpus = nodes * gpus_per_node
    pref_gpu = gpu_type.lower() if gpu_type else ""
    me = DEFAULT_USER

    def _fetch_parallel():
        with ThreadPoolExecutor(max_workers=4) as pool:
            f_alloc = pool.submit(_wts_alloc)
            f_fs = pool.submit(_wts_fs)
            f_parts = pool.submit(_wts_ps)
            f_tj = pool.submit(lambda: {c: _wts_tj(c) for c in CLUSTERS if c != "local"})
        return f_alloc.result(), f_fs.result(), f_parts.result(), f_tj.result()

    try:
        alloc, my_fs, part_clusters, tj_clusters = _fetch_parallel()
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500

    if not alloc:
        return jsonify({"status": "error", "error": "Could not fetch allocation data"}), 500

    my_fs_clusters = (my_fs or {}).get("clusters", {})
    part_clusters = part_clusters or {}
    tj_clusters = tj_clusters or {}
    team_allocs = settings_response().get("team_gpu_allocations", {})

    recommendations = []
    my_total_running = 0
    my_total_pending = 0

    all_cluster_names = set(alloc.get("clusters", {}).keys())
    for cn in CLUSTERS:
        if cn != "local":
            all_cluster_names.add(cn)

    import math
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
        wds = 0
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
                a_wds = max(0, min(100, round(100 * resource_gate * priority_blend * machine_score * team_penalty * occupancy_factor)))
                accounts.append({
                    "account": acct_name, "account_short": acct_name.split("_")[-1] if "_" in acct_name else acct_name,
                    "wds": a_wds, "ppp_level_fs": round(level_fs, 2), "my_level_fs": my_level_fs,
                    "headroom": ppp_headroom, "free_for_team": free,
                    "gpus_consumed": ad.get("gpus_consumed", 0), "gpus_allocated": ad.get("gpus_allocated", 0),
                })
            accounts.sort(key=lambda a: -a["wds"])
        else:
            resource_gate = min(1, idle_nodes / max(nodes, 1))
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
    return jsonify({"status": "ok", "recommendations": recommendations,
                    "my_total_running": my_total_running, "my_total_pending": my_total_pending,
                    "job_gpus_requested": job_gpus})


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
        _log.exception("recommend failed")
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
    cluster = request.args.get("cluster", "")
    clusters = [c.strip() for c in cluster.split(",") if c.strip()] or None
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_alloc(accounts=acct_list, clusters=clusters, force=force)
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
    cluster = request.args.get("cluster", "")
    clusters = [c.strip() for c in cluster.split(",") if c.strip()] or None
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_team_overlay(clusters=clusters, force=force)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/aihub/my_fairshare")
def api_aihub_my_fairshare():
    cluster = request.args.get("cluster", "")
    clusters = [c.strip() for c in cluster.split(",") if c.strip()] or None
    force = request.args.get("force", "0") == "1"
    try:
        data = _aihub_my_fairshare(clusters=clusters, force=force)
        return jsonify({"status": "ok", **data})
    except Exception as exc:
        return jsonify({"status": "error", "error": str(exc)}), 500


@api.route("/api/wait_calibration")
def api_wait_calibration():
    from .wds import get_wait_calibration
    try:
        return jsonify(get_wait_calibration())
    except Exception as exc:
        _log.exception("wait_calibration failed")
        return jsonify({"error": str(exc)}), 500


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
        _log.exception("wds_history failed")
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
    pinned = payload.get("pinned")
    new_project = payload.get("new_project")
    result = _lb_update(
        project, entry_id,
        title=title, body=body, entry_type=entry_type,
        pinned=pinned, new_project=new_project,
    )
    status = result.get("status")
    if status == "error":
        return jsonify(result), 404
    if status == "error_validation":
        # Drop the internal status discriminator from the public response
        # body but keep the error message; surface as 400 instead of 404.
        return jsonify({"status": "error", "error": result.get("error", "")}), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/entries/<int:entry_id>/pin", methods=["POST"])
def api_logbook_pin(project, entry_id):
    pinned = (request.get_json(silent=True) or {}).get("pinned", True)
    with db_write() as con:
        con.execute("UPDATE logbook_entries SET pinned=? WHERE id=? AND project=?",
                    (1 if pinned else 0, entry_id, project))
    return jsonify({"status": "ok"})


@api.route("/api/logbook/<project>/entries/<int:entry_id>", methods=["DELETE"])
def api_logbook_delete(project, entry_id):
    result = _lb_delete(project, entry_id)
    if result.get("status") == "error":
        return jsonify(result), 404
    return jsonify(result)


@api.route("/api/logbook/<project>/entries/<int:entry_id>/export/docx")
def api_logbook_export_docx(project, entry_id):
    import io
    from flask import send_file
    from .docx_export import export_entry_docx
    entry = _lb_get(project, entry_id)
    if entry.get("status") == "error":
        return jsonify(entry), 404
    try:
        docx_bytes = export_entry_docx(project, entry)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(exc)}), 500
    safe_title = re.sub(r'[^\w\s\-]', '', entry.get("title", "export"))[:80].strip() or "export"
    filename = f"{safe_title}.docx"
    return send_file(
        io.BytesIO(docx_bytes),
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=True,
        download_name=filename,
    )


@api.route("/api/logbook/bulk_read", methods=["POST"])
def api_logbook_bulk_read():
    """Bulk-read full logbook entries (MCP proxy target)."""
    from .logbooks import list_logbook_projects
    payload = request.get_json(silent=True) or {}
    project = payload.get("project", "")
    entry_type = payload.get("entry_type", "")
    sort = payload.get("sort", "created_at")
    limit_per_project = int(payload.get("limit_per_project", 200))
    max_entries = int(payload.get("max_entries", 1000))

    if sort not in ("edited_at", "created_at", "title"):
        return jsonify({"status": "error", "error": "sort must be one of: edited_at, created_at, title"}), 400
    if entry_type and entry_type not in ("note", "plan"):
        return jsonify({"status": "error", "error": "entry_type must be 'note', 'plan', or omitted"}), 400

    projects = [project] if project else list_logbook_projects()
    if not projects:
        return jsonify({"status": "ok", "count": 0, "truncated": False, "projects": [], "entries": [], "errors": {}})

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
    return jsonify({"status": "ok", "count": len(entries), "truncated": truncated,
                    "projects": projects, "entries": entries, "errors": errors})


@api.route("/api/logbook/find", methods=["POST"])
def api_logbook_find():
    """Find logbook entries by substring/regex (MCP proxy target)."""
    import re as _re
    from .logbooks import list_logbook_projects
    payload = request.get_json(silent=True) or {}
    pattern = payload.get("pattern", "")
    project = payload.get("project", "")
    field = payload.get("field", "title")
    use_regex = bool(payload.get("regex", False))
    entry_type = payload.get("entry_type", "")
    full_body = payload.get("full_body", True)
    limit = int(payload.get("limit", 50))

    if field not in ("title", "body", "both"):
        return jsonify({"status": "error", "error": "field must be 'title', 'body', or 'both'"}), 400

    if use_regex:
        try:
            compiled = _re.compile(pattern, _re.IGNORECASE)
        except _re.error as e:
            return jsonify({"status": "error", "error": f"Invalid regex: {e}"}), 400
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
    return jsonify({"status": "ok", "count": len(results), "entries": results})


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
    from .config import get_project_color, get_project_emoji, PROJECTS
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"projects": [], "logbook": [], "history": []})

    ql = q.lower()

    def _search_projects():
        return [
            {"project": p["project"], "emoji": get_project_emoji(p["project"]),
             "color": get_project_color(p["project"]), "job_count": p["job_count"]}
            for p in get_projects()
            if p.get("project") in PROJECTS and ql in p["project"].lower()
        ][:8]

    def _search_logbook():
        try:
            return _lb_search(q, limit=8)
        except Exception:
            return []

    def _search_history():
        return [
            {"cluster": r["cluster"], "job_id": r.get("job_id") or r.get("jobid", ""),
             "job_name": r.get("job_name") or r.get("name", ""),
             "state": r.get("state", ""), "project": r.get("project", ""),
             "started": r.get("started", "")}
            for r in get_history(limit=8, search=q)
        ]

    f_proj = _shared_pool.submit(_search_projects)
    f_lb = _shared_pool.submit(_search_logbook)
    f_hist = _shared_pool.submit(_search_history)

    return jsonify({
        "projects": f_proj.result(),
        "logbook": f_lb.result(),
        "history": f_hist.result(),
    })


# ── SDK event ingest ─────────────────────────────────────────────────────────


def _adopt_matching_slurm_jobs(cluster, expname, sdk_run_id):
    """Link existing real Slurm jobs whose name contains the SDK run's expname."""
    if not cluster or not expname or not sdk_run_id:
        return
    try:
        with db_write() as con:
            rows = con.execute(
                """SELECT job_id, log_path FROM job_history
                   WHERE cluster=? AND job_name LIKE ? AND job_id NOT LIKE 'sdk-%'
                   AND (run_id IS NULL OR run_id != ?)""",
                (cluster, f"%{expname}%", sdk_run_id),
            ).fetchall()
            if rows:
                job_ids = [r["job_id"] for r in rows]
                placeholders = ",".join("?" for _ in job_ids)
                con.execute(
                    f"UPDATE job_history SET run_id=? WHERE cluster=? AND job_id IN ({placeholders})",
                    [sdk_run_id, cluster] + job_ids,
                )
                for r in rows:
                    if r["log_path"]:
                        con.execute(
                            "UPDATE runs SET primary_output_dir=? WHERE id=? AND (primary_output_dir IS NULL OR primary_output_dir='')",
                            (os.path.dirname(os.path.dirname(r["log_path"])), sdk_run_id),
                        )
                        break
    except Exception:
        pass

@api.route("/api/sdk/events", methods=["POST"])
def api_sdk_ingest():
    """Accept batched SDK events and persist runs/jobs/metrics immediately."""
    from .config import SDK_INGEST_TOKEN, extract_project
    from .db import (
        upsert_run_from_sdk,
        store_sdk_event,
        finalize_sdk_run,
        get_run_by_uuid,
        invalidate_pinned_cache,
    )
    from .poller import bump_version

    if SDK_INGEST_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {SDK_INGEST_TOKEN}":
            return jsonify({"status": "error", "error": "unauthorized"}), 401

    events = request.get_json(force=True, silent=True)
    if not isinstance(events, list):
        return jsonify({"status": "error", "error": "expected JSON array"}), 400

    accepted = 0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        run_uuid = ev.get("run_uuid", "")
        event_type = ev.get("event_type", "")
        event_seq = ev.get("event_seq", 0)
        ts = ev.get("ts", 0.0)
        payload = ev.get("payload", {})
        if not run_uuid or not event_type:
            continue

        import json as _j
        payload_json = _j.dumps(payload, default=str)
        store_sdk_event(run_uuid, event_type, event_seq, ts, payload_json)

        if event_type == "run_started":
            expname = payload.get("expname", "")
            cluster = payload.get("cluster", "")
            project = extract_project(expname)
            run_id = upsert_run_from_sdk(run_uuid, cluster, expname, project, payload)
            _adopt_matching_slurm_jobs(cluster, expname, run_id)
            bump_version()

        elif event_type in ("job_prepared", "job_submitted"):
            run = get_run_by_uuid(run_uuid)
            if run:
                cluster = payload.get("cluster") or run.get("cluster", "")
                partition = payload.get("partition", "")
                account = payload.get("account", "")
                num_nodes = payload.get("num_nodes", 0)
                num_gpus = payload.get("num_gpus")
                synthetic_job_id = f"sdk-{run_uuid[:12]}"
                with db_write() as con:
                    sets, params = [], []
                    if event_type == "job_submitted":
                        sets.append("state = CASE WHEN state = 'SUBMITTING' THEN 'PENDING' ELSE state END")
                        con.execute(
                            "UPDATE runs SET sdk_status='active' WHERE id=? AND sdk_status='submitting'",
                            (run["id"],),
                        )
                    if partition:
                        sets.append("partition = COALESCE(NULLIF(?, ''), partition)")
                        params.append(partition)
                    if account:
                        sets.append("account = COALESCE(NULLIF(?, ''), account)")
                        params.append(account)
                    if num_nodes:
                        sets.append("nodes = ?")
                        params.append(str(num_nodes))
                    if num_gpus is not None:
                        gres_val = f"gpu:{num_gpus}" if num_gpus else ""
                        sets.append("gres = COALESCE(NULLIF(?, ''), gres)")
                        params.append(gres_val)
                    if sets:
                        params.extend([cluster, synthetic_job_id])
                        con.execute(
                            f"UPDATE job_history SET {', '.join(sets)} WHERE cluster=? AND job_id=?",
                            params,
                        )
                bump_version()

        elif event_type in ("run_finished", "run_failed"):
            if event_type == "run_failed":
                status = payload.get("status", "failed")
            else:
                status = payload.get("status", "completed")
            finalize_sdk_run(run_uuid, status)
            bump_version()

        elif event_type == "job_state":
            _ingest_job_state(run_uuid, payload)
            bump_version()

        if event_type == "metric_logged":
            if payload.get("key") == "gpu_telemetry":
                _ingest_gpu_telemetry(run_uuid, payload)
            elif payload.get("key") == "progress":
                _ingest_progress(run_uuid, payload)
                bump_version()

        accepted += 1

    return jsonify({"status": "ok", "accepted": accepted})


def _ingest_progress(run_uuid, payload):
    """Handle progress metric events from the in-container monitor."""
    try:
        from .config import _cache_set, _progress_cache, _progress_source_cache, PROGRESS_TTL_SEC
        from .db import get_run_by_uuid, cache_db_put

        pct = payload.get("value")
        if pct is None or not isinstance(pct, (int, float)):
            return
        pct = int(pct)
        if not (0 <= pct <= 100):
            return

        context = payload.get("context", {})
        slurm_job_id = context.get("slurm_job_id", "")

        run = get_run_by_uuid(run_uuid)
        if not run:
            return
        cluster = run.get("cluster", "")

        job_ids = [slurm_job_id] if slurm_job_id and slurm_job_id != "unknown" else []
        job_ids.append(f"sdk-{run_uuid[:12]}")

        for jid in job_ids:
            _cache_set(_progress_cache, (cluster, jid), pct)
            _cache_set(_progress_source_cache, (cluster, jid), "sdk monitor")
            try:
                cache_db_put("progress", f"{cluster}:{jid}", pct, PROGRESS_TTL_SEC)
                cache_db_put("progress_source", f"{cluster}:{jid}", "sdk monitor", PROGRESS_TTL_SEC)
            except Exception:
                pass
    except Exception:
        pass


def _ingest_gpu_telemetry(run_uuid, payload):
    """Write GPU telemetry from the monitor into job_stats_snapshots."""
    try:
        from .db import get_run_by_uuid
        from datetime import datetime

        context = payload.get("context", {})
        slurm_job_id = context.get("slurm_job_id", "")
        gpus = payload.get("value", [])
        if not gpus or not isinstance(gpus, list):
            return

        run = get_run_by_uuid(run_uuid)
        if not run:
            return
        cluster = run.get("cluster", "")
        job_id = slurm_job_id or f"sdk-{run_uuid[:12]}"

        utils = [g.get("util", 0) for g in gpus if isinstance(g, dict)]
        mems_used = [g.get("mem_used", 0) for g in gpus if isinstance(g, dict)]
        mems_total = [g.get("mem_total", 0) for g in gpus if isinstance(g, dict)]

        gpu_util = round(sum(utils) / len(utils), 1) if utils else None
        gpu_mem_used = round(sum(mems_used) / len(mems_used), 1) if mems_used else None
        gpu_mem_total = round(sum(mems_total) / len(mems_total), 1) if mems_total else None

        import json as _j
        gpu_details = _j.dumps([
            {"index": str(g.get("index", i)), "name": "", "util": f"{g.get('util', 0)}%",
             "mem": f"{g.get('mem_used', 0)}/{g.get('mem_total', 0)} MiB"}
            for i, g in enumerate(gpus) if isinstance(g, dict)
        ])

        now = datetime.now().isoformat(timespec="seconds")
        with db_write() as con:
            con.execute(
                """INSERT INTO job_stats_snapshots
                   (cluster, job_id, ts, gpu_util, gpu_mem_used, gpu_mem_total, cpu_util, rss_used, max_rss, gpu_details)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (cluster, job_id, now, gpu_util, gpu_mem_used, gpu_mem_total, "", None, None, gpu_details),
            )
    except Exception:
        pass


def _ingest_job_state(run_uuid, payload):
    """Handle job_state events from the in-container exit-status wrapper.

    Updates the real Slurm job and SDK synthetic job to the terminal state,
    and finalizes the run if all jobs are done.
    """
    try:
        from .db import get_run_by_uuid, invalidate_pinned_cache
        from datetime import datetime

        state = payload.get("state", "")
        exit_code = payload.get("exit_code")
        slurm_job_id = payload.get("slurm_job_id", "")
        if not state:
            return

        run = get_run_by_uuid(run_uuid)
        if not run:
            return
        cluster = run.get("cluster", "")
        now = datetime.now().isoformat(timespec="seconds")

        with db_write() as con:
            if slurm_job_id and slurm_job_id != "unknown":
                con.execute(
                    """UPDATE job_history SET
                        state = ?, exit_code = ?, ended_at = COALESCE(ended_at, ?)
                       WHERE cluster = ? AND job_id = ?""",
                    (state, str(exit_code) if exit_code is not None else None, now, cluster, slurm_job_id),
                )

            synthetic_job_id = f"sdk-{run_uuid[:12]}"
            con.execute(
                """UPDATE job_history SET
                    state = CASE WHEN state IN ('SUBMITTING','PENDING','RUNNING') THEN ? ELSE state END,
                    exit_code = COALESCE(exit_code, ?),
                    ended_at = COALESCE(ended_at, ?)
                   WHERE cluster = ? AND job_id = ?""",
                (state, str(exit_code) if exit_code is not None else None, now, cluster, synthetic_job_id),
            )

            sdk_status = "completed" if state == "COMPLETED" else "failed"
            con.execute(
                "UPDATE runs SET sdk_status = ?, ended_at = COALESCE(ended_at, ?) WHERE run_uuid = ? AND sdk_status NOT IN ('completed', 'failed')",
                (sdk_status, now, run_uuid),
            )

        invalidate_pinned_cache(cluster)
    except Exception:
        pass
