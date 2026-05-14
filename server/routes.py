"""Flask route handlers as a Blueprint."""

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from flask import Blueprint, g, jsonify, request, render_template, make_response

_log = logging.getLogger(__name__)
_SLOW_REQUEST_MS = 2000
_shared_pool = ThreadPoolExecutor(max_workers=4)
_DEBUG_LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".cursor", "debug-41bcda.log")
_DEBUG_SESSION_ID = "41bcda"

from .config import (
    CLUSTERS, DEFAULT_USER, TERMINAL_STATES, RESULT_DIR_NAMES,
    _cache_lock, _cache,
    _cache_get, _cache_set,
    _log_content_cache, _dir_list_cache, _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    _team_usage_cache,
    LOG_CONTENT_TTL_SEC, DIR_LIST_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC, EST_START_TTL_SEC,
    TEAM_USAGE_TTL_SEC,
    settings_response, invalidate_log_index,
    get_project_color, get_project_emoji, extract_project, extract_campaign,
)
from .db import (
    dismiss_job, dismiss_by_state_prefix,
    get_history, get_projects, get_db, db_write,
    cache_db_get, cache_db_get_stale, cache_db_get_all, cache_db_get_all_multi,
    cache_db_put,
    get_custom_log_dir, set_custom_log_dir, set_custom_log_dir_bulk,
    get_custom_metrics_config, set_custom_metrics_config, set_custom_metrics_config_bulk,
    get_jobs_in_run, get_run_id_for_job,
)
from .ssh import (
    ssh_run, ssh_run_with_timeout, ssh_run_data, ssh_run_data_with_timeout,
    get_circuit_breaker_status, reset_circuit_breaker, cancel_jobs_with_report,
    enable_standalone_ssh,
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
    extract_custom_metrics, normalize_metrics_config,
)
from .jobs import (
    schedule_prefetch,
    get_job_stats_cached,
    create_run_on_demand,
    fetch_team_jobs,
    fetch_team_usage,
)
from .poller import get_poller, get_version, bump_version, touch_demand
from .db import (
    get_run_by_hash, get_run_hash, get_run_with_jobs, update_run_fields,
    resolve_run_hash_prefix,
    list_metrics_views, get_metrics_view, create_metrics_view,
    update_metrics_view, delete_metrics_view,
    find_sdk_run_uuid_by_output_dir,
)
from .board import build_board_snapshot, build_cluster_board_entry, _fill_output_dirs

api = Blueprint("api", __name__)


def _path_is_under(path, root):
    if not path or not root:
        return False
    root = root.rstrip("/")
    return path == root or path.startswith(root + "/")


def _run_root_from_path(path, known_root=""):
    """Infer the run directory that should anchor the explorer tree."""
    if known_root and _path_is_under(path, known_root):
        return known_root.rstrip("/")
    parts = path.split("/")
    result_dirs = set(RESULT_DIR_NAMES) | {"logs", "output"}
    for idx, part in enumerate(parts):
        if part in result_dirs and idx > 0:
            root = "/".join(parts[:idx])
            return root or "/"
    parent = os.path.dirname(path.rstrip("/"))
    return parent or path


def _search_file_paths_for_spotlight(q, limit=8):
    """Return Spotlight results that can open directly in the file explorer."""
    query = (q or "").strip()
    if not query:
        return []
    ql = query.lower()
    query_looks_path = query.startswith(("/", "~")) or "/" in query

    con = get_db()
    rows = con.execute(
        """SELECT jh.cluster, jh.job_id, jh.job_name, jh.state, jh.log_path,
                  r.root_job_id, r.run_name, r.run_uuid, r.primary_output_dir, r.project
           FROM job_history jh
           LEFT JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster
           WHERE COALESCE(jh.log_path, '') != ''
              OR COALESCE(r.primary_output_dir, '') != ''
           ORDER BY COALESCE(jh.started, jh.submitted, jh.ended_at, '') DESC
           LIMIT 2000"""
    ).fetchall()
    con.close()

    out = []
    seen = set()

    def add_result(row, path, root_dir, source):
        if not path:
            return
        key = (row["cluster"], path)
        if key in seen or len(out) >= limit:
            return
        seen.add(key)
        root_job_id = row["root_job_id"] or row["job_id"]
        run_hash = get_run_hash(row["cluster"], root_job_id, row["run_uuid"] or "")
        out.append({
            "cluster": row["cluster"],
            "job_id": row["job_id"],
            "job_name": row["job_name"] or "",
            "state": row["state"] or "",
            "path": path,
            "filename": os.path.basename(path.rstrip("/")) or path,
            "root_dir": root_dir or _run_root_from_path(path),
            "run_name": row["run_name"] or row["job_name"] or "",
            "run_hash": run_hash,
            "project": row["project"] or "",
            "source": source,
        })

    for row in rows:
        log_path = row["log_path"] or ""
        primary_output_dir = (row["primary_output_dir"] or "").rstrip("/")
        log_root = _run_root_from_path(log_path, primary_output_dir) if log_path else primary_output_dir

        if log_path and ql in log_path.lower():
            add_result(row, log_path, log_root, "known log")

        if query_looks_path and primary_output_dir and _path_is_under(query, primary_output_dir):
            add_result(row, query, primary_output_dir, "run path")
        elif query_looks_path and log_root and _path_is_under(query, log_root):
            add_result(row, query, log_root, "run path")

        if len(out) >= limit:
            return out

    if query_looks_path and query.startswith("/"):
        for cluster, cfg in CLUSTERS.items():
            if len(out) >= limit:
                break
            if cluster == "local" or not cfg.get("enabled", 1):
                continue
            remote_bases = [
                str(p).replace("$USER", DEFAULT_USER).rstrip("/")
                for p in (cfg.get("mount_paths") or [])
                if p
            ]
            if remote_bases and not any(_path_is_under(query, base) for base in remote_bases):
                continue
            key = (cluster, query)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "cluster": cluster,
                "job_id": "__dir__",
                "job_name": "",
                "state": "",
                "path": query,
                "filename": os.path.basename(query.rstrip("/")) or query,
                "root_dir": _run_root_from_path(query),
                "run_name": "",
                "run_hash": "",
                "project": "",
                "source": "direct path",
            })

    return out


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
@api.route("/live")
@api.route("/runs")
@api.route("/history")
@api.route("/metrics")
@api.route("/compute")
@api.route("/project/<path:_subpath>")
@api.route("/logbook")
@api.route("/logbook/<path:_subpath>")
@api.route("/run/<path:_subpath>")
@api.route("/explorer/<path:_subpath>")
def index(_subpath=None):
    from .settings import get_custom_metrics_enabled, get_team_name
    resp = make_response(render_template(
        "index.html", clusters=CLUSTERS, username=DEFAULT_USER,
        team=get_team_name(),
        custom_metrics_enabled=get_custom_metrics_enabled(),
    ))
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
    bump_version()
    return jsonify({"status": "ok"})


@api.route("/api/clear_cancelled/<cluster>", methods=["POST"])
def api_clear_cancelled(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, ["CANCELLED", "COMPLETING"])
    bump_version()
    return jsonify({"status": "ok"})


@api.route("/api/clear_completed/<cluster>", methods=["POST"])
def api_clear_completed(cluster):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_by_state_prefix(cluster, ["COMPLETED"])
    bump_version()
    return jsonify({"status": "ok"})


@api.route("/api/clear_failed_job/<cluster>/<job_id>", methods=["POST"])
def api_clear_failed_job(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    dismiss_job(cluster, job_id)
    bump_version()
    return jsonify({"status": "ok"})


def _get_run_sibling_job_ids(cluster, job_id):
    run_id = get_run_id_for_job(cluster, job_id)
    if not run_id:
        return []
    return [
        jid
        for jid in (job.get("job_id", "") for job in get_jobs_in_run(cluster, run_id))
        if jid and jid != str(job_id)
    ]


def _propagate_to_run(cluster, job_id, setter_fn, value, *, bulk_setter_fn=None, invalidate_logs=False):
    sibling_ids = _get_run_sibling_job_ids(cluster, job_id)
    if not sibling_ids:
        return 0
    if bulk_setter_fn:
        bulk_setter_fn(cluster, sibling_ids, value)
    else:
        for sibling_id in sibling_ids:
            setter_fn(cluster, sibling_id, value)
    if invalidate_logs:
        for sibling_id in sibling_ids:
            invalidate_log_index(cluster, sibling_id)
    return len(sibling_ids)


def _normalize_metrics_config(cfg):
    normalized, _, error = normalize_metrics_config(cfg)
    return normalized, error


def _load_metrics_config(raw):
    if not raw:
        return {}, None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None, "Invalid metrics config JSON"
    return _normalize_metrics_config(parsed)


@api.route("/api/custom_log_dir/<cluster>/<job_id>", methods=["GET", "POST"])
def api_custom_log_dir(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if request.method == "GET":
        return jsonify({"status": "ok", "custom_log_dir": get_custom_log_dir(cluster, job_id)})
    payload = request.get_json(silent=True) or {}
    path = payload.get("path", "").strip()
    set_custom_log_dir(cluster, job_id, path)
    invalidate_log_index(cluster, job_id)
    siblings = _propagate_to_run(
        cluster,
        job_id,
        set_custom_log_dir,
        path,
        bulk_setter_fn=set_custom_log_dir_bulk,
        invalidate_logs=True,
    )
    return jsonify({"status": "ok", "custom_log_dir": path, "applied_to_run": siblings})


@api.route("/api/custom_metrics_config/<cluster>/<job_id>", methods=["GET", "POST"])
def api_custom_metrics_config(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if request.method == "GET":
        raw = get_custom_metrics_config(cluster, job_id)
        cfg, error = _load_metrics_config(raw)
        if error:
            return jsonify({"status": "error", "error": error, "config": {}})
        return jsonify({"status": "ok", "config": cfg})
    payload = request.get_json(silent=True) or {}
    cfg, error = _normalize_metrics_config(payload.get("config", {}))
    if error:
        return jsonify({"status": "error", "error": error}), 400
    cfg_str = json.dumps(cfg) if cfg else ""
    set_custom_metrics_config(cluster, job_id, cfg_str)
    siblings = _propagate_to_run(
        cluster,
        job_id,
        set_custom_metrics_config,
        cfg_str,
        bulk_setter_fn=set_custom_metrics_config_bulk,
    )
    return jsonify({"status": "ok", "config": cfg, "applied_to_run": siblings})


@api.route("/api/custom_metrics_config/<cluster>/<job_id>/apply_to_run", methods=["POST"])
def api_custom_metrics_apply_to_run(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run_id = get_run_id_for_job(cluster, job_id)
    if not run_id:
        return jsonify({"status": "error", "error": "Job is not part of a run"}), 400
    src_cfg = get_custom_metrics_config(cluster, job_id)
    if not src_cfg:
        return jsonify({"status": "error", "error": "No metrics config on this job"}), 400
    cfg, error = _load_metrics_config(src_cfg)
    if error:
        return jsonify({"status": "error", "error": error}), 400
    cfg_str = json.dumps(cfg) if cfg else ""
    sibling_ids = _get_run_sibling_job_ids(cluster, job_id)
    if sibling_ids:
        set_custom_metrics_config_bulk(cluster, sibling_ids, cfg_str)
    return jsonify({"status": "ok", "applied_to": len(sibling_ids)})


@api.route("/api/copy_metrics_config/<cluster>/<job_id>", methods=["POST"])
def api_copy_metrics_config(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    payload = request.get_json(silent=True) or {}
    src_cluster = payload.get("src_cluster", cluster)
    if src_cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown source cluster"}), 400
    src_job_id = str(payload.get("src_job_id", "")).strip()
    if not src_job_id:
        return jsonify({"status": "error", "error": "No src_job_id provided"}), 400

    src_log_dir = get_custom_log_dir(src_cluster, src_job_id)
    src_metrics = get_custom_metrics_config(src_cluster, src_job_id)
    if not src_log_dir and not src_metrics:
        return jsonify({"status": "error", "error": f"No custom config found on source job {src_cluster}/{src_job_id}"}), 400

    copied = []
    cfg = {}
    if src_metrics:
        cfg, error = _load_metrics_config(src_metrics)
        if error:
            return jsonify({"status": "error", "error": f"Invalid metrics config on source job {src_cluster}/{src_job_id}: {error}"}), 400
        src_metrics = json.dumps(cfg) if cfg else ""
        set_custom_metrics_config(cluster, job_id, src_metrics)
        _propagate_to_run(
            cluster,
            job_id,
            set_custom_metrics_config,
            src_metrics,
            bulk_setter_fn=set_custom_metrics_config_bulk,
        )
        copied.append("metrics_config")
    if src_log_dir:
        set_custom_log_dir(cluster, job_id, src_log_dir)
        invalidate_log_index(cluster, job_id)
        _propagate_to_run(
            cluster,
            job_id,
            set_custom_log_dir,
            src_log_dir,
            bulk_setter_fn=set_custom_log_dir_bulk,
            invalidate_logs=True,
        )
        copied.append("custom_log_dir")

    return jsonify({
        "status": "ok",
        "copied": copied,
        "custom_log_dir": src_log_dir or "",
        "config": cfg,
    })


@api.route("/api/custom_metrics/<cluster>/<job_id>")
def api_custom_metrics(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    return jsonify(extract_custom_metrics(cluster, job_id))


@api.route("/api/custom_metrics_run/<cluster>/<root_job_id>")
def api_custom_metrics_run(cluster, root_job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404

    from .db import get_run

    run = get_run(cluster, str(root_job_id))
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404

    jobs = get_jobs_in_run(cluster, run["id"])
    if not jobs:
        return jsonify({"status": "ok", "jobs": [], "aggregates": []})

    def _extract_job_metrics(job):
        enable_standalone_ssh()
        jid = job.get("job_id", "")
        result = extract_custom_metrics(cluster, jid)
        return {
            "job_id": jid,
            "job_name": job.get("job_name", ""),
            "state": job.get("state", ""),
            "metrics": result.get("metrics", []),
            "error": result.get("error", ""),
        }

    max_workers = min(6, max(1, len(jobs)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        per_job = list(pool.map(_extract_job_metrics, jobs))

    has_any = any(job["metrics"] for job in per_job)
    aggregates = []
    if has_any:
        metric_names = []
        seen = set()
        for job in per_job:
            for metric in job["metrics"]:
                if metric["name"] not in seen:
                    seen.add(metric["name"])
                    metric_names.append(metric["name"])

        for name in metric_names:
            nums = []
            for job in per_job:
                value = next((metric["value"] for metric in job["metrics"] if metric["name"] == name), None)
                if value is None:
                    continue
                try:
                    nums.append(float(value))
                except (ValueError, TypeError):
                    pass
            if nums:
                aggregates.append({
                    "name": name,
                    "avg": round(sum(nums) / len(nums), 2),
                    "min": min(nums),
                    "max": max(nums),
                    "sum": sum(nums),
                    "count": len(nums),
                })

    return jsonify({"status": "ok", "jobs": per_job, "aggregates": aggregates})


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
    skipped = []
    if force:
        # Fan out fresh fetches across clusters. Sequentially this loop
        # was dominated by the slowest single cluster's SSH (observed
        # 2-4 s for /api/team_jobs in production logs); parallel keeps
        # us bounded by that slowest cluster instead of summing them.
        # Clusters whose SSH circuit breaker is open are skipped so a
        # known-broken cluster doesn't cost the request a guaranteed
        # SSH timeout.
        from .ssh import is_cluster_reachable
        all_targets = [c for c in cluster_list if c in CLUSTERS and c != "local"]
        targets = [c for c in all_targets if is_cluster_reachable(c)]
        skipped = [c for c in all_targets if c not in targets]
        if targets:
            with ThreadPoolExecutor(max_workers=min(8, len(targets))) as pool:
                futs = {pool.submit(fetch_team_jobs, c): c for c in targets}
                for fut in as_completed(futs):
                    c = futs[fut]
                    try:
                        fetched = fut.result()
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

    return jsonify({"status": "ok", "clusters": results, "skipped_clusters": skipped})


@api.route("/api/cancel/<cluster>/<job_id>", methods=["POST"])
def api_cancel(cluster, job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    if str(job_id).startswith("sdk-"):
        from .db import cancel_sdk_job
        cancel_sdk_job(str(job_id))
        bump_version()
        return jsonify({"status": "ok", "note": "SDK run cancelled"})
    try:
        if cluster == "local":
            os.kill(int(job_id), 15)
            bump_version()
            return jsonify({"status": "ok"})
        result = cancel_jobs_with_report(cluster, [job_id], timeout_sec=10, chunk_size=1)
        if result["cancelled_ids"]:
            bump_version()
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
        for sid in sdk_ids:
            cancel_sdk_job(sid)
            sdk_cancelled += 1
        bump_version()

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
            bump_version()
            if errors:
                return jsonify({"status": "partial", "cancelled": len(sanitized) + sdk_cancelled - len(errors), "errors": errors})
            return jsonify({"status": "ok", "cancelled": len(sanitized) + sdk_cancelled})

        result = cancel_jobs_with_report(cluster, sanitized, timeout_sec=20, chunk_size=25)
        cancelled = len(result["cancelled_ids"]) + sdk_cancelled
        if result["cancelled_ids"]:
            bump_version()
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


def _inherit_sdk_by_run_uuid(run):
    """Hydrate a SLURM-detected child row with provenance from its SDK
    orchestrator sibling (matched by shared run_uuid).

    The SDK propagates ``CLAUSIUS_RUN_UUID`` to spawned NeMo-Run jobs; the
    poller records that UUID on the SLURM-side run row via
    ``_capture_run_metadata`` -> ``update_run_meta(sdk_run_uuid=...)``. Both
    rows therefore share a UUID, and SDK-side metrics already resolve via
    that UUID. This helper completes the story for params/metadata, which
    live as JSON columns on the orchestrator row, not in the metrics tables.
    """
    run_uuid = (run.get("run_uuid") or "").strip()
    if not run_uuid:
        return
    if run.get("source") == "sdk" and (run.get("params_json") or run.get("metadata_json")):
        return
    try:
        from .db import get_db
        con = get_db()
        sibling = con.execute(
            """SELECT submit_command, submit_cwd, git_commit, launcher_hostname,
                      primary_output_dir, params_json, metadata_json
               FROM runs
               WHERE run_uuid=? AND id != ? AND source='sdk'
               ORDER BY CASE WHEN submit_command != '' THEN 0 ELSE 1 END, id DESC
               LIMIT 1""",
            (run_uuid, run.get("id") or 0),
        ).fetchone()
        con.close()
    except Exception:
        return
    if not sibling:
        return
    for field in (
        "submit_command", "submit_cwd", "git_commit",
        "launcher_hostname", "primary_output_dir",
        "params_json", "metadata_json",
    ):
        if not run.get(field) and sibling[field]:
            run[field] = sibling[field]
    if not run.get("source") or run["source"] == "legacy":
        run["source"] = "sdk+legacy"


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


def _load_run_by_ref(cluster, run_ref, *, allow_on_demand=True):
    run = get_run_with_jobs(cluster, run_ref)
    if not run and allow_on_demand:
        actual_root = create_run_on_demand(cluster, run_ref)
        if actual_root:
            run = get_run_with_jobs(cluster, actual_root)
    if not run:
        run = _resolve_run_via_job(cluster, run_ref)
    if not run:
        run_row = get_run_by_hash(cluster, run_ref)
        if run_row:
            run = get_run_with_jobs(cluster, run_row["root_job_id"])
    return run


def _run_info_response(cluster, run_ref, *, allow_on_demand=True):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run = _load_run_by_ref(cluster, run_ref, allow_on_demand=allow_on_demand)
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
    run["run_hash"] = get_run_hash(cluster, run.get("root_job_id", ""), run.get("run_uuid", ""))

    # When a SLURM-detected child row has been linked to an SDK orchestrator
    # via CLAUSIUS_RUN_UUID, hydrate the orchestrator's provenance/params/
    # metadata into this response so the popup shows a unified view. The
    # orchestrator typically lives on a sibling cluster (e.g.
    # `aws-dfw-science`) and carries the actual `set_metadata()` data.
    _inherit_sdk_by_run_uuid(run)

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

    raw_meta = run.pop("metadata_json", "") or ""
    if raw_meta:
        try:
            run["metadata"] = json.loads(raw_meta)
        except (ValueError, TypeError):
            run["metadata"] = {}
    else:
        run["metadata"] = {}

    run["malfunctioned"] = bool(int(run.get("malfunctioned") or 0))

    from .resubmit import eligibility as _resubmit_eligibility
    _can_resubmit, _resubmit_reason = _resubmit_eligibility(run)
    run["can_resubmit"] = _can_resubmit
    if not _can_resubmit:
        run["resubmit_blocked_reason"] = _resubmit_reason

    return jsonify({"status": "ok", "run": run})


@api.route("/api/run_info/<cluster>/<root_job_id>")
def api_run_info(cluster, root_job_id):
    return _run_info_response(cluster, root_job_id)


@api.route("/api/run_info_by_hash/<cluster>/<run_hash>")
def api_run_info_by_hash(cluster, run_hash):
    return _run_info_response(cluster, run_hash, allow_on_demand=False)


@api.route("/api/run_metrics/<cluster>/<root_job_id>")
def api_run_metrics(cluster, root_job_id):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run = _load_run_by_ref(cluster, root_job_id)
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404
    run_uuid = run.get("run_uuid") or ""
    if not run_uuid:
        return jsonify({"status": "ok", "metadata": {}, "series": {}, "latest": {}})
    from .db import get_run_metrics
    payload = get_run_metrics(run_uuid)
    return jsonify({"status": "ok", **payload})


@api.route("/api/run_metrics_by_hash/<cluster>/<run_hash>")
def api_run_metrics_by_hash(cluster, run_hash):
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run = _load_run_by_ref(cluster, run_hash, allow_on_demand=False)
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404
    run_uuid = run.get("run_uuid") or ""
    if not run_uuid:
        return jsonify({"status": "ok", "metadata": {}, "series": {}, "latest": {}})
    from .db import get_run_metrics
    payload = get_run_metrics(run_uuid)
    return jsonify({"status": "ok", **payload})


_RUN_RESULTS_MAX_METRICS_FILES = 12
_RUN_RESULTS_MAX_BYTES = 2 * 1024 * 1024
_RUN_RESULTS_STDOUT_RE = re.compile(r"(?:^|\s)StdOut=(\S+)", re.MULTILINE)


def _normalize_output_root(path, cwd=""):
    path = str(path or "").strip().rstrip("/")
    if not path:
        return ""
    if not path.startswith("/") and cwd:
        path = os.path.normpath(os.path.join(cwd, path))
    return path.rstrip("/")


def _run_result_root_from_log_path(log_path):
    log_path = str(log_path or "").strip().rstrip("/")
    if not log_path:
        return ""
    marker = "/eval-results/"
    if marker in log_path:
        return log_path.split(marker, 1)[0].rstrip("/")
    log_dir = os.path.dirname(log_path)
    root = os.path.dirname(log_dir)
    return root.rstrip("/") if root and root != log_dir else ""


def _shell_tokens(text):
    try:
        return shlex.split(str(text or ""))
    except ValueError:
        return str(text or "").split()


def _submit_command_cwd(command):
    for line in str(command or "").splitlines():
        tokens = _shell_tokens(line)
        if len(tokens) >= 2 and tokens[0] == "cd":
            return tokens[1]
    return ""


def _normalized_output_dir_key(key):
    return str(key or "").lstrip("-+").replace("-", "_").lower()


def _output_roots_from_command(command, cwd=""):
    command = str(command or "")
    effective_cwd = cwd or _submit_command_cwd(command)
    roots = []
    tokens = _shell_tokens(command)
    for idx, token in enumerate(tokens):
        if "=" in token:
            key, value = token.split("=", 1)
            if _normalized_output_dir_key(key) == "output_dir":
                roots.append(_normalize_output_root(value, effective_cwd))
            continue
        if _normalized_output_dir_key(token) != "output_dir":
            continue
        if idx + 1 >= len(tokens):
            continue
        value = tokens[idx + 1]
        if value.startswith("-"):
            continue
        roots.append(_normalize_output_root(value, effective_cwd))
    return [root for root in roots if root]


def _output_roots_from_params(value, cwd=""):
    roots = []
    if isinstance(value, dict):
        for key, child in value.items():
            if _normalized_output_dir_key(key) == "output_dir":
                if isinstance(child, str):
                    roots.append(_normalize_output_root(child, cwd))
                elif isinstance(child, (list, tuple)):
                    for item in child:
                        if isinstance(item, str):
                            roots.append(_normalize_output_root(item, cwd))
            if isinstance(child, (dict, list, tuple)):
                roots.extend(_output_roots_from_params(child, cwd=cwd))
    elif isinstance(value, (list, tuple)):
        for child in value:
            if isinstance(child, (dict, list, tuple)):
                roots.extend(_output_roots_from_params(child, cwd=cwd))
    return [root for root in roots if root]


def _output_roots_from_scontrol(raw):
    roots = []
    for match in _RUN_RESULTS_STDOUT_RE.finditer(str(raw or "").replace("\n", " ")):
        root = _run_result_root_from_log_path(match.group(1))
        if root:
            roots.append(root)
    return roots


def _sdk_event_output_roots(run):
    run_uuid = run.get("run_uuid") or ""
    if not run_uuid:
        return []
    try:
        from .db import get_run_uuid_family
        uuids = get_run_uuid_family(run_uuid) or [run_uuid]
        placeholders = ",".join("?" for _ in uuids)
        con = get_db()
        rows = con.execute(
            f"""SELECT payload_json FROM sdk_events
                WHERE run_uuid IN ({placeholders}) AND event_type='run_started'
                ORDER BY id DESC
                LIMIT 8""",
            uuids,
        ).fetchall()
        con.close()
    except Exception:
        return []

    roots = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, ValueError):
            continue
        cwd = payload.get("cwd") or run.get("submit_cwd") or ""
        roots.append(_normalize_output_root(payload.get("output_dir", ""), cwd=cwd))
        roots.extend(_output_roots_from_command(payload.get("command", ""), cwd=cwd))
        roots.extend(_output_roots_from_params(payload.get("params", {}), cwd=cwd))
    return [root for root in roots if root]


def _job_history_output_roots(cluster, run):
    clauses = []
    params = [cluster]
    run_id = run.get("id")
    if run_id:
        clauses.append("run_id=?")
        params.append(run_id)
    run_name = str(run.get("run_name") or "").strip()
    if len(run_name) >= 3:
        clauses.append("instr(COALESCE(job_name, ''), ?) > 0")
        params.append(run_name)
    if not cluster or not clauses:
        return []
    try:
        con = get_db()
        rows = con.execute(
            f"""SELECT log_path FROM job_history
                WHERE cluster=?
                  AND ({' OR '.join(clauses)})
                  AND COALESCE(log_path, '') != ''
                ORDER BY id DESC
                LIMIT 20""",
            params,
        ).fetchall()
        con.close()
    except Exception:
        return []
    roots = []
    for row in rows:
        root = _run_result_root_from_log_path(row["log_path"])
        if root:
            roots.append(root)
    return roots


def _run_result_roots(run, cluster=""):
    roots = []

    def add(path, cwd=""):
        path = _normalize_output_root(path, cwd=cwd)
        if path and path not in roots:
            roots.append(path)

    add(run.get("primary_output_dir", ""))
    add(run.get("output_dir", ""))
    add(run.get("submit_output_dir", ""))
    submit_cwd = run.get("submit_cwd", "")
    for root in _output_roots_from_command(run.get("submit_command", ""), cwd=submit_cwd):
        add(root)
    raw_params = run.get("params_json", "") or ""
    if raw_params:
        try:
            for root in _output_roots_from_params(json.loads(raw_params), cwd=submit_cwd):
                add(root)
        except (TypeError, ValueError):
            pass
    for job in run.get("jobs", []) or []:
        add(job.get("output_dir", ""))
        add(_run_result_root_from_log_path(job.get("log_path", "")))
        for root in _output_roots_from_scontrol(job.get("scontrol_raw", "")):
            add(root)
    for root in _output_roots_from_scontrol(run.get("scontrol_raw", "")):
        add(root)
    for root in _sdk_event_output_roots(run):
        add(root)
    for root in _job_history_output_roots(cluster, run):
        add(root)

    expanded = []
    for root in roots:
        expanded.append(root)
        marker = "/eval-results/"
        if marker in root:
            expanded.append(root.split(marker, 1)[0])
    deduped = []
    for root in expanded:
        if root and root not in deduped:
            deduped.append(root)
    return deduped


def _append_existing_metrics_file(matches, path):
    if os.path.isfile(path):
        matches.append({"path": path, "size_bytes": os.path.getsize(path)})


def _local_metrics_files(root, benchmark=""):
    matches = []
    root = root.rstrip("/")
    if not root or not os.path.isdir(root):
        return matches

    # NeMo-Skills writes final benchmark metrics only after summarize_results:
    #   <output_dir>/eval-results/<benchmark>/metrics.json
    # Some job records already point directly at eval-results/<benchmark>, in
    # which case metrics.json is directly under the recorded output_dir.
    _append_existing_metrics_file(matches, os.path.join(root, "metrics.json"))
    eval_root = root if os.path.basename(root) == "eval-results" else os.path.join(root, "eval-results")
    if benchmark:
        _append_existing_metrics_file(matches, os.path.join(eval_root, benchmark, "metrics.json"))
        return matches

    if os.path.isdir(eval_root):
        for name in sorted(os.listdir(eval_root)):
            _append_existing_metrics_file(matches, os.path.join(eval_root, name, "metrics.json"))
            if len(matches) >= _RUN_RESULTS_MAX_METRICS_FILES:
                break
    return matches


def _find_run_metrics_files(cluster, roots, benchmark=""):
    seen = set()
    matches = []
    remote_roots = []
    for root in roots:
        mounted = root if cluster == "local" else resolve_mounted_path(cluster, root, want_dir=True)
        if mounted:
            for item in _local_metrics_files(mounted, benchmark):
                rel_path = item["path"]
                if cluster != "local" and item["path"].startswith(mounted.rstrip("/") + "/"):
                    rel_path = root.rstrip("/") + item["path"][len(mounted.rstrip("/")):]
                if rel_path not in seen:
                    seen.add(rel_path)
                    matches.append({**item, "path": rel_path, "source": "mount" if cluster != "local" else "local"})
        else:
            remote_roots.append(root)
        if len(matches) >= _RUN_RESULTS_MAX_METRICS_FILES:
            return matches[:_RUN_RESULTS_MAX_METRICS_FILES]

    if remote_roots and cluster != "local":
        script = f"""python3 - <<'PY'
import json, os
roots = {json.dumps(remote_roots)}
benchmark = {json.dumps(benchmark or "")}
limit = {_RUN_RESULTS_MAX_METRICS_FILES}
matches = []
seen = set()

def add(path):
    if path in seen or not os.path.isfile(path):
        return
    seen.add(path)
    try:
        size = os.path.getsize(path)
    except OSError:
        size = None
    matches.append({{"path": path, "size_bytes": size, "source": "ssh"}})

for root in roots:
    root = root.rstrip("/")
    if not root or not os.path.isdir(root):
        continue
    add(os.path.join(root, "metrics.json"))
    eval_root = root if os.path.basename(root) == "eval-results" else os.path.join(root, "eval-results")
    if benchmark:
        add(os.path.join(eval_root, benchmark, "metrics.json"))
    elif os.path.isdir(eval_root):
        for name in sorted(os.listdir(eval_root)):
            add(os.path.join(eval_root, name, "metrics.json"))
            if len(matches) >= limit:
                break
    if len(matches) >= limit:
        break
print(json.dumps(matches))
PY"""
        out, _ = ssh_run_data_with_timeout(cluster, script, timeout_sec=30)
        try:
            for item in json.loads(out or "[]"):
                path = item.get("path", "")
                if path and path not in seen:
                    seen.add(path)
                    matches.append(item)
        except Exception:
            _log.warning("failed to parse remote metrics discovery for %s", cluster)
    return matches[:_RUN_RESULTS_MAX_METRICS_FILES]


def _read_run_metrics_file(cluster, path):
    local_path = path if cluster == "local" and os.path.isfile(path) else None
    source = "local"
    if cluster != "local":
        mounted = resolve_mounted_path(cluster, path, want_dir=False)
        if mounted:
            local_path = mounted
            source = "mount"
        else:
            source = "ssh"
    if local_path:
        with open(local_path, "rb") as fh:
            data = fh.read(_RUN_RESULTS_MAX_BYTES + 1)
        truncated = len(data) > _RUN_RESULTS_MAX_BYTES
        return data[:_RUN_RESULTS_MAX_BYTES].decode("utf-8", errors="replace"), source, truncated

    script = f"""python3 - <<'PY'
import pathlib
path = {json.dumps(path)}
limit = {_RUN_RESULTS_MAX_BYTES}
data = pathlib.Path(path).read_bytes()
print(data[:limit].decode("utf-8", errors="replace"), end="")
PY"""
    out, _ = ssh_run_data_with_timeout(cluster, script, timeout_sec=15)
    return out, source, False


@api.route("/api/run_results_by_hash/<cluster>/<run_hash>")
def api_run_results_by_hash(cluster, run_hash):
    """Return metrics.json file path(s) and content for a logical run."""
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    benchmark = request.args.get("benchmark", "").strip()
    run = _load_run_by_ref(cluster, run_hash, allow_on_demand=False)
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404

    roots = _run_result_roots(run, cluster=cluster)
    files = _find_run_metrics_files(cluster, roots, benchmark=benchmark)
    if not files:
        return jsonify({
            "status": "incomplete",
            "complete": False,
            "error": "No metrics.json found; NeMo-Skills run is not complete until summarize_results writes it",
            "cluster": cluster,
            "run_hash": run_hash,
            "run_name": run.get("run_name", ""),
            "searched_roots": roots,
            "benchmark": benchmark,
            "metrics_path": "",
            "metrics_json_content": "",
            "metrics": None,
            "metrics_files": [],
        })

    metrics_files = []
    for item in files:
        path = item.get("path", "")
        try:
            content, source, truncated = _read_run_metrics_file(cluster, path)
            parsed = json.loads(content) if content and not truncated else None
            metrics_files.append({
                "path": path,
                "content": content,
                "json": parsed,
                "source": source,
                "size_bytes": item.get("size_bytes"),
                "truncated": truncated,
            })
        except Exception as exc:
            metrics_files.append({
                "path": path,
                "content": "",
                "json": None,
                "source": item.get("source", ""),
                "size_bytes": item.get("size_bytes"),
                "truncated": False,
                "error": str(exc),
            })

    primary = metrics_files[0]
    return jsonify({
        "status": "ok",
        "complete": True,
        "cluster": cluster,
        "run_hash": run.get("run_hash") or get_run_hash(cluster, run.get("root_job_id", ""), run.get("run_uuid", "")),
        "run_name": run.get("run_name", ""),
        "benchmark": benchmark,
        "metrics_path": primary.get("path", ""),
        "metrics_json_content": primary.get("content", ""),
        "metrics": primary.get("json"),
        "metrics_files": metrics_files,
        "searched_roots": roots,
    })


@api.route("/api/resolve_run_hash/<run_hash>")
def api_resolve_run_hash(run_hash):
    cluster = request.args.get("cluster", "").strip()
    if cluster and cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    result = resolve_run_hash_prefix(run_hash, cluster=cluster)
    if result["status"] == "ok":
        run = result["run"]
        return jsonify({
            "status": "ok",
            "cluster": run.get("cluster", ""),
            "root_job_id": run.get("root_job_id", ""),
            "run_hash": run.get("run_hash", ""),
            "run_name": run.get("run_name", ""),
            "project": run.get("project", ""),
        })
    matches = [{
        "cluster": r.get("cluster", ""),
        "root_job_id": r.get("root_job_id", ""),
        "run_hash": r.get("run_hash", ""),
        "run_name": r.get("run_name", ""),
        "project": r.get("project", ""),
    } for r in result.get("matches", [])[:20]]
    if result["status"] == "ambiguous":
        return jsonify({"status": "error", "error": "Ambiguous run hash prefix", "matches": matches}), 409
    return jsonify({"status": "error", "error": "Run not found", "matches": []}), 404


@api.route("/api/runs_by_name")
def api_runs_by_name():
    """Find runs by run_name substring/prefix/suffix.

    Used by the Metrics Explorer to auto-discover runs from an AimQL query
    like ``run.name.contains("mcp_mcpv2lt")`` so users don't have to add
    each run by hash.

    Query params:
      - ``q``: search term (required, min 2 chars)
      - ``mode``: ``contains`` (default) | ``startswith`` | ``endswith`` | ``equals``
      - ``limit``: max results to return (default 50, max 200)
      - ``has_metrics``: ``1`` to restrict to runs that have at least one
        SDK metric or scalar logged (default off)
    """
    q = (request.args.get("q") or "").strip()
    if not q or len(q) < 2:
        return jsonify({"status": "ok", "runs": [], "total": 0})
    mode = (request.args.get("mode") or "contains").lower()
    if mode not in ("contains", "startswith", "endswith", "equals"):
        mode = "contains"
    try:
        limit = max(1, min(200, int(request.args.get("limit") or 50)))
    except ValueError:
        limit = 50
    has_metrics = (request.args.get("has_metrics") or "").lower() in ("1", "true", "yes")

    if mode == "startswith":
        like = f"{q}%"
    elif mode == "endswith":
        like = f"%{q}"
    elif mode == "equals":
        like = q
    else:
        like = f"%{q}%"

    con = get_db()
    try:
        sql = """SELECT r.cluster, r.root_job_id, r.run_name, r.run_uuid,
                        r.project, r.sdk_status, r.started_at, r.created_at
                 FROM runs r
                 WHERE r.run_name LIKE ? COLLATE NOCASE"""
        params = [like]
        if has_metrics:
            sql += """ AND (
                EXISTS (SELECT 1 FROM run_metrics m WHERE m.run_uuid = r.run_uuid LIMIT 1)
                OR EXISTS (SELECT 1 FROM run_scalars s WHERE s.run_uuid = r.run_uuid LIMIT 1)
            )"""
        sql += " ORDER BY COALESCE(r.started_at, r.created_at, '') DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()

    runs = []
    for r in rows:
        run_hash = get_run_hash(r["cluster"], r["root_job_id"], r["run_uuid"])
        if not run_hash:
            continue
        runs.append({
            "cluster": r["cluster"],
            "run_hash": run_hash,
            "run_name": r["run_name"] or "",
            "project": r["project"] or "",
            "sdk_status": r["sdk_status"] or "",
            "started_at": r["started_at"] or "",
        })
    return jsonify({"status": "ok", "runs": runs, "total": len(runs), "mode": mode})


@api.route("/api/sdk/resolve_run")
def api_sdk_resolve_run():
    """SDK resume-aware lookup. Called by the Clausius SDK's
    ``ClausiusSession.start_from_cli`` *before* it mints a fresh uuid so a
    repeat submission of the same expname (``ns eval ++skip_filled=True``
    resume, cluster-name drift, etc.) attaches to the existing run row
    instead of creating a duplicate.

    Query params:
      - ``output_dir`` (required): the run's primary_output_dir. This is the
        canonical experiment identifier — fresh-run-IDs ensure distinct
        output_dirs for distinct experiments.
      - ``run_name`` (optional): expname hint for disambiguation.
      - ``cluster`` (optional): cluster hint for disambiguation.

    Returns:
      - ``{"status": "ok", "exists": true,  "run_uuid": "<canonical>"}`` on match
      - ``{"status": "ok", "exists": false}`` when no SDK run covers this dir
    """
    output_dir = (request.args.get("output_dir") or "").strip()
    run_name = (request.args.get("run_name") or "").strip()
    cluster = (request.args.get("cluster") or "").strip()
    if not output_dir:
        return jsonify({"status": "ok", "exists": False})
    uuid = find_sdk_run_uuid_by_output_dir(output_dir, run_name=run_name, cluster=cluster)
    if uuid:
        return jsonify({"status": "ok", "exists": True, "run_uuid": uuid})
    return jsonify({"status": "ok", "exists": False})


# Catalog of (path → distinct values) used by the AimQL autocomplete to
# suggest values for `metric.name`, `metric.context.<k>`, `run.cluster`,
# `run.project`, `run.hparams.<k>`, etc. Results are sampled to stay fast
# even on large databases.
_METRIC_FIELD_VALUE_LIMIT = 200
_METRIC_FIELD_VALUE_SAMPLE = 10000


@api.route("/api/metric_field_values")
def api_metric_field_values():
    path = (request.args.get("path") or "").strip()
    q = (request.args.get("q") or "").strip().lower()
    try:
        limit = max(1, min(_METRIC_FIELD_VALUE_LIMIT, int(request.args.get("limit") or 100)))
    except ValueError:
        limit = 100
    if not path:
        return jsonify({"status": "ok", "path": path, "values": []})

    con = get_db()
    values = []
    try:
        if path in ("metric.name", "metric.key"):
            rows = con.execute("""
                SELECT key FROM (
                    SELECT DISTINCT key FROM run_metrics
                    UNION
                    SELECT DISTINCT key FROM run_scalars
                ) ORDER BY key
            """).fetchall()
            values = [r["key"] for r in rows]
        elif path == "metric.kind":
            values = ["series", "scalar"]
        elif path == "run.cluster":
            rows = con.execute(
                "SELECT DISTINCT cluster FROM runs WHERE cluster IS NOT NULL AND cluster != '' ORDER BY cluster"
            ).fetchall()
            values = [r["cluster"] for r in rows]
        elif path == "run.project":
            rows = con.execute(
                "SELECT DISTINCT project FROM runs WHERE project IS NOT NULL AND project != '' ORDER BY project"
            ).fetchall()
            values = [r["project"] for r in rows]
        elif path.startswith("metric.context."):
            ctx_key = path[len("metric.context."):]
            if not re.fullmatch(r"[A-Za-z0-9_\-]+", ctx_key):
                return jsonify({"status": "ok", "path": path, "values": []})
            # Sample distinct context values from both series and scalars.
            json_path = f"$.{ctx_key}"
            rows = con.execute(
                f"""
                SELECT DISTINCT json_extract(context_json, ?) AS v
                FROM (
                    SELECT context_json FROM run_metrics
                    WHERE context_json IS NOT NULL AND context_json != '' AND context_json != '{{}}'
                    LIMIT ?
                )
                WHERE v IS NOT NULL
                UNION
                SELECT DISTINCT json_extract(context_json, ?) AS v
                FROM (
                    SELECT context_json FROM run_scalars
                    WHERE context_json IS NOT NULL AND context_json != '' AND context_json != '{{}}'
                    LIMIT ?
                )
                WHERE v IS NOT NULL
                ORDER BY v
                """,
                (json_path, _METRIC_FIELD_VALUE_SAMPLE, json_path, _METRIC_FIELD_VALUE_SAMPLE),
            ).fetchall()
            values = sorted({str(r["v"]) for r in rows if r["v"] is not None})
        elif path.startswith("run.hparams."):
            hp_key = path[len("run.hparams."):]
            if not re.fullmatch(r"[A-Za-z0-9_\-]+", hp_key):
                return jsonify({"status": "ok", "path": path, "values": []})
            json_path = f"$.{hp_key}"
            rows = con.execute(
                f"""
                SELECT DISTINCT json_extract(params_json, ?) AS v
                FROM runs
                WHERE params_json IS NOT NULL AND params_json != ''
                LIMIT ?
                """,
                (json_path, _METRIC_FIELD_VALUE_SAMPLE),
            ).fetchall()
            values = sorted({str(r["v"]) for r in rows if r["v"] is not None})
        elif path.startswith("run.") and "." not in path[4:]:
            # Top-level metadata key on the runs row, e.g. run.model
            meta_key = path[4:]
            if not re.fullmatch(r"[A-Za-z0-9_\-]+", meta_key):
                return jsonify({"status": "ok", "path": path, "values": []})
            json_path = f"$.{meta_key}"
            rows = con.execute(
                f"""
                SELECT DISTINCT json_extract(metadata_json, ?) AS v
                FROM runs
                WHERE metadata_json IS NOT NULL AND metadata_json != ''
                LIMIT ?
                """,
                (json_path, _METRIC_FIELD_VALUE_SAMPLE),
            ).fetchall()
            values = sorted({str(r["v"]) for r in rows if r["v"] is not None})
    finally:
        con.close()

    if q:
        values = [v for v in values if q in v.lower()]
    return jsonify({"status": "ok", "path": path, "values": values[:limit], "total": len(values)})


@api.route("/api/metrics_views")
def api_metrics_views_list():
    views = []
    for view in list_metrics_views():
        try:
            state = json.loads(view.get("state_json") or "{}")
        except Exception:
            state = {}
        view = dict(view)
        view["state"] = state
        view.pop("state_json", None)
        views.append(view)
    return jsonify({"status": "ok", "views": views})


@api.route("/api/metrics_views", methods=["POST"])
def api_metrics_views_create():
    data = request.get_json(silent=True) or {}
    title = str(data.get("title") or "Untitled metrics view").strip() or "Untitled metrics view"
    state = data.get("state") if isinstance(data.get("state"), dict) else {}
    pinned = 1 if data.get("pinned") else 0
    view = create_metrics_view(title, json.dumps(state, default=str), pinned=pinned)
    view = dict(view)
    view["state"] = state
    view.pop("state_json", None)
    return jsonify({"status": "ok", "view": view})


@api.route("/api/metrics_views/<int:view_id>")
def api_metrics_views_get(view_id):
    view = get_metrics_view(view_id)
    if not view:
        return jsonify({"status": "error", "error": "Saved metrics view not found"}), 404
    try:
        state = json.loads(view.get("state_json") or "{}")
    except Exception:
        state = {}
    view["state"] = state
    view.pop("state_json", None)
    return jsonify({"status": "ok", "view": view})


@api.route("/api/metrics_views/<int:view_id>", methods=["PATCH"])
def api_metrics_views_patch(view_id):
    if not get_metrics_view(view_id):
        return jsonify({"status": "error", "error": "Saved metrics view not found"}), 404
    data = request.get_json(silent=True) or {}
    state_json = None
    if "state" in data:
        state_json = json.dumps(data.get("state") if isinstance(data.get("state"), dict) else {}, default=str)
    view = update_metrics_view(
        view_id,
        title=str(data.get("title")).strip() if "title" in data else None,
        state_json=state_json,
        pinned=bool(data.get("pinned")) if "pinned" in data else None,
    )
    try:
        state = json.loads(view.get("state_json") or "{}")
    except Exception:
        state = {}
    view["state"] = state
    view.pop("state_json", None)
    return jsonify({"status": "ok", "view": view})


@api.route("/api/metrics_views/<int:view_id>", methods=["DELETE"])
def api_metrics_views_delete(view_id):
    rows = delete_metrics_view(view_id)
    if not rows:
        return jsonify({"status": "error", "error": "Saved metrics view not found"}), 404
    return jsonify({"status": "ok", "deleted": view_id})


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


@api.route("/api/resubmit_by_hash/<cluster>/<run_hash>", methods=["POST"])
def api_resubmit_by_hash(cluster, run_hash):
    """Re-execute the captured ``submit_command`` for an SDK-tracked run.

    Eligibility: ``source='sdk'``, non-empty ``submit_command``, and no
    job in ``RUNNING/COMPLETING/PENDING/SUBMITTING``. On success the
    captured command is spawned as a detached ``bash -c`` child whose
    stdout/stderr stream into ``data/resubmit_logs/<run_uuid>__<ts>.log``.
    """
    if cluster not in CLUSTERS:
        return jsonify({"status": "error", "error": "Unknown cluster"}), 404
    run = _load_run_by_ref(cluster, run_hash, allow_on_demand=False)
    if not run:
        return jsonify({"status": "error", "error": "Run not found"}), 404
    run["run_hash"] = run.get("run_hash") or get_run_hash(
        cluster, run.get("root_job_id", ""), run.get("run_uuid", "")
    )
    from .resubmit import eligibility as _resubmit_eligibility, spawn as _resubmit_spawn
    can, reason = _resubmit_eligibility(run)
    if not can:
        return jsonify({"status": "error", "error": reason}), 400
    try:
        info = _resubmit_spawn(run)
    except Exception as exc:
        _log.exception("resubmit %s/%s failed", cluster, run_hash)
        return jsonify({"status": "error", "error": str(exc)}), 500
    return jsonify({
        "status": "ok",
        "pid": info["pid"],
        "log_name": info["log_name"],
        "log_url": info["log_url"],
        "had_conda_prefix": info["had_conda_prefix"],
    })


@api.route("/api/resubmit_log/<filename>")
def api_resubmit_log(filename):
    """Serve a resubmit log file as plain text (capped at 1 MB)."""
    from .resubmit import read_log as _resubmit_read_log
    ok, content, status = _resubmit_read_log(filename)
    if not ok:
        return jsonify({"status": "error", "error": content}), status
    resp = make_response(content, status)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    resp.headers["Cache-Control"] = "no-store"
    return resp


@api.route("/api/run/<int:run_id>", methods=["PATCH"])
def api_update_run(run_id):
    """Partial update of user-editable run fields (starred, notes, malfunctioned)."""
    data = request.get_json(force=True, silent=True) or {}
    starred = data.get("starred")
    notes = data.get("notes")
    malfunctioned = data.get("malfunctioned")
    if starred is None and notes is None and malfunctioned is None:
        return jsonify({"status": "error", "error": "No fields to update"}), 400
    update_run_fields(run_id, starred=starred, notes=notes, malfunctioned=malfunctioned)
    return jsonify({"status": "ok"})


@api.route("/api/run/<int:run_id>", methods=["DELETE"])
def api_delete_run(run_id):
    """Hard-delete a run row and every SDK metric/event/alias tied to it.

    Query string:
      - ``delete_jobs=1`` also removes the linked ``job_history`` rows and
        their ``job_stats_snapshots``. Default (omitted) keeps the Slurm
        history rows and only unlinks them from the run.

    Returns ``{"status": "ok", "counts": {...}}`` with per-table delete
    counts or ``{"status": "not_found"}`` (404) when the id is unknown.
    """
    from .db import delete_run_completely

    delete_jobs = (request.args.get("delete_jobs") or "").strip().lower() in {"1", "true", "yes"}
    result = delete_run_completely(run_id, delete_jobs=delete_jobs)
    if result.get("status") == "not_found":
        return jsonify(result), 404
    bump_version()
    return jsonify(result)


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
    """Activity-based project list for the sidebar (and similar UIs).

    By default only projects with ``status='active'`` are returned —
    backlogged projects keep their prefix/color/emoji wiring but disappear
    from the sidebar until the user reactivates them in Settings. Pass
    ``?include=all`` to opt in to the full list, or ``?status=backlog`` to
    list only hidden projects.
    """
    from .config import get_project_color as _color, get_project_emoji as _emoji, PROJECTS
    include = (request.args.get("include") or "").strip().lower()
    status_filter = (request.args.get("status") or "").strip().lower()
    if status_filter not in ("active", "backlog"):
        status_filter = "" if include == "all" else "active"
    # Only registered settings projects — history may still list removed keys.
    projects = []
    for p in get_projects():
        cfg = PROJECTS.get(p.get("project"))
        if not cfg:
            continue
        if status_filter and (cfg.get("status") or "active") != status_filter:
            continue
        p["color"] = _color(p["project"])
        p["emoji"] = _emoji(p["project"])
        p["status"] = cfg.get("status") or "active"
        projects.append(p)
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
        status=payload.get("status") or "active",
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
        "campaign_delimiter", "description", "status",
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
    resp = {
        "status": "ok",
        "files": files,
        "dirs": dirs,
        "error": result.get("error", ""),
        "custom_log_dir": result.get("custom_log_dir", ""),
    }

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
    reset_circuit_breaker(cluster)
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
    """Deprecated in v4 — replaced by per-namespace endpoints.

    Returns 410 Gone with a pointer to the new endpoints. The v3
    blob-shaped patch was unsafe at scale (whole-config rewrites on
    every change); per-namespace endpoints in
    ``/api/clusters``, ``/api/team/*``, ``/api/paths/*``,
    ``/api/process_filters/*``, ``/api/settings/<key>`` replace it.
    """
    return jsonify({
        "status": "error",
        "error": "POST /api/settings was removed in clausius v4. Use the per-namespace "
                 "endpoints: /api/clusters, /api/team/members, /api/team/ppps, "
                 "/api/paths/<kind>, /api/process_filters/<mode>, /api/settings/<key>.",
    }), 410


# ─── v4 per-namespace config endpoints ──────────────────────────────────────

@api.route("/api/settings/<key>", methods=["GET"])
def api_setting_get(key):
    """Read a single ``app_settings`` key.

    Returns ``{"key", "value", "default", "description", "source"}``.
    ``source`` is ``"db"`` for an explicit override or ``"default"`` for
    the registered fallback.
    """
    from .settings import list_settings
    entry = list_settings().get(key)
    if entry is None:
        return jsonify({"status": "error", "error": f"unknown setting key {key!r}"}), 404
    return jsonify({"status": "ok", "key": key, **entry})


@api.route("/api/settings/<key>", methods=["PUT"])
def api_setting_put(key):
    """Update one ``app_settings`` key. JSON body must be ``{"value": ...}``."""
    from .settings import set_setting
    body = request.get_json(silent=True) or {}
    if "value" not in body:
        return jsonify({"status": "error", "error": "JSON body must include a 'value' field"}), 400
    result = set_setting(key, body["value"])
    return jsonify(result), (200 if result.get("status") == "ok" else 400)


@api.route("/api/settings/<key>", methods=["DELETE"])
def api_setting_delete(key):
    """Drop a stored override so the registered default takes over again."""
    from .settings import delete_setting
    return jsonify(delete_setting(key))


# Clusters --------------------------------------------------------------------

@api.route("/api/clusters", methods=["GET"])
def api_clusters_list():
    """List every registered cluster (excludes the synthetic ``local``)."""
    from .clusters import list_clusters
    enabled_only = request.args.get("enabled_only", "0") in ("1", "true", "True")
    return jsonify(list_clusters(include_local=False, only_enabled=enabled_only))


@api.route("/api/clusters", methods=["POST"])
def api_clusters_create():
    """Register a new cluster.

    Body must include ``name`` and ``host``; every other field is
    optional. Returns ``{"status": "ok"|"error", ...}`` with a 400 on
    validation failure.
    """
    from .clusters import add_cluster
    body = request.get_json(silent=True) or {}
    name = body.pop("name", "")
    if not name:
        return jsonify({"status": "error", "error": "name is required"}), 400
    if "host" not in body:
        return jsonify({"status": "error", "error": "host is required"}), 400
    result = add_cluster(name, **body)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@api.route("/api/clusters/<name>", methods=["GET"])
def api_clusters_get(name):
    from .clusters import get_cluster
    cluster = get_cluster(name)
    if cluster is None:
        return jsonify({"status": "error", "error": f"cluster {name!r} not found"}), 404
    return jsonify({"status": "ok", "cluster": cluster})


@api.route("/api/clusters/<name>", methods=["PUT"])
def api_clusters_update(name):
    from .clusters import update_cluster
    body = request.get_json(silent=True) or {}
    result = update_cluster(name, **body)
    code = 200 if result.get("status") == "ok" else (
        404 if "not found" in result.get("error", "") else 400
    )
    return jsonify(result), code


@api.route("/api/clusters/<name>", methods=["DELETE"])
def api_clusters_delete(name):
    from .clusters import remove_cluster
    result = remove_cluster(name)
    code = 200 if result.get("status") == "ok" else (
        404 if "not found" in result.get("error", "") else 400
    )
    return jsonify(result), code


@api.route("/api/cluster_resolve", methods=["GET"])
def api_cluster_resolve():
    """Resolve a cluster name (canonical or alias) to its canonical form.

    Query params:
      - ``name`` (required, string): the cluster name as the caller knows
        it. May be a canonical name, an alias, or an unknown string.
      - ``host`` (optional, string): the SSH login hostname; used as a
        fallback signal when ``name`` doesn't match.

    Returns ``200`` with ``{"canonical", "source", "matched_alias"?}`` on
    success or ``404`` with ``{"error": "no_match", "name": "<input>"}``
    when nothing matches. The endpoint never invents a canonical name by
    guessing: callers can rely on a positive response being authoritative.

    No write side effects, no auth required (mirrors the other read-only
    cluster endpoints).
    """
    from .clusters import resolve_canonical_cluster

    name = (request.args.get("name") or "").strip()
    host = (request.args.get("host") or "").strip()
    if not name and not host:
        return jsonify({"error": "no_match", "name": name}), 404

    hit = resolve_canonical_cluster(name, host=host)
    if hit is None:
        return jsonify({"error": "no_match", "name": name}), 404
    return jsonify(hit), 200


# Team members ----------------------------------------------------------------

@api.route("/api/team/members", methods=["GET"])
def api_team_members_list():
    from .team import list_team_members
    return jsonify(list_team_members())


@api.route("/api/team/members", methods=["POST"])
def api_team_members_create():
    from .team import add_team_member
    body = request.get_json(silent=True) or {}
    username = body.pop("username", "")
    if not username:
        return jsonify({"status": "error", "error": "username is required"}), 400
    result = add_team_member(username, **body)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@api.route("/api/team/members/<username>", methods=["DELETE"])
def api_team_members_delete(username):
    from .team import remove_team_member
    result = remove_team_member(username)
    code = 200 if result.get("status") == "ok" else 404
    return jsonify(result), code


# PPP accounts ---------------------------------------------------------------

@api.route("/api/team/ppps", methods=["GET"])
def api_team_ppps_list():
    from .team import list_ppp_accounts
    return jsonify(list_ppp_accounts())


@api.route("/api/team/ppps", methods=["POST"])
def api_team_ppps_create():
    from .team import add_ppp_account
    body = request.get_json(silent=True) or {}
    name = body.pop("name", "")
    if not name:
        return jsonify({"status": "error", "error": "name is required"}), 400
    result = add_ppp_account(name, **body)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@api.route("/api/team/ppps/<name>", methods=["PUT"])
def api_team_ppps_update(name):
    from .team import update_ppp_account
    body = request.get_json(silent=True) or {}
    result = update_ppp_account(name, **body)
    code = 200 if result.get("status") == "ok" else (
        404 if "not found" in result.get("error", "") else 400
    )
    return jsonify(result), code


@api.route("/api/team/ppps/<name>", methods=["DELETE"])
def api_team_ppps_delete(name):
    from .team import remove_ppp_account
    result = remove_ppp_account(name)
    code = 200 if result.get("status") == "ok" else 404
    return jsonify(result), code


# Path lists -----------------------------------------------------------------

@api.route("/api/paths/<kind>", methods=["GET"])
def api_paths_list(kind):
    from .paths import PATH_KINDS, list_path_bases
    if kind not in PATH_KINDS:
        return jsonify({"status": "error", "error": f"unknown kind {kind!r}"}), 404
    return jsonify(list_path_bases(kind=kind))


@api.route("/api/paths/<kind>", methods=["POST"])
def api_paths_add(kind):
    from .paths import add_path_base
    body = request.get_json(silent=True) or {}
    path = body.get("path", "")
    result = add_path_base(kind, path)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@api.route("/api/paths/<kind>", methods=["DELETE"])
def api_paths_remove(kind):
    """Remove a path entry. JSON body must include ``path`` (or ``id``)."""
    from .paths import remove_path_base, remove_path_base_by_id
    body = request.get_json(silent=True) or {}
    if "id" in body:
        result = remove_path_base_by_id(int(body["id"]))
    else:
        path = body.get("path", "")
        result = remove_path_base(kind, path)
    code = 200 if result.get("status") == "ok" else 404
    return jsonify(result), code


# Process filters ------------------------------------------------------------

@api.route("/api/process_filters/<mode>", methods=["GET"])
def api_filters_list(mode):
    from .paths import FILTER_MODES, list_process_filters
    if mode not in FILTER_MODES:
        return jsonify({"status": "error", "error": f"unknown mode {mode!r}"}), 404
    return jsonify(list_process_filters(mode=mode))


@api.route("/api/process_filters/<mode>", methods=["POST"])
def api_filters_add(mode):
    from .paths import add_process_filter
    body = request.get_json(silent=True) or {}
    pattern = body.get("pattern", "")
    result = add_process_filter(mode, pattern)
    code = 200 if result.get("status") == "ok" else 400
    return jsonify(result), code


@api.route("/api/process_filters/<mode>", methods=["DELETE"])
def api_filters_remove(mode):
    from .paths import remove_process_filter, remove_process_filter_by_id
    body = request.get_json(silent=True) or {}
    if "id" in body:
        result = remove_process_filter_by_id(int(body["id"]))
    else:
        pattern = body.get("pattern", "")
        result = remove_process_filter(mode, pattern)
    code = 200 if result.get("status") == "ok" else 404
    return jsonify(result), code


# ─── Logbook routes ──────────────────────────────────────────────────────────

from .cluster_dashboard import get_cluster_utilization
from .settings import get_dashboard_url as _DASHBOARD_URL
from .storage_quota import fetch_storage_quota  # noqa: used only as SSH fallback if needed


@api.route("/api/user_avatar")
def api_user_avatar():
    """Proxy the user's avatar image from the Science dashboard."""
    import urllib.request as _ur
    user = request.args.get("user", DEFAULT_USER)
    url = f"{_DASHBOARD_URL()}/images/{user}.png"
    try:
        with _ur.urlopen(url, timeout=5) as resp:
            data = resp.read()
            ct = resp.headers.get("Content-Type", "image/png")
            r = make_response(data)
            r.headers["Content-Type"] = ct
            r.headers["Cache-Control"] = "public, max-age=86400"
            return r
    except Exception:
        url_jpg = f"{_DASHBOARD_URL()}/images/{user}.jpeg"
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
    """Rank clusters by WDS score for job submission (MCP proxy target).

    Results are cached for ``AGGREGATOR_CACHE_TTL_SEC`` keyed by the
    request payload so multiple agents calling the same tool within the
    window share one underlying multi-cluster fetch instead of each one
    triggering its own SSH wave.
    """
    from .aihub import get_ppp_allocations as _wts_alloc, get_my_fairshare as _wts_fs
    from .partitions import get_partition_summary as _wts_ps
    from .jobs import fetch_team_jobs as _wts_tj
    from .config import _aggregator_cache, AGGREGATOR_CACHE_TTL_SEC
    from .wds import _compute_wds

    payload = request.get_json(silent=True) or {}
    nodes = int(payload.get("nodes", 1))
    gpus_per_node = int(payload.get("gpus_per_node", 8))
    gpu_type = payload.get("gpu_type", "")
    bypass_cache = bool(payload.get("force"))

    cache_key = f"wts:{nodes}:{gpus_per_node}:{gpu_type}"
    if not bypass_cache:
        cached = _cache_get(_aggregator_cache, cache_key, AGGREGATOR_CACHE_TTL_SEC)
        if cached is not None:
            return jsonify(cached)

    job_gpus = nodes * gpus_per_node
    pref_gpu = gpu_type.lower() if gpu_type else ""
    me = DEFAULT_USER

    def _fetch_parallel():
        # Fan out the per-cluster team_jobs fetches alongside alloc / fs /
        # partition_summary. Pre-fix this branch ran a single worker that
        # iterated CLUSTERS in a list comp, so 9 clusters × ~1.5 s each
        # serialized into the wall time of the whole call (observed
        # 12-17 s in production). Now every fetch is its own future and
        # the overall call is bounded by the slowest single cluster.
        #
        # Clusters whose SSH circuit breaker is open are skipped — they
        # would otherwise cost ~3 s of guaranteed-failed SSH each.
        from .ssh import is_cluster_reachable
        all_names = [c for c in CLUSTERS if c != "local"]
        cluster_names = [c for c in all_names if is_cluster_reachable(c)]
        skipped = [c for c in all_names if c not in cluster_names]
        n_workers = min(16, 3 + len(cluster_names) or 1)
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            f_alloc = pool.submit(_wts_alloc)
            f_fs = pool.submit(_wts_fs)
            f_parts = pool.submit(_wts_ps)
            tj_futs = {pool.submit(_wts_tj, c): c for c in cluster_names}
            tj = {}
            for fut in as_completed(tj_futs):
                cn = tj_futs[fut]
                try:
                    tj[cn] = fut.result()
                except Exception:
                    _log.exception("where_to_submit: team_jobs fetch failed for %s", cn)
                    tj[cn] = None
        return f_alloc.result(), f_fs.result(), f_parts.result(), tj, skipped

    try:
        alloc, my_fs, part_clusters, tj_clusters, skipped_clusters = _fetch_parallel()
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

    for cn in all_cluster_names:
        cd = alloc.get("clusters", {}).get(cn, {})
        has_ppp = bool(cd.get("accounts"))
        cluster_gpu = (cd.get("gpu_type") or CLUSTERS.get(cn, {}).get("gpu_type") or "").lower()
        ta = team_allocs.get(cn)
        team_num = int(ta) if isinstance(ta, (int, float)) and ta > 0 else (None if ta == "any" else None)
        tj = (tj_clusters.get(cn) or {}).get("summary", {})
        tj_users = tj.get("by_user", {})
        team_running = tj.get("total_running", 0)
        team_pending = tj.get("total_pending", 0)
        team_dependent = tj.get("total_dependent", 0)
        my_data = tj_users.get(me, {})
        my_r = my_data.get("running", 0)
        my_p = my_data.get("pending", 0)
        my_dependent = my_data.get("dependent", 0)
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
                if ppp_headroom <= 0:
                    notes.append(f"PPP allocation over consumed ({ppp_headroom} GPUs headroom)")
                raw_free = min(ppp_headroom, max(0, team_num - team_running)) if team_num is not None else ppp_headroom
                free = max(0, raw_free)
                occ = cd.get("cluster_occupied_gpus", 0)
                tot = cd.get("cluster_total_gpus", 0)
                occ_pct = round(occ / tot * 100) if tot > 0 else 0
                machine_score = 1.0 if same_gpu else 0.85
                factors = _compute_wds(
                    free, ppp_headroom, idle_nodes, pending_queue,
                    my_level_fs, level_fs, team_num, occ_pct=occ_pct,
                    req_nodes=nodes, req_gpn=gpus_per_node,
                    my_running=my_r, my_pending=my_p,
                    team_running=team_running, team_pending=team_pending,
                    machine_score=machine_score,
                )
                a_wds = factors["wds"]
                accounts.append({
                    "account": acct_name, "account_short": acct_name.split("_")[-1] if "_" in acct_name else acct_name,
                    "wds": a_wds, "ppp_level_fs": round(level_fs, 2), "my_level_fs": my_level_fs,
                    "headroom": ppp_headroom, "free_for_team": free,
                    "gpus_consumed": ad.get("gpus_consumed", 0), "gpus_allocated": ad.get("gpus_allocated", 0),
                    "resource_gate": factors["resource_gate"], "capacity_gate": factors["capacity_gate"],
                    "live_gate": factors["live_gate"], "queue_score": factors["queue_score"],
                    "my_wait_factor": factors["my_wait_factor"], "live_factor": factors["live_factor"],
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
            "team_dependent": team_dependent,
            "team_alloc": ta if ta is not None else "not set",
            "my_running": my_r,
            "my_pending": my_p,
            "my_dependent": my_dependent,
            "notes": notes,
        })

    recommendations.sort(key=lambda r: -r["wds"])
    result = {"status": "ok", "recommendations": recommendations,
              "my_total_running": my_total_running, "my_total_pending": my_total_pending,
              "job_gpus_requested": job_gpus,
              "skipped_clusters": skipped_clusters}
    _cache_set(_aggregator_cache, cache_key, result)
    return jsonify(result)


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
    list_campaigns as _lb_campaigns,
    get_campaign_board as _lb_get_campaign_board,
    list_campaign_boards as _lb_list_campaign_boards,
    update_campaign_board_by_campaign as _lb_update_campaign_board,
    get_mind_map as _lb_get_mind_map,
    list_mind_maps as _lb_list_mind_maps,
    update_mind_map_by_campaign as _lb_update_mind_map,
    patch_mind_map_by_campaign as _lb_patch_mind_map,
    convert_campaign_board_to_mind_map as _lb_convert_board_to_mind_map,
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
    campaign = request.args.get("campaign", "")
    return jsonify(_lb_list(project, query=q or None, sort=sort, limit=limit, offset=offset, entry_type=entry_type or None, campaign=campaign or None))


@api.route("/api/logbook/<project>/campaigns")
def api_logbook_campaigns(project):
    return jsonify(_lb_campaigns(project))


@api.route("/api/logbook/<project>/campaign_board")
def api_logbook_campaign_board_get(project):
    campaign = request.args.get("campaign", "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign query parameter is required"}), 400
    result = _lb_get_campaign_board(project, campaign)
    if result.get("status") == "not_found":
        return jsonify(result), 404
    if result.get("status") == "error":
        return jsonify(result), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/campaign_boards")
def api_logbook_campaign_boards_list(project):
    return jsonify(_lb_list_campaign_boards(project))


@api.route("/api/logbook/<project>/campaign_board", methods=["POST"])
def api_logbook_campaign_board_create(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    title = (payload.get("title") or "").strip() or f"Campaign board: {campaign}"
    body = (payload.get("body") or "").strip()
    board_json = payload.get("board_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_create(
        project, title, body,
        entry_type="campaign_board",
        campaign=campaign,
        board_json=board_json,
        campaign_goal=campaign_goal,
    )
    if result.get("status") == "error_validation":
        code = 409 if "already exists" in (result.get("error") or "") else 400
        return jsonify(result), code
    return jsonify(result)


@api.route("/api/logbook/<project>/campaign_board", methods=["PUT"])
def api_logbook_campaign_board_put(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or request.args.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    title = payload.get("title")
    if title is not None:
        title = title.strip()
    body = payload.get("body")
    if body is not None:
        body = body.strip()
    board_json = payload.get("board_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_update_campaign_board(
        project, campaign, title=title, body=body, board_json=board_json, campaign_goal=campaign_goal
    )
    if result.get("status") == "not_found":
        return jsonify(result), 404
    if result.get("status") == "error_validation":
        return jsonify({"status": "error", "error": result.get("error", "")}), 400
    return jsonify(result)


# ── Mind-map routes (per-campaign DAG; replaces campaign_board) ──────────────


@api.route("/api/logbook/<project>/mind_map")
def api_logbook_mind_map_get(project):
    campaign = request.args.get("campaign", "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign query parameter is required"}), 400
    result = _lb_get_mind_map(project, campaign)
    if result.get("status") == "not_found":
        return jsonify(result), 404
    if result.get("status") == "error":
        return jsonify(result), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/mind_maps")
def api_logbook_mind_maps_list(project):
    return jsonify(_lb_list_mind_maps(project))


@api.route("/api/logbook/<project>/mind_map", methods=["POST"])
def api_logbook_mind_map_create(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    title = (payload.get("title") or "").strip() or f"Mind map: {campaign}"
    body = (payload.get("body") or "").strip()
    graph_json = payload.get("graph_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_create(
        project, title, body,
        entry_type="mind_map",
        campaign=campaign,
        graph_json=graph_json,
        campaign_goal=campaign_goal,
    )
    if result.get("status") == "error_validation":
        code = 409 if "already exists" in (result.get("error") or "") else 400
        return jsonify(result), code
    return jsonify(result)


@api.route("/api/logbook/<project>/mind_map", methods=["PUT"])
def api_logbook_mind_map_put(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or request.args.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    title = payload.get("title")
    if title is not None:
        title = title.strip()
    body = payload.get("body")
    if body is not None:
        body = body.strip()
    graph_json = payload.get("graph_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_update_mind_map(
        project, campaign, title=title, body=body, graph_json=graph_json, campaign_goal=campaign_goal
    )
    if result.get("status") == "not_found":
        return jsonify(result), 404
    if result.get("status") == "error_validation":
        return jsonify({"status": "error", "error": result.get("error", "")}), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/mind_map", methods=["PATCH"])
def api_logbook_mind_map_patch(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or request.args.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    ops = payload.get("ops")
    if not isinstance(ops, list) or not ops:
        return jsonify({"status": "error", "error": "ops must be a non-empty list"}), 400
    result = _lb_patch_mind_map(project, campaign, ops)
    if result.get("status") == "not_found":
        return jsonify(result), 404
    if result.get("status") == "error_validation":
        return jsonify({"status": "error", "error": result.get("error", "")}), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/mind_map/from_campaign_board", methods=["POST"])
def api_logbook_mind_map_convert(project):
    payload = request.get_json(silent=True) or {}
    campaign = (payload.get("campaign") or "").strip().lower()
    if not campaign:
        return jsonify({"status": "error", "error": "campaign is required"}), 400
    result = _lb_convert_board_to_mind_map(project, campaign)
    status = result.get("status")
    if status == "not_found":
        return jsonify(result), 404
    if status == "exists":
        return jsonify(result), 409
    if status == "error_validation":
        return jsonify({"status": "error", "error": result.get("error", "")}), 400
    return jsonify(result)


@api.route("/api/logbook/<project>/entries", methods=["POST"])
def api_logbook_create(project):
    payload = request.get_json(silent=True) or {}
    title = (payload.get("title") or "").strip()
    if not title:
        return jsonify({"status": "error", "error": "Title is required"}), 400
    body = (payload.get("body") or "").strip()
    entry_type = (payload.get("entry_type") or "note").strip()
    campaign = payload.get("campaign")
    board_json = payload.get("board_json")
    graph_json = payload.get("graph_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_create(
        project, title, body, entry_type=entry_type, campaign=campaign,
        board_json=board_json, graph_json=graph_json, campaign_goal=campaign_goal,
    )
    if result.get("status") == "error_validation":
        code = 409 if "already exists" in (result.get("error") or "") else 400
        return jsonify(result), code
    return jsonify(result)


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
    campaign = payload.get("campaign")
    board_json = payload.get("board_json")
    graph_json = payload.get("graph_json")
    campaign_goal = payload.get("campaign_goal")
    result = _lb_update(
        project, entry_id,
        title=title, body=body, entry_type=entry_type,
        pinned=pinned, new_project=new_project, campaign=campaign,
        board_json=board_json, graph_json=graph_json, campaign_goal=campaign_goal,
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
    if entry_type and entry_type not in ("note", "plan", "campaign_board", "mind_map"):
        return jsonify({"status": "error", "error": "entry_type must be 'note', 'plan', 'campaign_board', 'mind_map', or omitted"}), 400

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
            if (
                p.get("project") in PROJECTS
                and (PROJECTS[p["project"]].get("status") or "active") == "active"
                and ql in p["project"].lower()
            )
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
             "started": r.get("started", ""), "run_hash": r.get("run_hash", ""),
             "run_root_job_id": r.get("run_root_job_id", "")}
            for r in get_history(limit=8, search=q)
        ]

    def _search_runs():
        try:
            def _run_result(row):
                rh = get_run_hash(row["cluster"], row["root_job_id"], row["run_uuid"])
                return {
                    "cluster": row["cluster"],
                    "root_job_id": row["root_job_id"],
                    "run_hash": rh,
                    "run_name": row["run_name"],
                    "run_uuid": row["run_uuid"],
                    "sdk_status": row["sdk_status"],
                    "project": row["project"],
                    "started_at": row["started_at"],
                    "job_count": row["job_count"],
                }

            con = get_db()
            if re.fullmatch(r"[0-9a-fA-F]{4,12}", q):
                rows = con.execute(
                    """SELECT r.cluster, r.root_job_id, r.run_name, r.run_uuid,
                              r.sdk_status, r.project, r.started_at, COUNT(jh.job_id) AS job_count
                       FROM runs r
                       LEFT JOIN job_history jh ON jh.run_id = r.id AND jh.cluster = r.cluster
                       GROUP BY r.id
                       ORDER BY COALESCE(r.started_at, r.created_at, '') DESC"""
                ).fetchall()
                direct = [
                    _run_result(r) for r in rows
                    if get_run_hash(r["cluster"], r["root_job_id"], r["run_uuid"]).lower().startswith(ql)
                ]
                if direct:
                    con.close()
                    return direct[:8]

            rows = con.execute(
                """SELECT r.cluster, r.root_job_id, r.run_name, r.run_uuid,
                          r.sdk_status, r.project, r.started_at, COUNT(jh.job_id) AS job_count
                   FROM runs r
                   LEFT JOIN job_history jh ON jh.run_id = r.id AND jh.cluster = r.cluster
                   GROUP BY r.id
                   ORDER BY COALESCE(r.started_at, r.created_at, '') DESC
                   LIMIT 2000"""
            ).fetchall()
            con.close()
            out = []
            for r in rows:
                rh = get_run_hash(r["cluster"], r["root_job_id"], r["run_uuid"])
                haystack = " ".join([
                    rh,
                    r["run_uuid"] or "",
                    r["root_job_id"] or "",
                    r["run_name"] or "",
                    r["project"] or "",
                    r["cluster"] or "",
                ]).lower()
                if ql not in haystack:
                    continue
                out.append(_run_result(r))
                if len(out) >= 8:
                    break
            return out
        except Exception:
            return []

    def _search_files():
        try:
            return _search_file_paths_for_spotlight(q, limit=8)
        except Exception:
            _log.exception("spotlight file path search failed")
            return []

    f_proj = _shared_pool.submit(_search_projects)
    f_lb = _shared_pool.submit(_search_logbook)
    f_hist = _shared_pool.submit(_search_history)
    f_runs = _shared_pool.submit(_search_runs)
    f_files = _shared_pool.submit(_search_files)

    return jsonify({
        "projects": f_proj.result(),
        "logbook": f_lb.result(),
        "runs": f_runs.result(),
        "files": f_files.result(),
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
    from .config import extract_project
    from .clusters import normalize_cluster_name
    from .settings import get_sdk_ingest_token
    from .db import (
        upsert_run_from_sdk,
        store_sdk_event,
        store_run_metric,
        store_run_scalar,
        merge_run_metadata,
        finalize_sdk_run,
        get_run_by_uuid,
        invalidate_pinned_cache,
    )

    sdk_token = get_sdk_ingest_token()
    if sdk_token:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {sdk_token}":
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
        inserted = store_sdk_event(run_uuid, event_type, event_seq, ts, payload_json)
        if not inserted:
            continue

        if event_type == "run_started":
            expname = payload.get("expname", "")
            cluster = normalize_cluster_name(payload.get("cluster", ""))
            project = extract_project(expname)
            run_id = upsert_run_from_sdk(run_uuid, cluster, expname, project, payload)
            _adopt_matching_slurm_jobs(cluster, expname, run_id)
            bump_version()

        elif event_type in ("job_prepared", "job_submitted"):
            run = get_run_by_uuid(run_uuid)
            if run:
                cluster = normalize_cluster_name(
                    payload.get("cluster") or run.get("cluster", "")
                )
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
            elif payload.get("step") is None:
                store_run_scalar(run_uuid, event_seq, ts, payload)
            else:
                store_run_metric(run_uuid, event_seq, ts, payload)

        elif event_type == "scalar_logged":
            store_run_scalar(run_uuid, event_seq, ts, payload)

        elif event_type == "metadata_logged":
            merge_run_metadata(run_uuid, payload.get("metadata", {}))

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
        from .clusters import normalize_cluster_name
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
        cluster = normalize_cluster_name(run.get("cluster", ""))
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
                (sdk_status, now, run.get("run_uuid") or run_uuid),
            )

        invalidate_pinned_cache(cluster)
    except Exception:
        pass
