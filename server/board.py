"""Shared board snapshot builders for HTTP routes and MCP tools."""

import re

from .config import (
    CLUSTERS,
    _cache_get,
    _progress_cache,
    _progress_source_cache,
    _crash_cache,
    _est_start_cache,
    PROGRESS_TTL_SEC,
    CRASH_TTL_SEC,
    EST_START_TTL_SEC,
    extract_project,
    extract_campaign,
    get_project_color,
    get_project_emoji,
)
from .db import (
    _restore_dependency_fields,
    cache_db_get_all_multi,
    get_board_pinned,
    get_db,
    get_live_board,
    get_live_jobs_for_cluster,
    get_run_hash,
    normalize_job_times_local,
)
from .jobs import parse_dependency, schedule_prefetch


_BOARD_OVERLAY_NAMESPACES = ["progress", "progress_source", "crash", "est_start"]


def _normalize_job_shape(job):
    row = dict(job)
    if row.get("job_id") and not row.get("jobid"):
        row["jobid"] = row["job_id"]
    if row.get("job_name") and not row.get("name"):
        row["name"] = row["job_name"]
    return row


def _load_board_overlays():
    return cache_db_get_all_multi(_BOARD_OVERLAY_NAMESPACES)


def _fill_run_ids(cluster, jobs):
    need = [j for j in jobs if not j.get("run_id") and not j.get("_pinned")]
    if not need:
        _fill_starred(cluster, jobs)
        return
    jid_map = {}
    for job in need:
        jid = str(job.get("jobid") or job.get("job_id") or "").strip()
        if jid:
            jid_map.setdefault(jid, []).append(job)
    if not jid_map:
        _fill_starred(cluster, jobs)
        return
    con = get_db()
    placeholders = ",".join("?" for _ in jid_map)
    rows = con.execute(
        f"SELECT job_id, run_id FROM job_history WHERE cluster=? AND job_id IN ({placeholders})",
        (cluster, *jid_map.keys()),
    ).fetchall()
    con.close()
    for row in rows:
        if not row["run_id"]:
            continue
        for job in jid_map.get(row["job_id"], []):
            job["run_id"] = row["run_id"]
    _fill_starred(cluster, jobs)


def _fill_starred(cluster, jobs):
    """Copy run metadata from runs table onto each job that has a run_id."""
    run_ids = set()
    for j in jobs:
        rid = j.get("run_id")
        if rid:
            run_ids.add(int(rid))
    if not run_ids:
        return
    con = get_db()
    placeholders = ",".join("?" for _ in run_ids)
    rows = con.execute(
        f"""SELECT id, root_job_id, run_name, run_uuid, starred
            FROM runs WHERE id IN ({placeholders})""",
        list(run_ids),
    ).fetchall()
    con.close()
    run_map = {row["id"]: row for row in rows}
    for j in jobs:
        rid = j.get("run_id")
        if rid and int(rid) in run_map:
            row = run_map[int(rid)]
            j["starred"] = row["starred"]
            j["run_root_job_id"] = row["root_job_id"]
            j["run_name"] = row["run_name"]
            j["run_uuid"] = row["run_uuid"]
            j["run_hash"] = get_run_hash(cluster, row["root_job_id"], row["run_uuid"])


_STDOUT_RE = re.compile(r'(?:^|\s)StdOut=(\S+)', re.MULTILINE)


def _output_dir_from_log_path(log_path):
    import os
    if not log_path:
        return None
    log_dir = os.path.dirname(log_path)
    output_dir = os.path.dirname(log_dir)
    if not output_dir or output_dir == log_dir:
        return None
    return output_dir


def _fill_output_dirs(cluster, jobs):
    """Fetch log_path from job_history and derive output_dir for each job.

    output_dir is the parent of the log directory and is used by the frontend
    to group continuation runs (same experiment restarted) into a single entity.

    Two sources are tried in order:
      1. job_history.log_path  — available once scontrol has been queried
         (running/completing jobs and completed jobs)
      2. runs.scontrol_raw StdOut= line  — available for pending jobs whose
         run metadata has been captured but stdout path not yet written to
         job_history
    """
    jid_map = {}
    for job in jobs:
        if job.get("output_dir"):
            continue
        jid = str(job.get("jobid") or job.get("job_id") or "").strip()
        if jid:
            jid_map.setdefault(jid, []).append(job)
    if not jid_map:
        return
    con = get_db()
    placeholders = ",".join("?" for _ in jid_map)

    # Pass 1: log_path stored directly on the job.
    rows = con.execute(
        f"SELECT job_id, log_path FROM job_history WHERE cluster=? AND job_id IN ({placeholders})",
        (cluster, *jid_map.keys()),
    ).fetchall()
    still_missing = {}
    for row in rows:
        output_dir = _output_dir_from_log_path(row["log_path"])
        if output_dir:
            for job in jid_map.get(row["job_id"], []):
                job["output_dir"] = output_dir
        else:
            for job in jid_map.get(row["job_id"], []):
                still_missing.setdefault(row["job_id"], []).append(job)

    # Also track jobs that had no job_history row yet (newly pending).
    seen_jids = {row["job_id"] for row in rows}
    for jid, job_list in jid_map.items():
        if jid not in seen_jids:
            still_missing[jid] = job_list

    if still_missing:
        # Pass 2: fall back to StdOut= in the run's scontrol_raw for jobs
        # that have a run_id but no log_path yet (e.g. pending jobs).
        run_placeholders = ",".join("?" for _ in still_missing)
        run_rows = con.execute(
            f"""SELECT jh.job_id, r.scontrol_raw
                FROM job_history jh
                JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster
                WHERE jh.cluster=? AND jh.job_id IN ({run_placeholders})
                  AND r.scontrol_raw != ''""",
            (cluster, *still_missing.keys()),
        ).fetchall()
        for row in run_rows:
            m = _STDOUT_RE.search(row["scontrol_raw"] or "")
            if not m:
                continue
            output_dir = _output_dir_from_log_path(m.group(1))
            if output_dir:
                for job in still_missing.get(row["job_id"], []):
                    job["output_dir"] = output_dir

    con.close()


def _apply_job_overlays(cluster, jobs, overlays):
    db_progress = overlays["progress"]
    db_progress_src = overlays["progress_source"]
    db_crash = overlays["crash"]
    db_est_start = overlays["est_start"]

    for job in jobs:
        jid = str(job.get("jobid") or job.get("job_id") or "").strip()
        if not jid:
            continue
        state = str(job.get("state", "")).upper()
        cache_key = f"{cluster}:{jid}"

        if state in {"RUNNING", "COMPLETING"}:
            progress = _cache_get(_progress_cache, (cluster, jid), PROGRESS_TTL_SEC)
            if progress is None:
                progress = db_progress.get(cache_key)
            if progress is not None:
                job["progress"] = progress
                source = _cache_get(
                    _progress_source_cache,
                    (cluster, jid),
                    PROGRESS_TTL_SEC,
                ) or db_progress_src.get(cache_key)
                if source:
                    job["progress_source"] = source
            crash = _cache_get(_crash_cache, (cluster, jid), CRASH_TTL_SEC)
            if crash is None:
                crash = db_crash.get(cache_key)
            if crash:
                job["crash_detected"] = crash

        if state == "PENDING":
            est_start = _cache_get(_est_start_cache, (cluster, jid), EST_START_TTL_SEC)
            if est_start is None:
                est_start = db_est_start.get(cache_key)
            if est_start:
                job["est_start"] = est_start

        if not job.get("project"):
            job["project"] = extract_project(job.get("name") or job.get("job_name") or "")
        project = job.get("project", "")
        if project:
            job["project_color"] = get_project_color(project)
            job["project_emoji"] = get_project_emoji(project)
            job_name = job.get("name") or job.get("job_name") or ""
            job["campaign"] = extract_campaign(job_name, project)


def _merge_live_and_pinned_jobs(cluster, live_jobs, pinned_jobs):
    jobs = [_normalize_job_shape(job) for job in live_jobs]
    live_ids = {str(job.get("jobid") or "") for job in jobs}

    for pinned in pinned_jobs:
        pinned_id = str(pinned.get("job_id") or pinned.get("jobid") or "")
        if not pinned_id or pinned_id in live_ids:
            continue
        if pinned_id.startswith("sdk-"):
            continue
        jobs.append(_normalize_job_shape({
            **pinned,
            "_pinned": True,
            "jobid": pinned_id,
            "name": pinned.get("job_name", ""),
        }))

    _restore_dependency_fields(jobs, parse_dependency)
    _fill_run_ids(cluster, jobs)
    _fill_output_dirs(cluster, jobs)
    jobs = [normalize_job_times_local(job) for job in jobs]
    return jobs


def _prefetch_active_jobs(cluster, jobs):
    if cluster == "local":
        return
    active_jobs = [
        job for job in jobs
        if str(job.get("state", "")).upper() in {"RUNNING", "COMPLETING"}
        and not job.get("_pinned")
    ][:3]
    for job in active_jobs:
        schedule_prefetch(cluster, job.get("jobid"))


def build_cluster_board_entry(
    cluster,
    *,
    live_jobs=None,
    state=None,
    pinned_jobs=None,
    overlays=None,
    schedule_prefetch_active=False,
):
    """Build one normalized cluster board entry."""
    if live_jobs is None or state is None:
        live_jobs, state = get_live_jobs_for_cluster(cluster)
    if overlays is None:
        overlays = _load_board_overlays()
    if pinned_jobs is None:
        pinned_jobs = get_board_pinned(cluster)

    entry = {
        "status": (state or {}).get("status", "ok"),
        "jobs": [_normalize_job_shape(job) for job in (live_jobs or [])],
        "updated": (state or {}).get("updated"),
    }
    err = (state or {}).get("last_error")
    if err:
        if entry["status"] == "error":
            entry["error"] = err
        else:
            entry["last_error"] = err

    if entry["status"] == "ok":
        entry["jobs"] = _merge_live_and_pinned_jobs(cluster, entry["jobs"], pinned_jobs or [])
        _apply_job_overlays(cluster, entry["jobs"], overlays)
        if schedule_prefetch_active:
            _prefetch_active_jobs(cluster, entry["jobs"])
    return entry


def build_board_snapshot(*, schedule_prefetch_active=False):
    """Build normalized board data for every configured cluster."""
    board, states = get_live_board()
    pinned_rows = get_board_pinned()
    pinned_by_cluster = {}
    for row in pinned_rows:
        pinned_by_cluster.setdefault(row["cluster"], []).append(row)

    overlays = _load_board_overlays()
    snapshot = {}
    for cluster in CLUSTERS:
        snapshot[cluster] = build_cluster_board_entry(
            cluster,
            live_jobs=board.get(cluster, []),
            state=states.get(cluster, {}),
            pinned_jobs=pinned_by_cluster.get(cluster, []),
            overlays=overlays,
            schedule_prefetch_active=schedule_prefetch_active,
        )
    return snapshot
