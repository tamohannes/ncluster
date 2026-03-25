"""Job fetching, parsing, polling, prefetch, and stats."""

import os
import re
import subprocess
import threading
import time
from datetime import datetime

from .config import (
    APP_ROOT, CLUSTERS, DEFAULT_USER, STATE_ORDER, SQUEUE_FMT, SQUEUE_HDR,
    LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE,
    SSH_TIMEOUT, CACHE_FRESH_SEC,
    _cache_lock, _cache, _seen_jobs, _last_polled,
    _cache_get, _cache_set,
    _log_index_cache, _log_content_cache, _stats_cache,
    _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    _prefetch_last, _warm_lock,
    LOG_INDEX_TTL_SEC, STATS_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC,
    EST_START_TTL_SEC, PREFETCH_MIN_GAP_SEC,
    extract_project,
)
from .ssh import ssh_run, ssh_run_with_timeout, enable_standalone_ssh
from .db import (
    upsert_job, get_db, get_board_pinned,
    upsert_run, update_run_meta, update_run_times, associate_jobs_to_run, get_run,
)
from .logs import (
    get_job_log_files, fetch_log_tail, extract_progress, detect_crash,
    detect_soft_failure, label_and_sort_files,
)

_DEP_RE = re.compile(r'(after\w*):(\d+)')
_EVAL_PREFIX_RE = re.compile(r'^(eval-[a-z0-9_]+)', re.I)
_stdout_captured = set()
_run_meta_fetched = {}          # (cluster, job_id) -> timestamp
_RUN_META_TTL_SEC = 300


def parse_dependency(raw):
    if not raw or raw.strip() in {"", "(null)"}:
        return []
    return [{"type": m.group(1), "job_id": m.group(2)} for m in _DEP_RE.finditer(raw)]


def parse_squeue_output(out):
    jobs = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) < len(SQUEUE_HDR):
            parts += [""] * (len(SQUEUE_HDR) - len(parts))
        jobs.append(dict(zip(SQUEUE_HDR, parts)))

    live_ids = {j["jobid"] for j in jobs}
    for j in jobs:
        deps = parse_dependency(j.get("dependency", ""))
        j["depends_on"] = [d["job_id"] for d in deps if d["job_id"] in live_ids]
        j["dep_details"] = deps

    children_map = {}
    for j in jobs:
        for pid in j["depends_on"]:
            children_map.setdefault(pid, []).append(j["jobid"])
    for j in jobs:
        j["dependents"] = children_map.get(j["jobid"], [])

    for j in jobs:
        j["project"] = extract_project(j.get("name", ""))

    jobs.sort(key=lambda j: j.get("submitted") or j.get("started") or "", reverse=True)
    jobs.sort(key=lambda j: STATE_ORDER.get(j.get("state", "").upper(), 99))
    return jobs


def fetch_jobs_remote(cluster_name):
    out, _ = ssh_run(cluster_name, f"squeue -u $USER --noheader -o '{SQUEUE_FMT}'")
    return parse_squeue_output(out)


def fetch_jobs_local():
    try:
        result = subprocess.run(
            ["squeue", "-u", os.environ.get("USER", DEFAULT_USER), "--noheader", f"-o{SQUEUE_FMT}"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            return parse_squeue_output(result.stdout.strip())
    except FileNotFoundError:
        pass

    result = subprocess.run(["ps", "aux", "--sort=-%cpu"], capture_output=True, text=True, timeout=5)
    jobs = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split(None, 10)
        if len(parts) < 11:
            continue
        cmd = parts[10]
        cmd_l = cmd.lower()
        include_tokens = LOCAL_PROC_INCLUDE
        exclude_tokens = [APP_ROOT.lower()] + LOCAL_PROC_EXCLUDE
        if any(t in cmd_l for t in include_tokens) and not any(t in cmd_l for t in exclude_tokens):
            pid = parts[1]
            if not os.path.isdir(f"/proc/{pid}"):
                continue
            jobs.append({
                "jobid": pid, "name": cmd[:60], "state": "RUNNING", "reason": "",
                "elapsed": parts[9], "timelimit": "—", "nodes": "1", "cpus": pid,
                "gres": "local", "partition": "local", "submitted": "—", "log": "",
            })
    return jobs[:20]


def fetch_cluster_data(cluster_name):
    try:
        if cluster_name == "local":
            jobs = fetch_jobs_local()
        else:
            jobs = fetch_jobs_remote(cluster_name)
        return {"status": "ok", "jobs": jobs, "updated": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "error", "error": str(e), "jobs": [], "updated": datetime.now().isoformat()}


def sacct_final(cluster_name, job_id):
    try:
        fmt = "JobID,JobName,State,ExitCode,Elapsed,Start,End"
        if cluster_name == "local":
            user = os.environ.get("USER", DEFAULT_USER)
            result = subprocess.run(
                ["sacct", "-u", user, "-j", job_id, f"--format={fmt}", "--noheader", "-P"],
                capture_output=True, text=True, timeout=5
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run(cluster_name, f"sacct -u $USER -j {job_id} --format={fmt} --noheader -P 2>/dev/null | head -1")
        if not out:
            return {}
        parts = out.split("|")
        keys = ["jobid", "name", "state", "exit_code", "elapsed", "started", "ended_at"]
        return dict(zip(keys, parts + [""] * len(keys)))
    except Exception:
        return {}


def _try_get_stdout_path(cluster_name, job_id):
    """Best-effort attempt to get the StdOut path from scontrol before
    the job record disappears."""
    try:
        out, _ = ssh_run_with_timeout(
            cluster_name,
            f"scontrol show job {job_id} 2>/dev/null | tr ' ' '\\n' | grep -E '^(StdOut|UserId)=' | cut -d= -f2-",
            timeout_sec=5,
        )
        lines = out.strip().splitlines()
        path = ""
        is_mine = False
        for line in lines:
            if line.startswith("/"):
                path = line
            elif "(" in line:
                is_mine = line.split("(")[0].strip() == os.environ.get("USER", DEFAULT_USER)
            else:
                is_mine = line.strip() == os.environ.get("USER", DEFAULT_USER)
        if not is_mine:
            return ""
        return path if path and path != "(null)" else ""
    except Exception:
        return ""


def get_job_stats(cluster, job_id):
    if cluster == "local":
        return {"status": "error", "error": "Stats popup is supported for Slurm clusters only."}
    try:
        sq, _ = ssh_run_with_timeout(cluster, f"squeue -u $USER -j {job_id} -h -o '%T|%D|%C|%b|%N|%M'", timeout_sec=10)
        if not sq:
            sctl, _ = ssh_run_with_timeout(cluster, f"scontrol show job {job_id} 2>/dev/null", timeout_sec=10)
            if not sctl:
                return {"status": "error", "error": "Job not in queue anymore."}
            tokens = sctl.replace("\n", " ").split()
            kv = {}
            for t in tokens:
                if "=" in t:
                    k, v = t.split("=", 1)
                    kv[k] = v
            owner = kv.get("UserId", "").split("(")[0]
            if owner and owner != os.environ.get("USER", DEFAULT_USER):
                return {"status": "error", "error": "Job belongs to another user."}
            state, nodes, cpus = kv.get("JobState", ""), kv.get("NumNodes", ""), kv.get("NumCPUs", "")
            gres, node_list, elapsed = kv.get("TresPerNode", kv.get("Gres", "")), kv.get("NodeList", ""), kv.get("RunTime", "")
        else:
            state, nodes, cpus, gres, node_list, elapsed = (sq.split("|") + [""] * 6)[:6]

        sstat_out, _ = ssh_run_with_timeout(cluster, f"sstat -j {job_id}.batch --noheader -P --format=AveCPU,AveRSS,MaxRSS,MaxVMSize 2>/dev/null | head -1", timeout_sec=10)
        ave_cpu, ave_rss, max_rss, max_vms = (sstat_out.split("|") + ["", "", "", ""])[:4] if sstat_out else ("", "", "", "")

        tres_ave, _ = ssh_run_with_timeout(cluster, f"sstat -j {job_id}.batch --noheader -P --format=TresUsageInAve,TresUsageInMax 2>/dev/null | head -1", timeout_sec=10)
        tres_usage_text = tres_ave.strip()

        def _extract_tres_value(text, key):
            if not text:
                return ""
            for token in text.replace(" ", "").split(","):
                if token.startswith(key + "="):
                    return token.split("=", 1)[1]
            return ""

        gpuutil_ave = _extract_tres_value(tres_usage_text, "gres/gpuutil")
        gpumem_ave = _extract_tres_value(tres_usage_text, "gres/gpumem")

        gpu_rows = []
        gpu_probe_error = ""
        if "gpu" in (gres or "").lower() and state in ("RUNNING", "COMPLETING"):
            gpu_cmd = (f"srun --jobid {job_id} -N1 -n1 --overlap "
                       "bash -lc \"nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total "
                       "--format=csv,noheader,nounits\" 2>/dev/null | head -16")
            gpu_out, gpu_err = ssh_run_with_timeout(cluster, gpu_cmd, timeout_sec=20)
            for line in gpu_out.splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 5:
                    gpu_rows.append({"index": parts[0], "name": parts[1], "util": parts[2] + "%", "mem": f"{parts[3]}/{parts[4]} MiB"})
            if not gpu_rows and gpu_err:
                gpu_probe_error = gpu_err

        gpu_summary = ""
        if not gpu_rows:
            if gpuutil_ave or gpumem_ave:
                gpu_summary = f"Ave GPU util: {gpuutil_ave or 'n/a'} | Ave GPU mem: {gpumem_ave or 'n/a'}"
            elif "gpu" in (gres or "").lower() and state in ("RUNNING", "COMPLETING"):
                gpu_summary = "Per-GPU probe unavailable."
            elif "gpu" in (gres or "").lower():
                gpu_summary = "GPU job not running; stats unavailable."

        return {
            "status": "ok", "job_id": job_id, "state": state, "elapsed": elapsed,
            "nodes": nodes, "cpus": cpus, "gres": gres, "node_list": node_list,
            "ave_cpu": ave_cpu, "ave_rss": ave_rss, "max_rss": max_rss, "max_vmsize": max_vms,
            "gpuutil_ave": gpuutil_ave, "gpumem_ave": gpumem_ave,
            "gpu_summary": gpu_summary, "gpu_probe_error": gpu_probe_error, "gpus": gpu_rows,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_job_stats_cached(cluster, job_id, force=False):
    key = (cluster, str(job_id))
    if not force:
        cached = _cache_get(_stats_cache, key, STATS_TTL_SEC)
        if cached is not None and not cached.get("_partial"):
            return cached
    value = get_job_stats(cluster, str(job_id))
    _cache_set(_stats_cache, key, value)
    return value


# ─── Run detection & metadata ────────────────────────────────────────────────

def _group_key_for_job(name):
    """Server-side equivalent of the frontend's groupKeyForJob()."""
    n = (name or "").strip()
    if not n:
        return "misc"
    m = _EVAL_PREFIX_RE.match(n)
    if m:
        return m.group(1).lower()
    return re.sub(
        r'(?:-|_)rs\d+$', '',
        re.sub(r'(?:-|_)(?:judge|summarize[-_]results?)(?:-rs\d+)?$', '', n, flags=re.I),
        flags=re.I,
    ).lower()


def _group_jobs_for_runs(jobs):
    """Group jobs using union-find on dependency chains + name prefixes.

    Returns list of (group_key, root_job_id, [job_ids]).
    """
    by_id = {j["jobid"]: j for j in jobs}
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    for j in jobs:
        for pid in j.get("depends_on", []):
            if pid in by_id:
                union(j["jobid"], pid)

    name_groups = {}
    for j in jobs:
        key = _group_key_for_job(j.get("name", ""))
        name_groups.setdefault(key, []).append(j["jobid"])
    for ids in name_groups.values():
        for i in range(1, len(ids)):
            union(ids[0], ids[i])

    groups = {}
    for j in jobs:
        root = find(j["jobid"])
        groups.setdefault(root, []).append(j)

    result = []
    for grp in groups.values():
        root_job = next((j for j in grp if not j.get("depends_on")), grp[0])
        label = _group_key_for_job(root_job.get("name", ""))
        result.append((label, root_job["jobid"], [j["jobid"] for j in grp]))
    return result


def _detect_and_register_runs(cluster, jobs):
    """Create run records for job groups and schedule metadata capture."""
    if not jobs:
        return
    groups = _group_jobs_for_runs(jobs)
    for run_name, root_job_id, job_ids in groups:
        root_job = next((j for j in jobs if j["jobid"] == root_job_id), None)
        project = root_job.get("project", "") if root_job else ""
        run_id = upsert_run(cluster, root_job_id, run_name, project)
        associate_jobs_to_run(cluster, run_id, job_ids)

        started = root_job.get("started") or root_job.get("submitted") if root_job else None
        if started:
            update_run_times(run_id, started_at=started)

        key = (cluster, root_job_id)
        cached_ts = _run_meta_fetched.get(key)
        if cached_ts is None or (time.monotonic() - cached_ts) > _RUN_META_TTL_SEC:
            existing = get_run(cluster, root_job_id)
            if existing and not existing.get("meta_fetched"):
                _run_meta_fetched[key] = time.monotonic()
                t = threading.Thread(
                    target=_capture_run_metadata,
                    args=(cluster, root_job_id, run_id),
                    daemon=True,
                )
                t.start()


def _capture_run_metadata(cluster, root_job_id, run_id):
    """SSH to cluster and capture batch script, scontrol output, and conda state."""
    enable_standalone_ssh()
    script = f"""#!/bin/sh
SCTL=$(scontrol show job {root_job_id} 2>/dev/null)
OWNER=$(echo "$SCTL" | tr ' ' '\\n' | grep '^UserId=' | head -1 | cut -d= -f2- | cut -d'(' -f1)
if [ -n "$OWNER" ] && [ "$OWNER" != "$USER" ]; then
  echo "===SCONTROL_START==="
  echo "===SCONTROL_END==="
  echo "===BATCH_START==="
  echo "===BATCH_END==="
  echo "===CONDA_START==="
  echo "(wrong user)"
  echo "===CONDA_END==="
  exit 0
fi
echo "===SCONTROL_START==="
echo "$SCTL"
echo "===SCONTROL_END==="
echo "===BATCH_START==="
scontrol write batch_script {root_job_id} - 2>/dev/null
echo "===BATCH_END==="
echo "===CONDA_START==="
conda env export 2>/dev/null || conda list 2>/dev/null || pip freeze 2>/dev/null || echo "(conda/pip not available)"
echo "===CONDA_END==="
"""
    try:
        out, _ = ssh_run_with_timeout(cluster, script, timeout_sec=25)
    except Exception:
        _run_meta_fetched.pop((cluster, root_job_id), None)
        return

    def _extract(text, start_marker, end_marker):
        s = text.find(start_marker)
        e = text.find(end_marker)
        if s < 0 or e < 0:
            return ""
        return text[s + len(start_marker):e].strip()

    scontrol_raw = _extract(out, "===SCONTROL_START===", "===SCONTROL_END===")
    batch_script = _extract(out, "===BATCH_START===", "===BATCH_END===")
    conda_state = _extract(out, "===CONDA_START===", "===CONDA_END===")

    if not scontrol_raw:
        scontrol_raw = _sacct_fallback_metadata(cluster, root_job_id)

    env_vars = _parse_env_from_scontrol(scontrol_raw)

    success = any([batch_script, scontrol_raw, env_vars])
    update_run_meta(run_id, batch_script, scontrol_raw, env_vars, conda_state)

    if not success:
        _run_meta_fetched.pop((cluster, root_job_id), None)


def _sacct_fallback_metadata(cluster, job_id):
    """Fall back to sacct when scontrol returns nothing (job already completed)."""
    fmt = "JobID,JobName,Partition,Account,AllocCPUS,State,ExitCode,Start,End,Elapsed,MaxRSS,NodeList,WorkDir"
    try:
        if cluster == "local":
            result = subprocess.run(
                ["sacct", "-j", str(job_id), f"--format={fmt}", "--noheader", "--parsable2"],
                capture_output=True, text=True, timeout=10,
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run_with_timeout(
                cluster,
                f"sacct -j {job_id} --format={fmt} --noheader --parsable2 2>/dev/null",
                timeout_sec=15,
            )
            out = out.strip()
    except Exception:
        return ""
    if not out:
        return ""
    headers = fmt.split(",")
    lines = []
    for row in out.splitlines():
        parts = row.split("|")
        if len(parts) >= len(headers):
            lines.append("  ".join(f"{h}={parts[i]}" for i, h in enumerate(headers)))
    return "\n".join(lines) if lines else ""


def _parse_env_from_scontrol(scontrol_raw):
    """Extract environment-related fields from scontrol show job output."""
    if not scontrol_raw:
        return ""
    interesting = [
        "WorkDir", "Command", "StdOut", "StdErr", "Partition", "TimeLimit",
        "NumNodes", "NumCPUs", "NumTasks", "TRES", "TresPerNode",
        "MinMemoryNode", "Gres", "Constraint", "Features", "Account",
        "QOS", "Reservation",
    ]
    lines = []
    for token in scontrol_raw.replace("\n", " ").split():
        if "=" in token:
            key = token.split("=", 1)[0]
            if key in interesting:
                lines.append(token)
    return "\n".join(lines)


def fetch_run_metadata_sync(cluster, root_job_id):
    """Fetch run metadata synchronously (called when popup is opened and
    meta_fetched=0). Returns the updated run dict."""
    run = get_run(cluster, root_job_id)
    if not run:
        return None
    if not run.get("meta_fetched"):
        _capture_run_metadata(cluster, root_job_id, run["id"])
        run = get_run(cluster, root_job_id)
    return run


def create_run_on_demand(cluster, root_job_id):
    """Create a run record from existing DB jobs when no run exists yet.

    This handles the case where a user clicks a run badge for jobs that
    were already tracked before the run feature was deployed, or for
    pinned/historical jobs.
    """
    con = get_db()
    root_row = con.execute(
        "SELECT * FROM job_history WHERE cluster=? AND job_id=?",
        (cluster, root_job_id),
    ).fetchone()
    if not root_row:
        con.close()
        return None

    root_job = dict(root_row)
    run_name = _group_key_for_job(root_job.get("job_name", ""))
    project = root_job.get("project", "") or extract_project(root_job.get("job_name", ""))

    related_rows = con.execute(
        "SELECT * FROM job_history WHERE cluster=? AND project=? AND job_name LIKE ?",
        (cluster, project, f"%{run_name}%"),
    ).fetchall()
    con.close()

    if not related_rows:
        related_rows = [root_row]

    job_dicts = []
    for r in related_rows:
        d = dict(r)
        d["jobid"] = d["job_id"]
        d["name"] = d.get("job_name", "")
        deps = parse_dependency(d.get("dependency", ""))
        d["depends_on"] = [dep["job_id"] for dep in deps]
        d["dep_details"] = deps
        job_dicts.append(d)

    groups = _group_jobs_for_runs(job_dicts)

    target_group = None
    for gk, gk_root_id, gk_job_ids in groups:
        if root_job_id in gk_job_ids:
            target_group = (gk, gk_root_id, gk_job_ids)
            break

    if not target_group:
        target_group = (run_name, root_job_id, [root_job_id])

    gk, actual_root, job_ids = target_group

    run_id = upsert_run(cluster, actual_root, gk, project)
    associate_jobs_to_run(cluster, run_id, job_ids)

    started = root_job.get("started") or root_job.get("submitted")
    ended = root_job.get("ended_at")
    if started:
        update_run_times(run_id, started_at=started)
    if ended:
        update_run_times(run_id, ended_at=ended)

    return actual_root


# ─── Polling ─────────────────────────────────────────────────────────────────

def _is_cache_fresh(cluster_name):
    ts = _last_polled.get(cluster_name, 0.0)
    return (time.monotonic() - ts) < CACHE_FRESH_SEC


def refresh_all_clusters():
    stale = [n for n in CLUSTERS if not _is_cache_fresh(n)]
    if not stale:
        return
    threads = []
    for name in stale:
        _last_polled[name] = time.monotonic()
        t = threading.Thread(target=poll_cluster, args=(name,), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=SSH_TIMEOUT + 5)


def refresh_cluster(cluster_name):
    if _is_cache_fresh(cluster_name):
        return
    _last_polled[cluster_name] = time.monotonic()
    poll_cluster(cluster_name)


def poll_cluster(name):
    data = fetch_cluster_data(name)
    current_ids = {j["jobid"] for j in data.get("jobs", [])}

    with _cache_lock:
        prev_ids = _seen_jobs.get(name, set())
        prev_jobs = {j["jobid"]: j for j in _cache.get(name, {}).get("jobs", [])}
        _cache[name] = data
        _seen_jobs[name] = current_ids

    gone_ids = prev_ids - current_ids
    for job_id in gone_ids:
        _finalize_gone_job(name, job_id, prev_jobs.get(job_id, {}))

    if name != "local":
        for job in data.get("jobs", []):
            upsert_job(name, job, terminal=False)

        running_ids = [j["jobid"] for j in data.get("jobs", [])
                       if j.get("state", "").upper() in ("RUNNING", "COMPLETING")]
        uncaptured = [jid for jid in running_ids
                      if (name, jid) not in _stdout_captured]
        if uncaptured:
            threading.Thread(
                target=_capture_stdout_paths, args=(name, uncaptured), daemon=True,
            ).start()

        all_jobs_for_runs = list(data.get("jobs", []))
        pinned = get_board_pinned(name)
        live_ids = {j["jobid"] for j in all_jobs_for_runs}
        for p in pinned:
            pid = p.get("job_id", "")
            if pid and pid not in live_ids:
                all_jobs_for_runs.append({
                    "jobid": pid,
                    "name": p.get("job_name", ""),
                    "depends_on": p.get("depends_on", []),
                    "dep_details": p.get("dep_details", []),
                    "dependents": p.get("dependents", []),
                    "project": p.get("project", ""),
                    "state": p.get("state", ""),
                    "started": p.get("started", ""),
                    "submitted": p.get("submitted", ""),
                })
        _detect_and_register_runs(name, all_jobs_for_runs)

    if not prev_ids and name != "local":
        _reconcile_db_with_squeue(name, current_ids)

    if not _softfail_migrated:
        _schedule_softfail_migration()


def _capture_stdout_paths(cluster_name, job_ids):
    """Bulk-capture StdOut paths via scontrol for running jobs, store in DB.

    Runs in a background thread so it never blocks the main polling path.
    """
    enable_standalone_ssh()
    if not job_ids:
        return
    ids_str = " ".join(str(j) for j in job_ids)
    script = f"""for JOB in {ids_str}; do
  INFO=$(scontrol show job "$JOB" 2>/dev/null | tr ' ' '\\n')
  OWNER=$(echo "$INFO" | grep '^UserId=' | cut -d= -f2- | cut -d'(' -f1)
  [ "$OWNER" != "$USER" ] && continue
  STDOUT=$(echo "$INFO" | grep '^StdOut=' | cut -d= -f2-)
  [ -n "$STDOUT" ] && [ "$STDOUT" != "(null)" ] && echo "LOGPATH:$JOB:$STDOUT"
done"""
    try:
        out, _ = ssh_run_with_timeout(cluster_name, script, timeout_sec=15)
    except Exception:
        return

    con = get_db()
    for line in out.splitlines():
        if not line.startswith("LOGPATH:"):
            continue
        parts = line[8:].split(":", 1)
        if len(parts) != 2:
            continue
        jid, path = parts[0].strip(), parts[1].strip()
        if path:
            con.execute(
                "UPDATE job_history SET log_path=? WHERE cluster=? AND job_id=? AND (log_path IS NULL OR log_path='')",
                (path, cluster_name, jid),
            )
            _stdout_captured.add((cluster_name, jid))
    con.commit()
    con.close()


def _detect_crash_on_complete(cluster, job_id, log_path=""):
    """Check log content for crash patterns when sacct reports COMPLETED.

    Returns a short crash reason string, or None if no crash detected.
    """
    jid = str(job_id)

    cached = _cache_get(_crash_cache, (cluster, jid), CRASH_TTL_SEC)
    if cached:
        return cached

    try:
        if log_path:
            content = fetch_log_tail(cluster, log_path, lines=150)
        else:
            log_result = get_job_log_files(cluster, jid)
            files = log_result.get("files", [])
            if not files:
                return None
            content = fetch_log_tail(cluster, files[0]["path"], lines=150)
        return detect_crash(content)
    except Exception:
        return None


def _read_finalize_log(cluster, job_id, log_path=""):
    """Read the log tail for finalization analysis (crash + soft-failure)."""
    jid = str(job_id)
    try:
        if log_path:
            return fetch_log_tail(cluster, log_path, lines=150)
        log_result = get_job_log_files(cluster, jid)
        files = log_result.get("files", [])
        if not files:
            return None
        return fetch_log_tail(cluster, files[0]["path"], lines=150)
    except Exception:
        return None


def _finalize_gone_job(cluster, job_id, prev_job):
    prev_state = prev_job.get("state", "").upper()
    prev_reason = prev_job.get("reason", "")
    final = sacct_final(cluster, job_id)
    sacct_state = final.get("state", "").upper().split()[0] if final.get("state") else ""

    if sacct_state:
        final_state = sacct_state
    elif not final:
        if prev_state in ("PENDING", ""):
            final_state = "CANCELLED"
        else:
            final_state = "FAILED"
    else:
        final_state = prev_state or "COMPLETED"

    # Build reason: sacct state detail > previous squeue reason > generic
    reason = ""
    if final.get("state") and " " in final["state"]:
        reason = final["state"]  # e.g. "FAILED by 0" or "CANCELLED by 1000"
    elif prev_reason and prev_reason not in ("None", "Priority"):
        reason = prev_reason
    elif not final and prev_state not in ("PENDING", ""):
        reason = "killed externally (no sacct record)"

    # Try to grab output path from scontrol before it disappears
    log_path = prev_job.get("log_path", "") or ""
    if not log_path and cluster != "local":
        log_path = _try_get_stdout_path(cluster, job_id)

    # Analyze logs to catch two cases:
    # 1. sacct says COMPLETED but logs contain a real crash → upgrade to FAILED
    # 2. sacct says FAILED but logs show work was already done → downgrade to COMPLETED
    if cluster != "local" and final_state in ("COMPLETED", "FAILED"):
        content = _read_finalize_log(cluster, job_id, log_path)
        if content:
            crash = detect_crash(content)
            soft = detect_soft_failure(content)

            if final_state == "COMPLETED" and crash:
                if soft:
                    reason = f"soft-fail: {soft}"
                else:
                    final_state = "FAILED"
                    reason = f"log crash: {crash}"
                    _cache_set(_crash_cache, (cluster, str(job_id)), crash)
            elif final_state == "FAILED" and soft:
                final_state = "COMPLETED"
                reason = f"soft-fail: {soft}"

    record = final if final else {
        "jobid": job_id, "name": prev_job.get("name", ""), "state": final_state,
        "elapsed": prev_job.get("elapsed", ""), "nodes": prev_job.get("nodes", ""),
        "gres": prev_job.get("gres", ""), "partition": prev_job.get("partition", ""),
        "submitted": prev_job.get("submitted", ""), "ended_at": datetime.now().isoformat(),
    }
    if not record.get("name") and prev_job.get("name"):
        record["name"] = prev_job["name"]
    record["state"] = final_state
    if reason:
        record["reason"] = reason
    if not record.get("reason") and prev_reason:
        record["reason"] = prev_reason
    if final.get("exit_code"):
        record["exit_code"] = final["exit_code"]
    elif prev_job.get("exit_code"):
        record["exit_code"] = prev_job["exit_code"]
    if log_path:
        record["log_path"] = log_path
    if not record.get("ended_at"):
        record["ended_at"] = datetime.now().isoformat()
    upsert_job(cluster, record, terminal=True)


def _reconcile_db_with_squeue(cluster, live_ids):
    con = get_db()
    rows = con.execute(
        """SELECT job_id, state FROM job_history
           WHERE cluster=?
             AND state IN ('RUNNING','COMPLETING','PENDING')""",
        (cluster,)
    ).fetchall()
    con.close()
    for row in rows:
        jid = row["job_id"]
        if jid not in live_ids:
            final = sacct_final(cluster, jid)
            final_state = (final.get("state", "") or "COMPLETED").upper().split()[0]
            reason_override = None

            if cluster != "local" and final_state in ("COMPLETED", "FAILED"):
                content = _read_finalize_log(cluster, jid)
                if content:
                    crash = detect_crash(content)
                    soft = detect_soft_failure(content)

                    if final_state == "COMPLETED" and crash:
                        if soft:
                            reason_override = f"soft-fail: {soft}"
                        else:
                            final_state = "FAILED"
                            reason_override = f"log crash: {crash}"
                    elif final_state == "FAILED" and soft:
                        final_state = "COMPLETED"
                        reason_override = f"soft-fail: {soft}"

            record = final if final else {"jobid": jid, "state": final_state, "ended_at": datetime.now().isoformat()}
            record["state"] = final_state
            if reason_override:
                record["reason"] = reason_override
            record.setdefault("jobid", jid)
            record.setdefault("ended_at", datetime.now().isoformat())
            upsert_job(cluster, record, terminal=True)


# ─── Soft-fail migration for existing DB records ─────────────────────────────

_softfail_migrated = False

def _find_log_dir_for_job(cluster, job_id):
    """Find the log directory by checking parent dependencies, run siblings,
    and jobs with the same name — any job that shares the log directory."""
    con = get_db()
    row = con.execute(
        "SELECT run_id, dependency, job_name FROM job_history WHERE cluster=? AND job_id=?",
        (cluster, job_id),
    ).fetchone()
    if not row:
        con.close()
        return ""

    # 1. Check parent dependency jobs
    dep = row["dependency"] or ""
    for m in re.finditer(r'after\w+:(\d+)', dep):
        parent = con.execute(
            "SELECT log_path FROM job_history WHERE cluster=? AND job_id=? "
            "AND log_path IS NOT NULL AND log_path != ''",
            (cluster, m.group(1)),
        ).fetchone()
        if parent and parent["log_path"]:
            con.close()
            return os.path.dirname(parent["log_path"])

    # 2. Check sibling jobs in the same run
    if row["run_id"]:
        sibling = con.execute(
            "SELECT log_path FROM job_history "
            "WHERE cluster=? AND run_id=? AND log_path IS NOT NULL AND log_path != '' LIMIT 1",
            (cluster, row["run_id"]),
        ).fetchone()
        if sibling and sibling["log_path"]:
            con.close()
            return os.path.dirname(sibling["log_path"])

    # 3. Check jobs with same name (different chunks of the same eval)
    if row["job_name"]:
        same_name = con.execute(
            "SELECT log_path FROM job_history "
            "WHERE cluster=? AND job_name=? AND log_path IS NOT NULL AND log_path != '' LIMIT 1",
            (cluster, row["job_name"]),
        ).fetchone()
        if same_name and same_name["log_path"]:
            con.close()
            return os.path.dirname(same_name["log_path"])

    con.close()
    return ""


def reevaluate_failed_for_softfail():
    """Background one-shot: re-check board-pinned FAILED jobs for soft failure.

    Runs once after startup so that jobs finalized before the soft-fail
    feature was deployed get retroactively reclassified.
    """
    global _softfail_migrated
    if _softfail_migrated:
        return
    _softfail_migrated = True

    enable_standalone_ssh()
    con = get_db()
    rows = con.execute(
        "SELECT cluster, job_id, log_path FROM job_history "
        "WHERE board_visible=1 AND state='FAILED' AND cluster != 'local'"
    ).fetchall()
    con.close()

    if not rows:
        return

    # Group by cluster to batch SSH calls
    by_cluster = {}
    for row in rows:
        by_cluster.setdefault(row["cluster"], []).append(row)

    for cluster, cluster_rows in by_cluster.items():
        # Collect jobs that need log dir lookup from siblings
        need_search = []
        for row in cluster_rows:
            jid, log_path = row["job_id"], row["log_path"] or ""
            content = _read_finalize_log(cluster, jid, log_path) if log_path else None
            if content:
                soft = detect_soft_failure(content)
                if soft:
                    _update_to_softfail(cluster, jid, soft)
            else:
                log_dir = _find_log_dir_for_job(cluster, jid)
                if log_dir:
                    need_search.append((jid, log_dir))

        if not need_search:
            continue

        # Batch SSH: read main srun log for each job from the sibling log dir
        for jid, log_dir in need_search:
            try:
                cmd = f"ls '{log_dir}/' 2>/dev/null | grep '{jid}' | grep 'main.*srun' | head -1"
                out, _ = ssh_run_with_timeout(cluster, cmd, timeout_sec=10)
                fname = out.strip()
                if not fname:
                    cmd2 = f"ls '{log_dir}/' 2>/dev/null | grep '{jid}' | head -1"
                    out2, _ = ssh_run_with_timeout(cluster, cmd2, timeout_sec=10)
                    fname = out2.strip()
                if not fname:
                    continue
                full_path = f"{log_dir}/{fname}"
                content = fetch_log_tail(cluster, full_path, lines=150)
                if content:
                    soft = detect_soft_failure(content)
                    if soft:
                        _update_to_softfail(cluster, jid, soft)
            except Exception:
                continue


def _update_to_softfail(cluster, job_id, soft_reason):
    con = get_db()
    con.execute(
        "UPDATE job_history SET state='COMPLETED', reason=? "
        "WHERE cluster=? AND job_id=?",
        (f"soft-fail: {soft_reason}", cluster, job_id),
    )
    con.commit()
    con.close()


def _schedule_softfail_migration():
    """Called once from poll_cluster; spawns the migration in a background thread."""
    global _softfail_migrated
    if _softfail_migrated:
        return
    threading.Thread(target=reevaluate_failed_for_softfail, daemon=True).start()


# ─── Prefetch ────────────────────────────────────────────────────────────────

_prefetch_active = {}          # cluster -> count of active prefetch threads
_prefetch_active_lock = threading.Lock()
_MAX_PREFETCH_THREADS = 4      # per cluster


def schedule_prefetch(cluster, job_id):
    k = (cluster, str(job_id))
    now = time.monotonic()
    with _warm_lock:
        last = _prefetch_last.get(k, 0.0)
        if now - last < PREFETCH_MIN_GAP_SEC:
            return
        _prefetch_last[k] = now
    with _prefetch_active_lock:
        if _prefetch_active.get(cluster, 0) >= _MAX_PREFETCH_THREADS:
            return
        _prefetch_active[cluster] = _prefetch_active.get(cluster, 0) + 1
    t = threading.Thread(target=_prefetch_job_data, args=(cluster, str(job_id)), daemon=True)
    t.start()


def _extract_progress_with_source(cluster, job_id, files):
    """Try files in order, return (pct, label) from the first file with progress."""
    for f in files:
        content = fetch_log_tail(cluster, f["path"], lines=220)
        _cache_set(_log_content_cache, (cluster, job_id, f["path"]), content)
        pct = extract_progress(content)
        crash = detect_crash(content)
        if crash is not None:
            _cache_set(_crash_cache, (cluster, job_id), crash)
        if pct is not None:
            _cache_set(_progress_cache, (cluster, job_id), pct)
            _cache_set(_progress_source_cache, (cluster, job_id), f.get("label", ""))
            return pct, f.get("label", "")
    return None, ""


def _prefetch_job_data(cluster, job_id):
    enable_standalone_ssh()
    try:
        try:
            log_result = get_job_log_files(cluster, job_id)
            _cache_set(_log_index_cache, (cluster, job_id), log_result)
            files = log_result.get("files", [])
            if files:
                _extract_progress_with_source(cluster, job_id, files)
        except Exception:
            pass
        try:
            stats = get_job_stats(cluster, job_id)
            _cache_set(_stats_cache, (cluster, job_id), stats)
        except Exception:
            pass
    finally:
        with _prefetch_active_lock:
            _prefetch_active[cluster] = max(0, _prefetch_active.get(cluster, 0) - 1)


def fetch_est_start_bulk(cluster, pending_job_ids):
    """Fetch estimated start times for pending jobs via squeue --start."""
    enable_standalone_ssh()
    if not pending_job_ids or cluster == "local":
        return
    ids = [str(j) for j in pending_job_ids if j]
    ids_csv = ",".join(ids)
    try:
        out, _ = ssh_run_with_timeout(
            cluster,
            f'squeue -h -j "{ids_csv}" --start -o "%i|%S" 2>/dev/null',
            timeout_sec=10,
        )
    except Exception:
        return
    for line in out.splitlines():
        parts = line.strip().split("|", 1)
        if len(parts) != 2:
            continue
        jid, start = parts[0].strip(), parts[1].strip()
        if start and start not in ("N/A", "Unknown", "(null)"):
            _cache_set(_est_start_cache, (cluster, jid), start)


def prefetch_cluster_bulk(cluster, job_ids):
    enable_standalone_ssh()
    if cluster == "local" or not job_ids:
        return
    ids = [str(j) for j in job_ids if j]
    ids_csv = ",".join(ids)

    # Bulk stats via SSH (user-filtered squeue call)
    script = f"""#!/bin/sh
squeue -u $USER -h -j "{ids_csv}" -o "%i|%T|%D|%C|%b|%N|%M" | sed 's/^/STAT:/'
"""
    try:
        out, _ = ssh_run_with_timeout(cluster, script, timeout_sec=15)
    except Exception:
        out = ""

    for line in out.splitlines():
        if line.startswith("STAT:"):
            parts = line[5:].split("|")
            if len(parts) >= 7:
                jid = parts[0].strip()
                _cache_set(_stats_cache, (cluster, jid), {
                    "status": "ok", "job_id": jid, "state": parts[1].strip(),
                    "nodes": parts[2].strip(), "cpus": parts[3].strip(),
                    "gres": parts[4].strip(), "node_list": parts[5].strip(),
                    "elapsed": parts[6].strip(), "gpus": [],
                    "ave_cpu": "", "ave_rss": "", "max_rss": "", "max_vmsize": "", "_partial": True,
                })

    # Log discovery per job (SSH scontrol first, mount fallback)
    for jid in ids:
        from .logs import get_job_log_files
        log_result = get_job_log_files(cluster, jid)
        if log_result and log_result.get("files"):
            _cache_set(_log_index_cache, (cluster, jid), log_result)
            _extract_progress_with_source(cluster, jid, log_result["files"])
