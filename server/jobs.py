"""Job fetching, parsing, polling, prefetch, and stats."""

import os
import re
import subprocess
import threading
import time
from datetime import datetime

from .config import (
    APP_ROOT, CLUSTERS, DEFAULT_USER, STATE_ORDER, SQUEUE_FMT, SQUEUE_HDR,
    NEMO_RUN_BASES, RESULT_DIR_NAMES,
    LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE,
    SSH_TIMEOUT, CACHE_FRESH_SEC,
    _cache_lock, _cache, _seen_jobs, _last_polled,
    _cache_get, _cache_set,
    _log_index_cache, _log_content_cache, _stats_cache, _progress_cache,
    _prefetch_last, _warm_lock,
    LOG_INDEX_TTL_SEC, STATS_TTL_SEC, PROGRESS_TTL_SEC, PREFETCH_MIN_GAP_SEC,
    extract_project,
)
from .ssh import ssh_run, ssh_run_with_timeout
from .db import upsert_job, get_db
from .logs import (
    get_job_log_files, fetch_log_tail, extract_progress,
    label_and_sort_files,
)

_DEP_RE = re.compile(r'(after\w*):(\d+)')


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
            result = subprocess.run(
                ["sacct", "-j", job_id, f"--format={fmt}", "--noheader", "-P"],
                capture_output=True, text=True, timeout=5
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run(cluster_name, f"sacct -j {job_id} --format={fmt} --noheader -P 2>/dev/null | head -1")
        if not out:
            return {}
        parts = out.split("|")
        keys = ["jobid", "name", "state", "exit_code", "elapsed", "started", "ended_at"]
        return dict(zip(keys, parts + [""] * len(keys)))
    except Exception:
        return {}


def get_job_stats(cluster, job_id):
    if cluster == "local":
        return {"status": "error", "error": "Stats popup is supported for Slurm clusters only."}
    try:
        sq, _ = ssh_run_with_timeout(cluster, f"squeue -j {job_id} -h -o '%T|%D|%C|%b|%N|%M'", timeout_sec=10)
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

    if not prev_ids and name != "local":
        _reconcile_db_with_squeue(name, current_ids)


def _finalize_gone_job(cluster, job_id, prev_job):
    prev_state = prev_job.get("state", "").upper()
    final = sacct_final(cluster, job_id)
    final_state = (final.get("state", "") or prev_state or "COMPLETED").upper().split()[0]

    record = final if final else {
        "jobid": job_id, "name": prev_job.get("name", ""), "state": final_state,
        "elapsed": prev_job.get("elapsed", ""), "nodes": prev_job.get("nodes", ""),
        "gres": prev_job.get("gres", ""), "partition": prev_job.get("partition", ""),
        "submitted": prev_job.get("submitted", ""), "ended_at": datetime.now().isoformat(),
    }
    if not record.get("ended_at"):
        record["ended_at"] = datetime.now().isoformat()
    upsert_job(cluster, record, terminal=True)


def _reconcile_db_with_squeue(cluster, live_ids):
    con = get_db()
    rows = con.execute(
        """SELECT job_id, state FROM job_history
           WHERE cluster=? AND board_visible=0
             AND state IN ('RUNNING','COMPLETING','PENDING')""",
        (cluster,)
    ).fetchall()
    con.close()
    for row in rows:
        jid = row["job_id"]
        if jid not in live_ids:
            final = sacct_final(cluster, jid)
            final_state = (final.get("state", "") or "COMPLETED").upper().split()[0]
            record = final if final else {"jobid": jid, "state": final_state, "ended_at": datetime.now().isoformat()}
            record.setdefault("jobid", jid)
            record.setdefault("ended_at", datetime.now().isoformat())
            upsert_job(cluster, record, terminal=True)


# ─── Prefetch ────────────────────────────────────────────────────────────────

def schedule_prefetch(cluster, job_id):
    k = (cluster, str(job_id))
    now = time.monotonic()
    with _warm_lock:
        last = _prefetch_last.get(k, 0.0)
        if now - last < PREFETCH_MIN_GAP_SEC:
            return
        _prefetch_last[k] = now
    t = threading.Thread(target=_prefetch_job_data, args=(cluster, str(job_id)), daemon=True)
    t.start()


def _prefetch_job_data(cluster, job_id):
    try:
        log_result = get_job_log_files(cluster, job_id)
        _cache_set(_log_index_cache, (cluster, job_id), log_result)
        files = log_result.get("files", [])
        if files:
            first = files[0]["path"]
            content = fetch_log_tail(cluster, first, lines=220)
            _cache_set(_log_content_cache, (cluster, job_id, first), content)
            pct = extract_progress(content)
            if pct is not None:
                _cache_set(_progress_cache, (cluster, job_id), pct)
    except Exception:
        pass
    try:
        stats = get_job_stats(cluster, job_id)
        _cache_set(_stats_cache, (cluster, job_id), stats)
    except Exception:
        pass


def prefetch_cluster_bulk(cluster, job_ids):
    if cluster == "local" or not job_ids:
        return
    ids = [str(j) for j in job_ids if j]
    ids_csv = ",".join(ids)
    user = CLUSTERS[cluster]["user"]
    script = f"""#!/bin/sh
IDS="{ids_csv}"
USER="{user}"
squeue -h -j "$IDS" -o "%i|%T|%D|%C|%b|%N|%M" | sed 's/^/STAT:/'
for BASE in {" ".join(NEMO_RUN_BASES)}; do
  [ -d "$BASE" ] || continue
  find "$BASE" -maxdepth 5 -name "*sbatch.sh" -type f 2>/dev/null | while read SB; do
    OL=$(grep '#SBATCH --output=' "$SB" 2>/dev/null | head -1)
    [ -z "$OL" ] && continue
    D=$(dirname "$(echo "$OL" | sed 's/.*--output=//' | tr -d ' ')")
    [ -d "$D" ] || continue
    echo "LOGDIR:$D"
    find "$D" -maxdepth 1 -type f 2>/dev/null | sed 's/^/FILE:/'
  done
  break
done
"""
    try:
        out, _ = ssh_run_with_timeout(cluster, script, timeout_sec=20)
    except Exception:
        return

    stat_map = {}
    logdir = ""
    all_files = []
    for line in out.splitlines():
        if line.startswith("STAT:"):
            parts = line[5:].split("|")
            if len(parts) >= 7:
                jid = parts[0].strip()
                stat_map[jid] = {
                    "status": "ok", "job_id": jid, "state": parts[1].strip(),
                    "nodes": parts[2].strip(), "cpus": parts[3].strip(),
                    "gres": parts[4].strip(), "node_list": parts[5].strip(),
                    "elapsed": parts[6].strip(), "gpus": [],
                    "ave_cpu": "", "ave_rss": "", "max_rss": "", "max_vmsize": "", "_partial": True,
                }
        elif line.startswith("LOGDIR:"):
            logdir = line[7:].strip()
        elif line.startswith("FILE:"):
            fp = line[5:].strip()
            if fp:
                all_files.append(fp)

    for jid in ids:
        if jid in stat_map:
            _cache_set(_stats_cache, (cluster, jid), stat_map[jid])

    for jid in ids:
        matched = [p for p in all_files if jid in os.path.basename(p)]
        if matched:
            files = label_and_sort_files(matched)
            dirs = []
            if logdir:
                outdir = os.path.dirname(logdir)
                dirs = [{"label": dn, "path": outdir.rstrip("/") + "/" + dn} for dn in RESULT_DIR_NAMES]
            result = {"files": files, "dirs": dirs}
            _cache_set(_log_index_cache, (cluster, jid), result)
            first = files[0]["path"]
            content = fetch_log_tail(cluster, first, lines=220)
            _cache_set(_log_content_cache, (cluster, jid, first), content)
            pct = extract_progress(content)
            if pct is not None:
                _cache_set(_progress_cache, (cluster, jid), pct)
