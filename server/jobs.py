"""Job fetching, parsing, polling, prefetch, and stats."""

from collections import Counter
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime

from .config import (
    APP_ROOT, CLUSTERS, DEFAULT_USER, STATE_ORDER, SQUEUE_FMT, SQUEUE_HDR,
    LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE,
    PPP_ACCOUNTS, TEAM_MEMBERS,
    SSH_TIMEOUT, CACHE_FRESH_SEC,
    _cache_lock, _cache, _seen_jobs, _last_polled,
    _cache_get, _cache_set,
    _log_index_cache, _log_content_cache, _stats_cache,
    _progress_cache, _progress_source_cache, _crash_cache, _est_start_cache,
    _team_usage_cache,
    _prefetch_last, _warm_lock,
    LOG_INDEX_TTL_SEC, STATS_TTL_SEC, PROGRESS_TTL_SEC, CRASH_TTL_SEC,
    EST_START_TTL_SEC, PREFETCH_MIN_GAP_SEC,
    extract_project,
)
from .ssh import ssh_run, ssh_run_with_timeout, enable_standalone_ssh
from .db import (
    upsert_job, get_db, db_write, get_board_pinned, invalidate_pinned_cache,
    upsert_run, update_run_meta, update_run_times, associate_jobs_to_run, get_run,
    upsert_jobs_batch,
    replace_live_jobs, set_cluster_state, cache_db_put,
)
from .logs import (
    get_job_log_files, fetch_log_tail, extract_progress, detect_crash,
    detect_soft_failure, label_and_sort_files,
)

log = logging.getLogger(__name__)

_DEP_RE = re.compile(r'(after\w*):(\d+)')
_EVAL_PREFIX_RE = re.compile(r'^(eval-[a-z0-9_]+)', re.I)
_stdout_captured = set()
_run_meta_fetched = {}          # (cluster, job_id) -> timestamp
_RUN_META_TTL_SEC = 300
_RUN_NAME_MERGE_GAP_SEC = 300
_STALE_PINNED_ACTIVE_STATES = {"RUNNING", "COMPLETING", "PENDING"}
_SACCT_BATCH_SIZE = 200


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


def _enrich_missing_gres(cluster, jobs):
    """Stamp synthetic gres on jobs where squeue reports N/A.

    Some clusters (e.g. eos) never populate the GRES field in squeue output.
    We resolve the per-node GPU count from the partition cache (already in
    memory, no SSH cost) and write a canonical ``gpu:<N>`` value so every
    downstream consumer — live board summary, table column, DB, run overlay —
    gets correct GPU counts without its own fallback logic.
    """
    if cluster == "local":
        return

    gpn = 0
    try:
        from .partitions import _cache as _part_cache, _lock as _part_lock
        with _part_lock:
            rec = _part_cache.get(cluster)
        parts = rec["data"] if rec else []
        gpn = max(
            (int(p.get("gpus_per_node") or 0) for p in parts
             if not p.get("name", "").startswith("cpu") and p.get("name") not in ("defq", "fake")),
            default=0,
        )
    except Exception:
        pass
    if gpn <= 0:
        gpn = int(CLUSTERS.get(cluster, {}).get("gpus_per_node", 0) or 0) or 8

    for j in jobs:
        gres = (j.get("gres") or "").strip()
        if gres and gres not in ("N/A", "(null)"):
            continue
        part = (j.get("partition") or "").lower()
        if part.startswith("cpu") or part in ("defq", "fake"):
            continue
        j["gres"] = f"gpu:{gpn}"


def fetch_cluster_data(cluster_name):
    try:
        if cluster_name == "local":
            jobs = fetch_jobs_local()
        else:
            jobs = fetch_jobs_remote(cluster_name)
        return {"status": "ok", "jobs": jobs, "updated": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "error", "error": str(e), "jobs": [], "updated": datetime.now().isoformat()}


_SACCT_FMT = "JobID,JobName,State,ExitCode,Elapsed,Start,End"
_SACCT_KEYS = ["jobid", "name", "state", "exit_code", "elapsed", "started", "ended_at"]


def sacct_final(cluster_name, job_id):
    try:
        if cluster_name == "local":
            user = os.environ.get("USER", DEFAULT_USER)
            result = subprocess.run(
                ["sacct", "-u", user, "-j", job_id, f"--format={_SACCT_FMT}", "--noheader", "-P"],
                capture_output=True, text=True, timeout=5
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run(cluster_name, f"sacct -u $USER -j {job_id} --format={_SACCT_FMT} --noheader -P 2>/dev/null | head -1")
        if not out:
            return {}
        parts = out.split("|")
        return dict(zip(_SACCT_KEYS, parts + [""] * len(_SACCT_KEYS)))
    except Exception:
        return {}


def sacct_final_batch(cluster_name, job_ids):
    """Fetch sacct data for multiple jobs in a single SSH call.

    Returns a dict mapping job_id -> parsed record (same format as sacct_final).
    """
    if not job_ids:
        return {}
    ids_str = ",".join(str(j) for j in job_ids)
    try:
        if cluster_name == "local":
            user = os.environ.get("USER", DEFAULT_USER)
            result = subprocess.run(
                ["sacct", "-u", user, "-j", ids_str, f"--format={_SACCT_FMT}", "--noheader", "-P"],
                capture_output=True, text=True, timeout=10,
            )
            out = result.stdout.strip()
        else:
            out, _ = ssh_run_with_timeout(
                cluster_name,
                f"sacct -u $USER -j {ids_str} --format={_SACCT_FMT} --noheader -P 2>/dev/null",
                timeout_sec=15,
            )
    except Exception:
        return {}

    results = {}
    for line in (out or "").splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        record = dict(zip(_SACCT_KEYS, parts + [""] * len(_SACCT_KEYS)))
        jid = record.get("jobid", "")
        if "." in jid:
            continue
        if jid and jid not in results:
            results[jid] = record
    return results


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

        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _fetch_sstat_cpu():
            out, _ = ssh_run_with_timeout(cluster, f"sstat -j {job_id}.batch --noheader -P --format=AveCPU,AveRSS,MaxRSS,MaxVMSize 2>/dev/null | head -1", timeout_sec=10)
            return (out.split("|") + ["", "", "", ""])[:4] if out else ("", "", "", "")

        def _fetch_sstat_tres():
            out, _ = ssh_run_with_timeout(cluster, f"sstat -j {job_id}.batch --noheader -P --format=TresUsageInAve,TresUsageInMax 2>/dev/null | head -1", timeout_sec=10)
            return out.strip()

        def _fetch_gpu_probe():
            cluster_has_gpus = bool(CLUSTERS.get(cluster, {}).get("gpu_type"))
            gres_mentions_gpu = "gpu" in (gres or "").lower()
            if not ((gres_mentions_gpu or cluster_has_gpus) and state in ("RUNNING", "COMPLETING")):
                return [], "", gres_mentions_gpu, cluster_has_gpus
            gpu_cmd = (f"srun --jobid {job_id} -N1 -n1 --overlap "
                       "bash -lc \"nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used,memory.total "
                       "--format=csv,noheader,nounits\" 2>/dev/null | head -16")
            gpu_out, gpu_err = ssh_run_with_timeout(cluster, gpu_cmd, timeout_sec=20)
            rows = []
            for line in gpu_out.splitlines():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 5:
                    rows.append({"index": parts[0], "name": parts[1], "util": parts[2] + "%", "mem": f"{parts[3]}/{parts[4]} MiB"})
            return rows, gpu_err if not rows else "", gres_mentions_gpu, cluster_has_gpus

        with ThreadPoolExecutor(max_workers=3) as pool:
            f_cpu = pool.submit(_fetch_sstat_cpu)
            f_tres = pool.submit(_fetch_sstat_tres)
            f_gpu = pool.submit(_fetch_gpu_probe)

        ave_cpu, ave_rss, max_rss, max_vms = f_cpu.result()
        tres_usage_text = f_tres.result()
        gpu_rows, gpu_probe_error, gres_mentions_gpu, cluster_has_gpus = f_gpu.result()

        def _extract_tres_value(text, key):
            if not text:
                return ""
            for token in text.replace(" ", "").split(","):
                if token.startswith(key + "="):
                    return token.split("=", 1)[1]
            return ""

        gpuutil_ave = _extract_tres_value(tres_usage_text, "gres/gpuutil")
        gpumem_ave = _extract_tres_value(tres_usage_text, "gres/gpumem")

        gpu_summary = ""
        if not gpu_rows:
            if gpuutil_ave or gpumem_ave:
                gpu_summary = f"Ave GPU util: {gpuutil_ave or 'n/a'} | Ave GPU mem: {gpumem_ave or 'n/a'}"
            elif (gres_mentions_gpu or cluster_has_gpus) and state in ("RUNNING", "COMPLETING"):
                gpu_summary = "Per-GPU probe unavailable."
            elif gres_mentions_gpu or cluster_has_gpus:
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
    if value.get("status") == "ok":
        _cache_set(_stats_cache, key, value)
        try:
            cache_db_put("stats", f"{cluster}:{job_id}", value, STATS_TTL_SEC)
        except Exception:
            pass
    return value


# ─── Run detection & metadata ────────────────────────────────────────────────

_STAGE_SUFFIX_RE = re.compile(
    r'(?:-|_)(?:'
    r'(?:probes?|sep)[-_](?:server|l\d+)'
    r'|(?:paths?|server)[-_](?:probes?|paths?)'
    r'|path[-_](?:analytical|computational|knowledge)(?:-c\d+)?'
    r'|paths?[-_]server'
    r'|merge[-_](?:analytical|computational|knowledge)'
    r'|(?:eval[-_])?judge[-_](?:server|client|eval)'
    r'|gate(?:[-_](?:classify|prep))?'
    r'|chunk\d+'
    r'|server'
    r'|summarize(?:[-_]results?)?'
    r'|judge(?:[-_]rs\d+)?'
    r'|rs\d+(?:[-_]c\d+)?'
    r')$', re.I,
)


def _group_key_for_job(name):
    """Server-side equivalent of the frontend's groupKeyForJob()."""
    n = (name or "").strip()
    if not n:
        return "misc"
    m = _EVAL_PREFIX_RE.match(n)
    if m:
        return m.group(1).lower()
    prev = None
    while prev != n:
        prev = n
        n = _STAGE_SUFFIX_RE.sub('', n)
    return n.lower()


def _job_group_ts(job):
    """Best-effort timestamp for separating reruns with the same base name."""
    for key in ("submitted", "started", "ended_at"):
        raw = str(job.get(key, "") or "").strip()
        if not raw or raw in {"N/A", "Unknown", "None", "(null)"}:
            continue
        try:
            return datetime.fromisoformat(raw.replace(" ", "T")).timestamp()
        except ValueError:
            continue
    return None


def _bucket_same_name_jobs(jobs, gap_sec=_RUN_NAME_MERGE_GAP_SEC):
    """Split same-name jobs into submission-time buckets.

    This keeps reruns that reuse the exact same Slurm job name from being
    collapsed into one logical run just because their base names match.
    """
    if len(jobs) <= 1:
        return [jobs]

    stamped = []
    unstamped = []
    for job in jobs:
        ts = _job_group_ts(job)
        if ts is None:
            unstamped.append(job)
        else:
            stamped.append((ts, str(job.get("jobid", "")), job))

    if not stamped:
        return [jobs]

    stamped.sort(key=lambda item: (item[0], item[1]))
    buckets = [[stamped[0][2]]]
    prev_ts = stamped[0][0]
    for ts, _, job in stamped[1:]:
        if ts - prev_ts > gap_sec:
            buckets.append([])
        buckets[-1].append(job)
        prev_ts = ts

    # Missing timestamps are rare in practice; attach them to the nearest
    # active bucket so dependency-based grouping can still pull them in.
    for job in unstamped:
        buckets[-1].append(job)
    return buckets


def _buckets_separated_by_time_gap(bucket_a, bucket_b, gap_sec=_RUN_NAME_MERGE_GAP_SEC):
    """True if two job buckets are clearly different submission-time waves."""
    times_a = [_job_group_ts(j) for j in bucket_a if _job_group_ts(j) is not None]
    times_b = [_job_group_ts(j) for j in bucket_b if _job_group_ts(j) is not None]
    if not times_a or not times_b:
        return False
    max_a = max(times_a)
    min_b = min(times_b)
    max_b = max(times_b)
    min_a = min(times_a)
    return (min_b - max_a > gap_sec) or (min_a - max_b > gap_sec)


def _group_jobs_for_runs(jobs, cluster=None):
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
        name_groups.setdefault(key, []).append(j)

    # Look up existing run_id assignments so resubmissions (skip_filled)
    # that reuse the same job name merge into the original run even when
    # the submission-time gap exceeds _RUN_NAME_MERGE_GAP_SEC.
    existing_run_for_job = {}
    if cluster:
        all_ids = [j["jobid"] for j in jobs]
        if all_ids:
            con = get_db()
            ph = ",".join("?" for _ in all_ids)
            rows = con.execute(
                f"SELECT job_id, run_id FROM job_history "
                f"WHERE cluster=? AND job_id IN ({ph}) AND run_id IS NOT NULL AND run_id != ''",
                [cluster] + all_ids,
            ).fetchall()
            con.close()
            for r in rows:
                existing_run_for_job[r["job_id"]] = r["run_id"]

    for same_name_jobs in name_groups.values():
        buckets = _bucket_same_name_jobs(same_name_jobs)

        # Merge buckets that share an existing run_id — this handles
        # resubmissions where the time gap is large but the jobs belong
        # to the same logical experiment.
        if len(buckets) > 1 and existing_run_for_job:
            run_to_bucket = {}
            for bi, bucket in enumerate(buckets):
                for job in bucket:
                    rid = existing_run_for_job.get(job["jobid"])
                    if rid and rid not in run_to_bucket:
                        run_to_bucket[rid] = bi
            # Merge buckets that reference the same DB run_id (e.g. skip_filled
            # resubmits with a large time gap). Leave unrelated buckets separate.
            if run_to_bucket:
                anchor_bi = next(iter(run_to_bucket.values()))
                anchor_snapshot = list(buckets[anchor_bi])
                merged = list(anchor_snapshot)
                tail_buckets = []
                for bi, bucket in enumerate(buckets):
                    if bi == anchor_bi:
                        continue
                    bucket_rids = {
                        existing_run_for_job.get(j["jobid"])
                        for j in bucket
                    } - {None}
                    share_overlap = bool(bucket_rids & set(run_to_bucket))
                    time_gap = _buckets_separated_by_time_gap(anchor_snapshot, bucket)
                    orphan_bucket = not bucket_rids
                    if share_overlap and time_gap:
                        tail_buckets.append(bucket)
                    elif share_overlap or (orphan_bucket and not time_gap):
                        merged.extend(bucket)
                    elif orphan_bucket and time_gap:
                        tail_buckets.append(bucket)
                    else:
                        ids = [job["jobid"] for job in bucket]
                        for k in range(1, len(ids)):
                            union(ids[0], ids[k])
                buckets = [merged] + tail_buckets

        for bucket in buckets:
            ids = [job["jobid"] for job in bucket]
            for i in range(1, len(ids)):
                union(ids[0], ids[i])

    # Union across name groups: jobs that already share a run_id in the DB
    # belong together even when their names produce different group keys
    # (e.g. multi-benchmark submissions under one SDK run).
    # Only merge jobs whose group keys differ — within the same group key,
    # time-bucketing already decided whether they belong together.
    if existing_run_for_job:
        job_group_key = {j["jobid"]: _group_key_for_job(j.get("name", "")) for j in jobs}
        run_to_first = {}
        for jid, rid in existing_run_for_job.items():
            if jid not in by_id:
                continue
            if rid in run_to_first:
                first_jid = run_to_first[rid]
                if job_group_key.get(jid) != job_group_key.get(first_jid):
                    union(jid, first_jid)
            else:
                run_to_first[rid] = jid

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
    groups = _group_jobs_for_runs(jobs, cluster=cluster)

    con = get_db()
    all_job_ids = [jid for _, _, jids in groups for jid in jids]
    existing_run_ids = {}
    existing_run_roots = {}
    if all_job_ids:
        ph = ",".join("?" for _ in all_job_ids)
        rows = con.execute(
            f"SELECT job_id, run_id FROM job_history WHERE cluster=? AND job_id IN ({ph}) AND run_id IS NOT NULL",
            [cluster] + all_job_ids,
        ).fetchall()
        for r in rows:
            if r["run_id"]:
                existing_run_ids[r["job_id"]] = r["run_id"]
        run_ids = sorted(set(existing_run_ids.values()))
        if run_ids:
            ph = ",".join("?" for _ in run_ids)
            run_rows = con.execute(
                f"SELECT id, root_job_id FROM runs WHERE cluster=? AND id IN ({ph})",
                [cluster] + run_ids,
            ).fetchall()
            existing_run_roots = {row["id"]: row["root_job_id"] for row in run_rows}
    con.close()

    group_run_counters = [
        Counter(
            existing_run_ids[jid]
            for jid in job_ids
            if existing_run_ids.get(jid)
        )
        for _, _, job_ids in groups
    ]
    run_owner_idx = {}
    for rid, root_jid in existing_run_roots.items():
        root_owner = next(
            (idx for idx, (_, _, job_ids) in enumerate(groups) if root_jid in job_ids),
            None,
        )
        if root_owner is not None:
            run_owner_idx[rid] = root_owner
            continue
        best_idx = None
        best_score = None
        for idx, counter in enumerate(group_run_counters):
            count = counter.get(rid, 0)
            if not count:
                continue
            score = (count, len(groups[idx][2]))
            if best_score is None or score > best_score:
                best_idx = idx
                best_score = score
        if best_idx is not None:
            run_owner_idx[rid] = best_idx

    for idx, (run_name, root_job_id, job_ids) in enumerate(groups):
        root_job = next((j for j in jobs if j["jobid"] == root_job_id), None)
        project = root_job.get("project", "") if root_job else ""
        rid_counts = Counter(
            {
                rid: count
                for rid, count in group_run_counters[idx].items()
                if run_owner_idx.get(rid) == idx
            }
        )
        canonical_root_job_id = root_job_id
        if rid_counts:
            canonical_run_id, _ = rid_counts.most_common(1)[0]
            canonical_root_job_id = existing_run_roots.get(canonical_run_id, root_job_id)

        run_id = upsert_run(cluster, canonical_root_job_id, run_name, project)
        associate_jobs_to_run(cluster, run_id, job_ids)

        started = min(
            (
                ts for ts in (
                    (job.get("started") or job.get("submitted") or "")
                    for job in jobs if job["jobid"] in job_ids
                )
                if ts and ts not in {"N/A", "Unknown", "None", "(null)"}
            ),
            default=None,
        )
        if started:
            update_run_times(run_id, started_at=started)

        key = (cluster, canonical_root_job_id)
        cached_ts = _run_meta_fetched.get(key)
        if cached_ts is None or (time.monotonic() - cached_ts) > _RUN_META_TTL_SEC:
            existing = get_run(cluster, canonical_root_job_id)
            if existing and not existing.get("meta_fetched"):
                # SDK runs initially hold a synthetic "sdk-<uuid>" root; skip
                # the scontrol/sacct capture until adoption swaps it for a
                # real Slurm id, at which point this check will re-fire.
                if str(canonical_root_job_id).isdigit():
                    _run_meta_fetched[key] = time.monotonic()
                    t = threading.Thread(
                        target=_capture_run_metadata,
                        args=(cluster, canonical_root_job_id, run_id),
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

_SDK_POLL_MULTIPLIER = 4


def _is_cache_fresh(cluster_name):
    ts = _last_polled.get(cluster_name, 0.0)
    ttl = CACHE_FRESH_SEC
    if _cluster_is_sdk_only(cluster_name):
        ttl = CACHE_FRESH_SEC * _SDK_POLL_MULTIPLIER
    return (time.monotonic() - ts) < ttl


def _cluster_is_sdk_only(cluster_name):
    """True if all board-visible jobs for this cluster are SDK-tracked."""
    try:
        con = get_db()
        total = con.execute(
            "SELECT COUNT(*) as c FROM job_history WHERE cluster=? AND board_visible=1",
            (cluster_name,),
        ).fetchone()["c"]
        if total == 0:
            con.close()
            return False
        sdk_count = con.execute(
            """SELECT COUNT(*) as c FROM job_history jh
               JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster
               WHERE jh.cluster=? AND jh.board_visible=1 AND r.source='sdk'""",
            (cluster_name,),
        ).fetchone()["c"]
        con.close()
        return sdk_count == total
    except Exception:
        return False


_poll_inflight = {}            # cluster -> start timestamp
_poll_inflight_lock = threading.Lock()
_POLL_TIMEOUT_SEC = 30


def _start_poll(name):
    """Start a poll thread for *name* if one isn't already running.

    A stuck poll is automatically evicted after _POLL_TIMEOUT_SEC so a
    broken cluster can never permanently block its own refresh cycle.
    """
    now = time.monotonic()
    with _poll_inflight_lock:
        started_at = _poll_inflight.get(name)
        if started_at is not None:
            if now - started_at < _POLL_TIMEOUT_SEC:
                return
            _poll_inflight.pop(name, None)
        _poll_inflight[name] = now
    _last_polled[name] = now

    def _run():
        try:
            poll_cluster(name)
        finally:
            with _poll_inflight_lock:
                _poll_inflight.pop(name, None)

    threading.Thread(target=_run, daemon=True).start()


def refresh_all_clusters():
    """Kick off background refreshes for stale clusters.  Never blocks."""
    for name in CLUSTERS:
        if not _is_cache_fresh(name):
            _start_poll(name)


def refresh_cluster(cluster_name):
    """Kick off a background refresh for one cluster.  Never blocks."""
    if not _is_cache_fresh(cluster_name):
        _start_poll(cluster_name)


def prune_job_sets():
    """Remove entries from unbounded sets for jobs no longer tracked."""
    active = set()
    for ids in _seen_jobs.values():
        for jid in ids:
            active.add(jid)
    stale_captured = {k for k in _stdout_captured if k[1] not in active}
    _stdout_captured.difference_update(stale_captured)
    stale_prefetch = {k for k in _prefetch_last if k[1] not in active}
    for k in stale_prefetch:
        _prefetch_last.pop(k, None)
    stale_meta = {k for k in _run_meta_fetched if k[1] not in active}
    for k in stale_meta:
        _run_meta_fetched.pop(k, None)


_bookkeeping_lock = threading.Lock()
_bookkeeping_pending = {}   # cluster -> latest context dict
_bookkeeping_running = set()


def _schedule_cluster_bookkeeping(cluster, context):
    """Queue slow DB/SSH bookkeeping without blocking the live poll path."""
    with _bookkeeping_lock:
        _bookkeeping_pending[cluster] = context
        if cluster in _bookkeeping_running:
            return
        _bookkeeping_running.add(cluster)
    threading.Thread(
        target=_cluster_bookkeeping_worker,
        args=(cluster,),
        daemon=True,
        name=f"bookkeeping-{cluster}",
    ).start()


def _cluster_bookkeeping_worker(cluster):
    enable_standalone_ssh()
    while True:
        with _bookkeeping_lock:
            context = _bookkeeping_pending.pop(cluster, None)
            if context is None:
                _bookkeeping_running.discard(cluster)
                return
        try:
            _run_cluster_bookkeeping(cluster, context)
        except Exception:
            log.exception("bookkeeping failed for %s", cluster)


def _get_sdk_run_job_ids(cluster, job_ids):
    """Return the subset of job_ids that belong to an SDK-tracked run."""
    if not job_ids:
        return set()
    try:
        con = get_db()
        placeholders = ",".join("?" for _ in job_ids)
        rows = con.execute(
            f"""SELECT jh.job_id FROM job_history jh
                JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster
                WHERE jh.cluster=? AND jh.job_id IN ({placeholders}) AND r.source='sdk'""",
            [cluster] + list(job_ids),
        ).fetchall()
        con.close()
        return {r["job_id"] for r in rows}
    except Exception:
        return set()


def _get_sdk_run_jobs_for_stdout(cluster, job_ids):
    """Return job_ids that belong to SDK runs with primary_output_dir already set."""
    if not job_ids:
        return set()
    try:
        con = get_db()
        placeholders = ",".join("?" for _ in job_ids)
        rows = con.execute(
            f"""SELECT jh.job_id FROM job_history jh
                JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster
                WHERE jh.cluster=? AND jh.job_id IN ({placeholders})
                  AND r.source='sdk' AND r.primary_output_dir != ''""",
            [cluster] + list(job_ids),
        ).fetchall()
        con.close()
        return {r["job_id"] for r in rows}
    except Exception:
        return set()


def _run_cluster_bookkeeping(cluster, context):
    started = time.monotonic()
    live_jobs = list(context.get("live_jobs", []))
    current_ids = set(context.get("current_ids", set()))
    prev_jobs = dict(context.get("prev_jobs", {}))
    prev_ids = set(context.get("prev_ids", set()))
    first_poll = bool(context.get("first_poll"))

    gone_ids = prev_ids - current_ids
    if gone_ids:
        sdk_job_ids = _get_sdk_run_job_ids(cluster, gone_ids) if cluster != "local" else set()
        non_sdk_gone = [jid for jid in gone_ids if jid not in sdk_job_ids]
        sacct_batch = sacct_final_batch(cluster, non_sdk_gone) if non_sdk_gone and cluster != "local" else {}
        for job_id in gone_ids:
            if job_id in sdk_job_ids:
                continue
            _finalize_gone_job(
                cluster,
                job_id,
                prev_jobs.get(job_id, {}),
                sacct_record=sacct_batch.get(job_id),
            )

    if cluster != "local":
        upsert_jobs_batch(cluster, live_jobs, terminal=False)
        _reconcile_stale_pinned_active_rows(cluster, current_ids)

        running_ids = [
            job["jobid"] for job in live_jobs
            if job.get("state", "").upper() in ("RUNNING", "COMPLETING")
        ]
        uncaptured = [jid for jid in running_ids if (cluster, jid) not in _stdout_captured]
        if uncaptured:
            sdk_covered = _get_sdk_run_jobs_for_stdout(cluster, uncaptured)
            non_sdk_uncaptured = [jid for jid in uncaptured if jid not in sdk_covered]
            if non_sdk_uncaptured:
                _capture_stdout_paths(cluster, non_sdk_uncaptured)

        all_jobs_for_runs = list(live_jobs)
        pinned = get_board_pinned(cluster)
        live_id_set = {job["jobid"] for job in all_jobs_for_runs}
        for pinned_job in pinned:
            pid = pinned_job.get("job_id", "")
            if pid and pid not in live_id_set:
                all_jobs_for_runs.append({
                    "jobid": pid,
                    "name": pinned_job.get("job_name", ""),
                    "depends_on": pinned_job.get("depends_on", []),
                    "dep_details": pinned_job.get("dep_details", []),
                    "dependents": pinned_job.get("dependents", []),
                    "project": pinned_job.get("project", ""),
                    "state": pinned_job.get("state", ""),
                    "started": pinned_job.get("started", ""),
                    "submitted": pinned_job.get("submitted", ""),
                })
        _detect_and_register_runs(cluster, all_jobs_for_runs)

        if first_poll:
            _reconcile_db_with_squeue(cluster, current_ids)

    if not _softfail_migrated:
        _schedule_softfail_migration()

    log.debug(
        "bookkeeping cluster=%s live=%d gone=%d duration_ms=%d",
        cluster,
        len(live_jobs),
        len(gone_ids),
        round((time.monotonic() - started) * 1000),
    )


def poll_cluster(name):
    started = time.monotonic()
    data = fetch_cluster_data(name)

    if data["status"] == "error":
        with _cache_lock:
            prev = _cache.get(name, {})
            if prev.get("jobs"):
                prev["last_error"] = data["error"]
                prev["updated"] = data["updated"]
            else:
                _cache[name] = data
        has_live = bool(prev.get("jobs"))
        prev_updated = prev.get("updated")
        if not has_live:
            con = get_db()
            state_row = con.execute(
                "SELECT updated FROM cluster_state WHERE cluster=?",
                (name,),
            ).fetchone()
            has_live = bool(con.execute(
                "SELECT 1 FROM live_jobs WHERE cluster=? LIMIT 1", (name,),
            ).fetchone())
            con.close()
            if state_row and state_row["updated"]:
                prev_updated = state_row["updated"]

        if has_live:
            set_cluster_state(name, "ok", prev_updated or data["updated"], last_error=data.get("error"))
        else:
            set_cluster_state(name, "error", data["updated"], last_error=data.get("error"))

        duration_ms = round((time.monotonic() - started) * 1000)
        log.warning("poll_cluster error cluster=%s duration_ms=%d error=%s",
                    name, duration_ms, data.get("error"))
        return {
            "status": "error",
            "cluster": name,
            "updated": data["updated"],
            "error": data.get("error", "poll failed"),
            "duration_ms": duration_ms,
        }

    _enrich_missing_gres(name, data.get("jobs", []))
    data.pop("last_error", None)
    current_ids = {j["jobid"] for j in data.get("jobs", [])}

    with _cache_lock:
        prev_ids = _seen_jobs.get(name, set())
        prev_jobs = {j["jobid"]: j for j in _cache.get(name, {}).get("jobs", [])}
        _cache[name] = data
        _seen_jobs[name] = current_ids

    replace_live_jobs(name, data.get("jobs", []))
    set_cluster_state(name, "ok", data["updated"])

    if name != "local":
        _schedule_cluster_bookkeeping(name, {
            "live_jobs": data.get("jobs", []),
            "current_ids": current_ids,
            "prev_jobs": prev_jobs,
            "prev_ids": prev_ids,
            "first_poll": not prev_ids,
        })
    elif not _softfail_migrated:
        _schedule_softfail_migration()

    duration_ms = round((time.monotonic() - started) * 1000)
    gone_ids = prev_ids - current_ids
    log.debug(
        "poll_cluster ok cluster=%s live=%d gone=%d duration_ms=%d bookkeeping=%s",
        name,
        len(data.get("jobs", [])),
        len(gone_ids),
        duration_ms,
        "queued" if name != "local" else "none",
    )
    return {
        "status": "ok",
        "cluster": name,
        "updated": data["updated"],
        "live_jobs": len(data.get("jobs", [])),
        "gone_jobs": len(gone_ids),
        "bookkeeping": "queued" if name != "local" else "none",
        "duration_ms": duration_ms,
    }


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

    with db_write() as con:
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


def _finalize_gone_job(cluster, job_id, prev_job, sacct_record=None):
    prev_state = prev_job.get("state", "").upper()
    prev_reason = prev_job.get("reason", "")
    final = sacct_record if sacct_record is not None else sacct_final(cluster, job_id)
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


def _sacct_final_batched(cluster, job_ids, batch_size=_SACCT_BATCH_SIZE):
    """Fetch sacct results in bounded batches to avoid giant commands."""
    ids = [str(job_id) for job_id in job_ids if job_id]
    results = {}
    for i in range(0, len(ids), batch_size):
        results.update(sacct_final_batch(cluster, ids[i:i + batch_size]))
    return results


def _hide_pinned_jobs(cluster, job_ids):
    if not job_ids:
        return
    with db_write() as con:
        placeholders = ",".join("?" for _ in job_ids)
        con.execute(
            f"UPDATE job_history SET board_visible=0 WHERE cluster=? AND job_id IN ({placeholders})",
            (cluster, *job_ids),
        )
    invalidate_pinned_cache(cluster)


def _reconcile_stale_pinned_active_rows(cluster, live_ids):
    """Repair stale board rows that remain pinned with active states."""
    pinned = get_board_pinned(cluster)
    stale = [
        row for row in pinned
        if str(row.get("state", "")).upper() in _STALE_PINNED_ACTIVE_STATES
        and str(row.get("job_id") or row.get("jobid") or "") not in live_ids
    ]
    if not stale:
        return

    finals = _sacct_final_batched(
        cluster,
        [row.get("job_id") or row.get("jobid") for row in stale],
    )
    terminal_records = []
    hide_ids = []

    for row in stale:
        jid = str(row.get("job_id") or row.get("jobid") or "")
        if not jid:
            continue
        final = finals.get(jid, {})
        parts = (final.get("state", "") or "").upper().split()
        final_state = parts[0] if parts else ""
        if not final_state or final_state in _STALE_PINNED_ACTIVE_STATES:
            hide_ids.append(jid)
            continue

        record = dict(final)
        record.setdefault("jobid", jid)
        if not record.get("name"):
            record["name"] = row.get("job_name") or row.get("name") or ""
        if not record.get("reason") and final.get("state") and " " in final["state"]:
            record["reason"] = final["state"]
        for key in (
            "log_path", "submitted", "started", "elapsed", "nodes", "gres",
            "partition", "reason", "exit_code", "dependency", "project",
            "node_list", "account",
        ):
            if row.get(key) and not record.get(key):
                record[key] = row[key]
        record["state"] = final_state
        record.setdefault("ended_at", datetime.now().isoformat())
        terminal_records.append(record)

    if terminal_records:
        upsert_jobs_batch(cluster, terminal_records, terminal=True)
    if hide_ids:
        _hide_pinned_jobs(cluster, hide_ids)


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
    with db_write() as con:
        con.execute(
            "UPDATE job_history SET state='COMPLETED', reason=? "
            "WHERE cluster=? AND job_id=?",
            (f"soft-fail: {soft_reason}", cluster, job_id),
        )


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
    with _prefetch_active_lock:
        if _prefetch_active.get(cluster, 0) >= _MAX_PREFETCH_THREADS:
            return
        _prefetch_active[cluster] = _prefetch_active.get(cluster, 0) + 1
    with _warm_lock:
        _prefetch_last[k] = now
    t = threading.Thread(target=_prefetch_job_data, args=(cluster, str(job_id)), daemon=True)
    t.start()


_LOG_ERROR_PREFIXES = ("Could not read log:", "File not found on cluster:", "Invalid local process")

def _extract_progress_with_source(cluster, job_id, files):
    """Try files in order, return (pct, label) from the first file with progress."""
    for f in files:
        content = fetch_log_tail(cluster, f["path"], lines=220)
        if not any(content.startswith(p) for p in _LOG_ERROR_PREFIXES):
            _cache_set(_log_content_cache, (cluster, job_id, f["path"]), content)
        pct = extract_progress(content)
        crash = detect_crash(content)
        if crash is not None:
            _cache_set(_crash_cache, (cluster, job_id), crash)
            try:
                cache_db_put("crash", f"{cluster}:{job_id}", crash, CRASH_TTL_SEC)
            except Exception:
                pass
        if pct is not None:
            _cache_set(_progress_cache, (cluster, job_id), pct)
            src = f.get("label", "")
            _cache_set(_progress_source_cache, (cluster, job_id), src)
            try:
                cache_db_put("progress", f"{cluster}:{job_id}", pct, PROGRESS_TTL_SEC)
                cache_db_put("progress_source", f"{cluster}:{job_id}", src, PROGRESS_TTL_SEC)
            except Exception:
                pass
            return pct, src
    return None, ""


def _get_stats_interval():
    from .config import STATS_INTERVAL_SEC
    return STATS_INTERVAL_SEC


def _parse_rss_bytes(val):
    """Parse sstat RSS values like '1234K', '56M', '7G' into MB."""
    if not val:
        return None
    val = val.strip().rstrip("c")
    m = re.match(r'^([\d.]+)\s*([KMGTP]?)$', val, re.I)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2).upper()
    mult = {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024**2, "P": 1024**3}
    return round(num * mult.get(suffix, 1), 2)


def _save_stats_snapshot(cluster, job_id, stats):
    """Save a stats snapshot to DB if enough time has passed since the last one."""
    if not stats or stats.get("status") != "ok":
        return
    import json
    with db_write() as con:
        row = con.execute(
            "SELECT MAX(ts) as last_ts FROM job_stats_snapshots WHERE cluster = ? AND job_id = ?",
            (cluster, str(job_id)),
        ).fetchone()
        if row and row["last_ts"]:
            try:
                last = datetime.fromisoformat(row["last_ts"])
                if (datetime.now() - last).total_seconds() < _get_stats_interval():
                    return
            except Exception:
                pass

        gpu_rows = stats.get("gpus", [])
        gpu_util = None
        gpu_mem_used = None
        gpu_mem_total = None
        if gpu_rows:
            utils = []
            mems_used = []
            mems_total = []
            for g in gpu_rows:
                try:
                    utils.append(float(str(g.get("util", "0")).rstrip("%")))
                except (ValueError, TypeError):
                    pass
                mem = g.get("mem", "")
                if "/" in mem:
                    parts = mem.replace("MiB", "").strip().split("/")
                    try:
                        mems_used.append(float(parts[0].strip()))
                        mems_total.append(float(parts[1].strip()))
                    except (ValueError, IndexError):
                        pass
            if utils:
                gpu_util = round(sum(utils) / len(utils), 1)
            if mems_used:
                gpu_mem_used = round(sum(mems_used) / len(mems_used), 1)
            if mems_total:
                gpu_mem_total = round(sum(mems_total) / len(mems_total), 1)

        cpu_util = stats.get("ave_cpu", "") or ""
        rss_used = _parse_rss_bytes(stats.get("ave_rss", ""))
        max_rss = _parse_rss_bytes(stats.get("max_rss", ""))

        now = datetime.now().isoformat(timespec="seconds")
        con.execute(
            """INSERT INTO job_stats_snapshots
               (cluster, job_id, ts, gpu_util, gpu_mem_used, gpu_mem_total, cpu_util, rss_used, max_rss, gpu_details)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (cluster, str(job_id), now, gpu_util, gpu_mem_used, gpu_mem_total,
             cpu_util, rss_used, max_rss, json.dumps(gpu_rows) if gpu_rows else ""),
        )


def get_stats_snapshots(cluster, job_id):
    """Return historical stats snapshots for a job."""
    import json as _json
    con = get_db()
    rows = con.execute(
        """SELECT ts, gpu_util, gpu_mem_used, gpu_mem_total, cpu_util, rss_used, max_rss, gpu_details
           FROM job_stats_snapshots
           WHERE cluster = ? AND job_id = ?
           ORDER BY ts ASC""",
        (cluster, str(job_id)),
    ).fetchall()
    con.close()
    result = []
    for r in rows:
        d = dict(r)
        raw = d.pop("gpu_details", "") or ""
        try:
            d["per_gpu"] = _json.loads(raw) if raw else []
        except Exception:
            d["per_gpu"] = []
        result.append(d)
    return result


def _prefetch_job_data(cluster, job_id):
    enable_standalone_ssh()
    try:
        try:
            # Fast path: use db log_path and local mount to avoid expensive log discovery
            from .logs import _db_log_context, tail_local_file
            from .mounts import resolve_mounted_path
            db_path = _db_log_context(cluster, job_id).get("log_path", "")
            fast_pct = None
            if db_path:
                db_path = db_path.replace("%j", str(job_id))
                mt = resolve_mounted_path(cluster, db_path, want_dir=False)
                if mt and os.path.isfile(mt):
                    content = tail_local_file(mt, lines=220)
                    if content and not any(content.startswith(p) for p in _LOG_ERROR_PREFIXES):
                        _cache_set(_log_content_cache, (cluster, job_id, db_path), content)
                    fast_pct = extract_progress(content)
                    crash = detect_crash(content)
                    
                    if crash is not None:
                        _cache_set(_crash_cache, (cluster, job_id), crash)
                        try:
                            cache_db_put("crash", f"{cluster}:{job_id}", crash, CRASH_TTL_SEC)
                        except Exception:
                            pass
                    
                    if fast_pct is not None:
                        _cache_set(_progress_cache, (cluster, job_id), fast_pct)
                        src = "main output"
                        _cache_set(_progress_source_cache, (cluster, job_id), src)
                        try:
                            cache_db_put("progress", f"{cluster}:{job_id}", fast_pct, PROGRESS_TTL_SEC)
                            cache_db_put("progress_source", f"{cluster}:{job_id}", src, PROGRESS_TTL_SEC)
                        except Exception:
                            pass
            
            # If fast path failed to find progress, fall back to discovering all log files
            if fast_pct is None:
                log_result = get_job_log_files(cluster, job_id)
                if log_result and not log_result.get("error"):
                    _cache_set(_log_index_cache, (cluster, job_id), log_result)
                files = (log_result or {}).get("files", [])
                if files:
                    _extract_progress_with_source(cluster, job_id, files)
        except Exception as e:
            log.error(f"Error in prefetch fast path for {cluster}/{job_id}: {e}", exc_info=True)
            pass
        try:
            stats = get_job_stats(cluster, job_id)
            if stats.get("status") == "ok":
                _cache_set(_stats_cache, (cluster, job_id), stats)
                _save_stats_snapshot(cluster, job_id, stats)
                try:
                    cache_db_put("stats", f"{cluster}:{job_id}", stats, STATS_TTL_SEC)
                except Exception:
                    pass
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
            try:
                cache_db_put("est_start", f"{cluster}:{jid}", start, EST_START_TTL_SEC)
            except Exception:
                pass


_detected_accounts = {}

def fetch_team_usage(cluster):
    """Fetch per-user GPU breakdown for the team's Slurm account on a cluster.

    Auto-detects the account from the user's own jobs if not configured.
    Caches detected accounts in memory so detection only runs once.
    Returns {account, users: {user: {running_gpus, pending_gpus}}, total_running, total_pending}.
    """
    enable_standalone_ssh()
    if cluster == "local":
        return None

    cfg = CLUSTERS.get(cluster, {})
    gpus_per_node = cfg.get("gpus_per_node", 8) or 8
    account = cfg.get("account", "") or _detected_accounts.get(cluster, "")

    if not account:
        try:
            out, _ = ssh_run_with_timeout(
                cluster,
                'squeue -u $USER -h -o "%a" 2>/dev/null | head -1',
                timeout_sec=8,
            )
            account = out.strip().split()[0] if out.strip() else ""
        except Exception:
            pass
    if not account:
        try:
            out, _ = ssh_run_with_timeout(
                cluster,
                'sacctmgr show user $USER withassoc format=account%-60 -n 2>/dev/null | head -1',
                timeout_sec=8,
            )
            account = out.strip().split()[0] if out.strip() else ""
        except Exception:
            pass
    if not account:
        return None
    _detected_accounts[cluster] = account

    try:
        out, _ = ssh_run_with_timeout(
            cluster,
            f'squeue -A {account} -h -o "%u|%T|%D|%P" 2>/dev/null',
            timeout_sec=10,
        )
    except Exception:
        return None

    users = {}
    for line in out.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 3:
            continue
        user, state, nodes_str = parts[0].strip(), parts[1].strip().upper(), parts[2].strip()
        try:
            nodes = int(nodes_str)
        except ValueError:
            nodes = 1
        gpus = nodes * gpus_per_node

        if user not in users:
            users[user] = {"running_gpus": 0, "pending_gpus": 0}
        if state == "RUNNING":
            users[user]["running_gpus"] += gpus
        elif state == "PENDING":
            users[user]["pending_gpus"] += gpus

    total_running = sum(u["running_gpus"] for u in users.values())
    total_pending = sum(u["pending_gpus"] for u in users.values())

    result = {
        "account": account,
        "users": users,
        "total_running_gpus": total_running,
        "total_pending_gpus": total_pending,
    }
    _cache_set(_team_usage_cache, cluster, result)
    try:
        from .config import TEAM_USAGE_TTL_SEC as _tu_ttl
        cache_db_put("team_usage", cluster, result, _tu_ttl)
    except Exception:
        pass
    return result


_team_jobs_cache = {}
TEAM_JOBS_TTL_SEC = 120


def _parse_gres_gpu_count(gres_str):
    """Extract GPU count from a GRES string.

    Handles: 'gpu:8', 'gpu:a100:4', 'gres/gpu:4', 'gres/gpu:b200:4(S:0-1)'
    """
    if not gres_str or gres_str in ("N/A", "(null)"):
        return 0
    for part in gres_str.split(","):
        part = part.strip().lower()
        if "gpu" not in part:
            continue
        idx = part.find("gpu")
        gpu_part = part[idx:]
        segs = gpu_part.split(":")
        for seg in reversed(segs):
            cleaned = seg.split("(")[0]
            try:
                return int(cleaned)
            except ValueError:
                continue
        return 1
    return 0


def fetch_team_jobs(cluster):
    """Fetch per-job breakdown for all PPP accounts on a cluster.

    Returns jobs list and summary with running/pending/dependent counts,
    filtered to team members only.
    """
    enable_standalone_ssh()
    if cluster == "local":
        return None

    cached = _cache_get(_team_jobs_cache, cluster, TEAM_JOBS_TTL_SEC)
    if cached is not None:
        return cached

    accounts = PPP_ACCOUNTS
    if not accounts:
        return None

    accts_csv = ",".join(accounts)
    cfg = CLUSTERS.get(cluster, {})
    gpus_per_node = cfg.get("gpus_per_node", 8) or 8
    team_set = set(TEAM_MEMBERS) if TEAM_MEMBERS else None

    fmt = "%i|%u|%T|%r|%D|%b|%P|%a|%j|%l|%E"
    try:
        out, _ = ssh_run_with_timeout(
            cluster,
            f'squeue -A {accts_csv} -h -o "{fmt}" 2>/dev/null',
            timeout_sec=12,
        )
    except Exception:
        return None

    # --- Pass 1: parse all lines, build job-name index ---
    parsed = []
    name_by_id = {}
    for line in out.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 10:
            continue
        jobid = parts[0].strip()
        user = parts[1].strip()
        state = parts[2].strip().upper()
        reason = parts[3].strip()
        try:
            nodes = int(parts[4].strip())
        except ValueError:
            nodes = 1
        gres_str = parts[5].strip()
        partition = parts[6].strip()
        account = parts[7].strip()
        job_name = parts[8].strip()
        timelimit = parts[9].strip()
        dep_expr = parts[10].strip() if len(parts) > 10 else ""

        gpu_per_node = _parse_gres_gpu_count(gres_str)
        is_cpu = partition.startswith("cpu")
        if not is_cpu and gpu_per_node == 0:
            gpu_per_node = gpus_per_node
        gpus = gpu_per_node * nodes if not is_cpu else 0

        is_dependent = state == "PENDING" and "depend" in reason.lower()
        dep_details = parse_dependency(dep_expr) if dep_expr else []

        name_by_id[jobid] = job_name
        parsed.append((user, state, reason, nodes, gpus, is_cpu, partition,
                        account, job_name, timelimit, is_dependent, dep_details))

    # --- Pass 2: classify dependent vs backup using name index ---
    jobs = []
    by_user = {}
    by_account = {}
    total_running = total_pending = total_dependent = 0

    for (user, state, reason, nodes, gpus, is_cpu, partition,
         account, job_name, timelimit, is_dependent, dep_details) in parsed:

        is_backup = False
        if is_dependent and dep_details:
            is_backup = all(
                d["type"] == "afternotok"
                or (d["type"] == "afterany"
                    and name_by_id.get(d["job_id"]) == job_name)
                for d in dep_details
            )

        if is_backup:
            job_state = "BACKUP"
        elif is_dependent:
            job_state = "DEPENDENT"
        else:
            job_state = state

        jobs.append({
            "user": user,
            "state": job_state,
            "reason": reason if state == "PENDING" else "",
            "nodes": nodes,
            "gpus": gpus,
            "is_gpu": not is_cpu,
            "partition": partition,
            "account": account,
            "job_name": job_name,
            "timelimit": timelimit,
        })

        if user not in by_user:
            by_user[user] = {"running": 0, "pending": 0, "dependent": 0, "backup": 0}
        if is_backup:
            by_user[user]["backup"] += gpus
        elif is_dependent:
            by_user[user]["dependent"] += gpus
            total_dependent += gpus
        elif state == "RUNNING":
            by_user[user]["running"] += gpus
            total_running += gpus
        elif state == "PENDING":
            by_user[user]["pending"] += gpus
            total_pending += gpus

        acct_short = account.split("_")[-1] if "_" in account else account
        if acct_short not in by_account:
            by_account[acct_short] = {"running": 0, "pending": 0, "dependent": 0, "backup": 0}
        if is_backup:
            by_account[acct_short]["backup"] += gpus
        elif is_dependent:
            by_account[acct_short]["dependent"] += gpus
        elif state == "RUNNING":
            by_account[acct_short]["running"] += gpus
        elif state == "PENDING":
            by_account[acct_short]["pending"] += gpus

    result = {
        "jobs": jobs,
        "summary": {
            "by_user": by_user,
            "by_account": by_account,
            "total_running": total_running,
            "total_pending": total_pending,
            "total_dependent": total_dependent,
        },
    }
    _cache_set(_team_jobs_cache, cluster, result)
    return result


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
                existing = _cache_get(_stats_cache, (cluster, jid), STATS_TTL_SEC)
                if existing and existing.get("status") == "ok" and not existing.get("_partial"):
                    continue
                partial = {
                    "status": "ok", "job_id": jid, "state": parts[1].strip(),
                    "nodes": parts[2].strip(), "cpus": parts[3].strip(),
                    "gres": parts[4].strip(), "node_list": parts[5].strip(),
                    "elapsed": parts[6].strip(), "gpus": [],
                    "ave_cpu": "", "ave_rss": "", "max_rss": "", "max_vmsize": "", "_partial": True,
                }
                _cache_set(_stats_cache, (cluster, jid), partial)
                try:
                    cache_db_put("stats", f"{cluster}:{jid}", partial, STATS_TTL_SEC)
                except Exception:
                    pass

    for jid in ids:
        from .logs import get_job_log_files
        log_result = get_job_log_files(cluster, jid)
        if log_result and log_result.get("files") and not log_result.get("error"):
            _cache_set(_log_index_cache, (cluster, jid), log_result)
            _extract_progress_with_source(cluster, jid, log_result["files"])
