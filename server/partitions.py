"""Fetch, parse, and cache Slurm partition data from clusters via SSH.

Runs a combined sinfo + scontrol + squeue script on each cluster's login
node and returns structured partition metadata.  Results are cached
in-memory with a configurable TTL (default 120 s, on-demand only).
"""

import logging
import re
import threading
import time

from .config import CLUSTERS
from .ssh import ssh_run_with_timeout

log = logging.getLogger(__name__)

PARTITION_CACHE_TTL_SEC = 120
_lock = threading.Lock()
_cache = {}  # cluster -> {"ts": float, "data": list[dict]}

_FETCH_SCRIPT = r"""
echo '===SINFO==='
sinfo -o '%P|%a|%l|%D|%F|%c|%G' --noheader
echo '===SCONTROL==='
scontrol show partition -o 2>/dev/null
echo '===SQUEUE==='
squeue -h -o '%P|%T' 2>/dev/null | sort | uniq -c
""".strip()


def _parse_timelimit(s):
    """Parse Slurm time-limit string to seconds.

    Formats: "UNLIMITED", "4:00:00", "7-00:00:00", "30:00", "1-00:00:00"
    """
    if not s or s.upper() in ("UNLIMITED", "INFINITE"):
        return None
    s = s.strip()
    days = 0
    if "-" in s:
        parts = s.split("-", 1)
        days = int(parts[0])
        s = parts[1]
    parts = s.split(":")
    if len(parts) == 3:
        h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
    elif len(parts) == 2:
        h, m, sec = 0, int(parts[0]), int(parts[1])
    else:
        return None
    return days * 86400 + h * 3600 + m * 60 + sec


def _parse_gres_gpus(gres_str):
    """Extract GPU count per node from GRES string.

    Formats: "gpu:8", "gpu:4(S:0-1)", "gpu:h100:8", "gpu:h100:8(S:0-3)", "(null)"
    """
    if not gres_str or gres_str.strip() in ("(null)", "N/A", ""):
        return 0
    m = re.search(r'gpu:(?:[a-zA-Z]\w*:)?(\d+)', gres_str)
    return int(m.group(1)) if m else 0


def _parse_sinfo(text):
    """Parse sinfo output into {partition_name: {...}} dicts."""
    partitions = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) < 6:
            continue
        name_raw = parts[0]
        is_default = name_raw.endswith("*")
        name = name_raw.rstrip("*")

        avail = parts[1]
        timelimit = parts[2]
        node_count = int(parts[3]) if parts[3].isdigit() else 0

        node_states = parts[4]  # "A/I/O/T"
        alloc, idle, other, total = 0, 0, 0, 0
        ns = node_states.split("/")
        if len(ns) == 4:
            alloc = int(ns[0]) if ns[0].isdigit() else 0
            idle = int(ns[1]) if ns[1].isdigit() else 0
            other = int(ns[2]) if ns[2].isdigit() else 0
            total = int(ns[3]) if ns[3].isdigit() else node_count

        cpus = int(parts[5]) if parts[5].isdigit() else 0
        gpus_per_node = _parse_gres_gpus(parts[6]) if len(parts) > 6 else 0

        if name not in partitions:
            partitions[name] = {
                "name": name,
                "state": avail.upper(),
                "is_default": is_default,
                "max_time": timelimit,
                "max_time_sec": _parse_timelimit(timelimit),
                "total_nodes": total,
                "alloc_nodes": alloc,
                "idle_nodes": idle,
                "other_nodes": other,
                "total_cpus": cpus * total if cpus else 0,
                "gpus_per_node": gpus_per_node,
            }
        else:
            rec = partitions[name]
            rec["total_nodes"] += total
            rec["alloc_nodes"] += alloc
            rec["idle_nodes"] += idle
            rec["other_nodes"] += other
            if gpus_per_node > rec.get("gpus_per_node", 0):
                rec["gpus_per_node"] = gpus_per_node
    return partitions


def _parse_scontrol(text, partitions):
    """Enrich partition dicts with scontrol show partition data."""
    for block in text.strip().split("\n"):
        block = block.strip()
        if not block or not block.startswith("PartitionName="):
            continue
        fields = {}
        for token in re.split(r'\s+', block):
            if "=" in token:
                k, v = token.split("=", 1)
                fields[k] = v

        name = fields.get("PartitionName", "")
        if not name or name not in partitions:
            if name:
                partitions[name] = {
                    "name": name, "state": fields.get("State", "UP"),
                    "is_default": False, "max_time": fields.get("MaxTime", ""),
                    "max_time_sec": _parse_timelimit(fields.get("MaxTime", "")),
                    "total_nodes": int(fields.get("TotalNodes", 0)),
                    "alloc_nodes": 0, "idle_nodes": 0, "other_nodes": 0,
                    "total_cpus": int(fields.get("TotalCPUs", 0)),
                }

        if name not in partitions:
            continue
        rec = partitions[name]
        rec["priority_tier"] = int(fields.get("PriorityTier", 0))
        rec["preempt_mode"] = fields.get("PreemptMode", "OFF")
        rec["grace_time_sec"] = int(fields.get("GraceTime", 0))
        rec["default_time"] = fields.get("DefaultTime", "")

        allow = fields.get("AllowAccounts", "ALL")
        rec["allow_accounts"] = allow

        min_n = fields.get("MinNodes", "0")
        rec["min_nodes"] = int(min_n) if min_n.isdigit() else 0

        max_n = fields.get("MaxNodes", "UNLIMITED")
        rec["max_nodes"] = None if max_n == "UNLIMITED" else int(max_n) if max_n.isdigit() else None

        if "TotalCPUs" in fields and not rec.get("total_cpus"):
            rec["total_cpus"] = int(fields["TotalCPUs"])


def _parse_squeue_counts(text, partitions):
    """Parse aggregated squeue output to fill running_jobs and pending_jobs."""
    for name in partitions:
        partitions[name].setdefault("running_jobs", 0)
        partitions[name].setdefault("pending_jobs", 0)

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'(\d+)\s+(\S+)\|(\S+)', line)
        if not m:
            continue
        count = int(m.group(1))
        part_field = m.group(2)
        state = m.group(3)

        part_names = [p.rstrip("*") for p in part_field.split(",")]
        for pname in part_names:
            if pname in partitions:
                if state == "RUNNING":
                    partitions[pname]["running_jobs"] += count
                elif state == "PENDING":
                    partitions[pname]["pending_jobs"] += count


def _classify_accessibility(partitions, user_account=""):
    """Mark partitions as user_accessible based on allow_accounts."""
    for rec in partitions.values():
        allow = rec.get("allow_accounts", "ALL")
        if allow == "ALL":
            rec["user_accessible"] = True
        elif user_account:
            accounts = {a.strip() for a in allow.split(",")}
            rec["user_accessible"] = user_account in accounts
        else:
            rec["user_accessible"] = allow == "ALL"


def _fetch_partitions(cluster_name):
    """SSH into a cluster and parse partition data. Returns list of dicts."""
    try:
        out, err = ssh_run_with_timeout(cluster_name, _FETCH_SCRIPT, timeout_sec=15)
    except Exception as exc:
        log.warning("partitions: SSH to %s failed: %s", cluster_name, exc)
        return None

    sections = {}
    current_key = None
    current_lines = []
    for line in out.splitlines():
        if line.startswith("===") and line.endswith("==="):
            if current_key:
                sections[current_key] = "\n".join(current_lines)
            current_key = line.strip("= ")
            current_lines = []
        else:
            current_lines.append(line)
    if current_key:
        sections[current_key] = "\n".join(current_lines)

    partitions = _parse_sinfo(sections.get("SINFO", ""))
    _parse_scontrol(sections.get("SCONTROL", ""), partitions)
    _parse_squeue_counts(sections.get("SQUEUE", ""), partitions)
    _classify_accessibility(partitions)

    for rec in partitions.values():
        rec.setdefault("priority_tier", 0)
        rec.setdefault("preempt_mode", "OFF")
        rec.setdefault("grace_time_sec", 0)
        rec.setdefault("default_time", "")
        rec.setdefault("allow_accounts", "ALL")
        rec.setdefault("min_nodes", 0)
        rec.setdefault("max_nodes", None)
        rec.setdefault("running_jobs", 0)
        rec.setdefault("pending_jobs", 0)
        rec.setdefault("user_accessible", True)
        rec.setdefault("gpus_per_node", 0)

    return sorted(partitions.values(), key=lambda p: (-p.get("is_default", False), p["name"]))


def get_partitions(cluster_name, force=False):
    """Return cached partition data for a single cluster, or fetch on demand."""
    if cluster_name not in CLUSTERS or cluster_name == "local":
        return None

    now = time.monotonic()
    with _lock:
        rec = _cache.get(cluster_name)
        if rec and not force and (now - rec["ts"]) < PARTITION_CACHE_TTL_SEC:
            return rec["data"]

    data = _fetch_partitions(cluster_name)
    if data is not None:
        with _lock:
            _cache[cluster_name] = {"ts": time.monotonic(), "data": data}
        return data

    with _lock:
        rec = _cache.get(cluster_name)
        return rec["data"] if rec else None


def get_all_partitions(force=False):
    """Return partition data for all configured clusters.

    Fetches sequentially (each call ~2s).  Returns {cluster: [partitions]}.
    """
    result = {}
    for name in CLUSTERS:
        if name == "local":
            continue
        data = get_partitions(name, force=force)
        if data is not None:
            result[name] = data
    return result


def _estimate_partition_wait(part):
    """Heuristic wait estimate for a single partition.

    Uses active capacity (total - other/drained), queue depth relative to
    active nodes, and idle-to-pending ratio.  Intentionally conservative:
    even when idle nodes exist, pending jobs ahead in the queue and
    fair-share priority mean the user's job rarely starts instantly.
    """
    total = part.get("total_nodes", 1) or 1
    alloc = part.get("alloc_nodes", 0)
    idle = part.get("idle_nodes", 0)
    other = part.get("other_nodes", 0)
    pending = part.get("pending_jobs", 0)

    active = total - other
    if active <= 0:
        return "unavail", "long"

    occupancy = alloc / active * 100
    queue_ratio = pending / active

    if idle > 0 and pending == 0:
        return "now", "fast"

    if idle > pending and pending < 5:
        return "< 5 min", "fast"

    if idle > 0 and queue_ratio < 0.05:
        return "< 5 min", "fast"
    if idle > 0 and queue_ratio < 0.2:
        return "~5-15 min", "fast"
    if idle > 0 and queue_ratio < 0.5:
        return "~15-30 min", "moderate"
    if idle > 0 and queue_ratio < 1.0:
        return "~30-60 min", "moderate"

    if queue_ratio < 0.3:
        return "~30-60 min", "moderate"
    if queue_ratio < 1.0:
        return "~1-2h", "slow"
    if queue_ratio < 3.0:
        return "~2-4h", "slow"
    return "4h+", "long"


def get_partition_summary():
    """Compact cross-cluster overview for quick agent decisions."""
    all_data = get_all_partitions()
    summary = {}
    for cluster_name, parts in all_data.items():
        accessible = [p for p in parts if p.get("user_accessible", True) and p.get("state") == "UP"]
        gpu_parts = [p for p in accessible
                     if not p["name"].startswith("cpu") and p["name"] not in ("defq", "fake")]
        total_nodes = max((p.get("total_nodes", 0) for p in gpu_parts), default=0)

        cluster_gpus_fallback = CLUSTERS.get(cluster_name, {}).get("gpus_per_node", 0)

        part_list = []
        for p in gpu_parts:
            wait_label, wait_cls = _estimate_partition_wait(p)
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
                "est_wait": wait_label,
                "est_wait_cls": wait_cls,
            })

        summary[cluster_name] = {
            "gpu_partitions": len(gpu_parts),
            "total_nodes": total_nodes,
            "idle_nodes": max((p.get("idle_nodes", 0) for p in gpu_parts), default=0),
            "pending_jobs": sum(p.get("pending_jobs", 0) for p in gpu_parts),
            "gpu_type": CLUSTERS.get(cluster_name, {}).get("gpu_type", ""),
            "partitions": part_list,
        }
    return summary
