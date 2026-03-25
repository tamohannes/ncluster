"""Recommendation engine for cluster+partition selection.

Given job requirements (nodes, time limit, account, preemption tolerance),
scores every eligible (cluster, partition) pair and returns a ranked list
with estimated wait times and actionable tips.
"""

import logging

from .config import CLUSTERS
from .partitions import get_all_partitions, _parse_timelimit, _estimate_partition_wait

log = logging.getLogger(__name__)

_SKIP_PARTITIONS = {"defq", "fake", "admin"}

_W_QUEUE = 0.35
_W_OCCUPANCY = 0.25
_W_PRIORITY = 0.25
_W_IDLE = 0.15


def _time_to_sec(time_limit):
    """Convert a time-limit string or int to seconds."""
    if isinstance(time_limit, (int, float)):
        return int(time_limit)
    return _parse_timelimit(str(time_limit)) or 14400  # default 4h


def _is_cpu_partition(name):
    return name.startswith("cpu") or name == "cpu_datamover"


def _is_eligible(part, nodes, time_sec, account, can_preempt):
    """Hard-constraint filter. Returns (eligible: bool, reason: str)."""
    if part.get("state", "").upper() != "UP":
        return False, "partition is DOWN"
    if part["name"] in _SKIP_PARTITIONS:
        return False, "system partition"
    if _is_cpu_partition(part["name"]):
        return False, "CPU-only partition"

    max_t = part.get("max_time_sec")
    if max_t is not None and time_sec > max_t:
        return False, f"time limit {part['max_time']} too short"

    min_n = part.get("min_nodes", 0)
    if min_n and nodes < min_n:
        return False, f"requires min {min_n} nodes"

    max_n = part.get("max_nodes")
    if max_n is not None and nodes > max_n:
        return False, f"max {max_n} nodes"

    if not can_preempt and part.get("preempt_mode", "OFF") not in ("OFF", ""):
        return False, "preemptable"

    allow = part.get("allow_accounts", "ALL")
    if allow != "ALL":
        if account:
            allowed_set = {a.strip() for a in allow.split(",")}
            if account not in allowed_set:
                return False, "account not allowed"
        else:
            return False, "restricted access"

    return True, ""




def _generate_tip(part, cluster_name, all_parts_for_cluster, score_rank):
    """Generate an actionable tip for this recommendation."""
    tips = []
    if part.get("is_default"):
        tips.append("default partition")
    tier = part.get("priority_tier", 0)
    if tier > 0:
        higher_tier_parts = [
            p for p in all_parts_for_cluster
            if p.get("priority_tier", 0) > tier
            and p.get("state") == "UP"
            and not _is_cpu_partition(p["name"])
            and p["name"] not in _SKIP_PARTITIONS
        ]
        if not higher_tier_parts:
            tips.append(f"highest priority tier ({tier})")
        else:
            tips.append(f"priority tier {tier}")

    preempt = part.get("preempt_mode", "OFF")
    if preempt not in ("OFF", ""):
        grace = part.get("grace_time_sec", 0)
        tips.append(f"preemptable ({preempt}, {grace}s grace)")

    idle = part.get("idle_nodes", 0)
    if idle > 0:
        tips.append(f"{idle} idle nodes")

    pending = part.get("pending_jobs", 0)
    if pending == 0:
        tips.append("no pending jobs")
    elif pending < 20:
        tips.append(f"only {pending} pending jobs")

    return "; ".join(tips) if tips else ""


def recommend(nodes=1, time_limit="4:00:00", account="", can_preempt=False,
              gpu_type="", clusters=None):
    """Return ranked list of (cluster, partition) recommendations.

    Args:
        nodes: Number of nodes needed.
        time_limit: Time limit string (e.g. "4:00:00") or seconds.
        account: Slurm account for access filtering (optional).
        can_preempt: Whether the job tolerates preemption.
        gpu_type: Filter clusters by GPU type (optional).
        clusters: List of cluster names to consider (optional, defaults to all).

    Returns:
        List of recommendation dicts, sorted by score (best first).
    """
    time_sec = _time_to_sec(time_limit)
    all_partitions = get_all_partitions()

    if not all_partitions:
        return []

    target_clusters = set(clusters) if clusters else set(all_partitions.keys())
    if gpu_type:
        target_clusters = {
            c for c in target_clusters
            if CLUSTERS.get(c, {}).get("gpu_type", "").lower() == gpu_type.lower()
        }

    max_tier_global = 1
    for cname, parts in all_partitions.items():
        if cname not in target_clusters:
            continue
        for p in parts:
            t = p.get("priority_tier", 0)
            if t > max_tier_global:
                max_tier_global = t

    candidates = []
    for cluster_name, parts in all_partitions.items():
        if cluster_name not in target_clusters:
            continue

        max_tier_cluster = max((p.get("priority_tier", 0) for p in parts), default=1) or 1

        for part in parts:
            eligible, reason = _is_eligible(part, nodes, time_sec, account, can_preempt)
            if not eligible:
                continue

            total = part.get("total_nodes", 1) or 1
            alloc = part.get("alloc_nodes", 0)
            idle = part.get("idle_nodes", 0)
            other = part.get("other_nodes", 0)
            pending = part.get("pending_jobs", 0)
            tier = part.get("priority_tier", 0)

            active = max(total - other, 1)
            occupancy_pct = (alloc / active) * 100
            queue_ratio = pending / active

            tier_norm = tier / max_tier_cluster
            idle_norm = idle / active

            score = (
                _W_QUEUE * min(queue_ratio, 3.0) / 3.0
                + _W_OCCUPANCY * (occupancy_pct / 100.0)
                + _W_PRIORITY * (1.0 - tier_norm)
                + _W_IDLE * (1.0 - idle_norm)
            )

            wait_label, wait_cls = _estimate_partition_wait(part)

            tip = _generate_tip(part, cluster_name, parts, 0)

            cluster_gpus_fallback = CLUSTERS.get(cluster_name, {}).get("gpus_per_node", 0)
            gpn = part.get("gpus_per_node", 0) or cluster_gpus_fallback

            candidates.append({
                "cluster": cluster_name,
                "partition": part["name"],
                "score": round(score, 4),
                "est_wait": wait_label,
                "est_wait_cls": wait_cls,
                "details": {
                    "total_nodes": total,
                    "idle_nodes": idle,
                    "alloc_nodes": alloc,
                    "other_nodes": other,
                    "gpus_per_node": gpn,
                    "pending_jobs": pending,
                    "running_jobs": part.get("running_jobs", 0),
                    "occupancy_pct": round(occupancy_pct, 1),
                    "priority_tier": tier,
                    "max_time": part.get("max_time", ""),
                    "preemptable": part.get("preempt_mode", "OFF") not in ("OFF", ""),
                    "is_default": part.get("is_default", False),
                    "gpu_type": CLUSTERS.get(cluster_name, {}).get("gpu_type", ""),
                },
                "tip": tip,
            })

    candidates.sort(key=lambda c: c["score"])
    for i, c in enumerate(candidates):
        c["rank"] = i + 1

    return candidates
