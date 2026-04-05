"""WDS (Where Do I Submit) score tracking.

Periodically snapshots WDS scores and their component factors into SQLite
so users can track how fairshare, queue pressure, and team usage affect
scheduling priority over time.
"""

import logging
import math
import time
from datetime import datetime, timedelta

from .config import (
    CLUSTERS, PPP_ACCOUNTS, TEAM_GPU_ALLOC, WDS_SNAPSHOT_INTERVAL,
    DB_PATH,
)

log = logging.getLogger(__name__)


def _compute_wds(free_for_team, ppp_headroom, idle_nodes, pending_queue,
                 my_level_fs, ppp_level_fs, team_num,
                 occ_pct=100, req_nodes=1, req_gpn=8):
    """Compute WDS score using the same equation as the frontend."""
    req_gpus = req_nodes * req_gpn

    hard_capacity = max(ppp_headroom, free_for_team)
    resource_gate = min(
        1,
        hard_capacity / max(req_gpus, 1),
        idle_nodes / max(req_nodes, 1),
    )

    team_penalty = 0.7 if (team_num is not None and team_num > 0 and free_for_team <= 0) else 1.0

    effective_my_fs = my_level_fs if my_level_fs > 0 else ppp_level_fs
    my_fs_score = min(effective_my_fs / 1.5, 1)
    ppp_fs_score = min(ppp_level_fs / 1.5, 1)
    queue_score = 1 - min(
        math.log1p(pending_queue / max(idle_nodes, 1)) / math.log1p(50), 1
    )
    occupancy_factor = 1.15 - 0.30 * min(occ_pct / 100, 1)

    priority_blend = 0.55 * my_fs_score + 0.20 * ppp_fs_score + 0.25 * queue_score
    wds = max(0, min(100, round(
        100 * resource_gate * priority_blend * team_penalty * occupancy_factor
    )))

    return {
        "wds": wds,
        "resource_gate": round(resource_gate, 3),
        "queue_score": round(queue_score, 3),
        "occupancy_factor": round(occupancy_factor, 3),
    }


def compute_wds_snapshot():
    """Compute WDS for all cluster/account pairs and store in the database."""
    from .aihub import get_ppp_allocations, get_my_fairshare
    from .partitions import get_partition_summary
    from .db import get_db

    try:
        alloc_data = get_ppp_allocations()
    except Exception as exc:
        log.warning("WDS snapshot: failed to get allocations: %s", exc)
        return 0

    try:
        my_fs_data = get_my_fairshare()
    except Exception:
        my_fs_data = {"clusters": {}}

    try:
        part_data = get_partition_summary()
    except Exception:
        part_data = {}

    try:
        from .jobs import fetch_team_jobs
    except Exception:
        fetch_team_jobs = None

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    rows = []

    for cn, cd in alloc_data.get("clusters", {}).items():
        ps = part_data.get(cn, {})
        idle_nodes = ps.get("idle_nodes", 0)
        pending_queue = ps.get("pending_jobs", 0)

        cl_occ = cd.get("cluster_occupied_gpus", 0)
        cl_tot = cd.get("cluster_total_gpus", 0)
        occ_pct = round(cl_occ / cl_tot * 100) if cl_tot > 0 else 100

        ta = TEAM_GPU_ALLOC.get(cn)
        team_num = None
        if ta == "any":
            team_num = None
        elif isinstance(ta, (int, float)) and ta > 0:
            team_num = int(ta)

        tj = None
        if fetch_team_jobs:
            try:
                tj = fetch_team_jobs(cn)
            except Exception:
                pass
        tj_summary = (tj or {}).get("summary", {})
        tj_users = tj_summary.get("by_user", {})
        team_running = tj_summary.get("total_running", 0)

        import os
        me = os.environ.get("USER", "")
        my_data = tj_users.get(me, {})
        my_running = my_data.get("running", 0)
        my_pending = my_data.get("pending", 0) + my_data.get("dependent", 0)

        for acct, ad in cd.get("accounts", {}).items():
            ppp_headroom = ad.get("headroom", 0)
            ppp_level_fs = ad.get("level_fs", 0)
            consumed = ad.get("gpus_consumed", 0)
            allocated = ad.get("gpus_allocated", 0)

            my_acct_fs = my_fs_data.get("clusters", {}).get(cn, {}).get(acct, {})
            my_level_fs = my_acct_fs.get("level_fs", 0)

            if team_num is not None:
                free_for_team = min(ppp_headroom, max(0, team_num - team_running))
            else:
                free_for_team = ppp_headroom

            result = _compute_wds(
                free_for_team, ppp_headroom, idle_nodes, pending_queue,
                my_level_fs, ppp_level_fs, team_num, occ_pct=occ_pct,
            )

            rows.append((
                ts, cn, acct, result["wds"],
                result["resource_gate"], my_level_fs, ppp_level_fs,
                result["queue_score"], idle_nodes, pending_queue,
                ppp_headroom, free_for_team, consumed, allocated,
                team_running, my_running, my_pending, 1, 8,
                result["occupancy_factor"],
            ))

    if not rows:
        return 0

    import sqlite3
    con = sqlite3.connect(DB_PATH)
    con.executemany("""
        INSERT INTO wds_history (
            ts, cluster, account, wds,
            resource_gate, my_level_fs, ppp_level_fs,
            queue_score, idle_nodes, pending_queue,
            ppp_headroom, free_for_team, gpus_consumed, gpus_allocated,
            team_running, my_running, my_pending, req_nodes, req_gpus_per_node,
            occupancy_factor
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    con.commit()
    con.close()

    log.info("WDS snapshot: %d rows stored at %s", len(rows), ts)
    return len(rows)


def wds_snapshot_loop():
    """Background loop: periodically snapshot WDS scores."""
    time.sleep(60)
    while True:
        try:
            compute_wds_snapshot()
        except Exception as e:
            log.warning("WDS snapshot loop error: %s", e)
        time.sleep(max(60, WDS_SNAPSHOT_INTERVAL))


def get_wds_history(cluster=None, account=None, days=30, limit=5000):
    """Query WDS history from the database."""
    import sqlite3
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    conditions = ["ts >= ?"]
    params = [cutoff]

    if cluster:
        conditions.append("cluster = ?")
        params.append(cluster)
    if account:
        if "_" not in account:
            conditions.append("account LIKE ?")
            params.append(f"%{account}%")
        else:
            conditions.append("account = ?")
            params.append(account)

    where = " AND ".join(conditions)
    rows = con.execute(
        f"SELECT * FROM wds_history WHERE {where} ORDER BY ts DESC LIMIT ?",
        params + [limit],
    ).fetchall()
    con.close()

    return [dict(r) for r in rows]
