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
    """Compute WDS score using the same equation as the frontend.

    The resource gate intentionally does NOT include ``idle_nodes``: on
    busy preemptable clusters every node is in mixed/alloc state most of
    the time, but jobs still start instantly thanks to fairshare and
    PPP-account headroom. ``idle_nodes`` still influences the score via
    ``queue_score`` (pending vs. idle ratio).
    """
    req_gpus = req_nodes * req_gpn

    hard_capacity = max(ppp_headroom, free_for_team)
    resource_gate = min(1, hard_capacity / max(req_gpus, 1))

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
    from concurrent.futures import ThreadPoolExecutor
    import os

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_alloc = pool.submit(get_ppp_allocations)
        f_fs = pool.submit(get_my_fairshare)
        f_parts = pool.submit(get_partition_summary)

    try:
        alloc_data = f_alloc.result()
    except Exception as exc:
        log.warning("WDS snapshot: failed to get allocations: %s", exc)
        return 0

    try:
        my_fs_data = f_fs.result()
    except Exception:
        my_fs_data = {"clusters": {}}

    try:
        part_data = f_parts.result()
    except Exception:
        part_data = {}

    try:
        from .jobs import fetch_team_jobs
    except Exception:
        fetch_team_jobs = None

    cluster_names = list(alloc_data.get("clusters", {}).keys())
    team_jobs_map = {}
    if fetch_team_jobs and cluster_names:
        with ThreadPoolExecutor(max_workers=len(cluster_names)) as pool:
            futs = {cn: pool.submit(fetch_team_jobs, cn) for cn in cluster_names}
        for cn, fut in futs.items():
            try:
                team_jobs_map[cn] = fut.result()
            except Exception:
                pass

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    rows = []
    me = os.environ.get("USER", "")

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

        tj = team_jobs_map.get(cn)
        tj_summary = (tj or {}).get("summary", {})
        tj_users = tj_summary.get("by_user", {})
        team_running = tj_summary.get("total_running", 0)

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

    from .db import db_write
    with db_write() as con:
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
    from .db import get_db
    con = get_db()

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


_wait_calibration_cache = None
_wait_calibration_ts = 0
_wait_calibration_computing = False
_WAIT_CAL_TTL = 1800

_WDS_BUCKETS = [0, 15, 30, 45, 60, 75]


def _refresh_calibration_bg():
    """Run calibration in background thread, update cache when done."""
    global _wait_calibration_cache, _wait_calibration_ts, _wait_calibration_computing
    try:
        result = _compute_wait_calibration()
        _wait_calibration_cache = result
        _wait_calibration_ts = time.time()
        log.info("Wait calibration updated: %d clusters",  len(result))
    except Exception as exc:
        log.warning("Wait calibration failed: %s", exc)
    finally:
        _wait_calibration_computing = False


def get_wait_calibration():
    """Return cached WDS-to-wait calibration, refreshing async if stale.

    Never blocks the caller. Returns stale/empty data immediately while
    a background thread recomputes.
    """
    global _wait_calibration_computing
    import threading

    now = time.time()
    stale = not _wait_calibration_cache or now - _wait_calibration_ts >= _WAIT_CAL_TTL

    if stale and not _wait_calibration_computing:
        _wait_calibration_computing = True
        threading.Thread(target=_refresh_calibration_bg, daemon=True).start()

    return _wait_calibration_cache or {}


def _compute_wait_calibration():
    import bisect
    from collections import defaultdict
    from .db import get_db

    con = get_db()

    try:
        jobs = con.execute("""
            SELECT cluster, submitted, started
            FROM job_history
            WHERE state IN ('COMPLETED', 'FAILED', 'TIMEOUT')
              AND submitted IS NOT NULL AND submitted != ''
              AND started IS NOT NULL AND started != ''
              AND julianday(started) >= julianday(submitted)
              AND ended_at >= date('now', '-14 days')
        """).fetchall()

        wds_rows = con.execute("""
            SELECT cluster, ts, MAX(wds) as wds
            FROM wds_history
            WHERE ts >= date('now', '-15 days')
            GROUP BY cluster, ts
            ORDER BY cluster, ts
        """).fetchall()
    finally:
        con.close()

    wds_by_cluster = {}
    for cluster, ts, wds in wds_rows:
        wds_by_cluster.setdefault(cluster, ([], []))
        wds_by_cluster[cluster][0].append(ts)
        wds_by_cluster[cluster][1].append(wds)

    buckets = defaultdict(lambda: defaultdict(list))

    for cluster, submitted, started in jobs:
        ts_list, wds_list = wds_by_cluster.get(cluster, ([], []))
        if not ts_list:
            continue
        idx = bisect.bisect_right(ts_list, submitted) - 1
        if idx < 0:
            continue
        wds = wds_list[idx]

        wait_sec = int((datetime.fromisoformat(started) -
                        datetime.fromisoformat(submitted)).total_seconds())
        if wait_sec < 0:
            continue

        wds_b = 0
        for threshold in _WDS_BUCKETS:
            if wds >= threshold:
                wds_b = threshold

        buckets[cluster][wds_b].append(wait_sec)

    result = {}
    for cluster in sorted(buckets):
        cluster_buckets = []
        for wds_min in sorted(buckets[cluster]):
            waits = sorted(buckets[cluster][wds_min])
            n = len(waits)
            if n < 5:
                continue
            p50 = waits[max(0, int(n * 0.50) - 1)]
            p75 = waits[max(0, int(n * 0.75) - 1)]
            cluster_buckets.append({
                "wds_min": wds_min,
                "n": n,
                "p50_s": p50,
                "p75_s": p75,
            })
        if cluster_buckets:
            result[cluster] = cluster_buckets

    return result
