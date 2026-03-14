"""Database operations for job history."""

import sqlite3
import subprocess
from datetime import datetime, timedelta

from .config import DB_PATH, PINNABLE_TERMINAL_STATES, RESULT_DIR_NAMES


def parse_slurm_elapsed_seconds(elapsed):
    if not elapsed or elapsed in {"—", "N/A", "Unknown"}:
        return None
    try:
        s = elapsed.strip()
        days = 0
        if "-" in s:
            d, s = s.split("-", 1)
            days = int(d)
        parts = [int(x) for x in s.split(":")]
        if len(parts) == 2:
            h, m, sec = 0, parts[0], parts[1]
        elif len(parts) == 3:
            h, m, sec = parts
        else:
            return None
        return days * 86400 + h * 3600 + m * 60 + sec
    except Exception:
        return None


def parse_dt_maybe(value):
    if not value:
        return None
    text = str(value).strip()
    if not text or text in {"Unknown", "N/A", "—", "None"}:
        return None
    try:
        return datetime.fromisoformat(text.replace(" ", "T"))
    except Exception:
        return None


def normalize_job_times_local(job):
    j = dict(job)
    state = str(j.get("state", "")).upper()
    elapsed_s = parse_slurm_elapsed_seconds(j.get("elapsed"))
    now = datetime.now()

    submitted = parse_dt_maybe(j.get("submitted"))
    started_raw = parse_dt_maybe(j.get("started") or j.get("start"))

    if state == "PENDING":
        j["started_local"] = submitted.isoformat(timespec="seconds") if submitted else ""
        j["ended_local"] = ""
        return j

    if state in {"RUNNING", "COMPLETING"}:
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (now - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")
        else:
            j["started_local"] = ""
        j["ended_local"] = ""
        return j

    ended = parse_dt_maybe(j.get("ended_at"))
    if ended:
        j["ended_local"] = ended.isoformat(timespec="seconds")
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (ended - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")
    else:
        j["ended_local"] = ""
        if started_raw:
            j["started_local"] = started_raw.isoformat(timespec="seconds")
        elif elapsed_s is not None:
            j["started_local"] = (now - timedelta(seconds=elapsed_s)).isoformat(timespec="seconds")
        elif submitted:
            j["started_local"] = submitted.isoformat(timespec="seconds")

    return j


def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = get_db()
    con.execute("""
        CREATE TABLE IF NOT EXISTS job_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster       TEXT NOT NULL,
            job_id        TEXT NOT NULL,
            job_name      TEXT,
            state         TEXT,
            exit_code     TEXT,
            reason        TEXT,
            elapsed       TEXT,
            nodes         TEXT,
            gres          TEXT,
            partition     TEXT,
            submitted     TEXT,
            ended_at      TEXT,
            log_path      TEXT,
            board_visible INTEGER DEFAULT 0,
            dependency    TEXT DEFAULT '',
            UNIQUE(cluster, job_id)
        )
    """)
    for col, default in [("board_visible", "INTEGER DEFAULT 0"),
                         ("started", "TEXT"),
                         ("dependency", "TEXT DEFAULT ''")]:
        try:
            con.execute(f"ALTER TABLE job_history ADD COLUMN {col} {default}")
        except Exception:
            pass
    con.commit()
    con.close()


def upsert_job(cluster, job, terminal=False, set_board_visible=None):
    con = get_db()
    row = con.execute(
        "SELECT board_visible FROM job_history WHERE cluster=? AND job_id=?",
        (cluster, job["jobid"])
    ).fetchone()
    current_visible = row["board_visible"] if row else None

    if set_board_visible is not None:
        bv = set_board_visible
    elif terminal:
        bv = 1 if current_visible != 0 else 0
    else:
        bv = current_visible if current_visible is not None else 0

    dep_raw = job.get("dependency", "")
    if dep_raw in ("(null)", "None", None):
        dep_raw = ""

    con.execute("""
        INSERT INTO job_history
            (cluster, job_id, job_name, state, exit_code, reason, elapsed,
             nodes, gres, partition, submitted, started, ended_at, log_path,
             board_visible, dependency)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(cluster, job_id) DO UPDATE SET
            job_name    = COALESCE(excluded.job_name, job_name),
            state       = excluded.state,
            exit_code   = COALESCE(excluded.exit_code, exit_code),
            reason      = COALESCE(excluded.reason, reason),
            elapsed     = COALESCE(excluded.elapsed, elapsed),
            nodes       = COALESCE(excluded.nodes, nodes),
            gres        = COALESCE(excluded.gres, gres),
            partition   = COALESCE(excluded.partition, partition),
            submitted   = COALESCE(excluded.submitted, submitted),
            started     = COALESCE(excluded.started, started),
            ended_at    = COALESCE(excluded.ended_at, ended_at),
            board_visible = excluded.board_visible,
            dependency  = COALESCE(NULLIF(excluded.dependency, ''), dependency)
    """, (
        cluster, job["jobid"],
        job.get("name") or job.get("job_name"),
        job.get("state"),
        job.get("exit_code"), job.get("reason"), job.get("elapsed"),
        job.get("nodes"), job.get("gres"), job.get("partition"),
        job.get("submitted"), job.get("started"),
        job.get("ended_at"), job.get("log_path"),
        bv, dep_raw,
    ))
    con.commit()
    con.close()


def upsert_history(cluster, job):
    upsert_job(cluster, job)


def get_board_pinned(cluster=None):
    from .jobs import parse_dependency
    con = get_db()
    if cluster:
        rows = con.execute(
            "SELECT * FROM job_history WHERE cluster=? AND board_visible=1 ORDER BY id DESC",
            (cluster,)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM job_history WHERE board_visible=1 ORDER BY id DESC"
        ).fetchall()
    con.close()
    jobs = [normalize_job_times_local(dict(r)) for r in rows]
    _restore_dependency_fields(jobs, parse_dependency)
    return jobs


def _restore_dependency_fields(jobs, parse_dependency_fn):
    import re
    id_set = {j.get("job_id") or j.get("jobid") for j in jobs}

    # Build a lookup by name for name-based inference.
    by_name = {}
    for j in jobs:
        name = j.get("job_name") or j.get("name") or ""
        if name:
            by_name[name] = j.get("job_id") or j.get("jobid", "")

    for j in jobs:
        deps = parse_dependency_fn(j.get("dependency", ""))
        j["dep_details"] = deps
        j["depends_on"] = [d["job_id"] for d in deps if d["job_id"] in id_set]

        # Name-based inference when no explicit dependency stored.
        if not j["depends_on"]:
            name = j.get("job_name") or j.get("name") or ""
            inferred = _infer_parent_from_name(name, by_name, id_set, j)
            if inferred:
                j["depends_on"] = [inferred]
                j["dep_details"] = [{"type": "afterany", "job_id": inferred}]

    children_map = {}
    for j in jobs:
        jid = j.get("job_id") or j.get("jobid", "")
        for pid in j.get("depends_on", []):
            children_map.setdefault(pid, []).append(jid)
    for j in jobs:
        jid = j.get("job_id") or j.get("jobid", "")
        j["dependents"] = children_map.get(jid, [])


def _infer_parent_from_name(name, by_name, id_set, job):
    """Infer parent job ID from naming convention (e.g., eval-judge depends on eval)."""
    import re
    if not name:
        return None
    jid = job.get("job_id") or job.get("jobid", "")

    # judge-rs0 depends on the base eval
    m = re.match(r'^(.+?)(?:-judge(?:-rs\d+)?)$', name)
    if m:
        parent_name = m.group(1)
        pid = by_name.get(parent_name)
        if pid and pid in id_set and pid != jid:
            return pid

    # summarize-results depends on judge-rs0
    m = re.match(r'^(.+?)(?:-summarize[-_]results?)$', name)
    if m:
        base = m.group(1)
        for suffix in ["-judge-rs0", "-judge"]:
            pid = by_name.get(base + suffix)
            if pid and pid in id_set and pid != jid:
                return pid
        # Fall back to base eval
        pid = by_name.get(base)
        if pid and pid in id_set and pid != jid:
            return pid

    return None


def dismiss_job(cluster, job_id):
    con = get_db()
    con.execute("UPDATE job_history SET board_visible=0 WHERE cluster=? AND job_id=?", (cluster, job_id))
    con.commit()
    con.close()


def dismiss_all(cluster):
    con = get_db()
    con.execute("UPDATE job_history SET board_visible=0 WHERE cluster=?", (cluster,))
    con.commit()
    con.close()


def dismiss_by_state_prefix(cluster, prefixes):
    con = get_db()
    if not prefixes:
        con.close()
        return
    where = " OR ".join(["state LIKE ?"] * len(prefixes))
    args = [cluster] + [f"{p}%" for p in prefixes]
    con.execute(f"UPDATE job_history SET board_visible=0 WHERE cluster=? AND ({where})", args)
    con.commit()
    con.close()


def get_history(cluster=None, limit=200):
    con = get_db()
    order = "ORDER BY COALESCE(ended_at, started, submitted, '9999') DESC, id DESC"
    if cluster and cluster != "all":
        rows = con.execute(f"SELECT * FROM job_history WHERE cluster=? {order} LIMIT ?", (cluster, limit)).fetchall()
    else:
        rows = con.execute(f"SELECT * FROM job_history {order} LIMIT ?", (limit,)).fetchall()
    con.close()
    return [dict(r) for r in rows]


def repin_recent_terminal_jobs():
    """Re-pin recent terminal jobs on startup."""
    con = get_db()
    terminal_like = " OR ".join(f"state LIKE '{s}%'" for s in PINNABLE_TERMINAL_STATES)
    con.execute(f"""
        UPDATE job_history SET board_visible=1
        WHERE ({terminal_like})
          AND cluster != 'local'
          AND board_visible != 1
          AND (ended_at >= datetime('now', '-3 days') OR ended_at IS NULL)
    """)
    con.commit()
    con.close()
