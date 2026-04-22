"""Database operations for job history."""

import json as _json
import sqlite3
import subprocess
import threading as _th_db
from contextlib import contextmanager
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


_DB_CONNECT_TIMEOUT_SEC = 15.0  # max time sqlite3.connect waits on cross-process locks
_DB_BUSY_TIMEOUT_MS = 15000     # max time SQLite operations wait on busy DB
_DB_WRITE_LOCK_TIMEOUT_SEC = 20.0  # max time db_write() waits on the in-process write lock


class DBWriteLockTimeoutError(RuntimeError):
    """Raised when db_write() cannot acquire the in-process write lock in
    DB_WRITE_LOCK_TIMEOUT_SEC seconds. Indicates either heavy write
    contention or a stuck writer holding the lock — surfaces as a 500
    response instead of letting the request thread block forever."""


_db_local = _th_db.local()


class _CachedConnection:
    """Thread-local SQLite connection wrapper that survives ``close()``.

    Why this exists: under cross-process WAL contention (gunicorn worker
    + multiple MCP server processes all hitting the same SQLite file),
    ``sqlite3.connect()`` itself can block for tens of seconds while
    waiting on file-level locks held by another process doing checkpoint
    or transaction work. Each ``/api/jobs`` call previously opened 10+
    fresh connections; multiplied by 32 worker threads and 2-3 sibling
    processes, this produced hundreds of connect/close cycles per second
    and any momentary slowness cascaded into worker-wide wedges (we
    captured 22/30 threads stuck in ``sqlite3.connect()`` in a live
    stack dump on 2026-04-20).

    With caching, each thread keeps a single long-lived connection.
    ``close()`` on this wrapper is a no-op — the underlying connection
    persists for the lifetime of the thread. Existing call sites that
    pattern-match ``con = get_db(); ...; con.close()`` continue to work
    unchanged but no longer churn file descriptors or trigger WAL
    housekeeping on the close path.

    The wrapper records the ``DB_PATH`` it was opened against so
    ``get_db()`` can detect a path change (e.g. a test monkeypatching
    the path, or any future runtime config reload) and discard the
    stale connection automatically.
    """

    __slots__ = ("_raw", "_db_path")

    def __init__(self, raw, db_path):
        self._raw = raw
        self._db_path = db_path

    def __getattr__(self, name):
        # Delegate everything except ``close`` and the dunder context
        # manager protocol (handled below) to the underlying connection.
        return getattr(self._raw, name)

    def __enter__(self):
        return self._raw.__enter__()

    def __exit__(self, exc_type, exc, tb):
        return self._raw.__exit__(exc_type, exc, tb)

    def close(self):
        # Intentionally a no-op. The cached connection is owned by the
        # thread-local store and released only on thread death or via
        # _force_close_thread_local_db().
        pass


def _open_raw_db():
    # ``timeout`` here is SQLite's busy_timeout for cross-process locking:
    # if another process holds the DB lock, operations wait up to N
    # seconds then raise OperationalError instead of blocking forever.
    # We also set the PRAGMA explicitly for clarity (Python's ``timeout``
    # and the PRAGMA are equivalent — having both is harmless).
    raw = sqlite3.connect(DB_PATH, timeout=_DB_CONNECT_TIMEOUT_SEC)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute(f"PRAGMA busy_timeout={_DB_BUSY_TIMEOUT_MS}")
    return raw


def get_db(*, fresh=False):
    """Return a thread-local cached SQLite connection.

    Pass ``fresh=True`` for one-off operations that shouldn't share state
    with the thread's cached connection (backups, schema migrations,
    integrity checks). The returned connection is a real
    ``sqlite3.Connection`` — close it yourself.

    A cached connection is automatically evicted and reopened if the
    module-level ``DB_PATH`` has changed since the connection was
    created. This keeps thread pools (e.g. ``_shared_pool`` in routes,
    or any future global pool) correct across test isolation, where
    each test monkeypatches DB_PATH to a fresh tmp file. In production
    DB_PATH never changes so this check is a single dict lookup.
    """
    if fresh:
        return _open_raw_db()

    cached = getattr(_db_local, "con", None)
    if cached is not None and cached._db_path != DB_PATH:
        # DB_PATH was rebound under us — drop the stale connection so
        # the next caller gets one pointed at the current DB.
        try:
            cached._raw.close()
        except Exception:
            pass
        cached = None
        _db_local.con = None
    if cached is None:
        cached = _CachedConnection(_open_raw_db(), DB_PATH)
        _db_local.con = cached
    return cached


def _force_close_thread_local_db():
    """Force-close and discard the current thread's cached connection.

    Use when the connection is in a known-bad state (corruption error,
    inconsistent transaction state) and the next ``get_db()`` call must
    open a fresh connection. Safe no-op if no cached connection exists.
    """
    cached = getattr(_db_local, "con", None)
    if cached is None:
        return
    try:
        cached._raw.close()
    except Exception:
        pass
    _db_local.con = None


# In-process serializer for DB writes so bookkeeping threads don't fight
# for the SQLite writer slot. We hold this lock ONLY around the
# write-and-commit window — never around `sqlite3.connect()` itself,
# because connect can block on cross-process WAL contention and we don't
# want one slow connect to wedge every other writer in the process.
_db_write_lock = _th_db.RLock()


@contextmanager
def db_write():
    """Serialize in-process writes so bookkeeping threads don't fight for SQLite.

    The lock is held around the actual write window only. Connection
    open/close happen outside the lock — opening a connection can block
    on cross-process WAL housekeeping, and if that happened while
    holding the lock, every other writer in the process would wedge
    behind it (which is exactly the failure mode we hit on 2026-04-19
    that took the worker down repeatedly).

    Uses the thread-local cached connection so we don't open + close a
    fresh connection per write — connect/close itself is the slow path
    under cross-process WAL contention.
    """
    con = get_db()
    acquired = _db_write_lock.acquire(timeout=_DB_WRITE_LOCK_TIMEOUT_SEC)
    if not acquired:
        raise DBWriteLockTimeoutError(
            f"db_write: could not acquire write lock in "
            f"{_DB_WRITE_LOCK_TIMEOUT_SEC:.0f}s — another writer is wedged"
        )
    try:
        yield con
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            _db_write_lock.release()
        except Exception:
            pass
        # Note: con.close() intentionally NOT called — the cached
        # connection persists for the thread's lifetime. See
        # _CachedConnection docstring for why.


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
                         ("dependency", "TEXT DEFAULT ''"),
                         ("project", "TEXT DEFAULT ''"),
                         ("run_id", "INTEGER DEFAULT NULL"),
                         ("node_list", "TEXT DEFAULT ''"),
                         ("account", "TEXT DEFAULT ''")]:
        try:
            con.execute(f"ALTER TABLE job_history ADD COLUMN {col} {default}")
        except Exception:
            pass

    con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster       TEXT NOT NULL,
            root_job_id   TEXT NOT NULL,
            run_name      TEXT DEFAULT '',
            project       TEXT DEFAULT '',
            batch_script  TEXT DEFAULT '',
            scontrol_raw  TEXT DEFAULT '',
            env_vars      TEXT DEFAULT '',
            conda_state   TEXT DEFAULT '',
            started_at    TEXT,
            ended_at      TEXT,
            created_at    TEXT DEFAULT (datetime('now')),
            meta_fetched  INTEGER DEFAULT 0,
            UNIQUE(cluster, root_job_id)
        )
    """)
    for col, default in [("starred", "INTEGER DEFAULT 0"),
                         ("notes", "TEXT DEFAULT ''"),
                         ("run_uuid", "TEXT DEFAULT ''"),
                         ("source", "TEXT DEFAULT 'legacy'"),
                         ("submit_command", "TEXT DEFAULT ''"),
                         ("submit_cwd", "TEXT DEFAULT ''"),
                         ("git_commit", "TEXT DEFAULT ''"),
                         ("launcher_hostname", "TEXT DEFAULT ''"),
                         ("primary_output_dir", "TEXT DEFAULT ''"),
                         ("sdk_status", "TEXT DEFAULT ''"),
                         ("params_json", "TEXT DEFAULT ''")]:
        try:
            con.execute(f"ALTER TABLE runs ADD COLUMN {col} {default}")
        except Exception:
            pass

    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_cluster_board ON job_history(cluster, board_visible)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_cluster_ended ON job_history(cluster, ended_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_project ON job_history(project)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_run_id ON job_history(run_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_runs_cluster_root ON runs(cluster, root_job_id)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS logbook_entries (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            project    TEXT NOT NULL,
            title      TEXT NOT NULL,
            body       TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            edited_at  TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_logbook_project ON logbook_entries(project)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_logbook_title ON logbook_entries(project, title)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_logbook_created ON logbook_entries(project, created_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_logbook_edited ON logbook_entries(project, edited_at)")

    for col, default in [
        ("entry_type", "TEXT NOT NULL DEFAULT 'note'"),
        ("pinned", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        try:
            con.execute(f"ALTER TABLE logbook_entries ADD COLUMN {col} {default}")
        except Exception:
            pass
    con.execute("CREATE INDEX IF NOT EXISTS idx_logbook_type ON logbook_entries(project, entry_type)")

    con.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS logbook_fts USING fts5(
            title, body,
            content=logbook_entries,
            content_rowid=id,
            tokenize='porter unicode61'
        )
    """)

    con.execute("""
        CREATE TRIGGER IF NOT EXISTS logbook_ai AFTER INSERT ON logbook_entries BEGIN
            INSERT INTO logbook_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
        END
    """)
    con.execute("""
        CREATE TRIGGER IF NOT EXISTS logbook_ad AFTER DELETE ON logbook_entries BEGIN
            INSERT INTO logbook_fts(logbook_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
        END
    """)
    con.execute("""
        CREATE TRIGGER IF NOT EXISTS logbook_au AFTER UPDATE ON logbook_entries BEGIN
            INSERT INTO logbook_fts(logbook_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
            INSERT INTO logbook_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
        END
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS job_stats_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster       TEXT NOT NULL,
            job_id        TEXT NOT NULL,
            ts            TEXT NOT NULL,
            gpu_util      REAL,
            gpu_mem_used  REAL,
            gpu_mem_total REAL,
            cpu_util      TEXT,
            rss_used      REAL,
            max_rss       REAL,
            gpu_details   TEXT DEFAULT ''
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_stats_cluster_job ON job_stats_snapshots(cluster, job_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_stats_cluster_job_ts ON job_stats_snapshots(cluster, job_id, ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_cluster_state ON job_history(cluster, state)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_cluster_jobname ON job_history(cluster, job_name)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_ended ON job_history(ended_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_jh_cluster_runid ON job_history(cluster, run_id)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS logbook_links (
            source_id INTEGER NOT NULL,
            target_id INTEGER NOT NULL,
            PRIMARY KEY (source_id, target_id),
            FOREIGN KEY (source_id) REFERENCES logbook_entries(id) ON DELETE CASCADE,
            FOREIGN KEY (target_id) REFERENCES logbook_entries(id) ON DELETE CASCADE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS wds_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              TEXT NOT NULL,
            cluster         TEXT NOT NULL,
            account         TEXT NOT NULL,
            wds             INTEGER NOT NULL,
            resource_gate   REAL,
            my_level_fs     REAL,
            ppp_level_fs    REAL,
            queue_score     REAL,
            idle_nodes      INTEGER,
            pending_queue   INTEGER,
            ppp_headroom    INTEGER,
            free_for_team   INTEGER,
            gpus_consumed   INTEGER,
            gpus_allocated  INTEGER,
            team_running    INTEGER,
            my_running      INTEGER,
            my_pending      INTEGER,
            req_nodes       INTEGER DEFAULT 1,
            req_gpus_per_node INTEGER DEFAULT 8,
            occupancy_factor REAL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_wds_ts ON wds_history(ts)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wds_cluster ON wds_history(cluster, account, ts)")

    try:
        con.execute("ALTER TABLE wds_history ADD COLUMN occupancy_factor REAL")
    except Exception:
        pass

    # ── v2 DB-first tables ────────────────────────────────────────────────
    con.execute("""
        CREATE TABLE IF NOT EXISTS live_jobs (
            cluster    TEXT NOT NULL,
            job_id     TEXT NOT NULL,
            data_json  TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (cluster, job_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cluster_state (
            cluster    TEXT PRIMARY KEY,
            status     TEXT NOT NULL DEFAULT 'ok',
            updated    TEXT,
            last_error TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cache_store (
            namespace  TEXT NOT NULL,
            key        TEXT NOT NULL,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            PRIMARY KEY (namespace, key)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_store(expires_at)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS sdk_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_uuid     TEXT NOT NULL,
            event_type   TEXT NOT NULL,
            event_seq    INTEGER NOT NULL,
            ts           REAL,
            payload_json TEXT DEFAULT '{}',
            UNIQUE(run_uuid, event_seq)
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_sdk_events_uuid ON sdk_events(run_uuid)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_runs_uuid ON runs(run_uuid)")

    con.execute("UPDATE job_history SET board_visible=0 WHERE job_id LIKE 'sdk-%' AND board_visible=1")

    con.commit()
    con.close()


def _resolve_board_visible(cluster, state, current_visible, terminal=False, set_board_visible=None):
    """Decide whether a job should stay pinned on the board.

    Only genuinely terminal states should be pinned through the terminal=True
    path. This prevents a disappeared job from being reinserted as pinned
    PENDING/RUNNING when sacct lags behind squeue.
    """
    state_upper = str(state or "").upper()
    if cluster == "local":
        return 0
    if set_board_visible is not None:
        return set_board_visible
    if terminal:
        return 1 if state_upper in PINNABLE_TERMINAL_STATES else 0
    return current_visible if current_visible is not None else 0


def upsert_job(cluster, job, terminal=False, set_board_visible=None):
    with db_write() as con:
        row = con.execute(
            "SELECT board_visible FROM job_history WHERE cluster=? AND job_id=?",
            (cluster, job["jobid"])
        ).fetchone()
        current_visible = row["board_visible"] if row else None
        bv = _resolve_board_visible(
            cluster,
            job.get("state"),
            current_visible,
            terminal=terminal,
            set_board_visible=set_board_visible,
        )

        dep_raw = job.get("dependency", "")
        if dep_raw in ("(null)", "None", None):
            dep_raw = ""

        from .config import extract_project
        job_name = job.get("name") or job.get("job_name") or ""
        project = job.get("project") or extract_project(job_name)

        node_list_raw = job.get("node_list", "")
        if node_list_raw in ("(null)", "None", None):
            node_list_raw = ""

        account_raw = job.get("account", "")
        if account_raw in ("(null)", "None", None):
            account_raw = ""

        con.execute("""
            INSERT INTO job_history
                (cluster, job_id, job_name, state, exit_code, reason, elapsed,
                 nodes, gres, partition, submitted, started, ended_at, log_path,
                 board_visible, dependency, project, node_list, account)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cluster, job_id) DO UPDATE SET
                job_name    = COALESCE(NULLIF(excluded.job_name, ''), job_name),
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
                log_path    = COALESCE(NULLIF(excluded.log_path, ''), log_path),
                board_visible = excluded.board_visible,
                dependency  = COALESCE(NULLIF(excluded.dependency, ''), dependency),
                project     = COALESCE(NULLIF(excluded.project, ''), project),
                node_list   = COALESCE(NULLIF(excluded.node_list, ''), node_list),
                account     = COALESCE(NULLIF(excluded.account, ''), account)
        """, (
            cluster, job["jobid"],
            job_name,
            job.get("state"),
            job.get("exit_code"), job.get("reason"), job.get("elapsed"),
            job.get("nodes"), job.get("gres"), job.get("partition"),
            job.get("submitted"), job.get("started"),
            job.get("ended_at"), job.get("log_path"),
            bv, dep_raw, project, node_list_raw, account_raw,
        ))
    if terminal or set_board_visible is not None:
        invalidate_pinned_cache(cluster)


def upsert_jobs_batch(cluster, jobs, terminal=False):
    """Batch-upsert multiple live jobs in a single transaction."""
    if not jobs:
        return
    from .config import extract_project

    with db_write() as con:
        jids = [j["jobid"] for j in jobs]
        placeholders = ",".join("?" for _ in jids)
        rows = con.execute(
            f"SELECT job_id, board_visible FROM job_history WHERE cluster=? AND job_id IN ({placeholders})",
            (cluster, *jids),
        ).fetchall()
        existing_bv = {r["job_id"]: r["board_visible"] for r in rows}

        params = []
        for job in jobs:
            jid = job["jobid"]
            current_visible = existing_bv.get(jid)
            bv = _resolve_board_visible(
                cluster,
                job.get("state"),
                current_visible,
                terminal=terminal,
            )

            dep_raw = job.get("dependency", "")
            if dep_raw in ("(null)", "None", None):
                dep_raw = ""
            job_name = job.get("name") or job.get("job_name") or ""
            project = job.get("project") or extract_project(job_name)
            node_list_raw = job.get("node_list", "")
            if node_list_raw in ("(null)", "None", None):
                node_list_raw = ""
            account_raw = job.get("account", "")
            if account_raw in ("(null)", "None", None):
                account_raw = ""

            params.append((
                cluster, jid, job_name,
                job.get("state"),
                job.get("exit_code"), job.get("reason"), job.get("elapsed"),
                job.get("nodes"), job.get("gres"), job.get("partition"),
                job.get("submitted"), job.get("started"),
                job.get("ended_at"), job.get("log_path"),
                bv, dep_raw, project, node_list_raw, account_raw,
            ))

        con.executemany("""
            INSERT INTO job_history
                (cluster, job_id, job_name, state, exit_code, reason, elapsed,
                 nodes, gres, partition, submitted, started, ended_at, log_path,
                 board_visible, dependency, project, node_list, account)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cluster, job_id) DO UPDATE SET
                job_name    = COALESCE(NULLIF(excluded.job_name, ''), job_name),
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
                log_path    = COALESCE(NULLIF(excluded.log_path, ''), log_path),
                board_visible = excluded.board_visible,
                dependency  = COALESCE(NULLIF(excluded.dependency, ''), dependency),
                project     = COALESCE(NULLIF(excluded.project, ''), project),
                node_list   = COALESCE(NULLIF(excluded.node_list, ''), node_list),
                account     = COALESCE(NULLIF(excluded.account, ''), account)
        """, params)
    if terminal:
        invalidate_pinned_cache(cluster)


def upsert_history(cluster, job):
    upsert_job(cluster, job)


_pinned_cache = {}
_pinned_cache_ts = {}
_pinned_cache_lock = _th_db.Lock()
_PINNED_CACHE_TTL = 12


def invalidate_pinned_cache(cluster=None):
    """Clear the pinned-jobs cache. Call after dismiss/upsert changes."""
    with _pinned_cache_lock:
        if cluster:
            _pinned_cache.pop(cluster, None)
            _pinned_cache_ts.pop(cluster, None)
        else:
            _pinned_cache.clear()
            _pinned_cache_ts.clear()
        _pinned_cache.pop("__all__", None)
        _pinned_cache_ts.pop("__all__", None)


def get_board_pinned(cluster=None):
    from .jobs import parse_dependency
    cache_key = cluster or "__all__"
    now = _th_db.monotonic_ns if hasattr(_th_db, 'monotonic_ns') else None

    import time as _time
    now = _time.monotonic()
    with _pinned_cache_lock:
        if cache_key in _pinned_cache and (now - _pinned_cache_ts.get(cache_key, 0)) < _PINNED_CACHE_TTL:
            return _pinned_cache[cache_key]

    con = get_db()
    if cluster:
        rows = con.execute(
            "SELECT * FROM job_history WHERE cluster=? AND board_visible=1 AND cluster != 'local' ORDER BY id DESC",
            (cluster,)
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM job_history WHERE board_visible=1 AND cluster != 'local' ORDER BY id DESC"
        ).fetchall()
    con.close()
    jobs = [normalize_job_times_local(dict(r)) for r in rows]
    _restore_dependency_fields(jobs, parse_dependency)

    with _pinned_cache_lock:
        _pinned_cache[cache_key] = jobs
        _pinned_cache_ts[cache_key] = now

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
    invalidate_pinned_cache(cluster)


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
    invalidate_pinned_cache(cluster)


def _csv_values(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    values = []
    for part in value:
        if part is None:
            continue
        values.extend(_csv_values(part))
    return values


def get_history(
    cluster=None,
    limit=200,
    project=None,
    search=None,
    state=None,
    campaign=None,
    partition=None,
    account=None,
    days=None,
):
    from .jobs import parse_dependency
    from .config import extract_campaign
    from datetime import datetime, timedelta

    con = get_db()
    order = "ORDER BY COALESCE(jh.ended_at, jh.started, jh.submitted, '9999') DESC, jh.id DESC"
    conditions = []
    params = []
    campaign_values = {v.lower() for v in _csv_values(campaign)}
    if cluster and cluster != "all":
        conditions.append("jh.cluster=?")
        params.append(cluster)
    if project:
        conditions.append("jh.project=?")
        params.append(project)
    if search:
        like = f"%{search}%"
        conditions.append(
            "("
            "LOWER(COALESCE(jh.job_name, '')) LIKE LOWER(?) OR "
            "CAST(jh.job_id AS TEXT) LIKE ? OR "
            "LOWER(COALESCE(r.run_name, '')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(jh.project, '')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(jh.partition, '')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(jh.account, '')) LIKE LOWER(?) OR "
            "LOWER(COALESCE(jh.cluster, '')) LIKE LOWER(?)"
            ")"
        )
        params.extend([like, like, like, like, like, like, like])
    state_values = [v.upper() for v in _csv_values(state)]
    if state_values:
        conditions.append("(" + " OR ".join(["UPPER(COALESCE(jh.state, '')) LIKE ?"] * len(state_values)) + ")")
        params.extend([f"{value}%" for value in state_values])
    if partition:
        conditions.append("LOWER(COALESCE(jh.partition, '')) = LOWER(?)")
        params.append(partition)
    if account:
        conditions.append("LOWER(COALESCE(jh.account, '')) = LOWER(?)")
        params.append(account)
    if days:
        try:
            days_int = int(days)
        except (TypeError, ValueError):
            days_int = 0
        if days_int > 0:
            cutoff = (datetime.now() - timedelta(days=days_int)).isoformat()
            conditions.append("COALESCE(jh.ended_at, jh.started, jh.submitted, '') >= ?")
            params.append(cutoff)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    query = (
        "SELECT jh.*, COALESCE(r.run_name, '') AS run_name, "
        "COALESCE(r.starred, 0) AS starred "
        "FROM job_history jh "
        "LEFT JOIN runs r ON r.id = jh.run_id AND r.cluster = jh.cluster "
        f"{where} {order}"
    )
    query_params = list(params)
    if not campaign_values:
        query += " LIMIT ?"
        query_params.append(limit)
    rows = con.execute(query, query_params).fetchall()
    con.close()
    jobs = [normalize_job_times_local(dict(r)) for r in rows]
    _restore_dependency_fields(jobs, parse_dependency)
    if campaign_values:
        jobs = [
            job for job in jobs
            if extract_campaign(job.get("job_name") or job.get("name") or "", job.get("project") or "") in campaign_values
        ]
        jobs = jobs[:limit]
    return jobs


def get_projects():
    """Return distinct projects with job count and latest activity."""
    con = get_db()
    rows = con.execute("""
        SELECT project,
               COUNT(*) as job_count,
               MAX(COALESCE(ended_at, started, submitted)) as last_active
        FROM job_history
        WHERE project != '' AND project IS NOT NULL
        GROUP BY project
        ORDER BY last_active DESC
    """).fetchall()
    con.close()
    return [dict(r) for r in rows]


def cleanup_local_on_startup():
    """Dismiss local process entries on startup.

    Local PIDs are ephemeral and meaningless after a restart, so clear
    them from the board.  Remote pinned jobs are left untouched — only
    the user can dismiss those via the UI.
    """
    con = get_db()
    con.execute("UPDATE job_history SET board_visible=0 WHERE cluster='local'")
    con.commit()
    con.close()


# Keep old name as alias so existing callers don't break.
repin_recent_terminal_jobs = cleanup_local_on_startup


# ─── Run CRUD ────────────────────────────────────────────────────────────────

def upsert_run(cluster, root_job_id, run_name="", project=""):
    """Create or return existing run. Returns the run id."""
    with db_write() as con:
        row = con.execute(
            "SELECT id FROM runs WHERE cluster=? AND root_job_id=?",
            (cluster, root_job_id),
        ).fetchone()
        if row:
            run_id = row["id"]
            if run_name or project:
                con.execute(
                    "UPDATE runs SET run_name=COALESCE(NULLIF(?,''), run_name), "
                    "project=COALESCE(NULLIF(?,''), project) WHERE id=?",
                    (run_name, project, run_id),
                )
        else:
            sdk_run = _find_sdk_run_for_name(con, cluster, run_name) if run_name else None
            if sdk_run:
                run_id = sdk_run["id"]
                con.execute(
                    "UPDATE runs SET run_name=COALESCE(NULLIF(?,''), run_name), "
                    "project=COALESCE(NULLIF(?,''), project) WHERE id=?",
                    (run_name, project, run_id),
                )
            else:
                cur = con.execute(
                    "INSERT INTO runs (cluster, root_job_id, run_name, project) VALUES (?,?,?,?)",
                    (cluster, root_job_id, run_name, project),
                )
                run_id = cur.lastrowid
        return run_id


def _find_sdk_run_for_name(con, cluster, run_name):
    """Check if an SDK run exists that matches a legacy run's name.

    The SDK expname (e.g. 'hle_test_eval-gpqa4') often appears as a
    substring in the legacy run_name (e.g. 'profiling_hle_test_eval-gpqa4-gpqa')
    because the poller appends benchmark/job suffixes and the cluster config
    adds a job_name_prefix. Try exact, LIKE-substring, and reverse-substring.
    """
    if not run_name:
        return None

    sdk_filter = "source='sdk' AND sdk_status NOT IN ('completed', 'failed')"

    row = con.execute(
        f"SELECT id, run_name FROM runs WHERE cluster=? AND {sdk_filter} AND run_name=? LIMIT 1",
        (cluster, run_name),
    ).fetchone()
    if row:
        return row

    candidates = [run_name]
    parts = run_name.split("_", 1)
    if len(parts) == 2:
        candidates.append(parts[1])

    for name in candidates:
        row = con.execute(
            f"SELECT id, run_name FROM runs WHERE cluster=? AND {sdk_filter} AND run_name LIKE ? ORDER BY id DESC LIMIT 1",
            (cluster, f"%{name}%"),
        ).fetchone()
        if row:
            return row

    for name in candidates:
        row = con.execute(
            f"SELECT id, run_name FROM runs WHERE cluster=? AND {sdk_filter} AND ? LIKE '%' || run_name || '%' ORDER BY id DESC LIMIT 1",
            (cluster, name),
        ).fetchone()
        if row:
            return row

    return None


def update_run_meta(run_id, batch_script="", scontrol_raw="", env_vars="", conda_state=""):
    has_data = any([batch_script, scontrol_raw, env_vars])
    with db_write() as con:
        con.execute("""
            UPDATE runs SET
                batch_script  = COALESCE(NULLIF(?, ''), batch_script),
                scontrol_raw  = COALESCE(NULLIF(?, ''), scontrol_raw),
                env_vars      = COALESCE(NULLIF(?, ''), env_vars),
                conda_state   = COALESCE(NULLIF(?, ''), conda_state),
                meta_fetched  = ?
            WHERE id = ?
        """, (batch_script, scontrol_raw, env_vars, conda_state,
              1 if has_data else 0, run_id))


def update_run_fields(run_id, starred=None, notes=None):
    """Partial update of user-editable run fields (starred, notes)."""
    sets, params = [], []
    if starred is not None:
        sets.append("starred = ?")
        params.append(int(starred))
    if notes is not None:
        sets.append("notes = ?")
        params.append(notes)
    if not sets:
        return
    params.append(run_id)
    with db_write() as con:
        con.execute(f"UPDATE runs SET {', '.join(sets)} WHERE id = ?", params)


def update_run_times(run_id, started_at=None, ended_at=None):
    with db_write() as con:
        if started_at:
            con.execute(
                "UPDATE runs SET started_at = ? WHERE id = ? AND (started_at IS NULL OR started_at > ?)",
                (started_at, run_id, started_at),
            )
        if ended_at:
            con.execute(
                "UPDATE runs SET ended_at = ? WHERE id = ? AND (ended_at IS NULL OR ended_at < ?)",
                (ended_at, run_id, ended_at),
            )


def get_run(cluster, root_job_id):
    """Return run record dict or None."""
    con = get_db()
    row = con.execute(
        "SELECT * FROM runs WHERE cluster=? AND root_job_id=?",
        (cluster, root_job_id),
    ).fetchone()
    con.close()
    return dict(row) if row else None


def get_run_by_uuid(run_uuid):
    """Return run record dict by SDK run_uuid, or None."""
    con = get_db()
    row = con.execute("SELECT * FROM runs WHERE run_uuid=?", (run_uuid,)).fetchone()
    con.close()
    return dict(row) if row else None


def _build_full_submit_command(provenance):
    """Reconstruct a full reproducible shell command from SDK provenance."""
    lines = []

    cwd = provenance.get("cwd", "")
    conda_env = provenance.get("conda_env", "")
    env_vars_set = provenance.get("env_vars_set", [])
    argv = provenance.get("argv", [])
    command = provenance.get("command", "")

    if argv and (not command or len(command) < len(" ".join(str(a) for a in argv))):
        command = " ".join(str(a) for a in argv)

    if conda_env and conda_env != "base":
        lines.append(f"conda activate {conda_env}")
    elif not conda_env:
        venv = provenance.get("env_subset", {}).get("VIRTUAL_ENV", "")
        if venv:
            lines.append(f"source {venv}/bin/activate")

    if cwd:
        lines.append(f"cd {cwd}")

    if env_vars_set:
        cmd_line = " \\\n  ".join(env_vars_set) + " \\\n  " + command
    else:
        cmd_line = command

    lines.append(cmd_line)
    return "\n".join(lines)


def upsert_run_from_sdk(run_uuid, cluster, expname, project, provenance):
    """Create or update a run from SDK run_started event. Returns run_id."""
    from datetime import datetime
    synthetic_job_id = f"sdk-{run_uuid[:12]}"

    command = _build_full_submit_command(provenance)
    params_obj = provenance.get("params") or {}
    params_json = _json.dumps(params_obj) if params_obj else ""

    with db_write() as con:
        row = con.execute("SELECT id FROM runs WHERE run_uuid=?", (run_uuid,)).fetchone()
        if row:
            run_id = row["id"]
            con.execute("""
                UPDATE runs SET
                    run_name       = COALESCE(NULLIF(?, ''), run_name),
                    project        = COALESCE(NULLIF(?, ''), project),
                    submit_command = COALESCE(NULLIF(?, ''), submit_command),
                    submit_cwd     = COALESCE(NULLIF(?, ''), submit_cwd),
                    git_commit     = COALESCE(NULLIF(?, ''), git_commit),
                    launcher_hostname = COALESCE(NULLIF(?, ''), launcher_hostname),
                    primary_output_dir = COALESCE(NULLIF(?, ''), primary_output_dir),
                    params_json    = COALESCE(NULLIF(?, ''), params_json),
                    sdk_status     = CASE WHEN sdk_status IN ('', 'submitting') THEN 'submitting' ELSE sdk_status END
                WHERE id = ?
            """, (
                expname, project,
                command,
                provenance.get("cwd", ""),
                provenance.get("git_commit", ""),
                provenance.get("hostname", ""),
                provenance.get("output_dir", ""),
                params_json,
                run_id,
            ))
        else:
            now = datetime.now().isoformat(timespec="seconds")
            cur = con.execute("""
                INSERT INTO runs
                    (cluster, root_job_id, run_name, project, run_uuid, source,
                     submit_command, submit_cwd, git_commit, launcher_hostname,
                     primary_output_dir, params_json, sdk_status,
                     started_at, created_at, meta_fetched)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                cluster, synthetic_job_id, expname, project, run_uuid, "sdk",
                command,
                provenance.get("cwd", ""),
                provenance.get("git_commit", ""),
                provenance.get("hostname", ""),
                provenance.get("output_dir", ""),
                params_json,
                "submitting", now, now, 1,
            ))
            run_id = cur.lastrowid

        env_json = _json.dumps(provenance.get("env_subset", {}))
        con.execute("""
            UPDATE runs SET env_vars = COALESCE(NULLIF(?, ''), env_vars) WHERE id = ?
        """, (env_json, run_id))

        from .config import extract_project
        job_project = project or extract_project(expname)
        now_ts = datetime.now().isoformat(timespec="seconds")
        con.execute("""
            INSERT INTO job_history
                (cluster, job_id, job_name, state, board_visible, project, run_id, submitted, started)
            VALUES (?, ?, ?, 'SUBMITTING', 0, ?, ?, ?, ?)
            ON CONFLICT(cluster, job_id) DO UPDATE SET
                state = CASE WHEN job_history.state IN ('COMPLETED','FAILED','CANCELLED','TIMEOUT','RUNNING','PENDING')
                             THEN job_history.state ELSE excluded.state END,
                run_id = excluded.run_id,
                project = COALESCE(NULLIF(excluded.project, ''), job_history.project)
        """, (cluster, synthetic_job_id, expname, job_project, run_id, now_ts, now_ts))

    return run_id


def store_sdk_event(run_uuid, event_type, event_seq, ts, payload_json):
    """Insert an SDK event, ignoring duplicates."""
    with db_write() as con:
        con.execute("""
            INSERT OR IGNORE INTO sdk_events (run_uuid, event_type, event_seq, ts, payload_json)
            VALUES (?, ?, ?, ?, ?)
        """, (run_uuid, event_type, event_seq, ts, payload_json))


def cancel_sdk_job(synthetic_job_id):
    """Cancel an SDK synthetic job by its job_id (sdk-xxxx). Updates job + run."""
    from datetime import datetime
    ts = datetime.now().isoformat(timespec="seconds")
    with db_write() as con:
        row = con.execute(
            "SELECT cluster, run_id FROM job_history WHERE job_id=?", (synthetic_job_id,)
        ).fetchone()
        if not row:
            return
        cluster = row["cluster"]
        run_id = row["run_id"]
        con.execute(
            "UPDATE job_history SET state='CANCELLED', ended_at=COALESCE(ended_at, ?) WHERE job_id=?",
            (ts, synthetic_job_id),
        )
        if run_id:
            con.execute(
                "UPDATE runs SET sdk_status='failed', ended_at=COALESCE(ended_at, ?) WHERE id=?",
                (ts, run_id),
            )
    invalidate_pinned_cache(cluster)


def finalize_sdk_run(run_uuid, status, ended_at=None):
    """Mark an SDK run as finished/failed and update the synthetic job."""
    from datetime import datetime
    ts = ended_at or datetime.now().isoformat(timespec="seconds")
    synthetic_job_id = f"sdk-{run_uuid[:12]}"
    if status == "submitted":
        with db_write() as con:
            con.execute(
                "UPDATE runs SET sdk_status='active' WHERE run_uuid=? AND sdk_status IN ('submitting', '')",
                (run_uuid,),
            )
            con.execute(
                """UPDATE job_history SET state = CASE WHEN state = 'SUBMITTING' THEN 'PENDING' ELSE state END
                   WHERE job_id = ? AND cluster = (SELECT cluster FROM runs WHERE run_uuid = ?)""",
                (synthetic_job_id, run_uuid),
            )
        return
    if status in ("failed", "submit_failed"):
        final_state = "FAILED"
    else:
        final_state = "COMPLETED"
    with db_write() as con:
        con.execute("""
            UPDATE runs SET sdk_status = ?, ended_at = COALESCE(ended_at, ?) WHERE run_uuid = ?
        """, (status, ts, run_uuid))

        run_row = con.execute("SELECT cluster FROM runs WHERE run_uuid = ?", (run_uuid,)).fetchone()
        if run_row:
            con.execute("""
                UPDATE job_history SET state = ?, ended_at = COALESCE(ended_at, ?)
                WHERE cluster = ? AND job_id = ?
            """, (final_state, ts, run_row["cluster"], synthetic_job_id))
    invalidate_pinned_cache(run_row["cluster"] if run_row else "")


def get_run_with_jobs(cluster, root_job_id):
    """Return run metadata + all associated jobs."""
    con = get_db()
    run_row = con.execute(
        "SELECT * FROM runs WHERE cluster=? AND root_job_id=?",
        (cluster, root_job_id),
    ).fetchone()
    if not run_row:
        con.close()
        return None
    run = dict(run_row)
    job_rows = con.execute(
        "SELECT * FROM job_history WHERE run_id=? ORDER BY submitted, id",
        (run["id"],),
    ).fetchall()
    con.close()
    from .jobs import parse_dependency
    jobs = [normalize_job_times_local(dict(r)) for r in job_rows]
    _restore_dependency_fields(jobs, parse_dependency)
    run["jobs"] = jobs
    return run


def associate_jobs_to_run(cluster, run_id, job_ids):
    """Set run_id on job_history rows for the given job IDs."""
    if not job_ids:
        return
    with db_write() as con:
        placeholders = ",".join("?" for _ in job_ids)
        con.execute(
            f"UPDATE job_history SET run_id=? WHERE cluster=? AND job_id IN ({placeholders})",
            [run_id, cluster] + list(job_ids),
        )


# ─── Safe DB access ─────────────────────────────────────────────────────────

@contextmanager
def db_connection():
    """Context manager for safe DB access with auto-close."""
    con = get_db()
    try:
        yield con
    finally:
        con.close()


# ─── DB-first v2: live board + persistent cache ──────────────────────────────

def replace_live_jobs(cluster, jobs):
    """Atomically replace all live jobs for a cluster."""
    now = datetime.now().isoformat(timespec="seconds")
    with db_write() as con:
        con.execute("DELETE FROM live_jobs WHERE cluster=?", (cluster,))
        if jobs:
            con.executemany(
                "INSERT INTO live_jobs (cluster, job_id, data_json, updated_at) VALUES (?, ?, ?, ?)",
                [(cluster, j.get("jobid", ""), _json.dumps(j, default=str), now) for j in jobs],
            )


def get_live_board():
    """Read full live board + cluster states from DB.

    Returns (board_dict, states_dict) where:
      board_dict  = {cluster: [job_dicts]}
      states_dict = {cluster: {"status", "updated", "last_error"}}
    """
    con = get_db()
    rows = con.execute("SELECT cluster, data_json FROM live_jobs").fetchall()
    states = con.execute("SELECT cluster, status, updated, last_error FROM cluster_state").fetchall()
    con.close()

    board = {}
    for row in rows:
        board.setdefault(row["cluster"], []).append(_json.loads(row["data_json"]))

    state_dict = {}
    for s in states:
        state_dict[s["cluster"]] = {
            "status": s["status"],
            "updated": s["updated"],
            "last_error": s["last_error"],
        }
    return board, state_dict


def get_live_jobs_for_cluster(cluster):
    """Read live jobs for one cluster. Returns (jobs_list, state_dict_or_None)."""
    con = get_db()
    rows = con.execute("SELECT data_json FROM live_jobs WHERE cluster=?", (cluster,)).fetchall()
    state = con.execute(
        "SELECT status, updated, last_error FROM cluster_state WHERE cluster=?", (cluster,),
    ).fetchone()
    con.close()
    jobs = [_json.loads(r["data_json"]) for r in rows]
    return jobs, (dict(state) if state else None)


def set_cluster_state(cluster, status, updated, last_error=None):
    """Upsert the poll state for a cluster."""
    with db_write() as con:
        if last_error is None:
            con.execute("""
                INSERT INTO cluster_state (cluster, status, updated)
                VALUES (?, ?, ?)
                ON CONFLICT(cluster) DO UPDATE SET
                    status=excluded.status, updated=excluded.updated, last_error=NULL
            """, (cluster, status, updated))
        else:
            con.execute("""
                INSERT INTO cluster_state (cluster, status, updated, last_error)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cluster) DO UPDATE SET
                    status=excluded.status, updated=excluded.updated,
                    last_error=excluded.last_error
            """, (cluster, status, updated, last_error))


def cache_db_put(namespace, key, value, ttl_sec):
    """Write a value to the persistent cache store with a TTL."""
    now = datetime.now()
    expires = now + timedelta(seconds=ttl_sec)
    with db_write() as con:
        con.execute("""
            INSERT INTO cache_store (namespace, key, value_json, updated_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value_json=excluded.value_json,
                updated_at=excluded.updated_at,
                expires_at=excluded.expires_at
        """, (namespace, key, _json.dumps(value, default=str),
              now.isoformat(timespec="seconds"),
              expires.isoformat(timespec="seconds")))


def cache_db_get(namespace, key):
    """Read a non-expired value from cache store. Returns None if missing/expired."""
    con = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    row = con.execute(
        "SELECT value_json FROM cache_store WHERE namespace=? AND key=? AND expires_at>?",
        (namespace, key, now),
    ).fetchone()
    con.close()
    return _json.loads(row["value_json"]) if row else None


def cache_db_get_stale(namespace, key):
    """Read from cache store even if expired.

    Returns (value, is_fresh) or (None, False) if not found.
    """
    con = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    row = con.execute(
        "SELECT value_json, expires_at FROM cache_store WHERE namespace=? AND key=?",
        (namespace, key),
    ).fetchone()
    con.close()
    if row:
        return _json.loads(row["value_json"]), row["expires_at"] > now
    return None, False


def cache_db_get_all(namespace):
    """Read all non-expired entries for a namespace. Returns {key: value}."""
    con = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    rows = con.execute(
        "SELECT key, value_json FROM cache_store WHERE namespace=? AND expires_at>?",
        (namespace, now),
    ).fetchall()
    con.close()
    return {r["key"]: _json.loads(r["value_json"]) for r in rows}


def cache_db_get_all_multi(namespaces):
    """Read all non-expired entries for multiple namespaces in one query.

    Returns {namespace: {key: value}}.
    """
    if not namespaces:
        return {}
    con = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    ph = ",".join("?" for _ in namespaces)
    rows = con.execute(
        f"SELECT namespace, key, value_json FROM cache_store WHERE namespace IN ({ph}) AND expires_at>?",
        list(namespaces) + [now],
    ).fetchall()
    con.close()
    result = {ns: {} for ns in namespaces}
    for r in rows:
        result[r["namespace"]][r["key"]] = _json.loads(r["value_json"])
    return result


def cache_db_gc():
    """Remove expired cache entries and stale live_jobs for removed clusters."""
    from .config import CLUSTERS
    con = get_db()
    now = datetime.now().isoformat(timespec="seconds")
    con.execute("DELETE FROM cache_store WHERE expires_at<?", (now,))
    if CLUSTERS:
        ph = ",".join("?" for _ in CLUSTERS)
        con.execute(f"DELETE FROM live_jobs WHERE cluster NOT IN ({ph})", list(CLUSTERS.keys()))
        con.execute(f"DELETE FROM cluster_state WHERE cluster NOT IN ({ph})", list(CLUSTERS.keys()))
    con.commit()
    con.close()
