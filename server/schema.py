"""Canonical SQLite schema for clausius v4.

Single source of truth for every table, index, trigger, and well-known
``app_settings`` key. ``server.db.init_db()`` runs every statement in
:data:`SCHEMA` (idempotent thanks to ``IF NOT EXISTS``) and every
``ALTER TABLE`` in :data:`MIGRATIONS` (each one wrapped in a
try/except so re-runs against an already-migrated DB are no-ops).

Why a single file instead of a migration framework
--------------------------------------------------
clausius runs on a single-user box backed by SQLite. We don't need
Alembic/yoyo: shipping the canonical table definitions in code is
clearer for new users (``python -m server.cli setup`` creates everything
from scratch) and the rare schema additions can be expressed as
idempotent ``ALTER TABLE`` statements at the bottom of this file.

When you add a NEW table:
  - Append it to :data:`SCHEMA` with a verbose docstring explaining
    purpose, columns, lifetime, and example rows.
  - Add the matching CRUD module under ``server/`` and tests.

When you add a NEW column to an EXISTING table:
  - Update the ``CREATE TABLE`` statement in :data:`SCHEMA` so fresh
    installs get the new column.
  - Append an ``ALTER TABLE … ADD COLUMN`` to :data:`MIGRATIONS` so old
    DBs pick up the column on next ``init_db()`` call.

When you add a NEW well-known app_settings key:
  - Register it in :data:`APP_SETTINGS_DEFAULTS` so ``server.settings``
    knows the type and default value.
"""

from __future__ import annotations

from typing import Any, Callable, Dict


# ─── Tables (CREATE … IF NOT EXISTS) ─────────────────────────────────────────

JOB_HISTORY = """
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
    started       TEXT,
    ended_at      TEXT,
    log_path      TEXT,
    board_visible INTEGER DEFAULT 0,
    dependency    TEXT DEFAULT '',
    project       TEXT DEFAULT '',
    run_id        INTEGER DEFAULT NULL,
    node_list     TEXT DEFAULT '',
    account       TEXT DEFAULT '',
    custom_log_dir TEXT DEFAULT '',
    custom_metrics_config TEXT DEFAULT '',
    waste_reason  TEXT NOT NULL DEFAULT '',
    waste_cancelled_at TEXT NOT NULL DEFAULT '',
    UNIQUE(cluster, job_id)
)
"""
"""One row per Slurm job ever observed (live + historical).

Populated by the poller (``server/poller.py``) and the SDK ingest
endpoint. ``board_visible=1`` keeps a terminal job pinned to the live
board until the user dismisses it.
"""

RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster            TEXT NOT NULL,
    root_job_id        TEXT NOT NULL,
    run_name           TEXT DEFAULT '',
    project            TEXT DEFAULT '',
    batch_script       TEXT DEFAULT '',
    scontrol_raw       TEXT DEFAULT '',
    env_vars           TEXT DEFAULT '',
    conda_state        TEXT DEFAULT '',
    started_at         TEXT,
    ended_at           TEXT,
    created_at         TEXT DEFAULT (datetime('now')),
    meta_fetched       INTEGER DEFAULT 0,
    starred            INTEGER DEFAULT 0,
    notes              TEXT DEFAULT '',
    run_uuid           TEXT DEFAULT '',
    source             TEXT DEFAULT 'legacy',
    submit_command     TEXT DEFAULT '',
    submit_cwd         TEXT DEFAULT '',
    git_commit         TEXT DEFAULT '',
    launcher_hostname  TEXT DEFAULT '',
    primary_output_dir TEXT DEFAULT '',
    sdk_status         TEXT DEFAULT '',
    params_json        TEXT DEFAULT '',
    metadata_json      TEXT DEFAULT '',
    malfunctioned      INTEGER NOT NULL DEFAULT 0,
    tags_json          TEXT NOT NULL DEFAULT '[]',
    wasteful           INTEGER NOT NULL DEFAULT 0,
    waste_reason       TEXT NOT NULL DEFAULT '',
    waste_detected_at  TEXT NOT NULL DEFAULT '',
    waste_cancelled_by_watcher INTEGER NOT NULL DEFAULT 0,
    UNIQUE(cluster, root_job_id)
)
"""
"""One row per logical experiment run (groups multiple Slurm jobs).

A run links 1..N rows in ``job_history`` via ``job_history.run_id``.
Created either by the poller (legacy: based on dependency-chain
detection) or by the SDK ingest endpoint (``source='sdk'``).
"""

RUN_TAGS = """
CREATE TABLE IF NOT EXISTS run_tags (
    run_id     INTEGER NOT NULL,
    tag        TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (run_id, tag),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
)
"""
"""Many-to-many-style tag assignment for runs.

Each row assigns one normalized tag to one logical run. ``runs.tags_json`` is
kept only as a legacy migration/cache column for older DBs and callers; this
table is the authoritative source for current tag reads and writes.
"""

RUN_TAG_DEFS = """
CREATE TABLE IF NOT EXISTS run_tag_defs (
    tag        TEXT PRIMARY KEY,
    color      TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""Shared metadata for run tags.

``run_tags`` stores assignments. ``run_tag_defs`` stores properties shared by
all runs carrying the same tag, currently the editable chip color.
"""

LOGBOOK_ENTRIES = """
CREATE TABLE IF NOT EXISTS logbook_entries (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    project    TEXT NOT NULL,
    title      TEXT NOT NULL,
    body       TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    edited_at  TEXT NOT NULL,
    entry_type TEXT NOT NULL DEFAULT 'note',
    pinned     INTEGER NOT NULL DEFAULT 0,
    campaign       TEXT NOT NULL DEFAULT '',
    board_json     TEXT NOT NULL DEFAULT '',
    campaign_goal  TEXT NOT NULL DEFAULT '',
    graph_json     TEXT NOT NULL DEFAULT ''
)
"""
"""Per-project structured notes/plans with FTS5 search.

``entry_type`` is ``'note'``, ``'plan'``, ``'campaign_board'`` (legacy), or
``'mind_map'``. ``campaign_board`` rows hold structured result grids in
``board_json`` (JSON) and at most one exists per ``(project, campaign)``;
new content is authored as ``mind_map`` instead, which stores a static DAG
of tasks/experiments/bugs/decisions in ``graph_json`` and is also singleton
per ``(project, campaign)``. Both types share an optional ``campaign_goal``
prose blurb. ``pinned=1`` floats an entry to the top of list views. Entry
IDs are globally unique across projects so ``#N`` cross-references work even
after a move.
"""

LOGBOOK_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS logbook_fts USING fts5(
    title, body,
    content=logbook_entries,
    content_rowid=id,
    tokenize='porter unicode61'
)
"""
"""FTS5 index over ``logbook_entries.title`` + ``body`` for BM25 search."""

LOGBOOK_LINKS = """
CREATE TABLE IF NOT EXISTS logbook_links (
    source_id INTEGER NOT NULL,
    target_id INTEGER NOT NULL,
    PRIMARY KEY (source_id, target_id),
    FOREIGN KEY (source_id) REFERENCES logbook_entries(id) ON DELETE CASCADE,
    FOREIGN KEY (target_id) REFERENCES logbook_entries(id) ON DELETE CASCADE
)
"""
"""Adjacency table for ``#N`` cross-references between logbook entries."""

JOB_STATS_SNAPSHOTS = """
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
"""
"""Periodic resource snapshots for running jobs (powers the stats charts)."""

WASTE_WATCHER_STATE = """
CREATE TABLE IF NOT EXISTS waste_watcher_state (
    cluster                       TEXT NOT NULL,
    job_id                        TEXT NOT NULL,
    state                         TEXT NOT NULL,
    state_entered_at              TEXT NOT NULL,
    last_probe_at                 TEXT,
    next_probe_due                TEXT NOT NULL,
    consecutive_zero_util_samples INTEGER NOT NULL DEFAULT 0,
    last_log_hash                 TEXT NOT NULL DEFAULT '',
    last_log_change_at            TEXT,
    last_sdk_heartbeat_at         TEXT,
    suspected_reason              TEXT NOT NULL DEFAULT '',
    suspected_confidence          TEXT NOT NULL DEFAULT '',
    exempt_until                  TEXT,
    last_notes                    TEXT NOT NULL DEFAULT '',
    updated_at                    TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (cluster, job_id)
)
"""
"""Per-job adaptive-sampling state for the WasteWatcher.

``state`` is one of ``cold``, ``warm``, ``suspicious``, ``wasteful``,
``exempt``. ``next_probe_due`` is the ISO-8601 timestamp the watcher loop
should not re-probe before. ``consecutive_zero_util_samples`` and the
log-hash freshness fields drive the cold->suspicious->wasteful
escalation. ``suspected_reason`` records which detection rule fired
(one of the ``WASTE_REASON_*`` constants in ``server/waste_watcher.py``).
``exempt_until`` is a timestamp; rows past it auto-re-enter ``cold``.
"""

WDS_HISTORY = """
CREATE TABLE IF NOT EXISTS wds_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                TEXT NOT NULL,
    cluster           TEXT NOT NULL,
    account           TEXT NOT NULL,
    wds               INTEGER NOT NULL,
    resource_gate     REAL,
    my_level_fs       REAL,
    ppp_level_fs      REAL,
    queue_score       REAL,
    idle_nodes        INTEGER,
    pending_queue     INTEGER,
    ppp_headroom      INTEGER,
    free_for_team     INTEGER,
    gpus_consumed     INTEGER,
    gpus_allocated    INTEGER,
    team_running      INTEGER,
    my_running        INTEGER,
    my_pending        INTEGER,
    req_nodes         INTEGER DEFAULT 1,
    req_gpus_per_node INTEGER DEFAULT 8,
    occupancy_factor  REAL
)
"""
"""'Where do I submit?' time-series snapshots from the WDS scorer."""

LIVE_JOBS = """
CREATE TABLE IF NOT EXISTS live_jobs (
    cluster    TEXT NOT NULL,
    job_id     TEXT NOT NULL,
    data_json  TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (cluster, job_id)
)
"""
"""DB-first live job board (survives gunicorn restarts; used by both
gunicorn and MCP processes)."""

CLUSTER_STATE = """
CREATE TABLE IF NOT EXISTS cluster_state (
    cluster    TEXT PRIMARY KEY,
    status     TEXT NOT NULL DEFAULT 'ok',
    updated    TEXT,
    last_error TEXT
)
"""
"""Per-cluster poll state: ``ok`` / ``unreachable`` / ``stale`` plus the
last error message to show in the UI."""

CACHE_STORE = """
CREATE TABLE IF NOT EXISTS cache_store (
    namespace  TEXT NOT NULL,
    key        TEXT NOT NULL,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (namespace, key)
)
"""
"""Persistent TTL cache shared across processes (gunicorn + MCP)."""

SDK_EVENTS = """
CREATE TABLE IF NOT EXISTS sdk_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_uuid     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    event_seq    INTEGER NOT NULL,
    ts           REAL,
    payload_json TEXT DEFAULT '{}',
    UNIQUE(run_uuid, event_seq)
)
"""
"""Append-only event log from the NeMo-Skills SDK ingest endpoint."""

SDK_RUN_ALIASES = """
CREATE TABLE IF NOT EXISTS sdk_run_aliases (
    alias_uuid     TEXT PRIMARY KEY,
    canonical_uuid TEXT NOT NULL,
    created_at     TEXT DEFAULT (datetime('now')),
    reason         TEXT DEFAULT ''
)
"""
"""Maps resume/resubmission SDK UUIDs back to the original logical run."""

RUN_METRICS = """
CREATE TABLE IF NOT EXISTS run_metrics (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_uuid     TEXT NOT NULL,
    event_seq    INTEGER NOT NULL DEFAULT 0,
    key          TEXT NOT NULL,
    step         INTEGER,
    ts           REAL,
    value_num    REAL,
    value_json   TEXT DEFAULT 'null',
    context_json TEXT DEFAULT '{}',
    UNIQUE(run_uuid, event_seq)
)
"""
"""Stepped Aim-style metric series emitted by the SDK."""

RUN_SCALARS = """
CREATE TABLE IF NOT EXISTS run_scalars (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_uuid     TEXT NOT NULL,
    event_seq    INTEGER NOT NULL DEFAULT 0,
    key          TEXT NOT NULL,
    ts           REAL,
    value_num    REAL,
    value_json   TEXT DEFAULT 'null',
    context_json TEXT DEFAULT '{}',
    UNIQUE(run_uuid, event_seq)
)
"""
"""Scalar SDK statistics emitted without a step."""

METRICS_VIEWS = """
CREATE TABLE IF NOT EXISTS metrics_views (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    state_json  TEXT NOT NULL DEFAULT '{}',
    pinned      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""Saved multi-run Metrics Explorer workspaces/bookmarks."""

PROJECTS = """
CREATE TABLE IF NOT EXISTS projects (
    name               TEXT PRIMARY KEY,
    color              TEXT NOT NULL DEFAULT '#9CA3AF',
    emoji              TEXT NOT NULL DEFAULT '📁',
    prefixes_json      TEXT NOT NULL DEFAULT '[]',
    campaign_delimiter TEXT NOT NULL DEFAULT '_',
    description        TEXT NOT NULL DEFAULT '',
    status             TEXT NOT NULL DEFAULT 'active',
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at         TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""Project registry. ``prefixes_json`` is a JSON array of
``{prefix, default_campaign?}`` objects; the longest matching prefix
across all projects wins for ``extract_project()``.

``status`` is ``'active'`` (default) for projects shown in the sidebar,
or ``'backlog'`` for projects hidden from the sidebar but still
preserved in the registry — their prefixes still auto-tag jobs and
their colors/emojis still apply, they're just demoted to Settings only
until the user reactivates them.
"""


# ─── v4 config tables ────────────────────────────────────────────────────────

CLUSTERS = """
CREATE TABLE IF NOT EXISTS clusters (
    name              TEXT PRIMARY KEY,
    host              TEXT NOT NULL,
    data_host         TEXT NOT NULL DEFAULT '',
    port              INTEGER NOT NULL DEFAULT 22,
    ssh_user          TEXT NOT NULL DEFAULT '',
    ssh_key           TEXT NOT NULL DEFAULT '',
    account           TEXT NOT NULL DEFAULT '',
    gpu_type          TEXT NOT NULL DEFAULT '',
    gpu_mem_gb        INTEGER NOT NULL DEFAULT 0,
    gpus_per_node     INTEGER NOT NULL DEFAULT 0,
    aihub_name        TEXT NOT NULL DEFAULT '',
    mount_paths_json  TEXT NOT NULL DEFAULT '[]',
    mount_aliases_json TEXT NOT NULL DEFAULT '{}',
    aliases_json      TEXT NOT NULL DEFAULT '[]',
    team_gpu_alloc    TEXT NOT NULL DEFAULT '',
    enabled           INTEGER NOT NULL DEFAULT 1,
    position          INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""Cluster registry (replaces ``conf/config.json:clusters`` in v4).

``ssh_user`` and ``ssh_key`` are empty when the cluster should inherit
the bootstrap defaults. ``mount_paths_json`` is a JSON array of remote
paths (``$USER`` substituted at read time). ``mount_aliases_json`` is a
JSON object mapping alias prefixes -> mount index. ``aliases_json`` is a
JSON array of alternate cluster names that resolve back to this canonical
row (e.g. NeMo-Skills' ``aws-cmh-science`` YAML alias of the physical
``aws-cmh`` cluster); uniqueness across the registry is enforced in the
application layer. ``team_gpu_alloc`` is the informal team GPU quota
('any' or an integer-as-string), the column type is TEXT to allow both
representations naturally.
"""

TEAM_MEMBERS = """
CREATE TABLE IF NOT EXISTS team_members (
    username     TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    email        TEXT NOT NULL DEFAULT '',
    notes        TEXT NOT NULL DEFAULT '',
    position     INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""Team roster used by the team-overlay heatmap and AI Hub queries."""

PPP_ACCOUNTS = """
CREATE TABLE IF NOT EXISTS ppp_accounts (
    name        TEXT PRIMARY KEY,
    ppp_id      TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    position    INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
)
"""
"""PPP (Performance Project) accounts tracked across clusters.

Replaces both ``ppps`` (account name -> id) and ``ppp_accounts`` (account
list) keys from the legacy ``config.json`` — they were always parallel
data so we collapsed them.
"""

PATH_BASES = """
CREATE TABLE IF NOT EXISTS path_bases (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    kind      TEXT NOT NULL,
    path      TEXT NOT NULL,
    position  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(kind, path)
)
"""
"""Generic path-list table for log search bases, NeMo-Run output dirs,
and Lustre mount prefixes.

``kind`` is one of:
  * ``log_search``           — directories scanned for SLURM stdout files
  * ``nemo_run``             — NeMo-Run experiment output roots
  * ``mount_lustre_prefix``  — Lustre prefixes that map to local SSHFS mounts

``path`` may contain ``$USER`` which is substituted at read time.
"""

PROCESS_FILTERS = """
CREATE TABLE IF NOT EXISTS process_filters (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    mode      TEXT NOT NULL,
    pattern   TEXT NOT NULL,
    position  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(mode, pattern)
)
"""
"""Substring filters used by the local-process scanner.

``mode`` is ``'include'`` or ``'exclude'``. A process command line
matches if it contains any include pattern AND none of the exclude
patterns. Replaces ``local_process_filters.{include,exclude}`` from the
legacy config.
"""

APP_SETTINGS = """
CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value_json  TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    description TEXT NOT NULL DEFAULT ''
)
"""
"""Singleton key-value store for global runtime tunables.

Values are stored as JSON so any type round-trips faithfully (str,
int, float, bool, list, dict). See :data:`APP_SETTINGS_DEFAULTS` for
the canonical key list with types and defaults.
"""


# ─── Indexes ────────────────────────────────────────────────────────────────

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_jh_cluster_board ON job_history(cluster, board_visible)",
    "CREATE INDEX IF NOT EXISTS idx_jh_cluster_ended ON job_history(cluster, ended_at)",
    "CREATE INDEX IF NOT EXISTS idx_jh_project ON job_history(project)",
    "CREATE INDEX IF NOT EXISTS idx_jh_run_id ON job_history(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_jh_cluster_state ON job_history(cluster, state)",
    "CREATE INDEX IF NOT EXISTS idx_jh_cluster_jobname ON job_history(cluster, job_name)",
    "CREATE INDEX IF NOT EXISTS idx_jh_ended ON job_history(ended_at)",
    "CREATE INDEX IF NOT EXISTS idx_jh_cluster_runid ON job_history(cluster, run_id)",
    "CREATE INDEX IF NOT EXISTS idx_runs_cluster_root ON runs(cluster, root_job_id)",
    "CREATE INDEX IF NOT EXISTS idx_runs_uuid ON runs(run_uuid)",
    "CREATE INDEX IF NOT EXISTS idx_run_tags_tag ON run_tags(tag)",
    "CREATE INDEX IF NOT EXISTS idx_run_tag_defs_updated ON run_tag_defs(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_project ON logbook_entries(project)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_title ON logbook_entries(project, title)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_created ON logbook_entries(project, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_edited ON logbook_entries(project, edited_at)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_type ON logbook_entries(project, entry_type)",
    "CREATE INDEX IF NOT EXISTS idx_stats_cluster_job ON job_stats_snapshots(cluster, job_id)",
    "CREATE INDEX IF NOT EXISTS idx_stats_cluster_job_ts ON job_stats_snapshots(cluster, job_id, ts)",
    "CREATE INDEX IF NOT EXISTS idx_wds_ts ON wds_history(ts)",
    "CREATE INDEX IF NOT EXISTS idx_wds_cluster ON wds_history(cluster, account, ts)",
    "CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_store(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_sdk_events_uuid ON sdk_events(run_uuid)",
    "CREATE INDEX IF NOT EXISTS idx_sdk_aliases_canonical ON sdk_run_aliases(canonical_uuid)",
    "CREATE INDEX IF NOT EXISTS idx_run_metrics_uuid_key_step ON run_metrics(run_uuid, key, step)",
    "CREATE INDEX IF NOT EXISTS idx_run_metrics_uuid_ts ON run_metrics(run_uuid, ts)",
    "CREATE INDEX IF NOT EXISTS idx_run_scalars_uuid_key ON run_scalars(run_uuid, key)",
    "CREATE INDEX IF NOT EXISTS idx_run_scalars_uuid_ts ON run_scalars(run_uuid, ts)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_views_updated ON metrics_views(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_metrics_views_pinned ON metrics_views(pinned, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_projects_name ON projects(name)",
    "CREATE INDEX IF NOT EXISTS idx_clusters_position ON clusters(position)",
    "CREATE INDEX IF NOT EXISTS idx_team_members_position ON team_members(position)",
    "CREATE INDEX IF NOT EXISTS idx_ppp_accounts_position ON ppp_accounts(position)",
    "CREATE INDEX IF NOT EXISTS idx_path_bases_kind ON path_bases(kind, position)",
    "CREATE INDEX IF NOT EXISTS idx_process_filters_mode ON process_filters(mode, position)",
    "CREATE INDEX IF NOT EXISTS idx_logbook_entries_project_campaign ON logbook_entries(project, campaign)",
    # WasteWatcher: fast lookup by state, fast 'is this job due for probe?' scans
    "CREATE INDEX IF NOT EXISTS idx_waste_state ON waste_watcher_state(state)",
    "CREATE INDEX IF NOT EXISTS idx_waste_due ON waste_watcher_state(next_probe_due)",
    "CREATE INDEX IF NOT EXISTS idx_jh_waste_reason ON job_history(waste_reason) WHERE waste_reason != ''",
    "CREATE INDEX IF NOT EXISTS idx_runs_wasteful ON runs(wasteful) WHERE wasteful = 1",
]


# ─── Triggers (FTS sync) ────────────────────────────────────────────────────

TRIGGERS = [
    """
    CREATE TRIGGER IF NOT EXISTS logbook_ai AFTER INSERT ON logbook_entries BEGIN
        INSERT INTO logbook_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS logbook_ad AFTER DELETE ON logbook_entries BEGIN
        INSERT INTO logbook_fts(logbook_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS logbook_au AFTER UPDATE ON logbook_entries BEGIN
        INSERT INTO logbook_fts(logbook_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
        INSERT INTO logbook_fts(rowid, title, body) VALUES (new.id, new.title, new.body);
    END
    """,
]


# ─── Idempotent ALTER TABLE migrations for older DBs ─────────────────────────
#
# Every entry is an ``ALTER TABLE … ADD COLUMN`` that the runner wraps in
# try/except so re-applying against an already-migrated DB is a no-op
# (SQLite raises ``OperationalError: duplicate column name``).
#
# Fresh installs already have these columns from the CREATE TABLE
# statements above; this list exists so existing v3 databases pick them
# up the first time v4 boots against them.
MIGRATIONS = [
    # job_history columns added across v1..v3
    ("job_history", "board_visible", "INTEGER DEFAULT 0"),
    ("job_history", "started", "TEXT"),
    ("job_history", "dependency", "TEXT DEFAULT ''"),
    ("job_history", "project", "TEXT DEFAULT ''"),
    ("job_history", "run_id", "INTEGER DEFAULT NULL"),
    ("job_history", "node_list", "TEXT DEFAULT ''"),
    ("job_history", "account", "TEXT DEFAULT ''"),
    ("job_history", "custom_log_dir", "TEXT DEFAULT ''"),
    ("job_history", "custom_metrics_config", "TEXT DEFAULT ''"),
    # runs columns added across v1..v3
    ("runs", "starred", "INTEGER DEFAULT 0"),
    ("runs", "notes", "TEXT DEFAULT ''"),
    ("runs", "run_uuid", "TEXT DEFAULT ''"),
    ("runs", "source", "TEXT DEFAULT 'legacy'"),
    ("runs", "submit_command", "TEXT DEFAULT ''"),
    ("runs", "submit_cwd", "TEXT DEFAULT ''"),
    ("runs", "git_commit", "TEXT DEFAULT ''"),
    ("runs", "launcher_hostname", "TEXT DEFAULT ''"),
    ("runs", "primary_output_dir", "TEXT DEFAULT ''"),
    ("runs", "sdk_status", "TEXT DEFAULT ''"),
    ("runs", "params_json", "TEXT DEFAULT ''"),
    ("runs", "metadata_json", "TEXT DEFAULT ''"),
    ("runs", "malfunctioned", "INTEGER NOT NULL DEFAULT 0"),
    ("runs", "tags_json", "TEXT NOT NULL DEFAULT '[]'"),
    # WasteWatcher run-level flags (v4+)
    ("runs", "wasteful", "INTEGER NOT NULL DEFAULT 0"),
    ("runs", "waste_reason", "TEXT NOT NULL DEFAULT ''"),
    ("runs", "waste_detected_at", "TEXT NOT NULL DEFAULT ''"),
    ("runs", "waste_cancelled_by_watcher", "INTEGER NOT NULL DEFAULT 0"),
    # WasteWatcher job-level audit columns (v4+)
    ("job_history", "waste_reason", "TEXT NOT NULL DEFAULT ''"),
    ("job_history", "waste_cancelled_at", "TEXT NOT NULL DEFAULT ''"),
    # logbook_entries columns added in v3
    ("logbook_entries", "entry_type", "TEXT NOT NULL DEFAULT 'note'"),
    ("logbook_entries", "pinned", "INTEGER NOT NULL DEFAULT 0"),
    # logbook_entries columns added in v4
    ("logbook_entries", "campaign", "TEXT NOT NULL DEFAULT ''"),
    ("logbook_entries", "board_json", "TEXT NOT NULL DEFAULT ''"),
    ("logbook_entries", "campaign_goal", "TEXT NOT NULL DEFAULT ''"),
    # logbook_entries.graph_json: per-campaign mind_map DAG (v4+)
    ("logbook_entries", "graph_json", "TEXT NOT NULL DEFAULT ''"),
    # wds_history column added later in v3
    ("wds_history", "occupancy_factor", "REAL"),
    # projects column added in v4 for sidebar visibility (active vs backlog)
    ("projects", "status", "TEXT NOT NULL DEFAULT 'active'"),
    # cluster name aliases used by SDK ingest + /api/cluster_resolve (v4+)
    ("clusters", "aliases_json", "TEXT NOT NULL DEFAULT '[]'"),
]


# ─── Tables in install order ────────────────────────────────────────────────

SCHEMA = [
    JOB_HISTORY,
    RUNS,
    RUN_TAGS,
    RUN_TAG_DEFS,
    LOGBOOK_ENTRIES,
    LOGBOOK_FTS,
    LOGBOOK_LINKS,
    JOB_STATS_SNAPSHOTS,
    WASTE_WATCHER_STATE,
    WDS_HISTORY,
    LIVE_JOBS,
    CLUSTER_STATE,
    CACHE_STORE,
    SDK_EVENTS,
    SDK_RUN_ALIASES,
    RUN_METRICS,
    RUN_SCALARS,
    METRICS_VIEWS,
    PROJECTS,
    CLUSTERS,
    TEAM_MEMBERS,
    PPP_ACCOUNTS,
    PATH_BASES,
    PROCESS_FILTERS,
    APP_SETTINGS,
]


# ─── Well-known app_settings keys ───────────────────────────────────────────
#
# Each entry: key -> (default, type_coercer, description). The coercer
# is applied to values pulled out of value_json before they are returned
# to callers, so a string "5" stored in the DB still becomes int(5) when
# the caller reads ``ssh_timeout``. When the caller writes a value, the
# coercer is applied first to fail loudly on bad input.
#
# This is the v4 replacement for the import-time globals SSH_TIMEOUT,
# CACHE_FRESH_SEC, etc. that used to live in server/config.py.

APP_SETTINGS_DEFAULTS: Dict[str, tuple[Any, Callable[[Any], Any], str]] = {
    "team_name": (
        "",
        str,
        "Team identifier shown in the UI and used by team-overlay queries.",
    ),
    "aihub_opensearch_url": (
        "",
        str,
        "OpenSearch endpoint for AI Hub allocation/fairshare queries. Empty disables AI Hub.",
    ),
    "dashboard_url": (
        "",
        str,
        "Science dashboard URL (used as fallback for team membership and avatars).",
    ),
    "aihub_cache_ttl_sec": (
        300,
        int,
        "How long AI Hub query results stay in the persistent cache.",
    ),
    "wds_snapshot_interval_sec": (
        900,
        int,
        "How often the WDS scorer takes a snapshot of cluster availability.",
    ),
    "ssh_timeout": (
        5,
        int,
        "Per-SSH-call wall-clock timeout in seconds.",
    ),
    "cache_fresh_sec": (
        30,
        int,
        "How long a polled cluster snapshot is considered fresh before re-polling.",
    ),
    "stats_interval_sec": (
        1800,
        int,
        "How often the GPU-stats scraper writes a job_stats_snapshots row.",
    ),
    "backup_interval_hours": (
        24,
        int,
        "Hours between automatic SQLite backup attempts (0 disables backups).",
    ),
    "backup_max_keep": (
        7,
        int,
        "Maximum number of dated backups to retain before pruning the oldest.",
    ),
    "sdk_ingest_token": (
        "",
        str,
        "Optional bearer token required by POST /api/sdk/events. Empty allows unauthenticated ingest.",
    ),
    "custom_metrics_enabled": (
        True,
        bool,
        "Show the custom metrics UI (log dir, extractors, stats panel). Disable to hide it globally.",
    ),
    # ── WasteWatcher tunables ────────────────────────────────────────────────
    "waste_watcher_enabled": (
        True,
        bool,
        "Master switch for the WasteWatcher daemon. Disable to stop detection entirely.",
    ),
    "waste_watcher_cancel_enabled": (
        True,
        bool,
        "When True, WasteWatcher auto-cancels verified-wasteful jobs after verification.",
    ),
    "waste_watcher_cancel_disabled_clusters": (
        "",
        str,
        "Comma-separated cluster names where auto-cancel is disabled even when cancel_enabled=True.",
    ),
    "waste_watcher_tick_sec": (
        30,
        int,
        "Seconds between WasteWatcher loop ticks (governs how often we evaluate due jobs).",
    ),
    "waste_watcher_cold_probe_sec": (
        60,
        int,
        "Probe cadence (seconds) for jobs that have not yet shown any GPU activity.",
    ),
    "waste_watcher_warm_probe_sec": (
        900,
        int,
        "Probe cadence (seconds) for jobs that have shown healthy GPU activity (back-off).",
    ),
    "waste_watcher_suspicious_probe_sec": (
        30,
        int,
        "Probe cadence (seconds) once a job is flagged suspicious (aggressive verification).",
    ),
    "waste_watcher_cold_grace_min": (
        15,
        int,
        "Minutes a job may sit cold (0% util) before transitioning to suspicious (vLLM weight-load tolerance).",
    ),
    "waste_watcher_warm_idle_min": (
        10,
        int,
        "Minutes a warm job must sit idle before re-entering suspicious.",
    ),
    "waste_watcher_suspicious_confirm_min": (
        5,
        int,
        "Minutes of continued idleness while suspicious before promotion to wasteful.",
    ),
    "waste_watcher_log_quiet_min": (
        5,
        int,
        "Minutes the log tail hash must be unchanged before counting toward waste verification.",
    ),
    "waste_watcher_sdk_heartbeat_stale_min": (
        5,
        int,
        "Minutes since last SDK heartbeat (when SDK-tracked) before considered stale.",
    ),
    "waste_watcher_util_busy_threshold": (
        5,
        int,
        "GPU utilization percentage above which a job is considered busy (resets idle streaks).",
    ),
    "waste_watcher_exempt_name_regex": (
        r"(judge-aggregate|manifest|summarize-results|wait-.*-sentinels)",
        str,
        "Regex matched against job names; matching jobs skip WasteWatcher entirely.",
    ),
    "waste_watcher_min_runtime_min": (
        5,
        int,
        "Skip jobs younger than this many minutes (avoid noise during job startup).",
    ),
    "waste_watcher_audit_project": (
        "compute",
        str,
        "Logbook project where WasteWatcher cancellation audit entries are recorded.",
    ),
}
