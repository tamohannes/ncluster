"""Shared configuration, constants, and mutable globals for clausius."""

import json
import os
import threading
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
APP_ROOT = PROJECT_ROOT
DEFAULT_USER = os.environ.get("CLAUSIUS_SSH_USER") or os.environ.get("USER") or "user"
DEFAULT_SSH_KEY = os.path.expanduser(
    os.environ.get("CLAUSIUS_SSH_KEY", "~/.ssh/id_ed25519")
)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "history.db")
SSH_TIMEOUT = 5
CACHE_FRESH_SEC = 30
STATS_INTERVAL_SEC = 1800
BACKUP_INTERVAL_HOURS = 24
BACKUP_MAX_KEEP = 7

CONFIG_PATH = os.path.join(PROJECT_ROOT, "conf", "config.json")
_CONFIG_EXAMPLE_PATH = os.path.join(PROJECT_ROOT, "conf", "config.example.json")

if os.path.isfile(CONFIG_PATH):
    with open(CONFIG_PATH) as _cf:
        _CONFIG = json.load(_cf)
elif os.path.isfile(_CONFIG_EXAMPLE_PATH):
    with open(_CONFIG_EXAMPLE_PATH) as _cf:
        _CONFIG = json.load(_cf)
else:
    raise SystemExit(
        f"Config file not found: {CONFIG_PATH}\n"
        "Copy config.example.json to config.json and fill in your cluster details."
    )

APP_PORT = _CONFIG.get("port", 7272)
TEAM_NAME = _CONFIG.get("team", "")
TEAM_GPU_ALLOC = _CONFIG.get("team_gpu_allocations", {})
PPPS = _CONFIG.get("ppps", {})
PPP_ACCOUNTS = _CONFIG.get("ppp_accounts", [])
TEAM_MEMBERS = _CONFIG.get("team_members", [])
AIHUB_OPENSEARCH_URL = _CONFIG.get("aihub_opensearch_url", "")
DASHBOARD_URL = _CONFIG.get("dashboard_url", "")
AIHUB_CACHE_TTL = _CONFIG.get("aihub_cache_ttl_sec", 300)
WDS_SNAPSHOT_INTERVAL = _CONFIG.get("wds_snapshot_interval_sec", 900)
SDK_INGEST_TOKEN = _CONFIG.get("sdk_ingest_token", "")
LOG_SEARCH_BASES = _CONFIG.get("log_search_bases", [])
NEMO_RUN_BASES = _CONFIG.get("nemo_run_bases", [])
MOUNT_LUSTRE_PREFIXES = _CONFIG.get("mount_lustre_prefixes", [])
_proc_filters = _CONFIG.get("local_process_filters", {})
LOCAL_PROC_INCLUDE = _proc_filters.get("include", [])
LOCAL_PROC_EXCLUDE = _proc_filters.get("exclude", [])

CLUSTERS = {}
for _name, _cfg in _CONFIG.get("clusters", {}).items():
    CLUSTERS[_name] = {
        "host": _cfg["host"],
        "data_host": _cfg.get("data_host", ""),
        "user": _cfg.get("user", DEFAULT_USER),
        "key": os.path.expanduser(_cfg.get("key", DEFAULT_SSH_KEY)),
        "port": _cfg.get("port", 22),
        "gpu_type": _cfg.get("gpu_type", ""),
        "gpu_mem_gb": _cfg.get("gpu_mem_gb", 0),
        "gpus_per_node": _cfg.get("gpus_per_node", 0),
        "account": _cfg.get("account", ""),
        # Optional AI Hub OpenSearch identifier for this cluster (the
        # ``s_cluster`` field value). Leave unset when the cluster is not
        # ingested into AI Hub — aihub.py will skip it.
        "aihub_name": _cfg.get("aihub_name", ""),
    }
CLUSTERS["local"] = {
    "host": None, "data_host": "", "user": None, "key": None,
    "port": None, "gpu_type": "local", "gpu_mem_gb": 0, "gpus_per_node": 0,
}


def _load_mount_map():
    """Build MOUNT_MAP: cluster -> list of local mount roots.

    With mount_paths config, each cluster has indexed subdirs:
      ~/.clausius/mounts/<cluster>/0/
      ~/.clausius/mounts/<cluster>/1/
      ...
    """
    home = os.path.expanduser("~")
    base = os.path.join(home, ".clausius", "mounts")
    raw = os.environ.get("CLAUSIUS_MOUNT_MAP", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                out = {}
                for name, roots in parsed.items():
                    if name not in CLUSTERS or name == "local":
                        continue
                    if isinstance(roots, str):
                        roots = [roots]
                    if not isinstance(roots, list):
                        continue
                    norm = [os.path.abspath(os.path.expanduser(r.strip()))
                            for r in roots if isinstance(r, str)]
                    if norm:
                        out[name] = norm
                if out:
                    return out
        except Exception:
            pass

    result = {}
    for name in CLUSTERS:
        if name == "local":
            continue
        cluster_base = os.path.join(base, name)
        mount_paths = _CONFIG.get("clusters", {}).get(name, {}).get("mount_paths", [])
        if mount_paths:
            roots = [os.path.join(cluster_base, str(i))
                     for i in range(len(mount_paths))]
        else:
            roots = [cluster_base]
        result[name] = roots
    return result


def _load_mount_remote_map():
    """Build a mapping: cluster -> list of remote paths (with $USER expanded).

    Used by find_job_logs_on_mount to convert local paths back to remote.
    """
    result = {}
    for name, ccfg in _CONFIG.get("clusters", {}).items():
        paths = ccfg.get("mount_paths", [])
        result[name] = [p.replace("$USER", DEFAULT_USER) for p in paths]
    return result


def _load_mount_aliases():
    """Build alias mapping: cluster -> list of (alias_prefix, mount_index).

    mount_aliases maps symlink paths on the cluster to the mount index they
    resolve to.  E.g. {"my-cluster": [("/path/to/symlink", 0)]} means that
    remote paths starting with that prefix should use mount root 0.
    """
    result = {}
    for name, ccfg in _CONFIG.get("clusters", {}).items():
        aliases = ccfg.get("mount_aliases", {})
        if aliases:
            result[name] = [
                (p.replace("$USER", DEFAULT_USER), int(idx))
                for p, idx in aliases.items()
            ]
    return result


MOUNT_MAP = _load_mount_map()
MOUNT_REMOTE_MAP = _load_mount_remote_map()
MOUNT_ALIASES = _load_mount_aliases()
MOUNT_SCRIPT_PATH = os.path.join(APP_ROOT, "scripts", "sshfs_logs.sh")

STATE_ORDER = {"RUNNING": 0, "COMPLETING": 1, "PENDING": 2, "FAILED": 3, "CANCELLED": 4}
SQUEUE_FMT = "%i|%j|%T|%r|%M|%l|%D|%C|%b|%P|%V|%S|%E|%N|%a"
SQUEUE_HDR = ["jobid", "name", "state", "reason", "elapsed", "timelimit", "nodes", "cpus", "gres", "partition", "submitted", "started", "dependency", "node_list", "account"]

# In-memory caches
_cache_lock = threading.Lock()
_cache = {}
_seen_jobs = {}
_last_polled = {}

_ssh_pool_lock = threading.Lock()
_ssh_pool = {}
_ssh_cluster_locks = {}
SSH_IDLE_TTL_SEC = 300

_warm_lock = threading.Lock()
_log_index_cache = {}
_log_content_cache = {}
_stats_cache = {}
_dir_list_cache = {}
_progress_cache = {}
_progress_source_cache = {}
_crash_cache = {}
_est_start_cache = {}
_team_usage_cache = {}
_prefetch_last = {}
LOG_INDEX_TTL_SEC = 120
LOG_CONTENT_TTL_SEC = 45
STATS_TTL_SEC = 15
DIR_LIST_TTL_SEC = 20
PROGRESS_TTL_SEC = 150
CRASH_TTL_SEC = 60
EST_START_TTL_SEC = 120
TEAM_USAGE_TTL_SEC = 120
PREFETCH_MIN_GAP_SEC = 60

TERMINAL_STATES = {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL"}
PINNABLE_TERMINAL_STATES = TERMINAL_STATES | {"COMPLETED", "COMPLETING"}
RESULT_DIR_NAMES = ["eval-logs", "eval-results", "tmp-eval-results"]

# ─── Projects ────────────────────────────────────────────────────────────────
#
# PROJECTS is a runtime cache populated from the SQLite ``projects`` table
# (see server/db.py). It is intentionally empty at import time; the cache
# is filled by ``reload_projects_cache()`` which is called from
# ``_shared_init()`` after ``init_db()`` and again after every project CRUD
# write so the cache stays in sync with the table.

PROJECTS = {}

_PROJECT_PALETTE = [
    "#e8f4fd", "#fef3e2", "#e8f5e9", "#fce4ec",
    "#ede7f6", "#fff8e1", "#e0f2f1", "#fbe9e7",
    "#e3f2fd", "#f3e5f5", "#e8eaf6", "#fff3e0",
]

_PROJECT_EMOJIS = [
    "🔬", "🧪", "🚀", "⚡", "🎯", "🔮", "🌊", "🔥",
    "💎", "🧬", "🏗️", "🎨",
]


def reload_projects_cache():
    """Refresh the in-process ``PROJECTS`` dict from the SQLite ``projects`` table.

    Imported lazily to avoid a circular import with ``server.db``. Safe to call
    repeatedly; replaces the dict's contents in-place so existing references
    (e.g. captured in tests) keep seeing the live state.
    """
    try:
        from .db import db_list_projects
    except Exception:
        return
    try:
        rows = db_list_projects()
    except Exception:
        return
    PROJECTS.clear()
    for row in rows:
        PROJECTS[row["name"]] = {
            "color": row.get("color") or "",
            "emoji": row.get("emoji") or "",
            "prefixes": row.get("prefixes") or [],
            "campaign_delimiter": row.get("campaign_delimiter") or "_",
            "description": row.get("description") or "",
        }


def _project_prefix_entries(cfg):
    """Return a list of (prefix, default_campaign) pairs for a project config.

    Supports both the legacy singular form (``{"prefix": "...", ...}``) and
    the multi-prefix form (``{"prefixes": [{"prefix": "...", "default_campaign":
    "..."}], ...}``).  Entries with empty prefixes are dropped.
    """
    if not cfg:
        return []
    out = []
    for entry in cfg.get("prefixes") or []:
        if not isinstance(entry, dict):
            continue
        prefix = entry.get("prefix", "") or ""
        if prefix:
            out.append((prefix, entry.get("default_campaign")))
    legacy_prefix = cfg.get("prefix", "") or ""
    if legacy_prefix:
        out.append((legacy_prefix, cfg.get("default_campaign")))
    return out


def extract_project(job_name):
    """Return the project key for ``job_name`` based on registered prefixes.

    Walks every configured prefix (``PROJECTS``) and returns the project whose
    *longest* prefix matches the start of the job name. Multiple prefixes per
    project are supported via the ``prefixes`` list — each one participates in
    the longest-match independently.

    Returns ``""`` for job names that do not match any registered prefix.
    Projects are created exclusively through ``db_create_project`` (or the
    matching MCP / REST endpoints); ``extract_project`` no longer registers
    new projects on the fly.
    """
    if not job_name:
        return ""
    candidates = []  # (prefix_len, project_name, prefix)
    for name, cfg in PROJECTS.items():
        for prefix, _default in _project_prefix_entries(cfg):
            candidates.append((len(prefix), name, prefix))
    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, name, prefix in candidates:
        if job_name.startswith(prefix):
            return name
    return ""


def extract_campaign(job_name, project=""):
    """Return the campaign key from a job name.

    The campaign is the first segment of the run name (the part after the
    matching project prefix), split on the project's ``campaign_delimiter``
    (default ``_``).  E.g. ``hle_mpsf_hle-nem120b`` → campaign ``mpsf``,
    ``n3ue_rprof_nano-stem-cot-r1`` → campaign ``rprof``.

    A project entry may also set ``default_campaign`` to force a fixed
    campaign for every job that matches that prefix.  This is useful when an
    existing naming pattern (e.g. ``hle_chem-omesilver-...``) should be
    re-grouped under a single campaign in the new project regardless of the
    rest of the job name.  In the multi-prefix form, each ``prefixes`` entry
    may carry its own ``default_campaign`` so legacy prefixes can keep a
    forced label while modern prefixes derive the campaign normally.

    Naming convention: ``<project>_<campaign>_<rest-of-run-name>``
    """
    if not job_name:
        return ""
    project_cfg = PROJECTS.get(project) if project else None
    delimiter = "_"
    matched_prefix = ""
    matched_default = None
    if project_cfg:
        delimiter = project_cfg.get("campaign_delimiter") or "_"
        # Pick the longest declared prefix that the job actually starts with.
        for prefix, default_campaign in sorted(
            _project_prefix_entries(project_cfg),
            key=lambda x: len(x[0]),
            reverse=True,
        ):
            if job_name.startswith(prefix):
                matched_prefix = prefix
                matched_default = default_campaign
                break
    if matched_default:
        return matched_default
    if not matched_prefix:
        import re
        m = re.match(r'^[a-zA-Z][a-zA-Z0-9-]*_', job_name)
        if m:
            matched_prefix = m.group(0)
    if matched_prefix and job_name.startswith(matched_prefix):
        remainder = job_name[len(matched_prefix):]
    else:
        remainder = job_name
    seg = remainder.split(delimiter)[0].lower()
    return seg if seg else ""


def get_project_color(project_name):
    """Return the color for a registered project, or ``""`` if not registered.

    Read-only: never mutates ``PROJECTS`` or persists state. Palette
    auto-assignment for missing colors happens once at ``db_create_project``
    time.
    """
    if not project_name:
        return ""
    cfg = PROJECTS.get(project_name)
    if not cfg:
        return ""
    return cfg.get("color") or ""


def get_project_emoji(project_name):
    """Return the emoji for a registered project, or ``""`` if not registered.

    Read-only: never mutates ``PROJECTS`` or persists state. Palette
    auto-assignment for missing emojis happens once at ``db_create_project``
    time.
    """
    if not project_name:
        return ""
    cfg = PROJECTS.get(project_name)
    if not cfg:
        return ""
    return cfg.get("emoji") or ""


def _sync_config():
    """Sync all live globals back into _CONFIG so disk writes are consistent."""
    _CONFIG["ssh_timeout"] = SSH_TIMEOUT
    _CONFIG["cache_fresh_sec"] = CACHE_FRESH_SEC
    _CONFIG["stats_interval_sec"] = STATS_INTERVAL_SEC
    _CONFIG["backup_interval_hours"] = BACKUP_INTERVAL_HOURS
    _CONFIG["backup_max_keep"] = BACKUP_MAX_KEEP
    _CONFIG["team"] = TEAM_NAME
    _CONFIG["team_gpu_allocations"] = dict(TEAM_GPU_ALLOC)
    _CONFIG["ppps"] = dict(PPPS)
    _CONFIG["log_search_bases"] = LOG_SEARCH_BASES
    _CONFIG["nemo_run_bases"] = NEMO_RUN_BASES
    _CONFIG["mount_lustre_prefixes"] = MOUNT_LUSTRE_PREFIXES
    _CONFIG["local_process_filters"] = {
        "include": LOCAL_PROC_INCLUDE,
        "exclude": LOCAL_PROC_EXCLUDE,
    }
    # Projects are stored in the SQLite ``projects`` table — intentionally
    # NOT mirrored back into config.json. Source of truth lives in the DB.
    _CONFIG.pop("projects", None)
    existing_clusters = _CONFIG.get("clusters", {})
    for cname, ccfg in CLUSTERS.items():
        if cname == "local":
            continue
        if cname in existing_clusters:
            for k in ("host", "data_host", "user", "port", "gpu_type", "gpus_per_node", "account"):
                if k in ccfg and ccfg[k]:
                    existing_clusters[cname][k] = ccfg[k]
        else:
            existing_clusters[cname] = {k: v for k, v in ccfg.items() if v}
    _CONFIG["clusters"] = existing_clusters


def _write_config():
    """Write _CONFIG to disk. Call _sync_config() first."""
    if os.path.isfile(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "w") as fh:
                json.dump(_CONFIG, fh, indent=2)
                fh.write("\n")
        except Exception:
            pass


def _dir_label(path):
    base = os.path.basename(path.rstrip("/"))
    if base in ("eval-logs", "eval-results", "tmp-eval-results"):
        return base
    return "output"


def _cache_get(store, key, ttl_sec):
    with _warm_lock:
        rec = store.get(key)
    if not rec:
        return None
    if time.monotonic() - rec["ts"] > ttl_sec:
        return None
    return rec["value"]


def _cache_set(store, key, value):
    with _warm_lock:
        store[key] = {"ts": time.monotonic(), "value": value}


def _cache_sweep_all():
    """Evict entries older than 2x their TTL from all TTL caches.

    Run periodically from a background thread to prevent unbounded growth.
    """
    now = time.monotonic()
    sweep_targets = [
        (_log_index_cache, LOG_INDEX_TTL_SEC),
        (_log_content_cache, LOG_CONTENT_TTL_SEC),
        (_stats_cache, STATS_TTL_SEC),
        (_dir_list_cache, DIR_LIST_TTL_SEC),
        (_progress_cache, PROGRESS_TTL_SEC),
        (_progress_source_cache, PROGRESS_TTL_SEC),
        (_crash_cache, CRASH_TTL_SEC),
        (_est_start_cache, EST_START_TTL_SEC),
        (_team_usage_cache, TEAM_USAGE_TTL_SEC),
    ]
    total = 0
    with _warm_lock:
        for store, ttl in sweep_targets:
            stale_keys = [k for k, rec in store.items() if now - rec["ts"] > ttl * 2]
            for k in stale_keys:
                del store[k]
            total += len(stale_keys)
    return total


def cache_gc_loop():
    """Background loop: sweep caches every 5 minutes."""
    import logging
    _log = logging.getLogger(__name__)
    while True:
        time.sleep(300)
        try:
            n = _cache_sweep_all()
            try:
                from .jobs import prune_job_sets
                prune_job_sets()
            except Exception:
                pass
            try:
                from .db import cache_db_gc
                cache_db_gc()
            except Exception:
                pass
            if n:
                _log.debug("cache GC: evicted %d stale entries", n)
        except Exception:
            pass


def reload_config(new_cfg):
    """Hot-reload mutable globals from a new config dict. Writes to disk first.

    Container-typed globals (``TEAM_GPU_ALLOC``, ``PPPS``, ``CLUSTERS``, ...)
    are mutated *in place* so consumers that imported the symbol by name
    (e.g. ``from .config import TEAM_GPU_ALLOC`` in ``wds.py`` /
    ``aihub.py``) keep seeing fresh values without a restart. Rebinding
    these names would leave callers with a stale reference to the old
    object.
    """
    global _CONFIG, SSH_TIMEOUT, CACHE_FRESH_SEC, STATS_INTERVAL_SEC, TEAM_NAME
    global BACKUP_INTERVAL_HOURS, BACKUP_MAX_KEEP
    global LOG_SEARCH_BASES, NEMO_RUN_BASES, MOUNT_LUSTRE_PREFIXES
    global LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE
    global PPP_ACCOUNTS, TEAM_MEMBERS, AIHUB_OPENSEARCH_URL, DASHBOARD_URL, AIHUB_CACHE_TTL

    with open(CONFIG_PATH, "w") as fh:
        json.dump(new_cfg, fh, indent=2)
        fh.write("\n")
    _CONFIG = new_cfg

    LOG_SEARCH_BASES = new_cfg.get("log_search_bases", [])
    NEMO_RUN_BASES = new_cfg.get("nemo_run_bases", [])
    MOUNT_LUSTRE_PREFIXES = new_cfg.get("mount_lustre_prefixes", [])
    pf = new_cfg.get("local_process_filters", {})
    LOCAL_PROC_INCLUDE = pf.get("include", [])
    LOCAL_PROC_EXCLUDE = pf.get("exclude", [])
    SSH_TIMEOUT = new_cfg.get("ssh_timeout", 5)
    CACHE_FRESH_SEC = new_cfg.get("cache_fresh_sec", 30)
    STATS_INTERVAL_SEC = new_cfg.get("stats_interval_sec", 1800)
    BACKUP_INTERVAL_HOURS = new_cfg.get("backup_interval_hours", 24)
    BACKUP_MAX_KEEP = new_cfg.get("backup_max_keep", 7)
    TEAM_NAME = new_cfg.get("team", "")
    TEAM_GPU_ALLOC.clear()
    TEAM_GPU_ALLOC.update(new_cfg.get("team_gpu_allocations", {}))
    PPPS.clear()
    PPPS.update(new_cfg.get("ppps", {}))
    PPP_ACCOUNTS = new_cfg.get("ppp_accounts", [])
    TEAM_MEMBERS = new_cfg.get("team_members", [])
    AIHUB_OPENSEARCH_URL = new_cfg.get("aihub_opensearch_url", "")
    DASHBOARD_URL = new_cfg.get("dashboard_url", "")
    AIHUB_CACHE_TTL = new_cfg.get("aihub_cache_ttl_sec", 300)
    WDS_SNAPSHOT_INTERVAL = new_cfg.get("wds_snapshot_interval_sec", 900)

    from .ssh import close_cluster_client

    new_clusters = {}
    for cname, ccfg in new_cfg.get("clusters", {}).items():
        new_clusters[cname] = {
            "host": ccfg.get("host", ""),
            "data_host": ccfg.get("data_host", ""),
            "user": ccfg.get("user", DEFAULT_USER),
            "key": os.path.expanduser(ccfg.get("key", DEFAULT_SSH_KEY)),
            "port": ccfg.get("port", 22),
            "gpu_type": ccfg.get("gpu_type", ""),
            "gpus_per_node": ccfg.get("gpus_per_node", 0),
            "account": ccfg.get("account", ""),
            "aihub_name": ccfg.get("aihub_name", ""),
        }
    new_clusters["local"] = {
        "host": None, "data_host": "", "user": None, "key": None,
        "port": None, "gpu_type": "local",
    }

    removed = set(CLUSTERS.keys()) - set(new_clusters.keys())
    for r in removed:
        close_cluster_client(r)

    CLUSTERS.clear()
    CLUSTERS.update(new_clusters)
    MOUNT_MAP.clear()
    MOUNT_MAP.update(_load_mount_map())
    MOUNT_REMOTE_MAP.clear()
    MOUNT_REMOTE_MAP.update(_load_mount_remote_map())
    MOUNT_ALIASES.clear()
    MOUNT_ALIASES.update(_load_mount_aliases())

    # Projects live in the SQLite ``projects`` table; nothing to refresh from
    # ``new_cfg`` here. The cache stays in sync via ``reload_projects_cache``.


def settings_response():
    """Build the settings payload for GET /api/settings."""
    _sync_config()
    cfg = dict(_CONFIG)
    cfg["ssh_timeout"] = SSH_TIMEOUT
    cfg["cache_fresh_sec"] = CACHE_FRESH_SEC
    cfg["stats_interval_sec"] = STATS_INTERVAL_SEC
    cfg["backup_interval_hours"] = BACKUP_INTERVAL_HOURS
    cfg["backup_max_keep"] = BACKUP_MAX_KEEP
    # Projects live in the SQLite ``projects`` table now; expose them in the
    # legacy ``{name: {color, emoji, prefixes, ...}}`` shape so existing
    # frontend / settings consumers keep working until they migrate to the
    # dedicated ``/api/projects/all`` endpoint.
    cfg["projects"] = {k: dict(v) for k, v in PROJECTS.items()}
    cfg["team"] = TEAM_NAME
    cfg["team_gpu_allocations"] = dict(TEAM_GPU_ALLOC)
    cfg["ppps"] = dict(PPPS)
    return cfg
