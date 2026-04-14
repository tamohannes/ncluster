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
        "gpus_per_node": _cfg.get("gpus_per_node", 0),
        "account": _cfg.get("account", ""),
    }
CLUSTERS["local"] = {
    "host": None, "data_host": "", "user": None, "key": None,
    "port": None, "gpu_type": "local", "gpus_per_node": 0,
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
    resolve to.  E.g. {"hsg": [("/lustre/fsw/.../htamoyan", 0)]} means that
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

PROJECTS = _CONFIG.get("projects", {})

_PROJECT_PALETTE = [
    "#e8f4fd", "#fef3e2", "#e8f5e9", "#fce4ec",
    "#ede7f6", "#fff8e1", "#e0f2f1", "#fbe9e7",
    "#e3f2fd", "#f3e5f5", "#e8eaf6", "#fff3e0",
]

_PROJECT_EMOJIS = [
    "🔬", "🧪", "🚀", "⚡", "🎯", "🔮", "🌊", "🔥",
    "💎", "🧬", "🏗️", "🎨",
]


def extract_project(job_name):
    """Return project key from job name.

    1. Check configured prefixes first.
    2. Auto-detect: if the job name starts with `word_` (letters/digits/hyphens
       followed by underscore), treat that as a new project and register it.
    """
    if not job_name:
        return ""
    for name, cfg in PROJECTS.items():
        prefix = cfg.get("prefix", "")
        if prefix and job_name.startswith(prefix):
            return name
    # Auto-detect: "myproject_eval-math" → project "myproject", prefix "myproject_"
    import re
    m = re.match(r'^([a-zA-Z][a-zA-Z0-9-]*)_', job_name)
    if m:
        proj_name = m.group(1).lower()
        prefix = m.group(0)  # includes the trailing underscore
        if proj_name not in PROJECTS:
            PROJECTS[proj_name] = {"prefix": prefix}
            get_project_color(proj_name)
            get_project_emoji(proj_name)
        return proj_name
    return ""


def extract_campaign(job_name, project=""):
    """Return the campaign key from a job name.

    The campaign is the first underscore-delimited segment of the run name
    (the part after the project prefix).  E.g. ``hle_mpsf_hle-nem120b``
    → campaign ``mpsf``, ``hle_text_kimi-k25`` → campaign ``text``.

    Naming convention: ``<project>_<campaign>_<rest-of-run-name>``
    Both project and campaign are separated by underscores.
    """
    if not job_name:
        return ""
    prefix = ""
    if project and project in PROJECTS:
        prefix = PROJECTS[project].get("prefix", "")
    if not prefix:
        import re
        m = re.match(r'^[a-zA-Z][a-zA-Z0-9-]*_', job_name)
        if m:
            prefix = m.group(0)
    if prefix and job_name.startswith(prefix):
        remainder = job_name[len(prefix):]
    else:
        remainder = job_name
    seg = remainder.split("_")[0].lower()
    return seg if seg else ""


def get_project_color(project_name):
    """Return the color for a project, auto-assigning from palette if needed."""
    if not project_name or project_name not in PROJECTS:
        return ""
    cfg = PROJECTS[project_name]
    if cfg.get("color"):
        return cfg["color"]
    used = {p.get("color") for p in PROJECTS.values() if p.get("color")}
    for c in _PROJECT_PALETTE:
        if c not in used:
            cfg["color"] = c
            _persist_projects()
            return c
    cfg["color"] = _PROJECT_PALETTE[len(PROJECTS) % len(_PROJECT_PALETTE)]
    _persist_projects()
    return cfg["color"]


def get_project_emoji(project_name):
    """Return the emoji for a project, auto-assigning if needed."""
    if not project_name or project_name not in PROJECTS:
        return ""
    cfg = PROJECTS[project_name]
    if cfg.get("emoji"):
        return cfg["emoji"]
    used = {p.get("emoji") for p in PROJECTS.values() if p.get("emoji")}
    for e in _PROJECT_EMOJIS:
        if e not in used:
            cfg["emoji"] = e
            _persist_projects()
            return e
    cfg["emoji"] = _PROJECT_EMOJIS[len(PROJECTS) % len(_PROJECT_EMOJIS)]
    _persist_projects()
    return cfg["emoji"]


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
    _CONFIG["projects"] = {k: dict(v) for k, v in PROJECTS.items()}
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


def _persist_projects():
    """Write current PROJECTS back into _CONFIG and save to disk."""
    _sync_config()
    _write_config()


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
    """Hot-reload mutable globals from a new config dict. Writes to disk first."""
    global _CONFIG, SSH_TIMEOUT, CACHE_FRESH_SEC, STATS_INTERVAL_SEC, TEAM_NAME, TEAM_GPU_ALLOC, PPPS
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
    TEAM_GPU_ALLOC = new_cfg.get("team_gpu_allocations", {})
    PPPS = new_cfg.get("ppps", {})
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

    PROJECTS.clear()
    PROJECTS.update(new_cfg.get("projects", {}))


def settings_response():
    """Build the settings payload for GET /api/settings."""
    _sync_config()
    cfg = dict(_CONFIG)
    cfg["ssh_timeout"] = SSH_TIMEOUT
    cfg["cache_fresh_sec"] = CACHE_FRESH_SEC
    cfg["stats_interval_sec"] = STATS_INTERVAL_SEC
    cfg["backup_interval_hours"] = BACKUP_INTERVAL_HOURS
    cfg["backup_max_keep"] = BACKUP_MAX_KEEP
    cfg["projects"] = {k: dict(v) for k, v in PROJECTS.items()}
    cfg["team"] = TEAM_NAME
    cfg["team_gpu_allocations"] = dict(TEAM_GPU_ALLOC)
    cfg["ppps"] = dict(PPPS)
    return cfg
