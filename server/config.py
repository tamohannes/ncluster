"""Shared configuration, constants, and mutable globals for the job monitor."""

import json
import os
import threading
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
APP_ROOT = PROJECT_ROOT
DEFAULT_USER = os.environ.get("JOB_MONITOR_SSH_USER") or os.environ.get("USER") or "user"
DEFAULT_SSH_KEY = os.path.expanduser(os.environ.get("JOB_MONITOR_SSH_KEY", "~/.ssh/id_ed25519"))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "history.db")
SSH_TIMEOUT = 8
CACHE_FRESH_SEC = 30

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
        "user": _cfg.get("user", DEFAULT_USER),
        "key": os.path.expanduser(_cfg.get("key", DEFAULT_SSH_KEY)),
        "port": _cfg.get("port", 22),
        "gpu_type": _cfg.get("gpu_type", ""),
    }
CLUSTERS["local"] = {
    "host": None, "user": None, "key": None,
    "port": None, "gpu_type": "local",
}


def _load_mount_map():
    home = os.path.expanduser("~")
    base = os.path.join(home, ".job-monitor", "mounts")
    defaults = {
        name: [os.path.join(base, name)]
        for name in CLUSTERS if name != "local"
    }
    raw = os.environ.get("JOB_MONITOR_MOUNT_MAP", "").strip()
    if not raw:
        return defaults
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return defaults
        out = {}
        for name, roots in parsed.items():
            if name not in CLUSTERS or name == "local":
                continue
            if isinstance(roots, str):
                roots = [roots]
            if not isinstance(roots, list):
                continue
            norm = []
            for r in roots:
                if not isinstance(r, str):
                    continue
                p = os.path.abspath(os.path.expanduser(r.strip()))
                if p:
                    norm.append(p)
            if norm:
                out[name] = norm
        return out or defaults
    except Exception:
        return defaults


MOUNT_MAP = _load_mount_map()
MOUNT_SCRIPT_PATH = os.path.join(APP_ROOT, "scripts", "sshfs_logs.sh")

STATE_ORDER = {"RUNNING": 0, "COMPLETING": 1, "PENDING": 2, "FAILED": 3, "CANCELLED": 4}
SQUEUE_FMT = "%i|%j|%T|%r|%M|%l|%D|%C|%b|%P|%V|%S|%E"
SQUEUE_HDR = ["jobid", "name", "state", "reason", "elapsed", "timelimit", "nodes", "cpus", "gres", "partition", "submitted", "started", "dependency"]

# In-memory caches
_cache_lock = threading.Lock()
_cache = {}
_seen_jobs = {}
_last_polled = {}

_ssh_pool_lock = threading.Lock()
_ssh_pool = {}
_ssh_cluster_locks = {}
SSH_IDLE_TTL_SEC = 180

_warm_lock = threading.Lock()
_log_index_cache = {}
_log_content_cache = {}
_stats_cache = {}
_dir_list_cache = {}
_progress_cache = {}
_prefetch_last = {}
LOG_INDEX_TTL_SEC = 120
LOG_CONTENT_TTL_SEC = 45
STATS_TTL_SEC = 15
DIR_LIST_TTL_SEC = 20
PROGRESS_TTL_SEC = 60
PREFETCH_MIN_GAP_SEC = 120

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
    # Auto-detect: "artsiv_eval-math" → project "artsiv", prefix "artsiv_"
    import re
    m = re.match(r'^([a-zA-Z][a-zA-Z0-9-]*)_', job_name)
    if m:
        proj_name = m.group(1).lower()
        prefix = m.group(0)  # includes the trailing underscore
        if proj_name not in PROJECTS:
            PROJECTS[proj_name] = {"prefix": prefix}
            get_project_color(proj_name)  # auto-assign a color and persist
        return proj_name
    return ""


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


def _persist_projects():
    """Write current PROJECTS back into _CONFIG and save to disk."""
    _CONFIG["projects"] = PROJECTS
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


def reload_config(new_cfg):
    """Hot-reload mutable globals from a new config dict. Writes to disk first."""
    global _CONFIG, SSH_TIMEOUT, CACHE_FRESH_SEC
    global LOG_SEARCH_BASES, NEMO_RUN_BASES, MOUNT_LUSTRE_PREFIXES
    global LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE

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
    SSH_TIMEOUT = new_cfg.get("ssh_timeout", 8)
    CACHE_FRESH_SEC = new_cfg.get("cache_fresh_sec", 30)

    from .ssh import close_cluster_client

    new_clusters = {}
    for cname, ccfg in new_cfg.get("clusters", {}).items():
        new_clusters[cname] = {
            "host": ccfg.get("host", ""),
            "user": ccfg.get("user", DEFAULT_USER),
            "key": os.path.expanduser(ccfg.get("key", DEFAULT_SSH_KEY)),
            "port": ccfg.get("port", 22),
            "gpu_type": ccfg.get("gpu_type", ""),
        }
    new_clusters["local"] = {
        "host": None, "user": None, "key": None,
        "port": None, "gpu_type": "local",
    }

    removed = set(CLUSTERS.keys()) - set(new_clusters.keys())
    for r in removed:
        close_cluster_client(r)

    CLUSTERS.clear()
    CLUSTERS.update(new_clusters)
    MOUNT_MAP.clear()
    MOUNT_MAP.update(_load_mount_map())

    PROJECTS.clear()
    PROJECTS.update(new_cfg.get("projects", {}))


def settings_response():
    """Build the settings payload for GET /api/settings."""
    cfg = dict(_CONFIG)
    cfg["ssh_timeout"] = SSH_TIMEOUT
    cfg["cache_fresh_sec"] = CACHE_FRESH_SEC
    cfg["projects"] = dict(PROJECTS)
    return cfg
