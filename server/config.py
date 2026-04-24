"""Live configuration accessors for clausius v4.

In v3 this module loaded ``conf/config.json`` at import time and exposed
the parsed dict as a tangle of module-level globals. v4 keeps the same
import names so consumers don't churn, but every value now resolves
against the SQLite database on access:

* :data:`CLUSTERS` is a :class:`_LiveMapping` that hits ``server.clusters``.
* :data:`TEAM_GPU_ALLOC`, :data:`PPPS`, :data:`MOUNT_MAP`,
  :data:`MOUNT_REMOTE_MAP`, :data:`MOUNT_ALIASES` are similar mappings.
* :data:`TEAM_MEMBERS`, :data:`PPP_ACCOUNTS`, :data:`LOG_SEARCH_BASES`,
  :data:`NEMO_RUN_BASES`, :data:`MOUNT_LUSTRE_PREFIXES`,
  :data:`LOCAL_PROC_INCLUDE`, :data:`LOCAL_PROC_EXCLUDE` are
  :class:`_LiveSequence` wrappers backed by ``server.team`` /
  ``server.paths``.
* Old scalar globals (:data:`TEAM_NAME`, :data:`SSH_TIMEOUT`, etc.) are
  resolved via PEP 562 module ``__getattr__`` so ``server.config.TEAM_NAME``
  always reads the current value. ``from server.config import TEAM_NAME``
  still binds at import time — new callers should prefer the typed
  accessors in :mod:`server.settings` (``get_team_name()``).

Pure constants (``STATE_ORDER``, ``SQUEUE_FMT``, TTL knobs, in-memory
caches) and the project palette helpers stay untouched — they were
always code-shaped, not user config.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Callable

from .bootstrap import PROJECT_ROOT, get_bootstrap


# ─── Bootstrap-derived constants ─────────────────────────────────────────────

APP_ROOT = PROJECT_ROOT
_boot = get_bootstrap()
DEFAULT_USER = _boot.ssh_user
DEFAULT_SSH_KEY = _boot.ssh_key
DB_PATH = _boot.db_path
APP_PORT = _boot.port
MOUNT_SCRIPT_PATH = os.path.join(APP_ROOT, "scripts", "sshfs_logs.sh")


# ─── Pure constants (stay code-only, never user-configurable) ───────────────

STATE_ORDER = {"RUNNING": 0, "COMPLETING": 1, "PENDING": 2, "FAILED": 3, "CANCELLED": 4}
SQUEUE_FMT = "%i|%j|%T|%r|%M|%l|%D|%C|%b|%P|%V|%S|%E|%N|%a"
SQUEUE_HDR = ["jobid", "name", "state", "reason", "elapsed", "timelimit", "nodes",
              "cpus", "gres", "partition", "submitted", "started", "dependency",
              "node_list", "account"]
TERMINAL_STATES = {"FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL"}
PINNABLE_TERMINAL_STATES = TERMINAL_STATES | {"COMPLETED", "COMPLETING"}
RESULT_DIR_NAMES = ["eval-logs", "eval-results", "tmp-eval-results"]


# ─── In-process tunables (developer knobs, not user-facing) ──────────────────

SSH_IDLE_TTL_SEC = 300
LOG_INDEX_TTL_SEC = 120
LOG_CONTENT_TTL_SEC = 45
STATS_TTL_SEC = 15
DIR_LIST_TTL_SEC = 20
PROGRESS_TTL_SEC = 150
CRASH_TTL_SEC = 60
EST_START_TTL_SEC = 120
TEAM_USAGE_TTL_SEC = 120
PREFETCH_MIN_GAP_SEC = 60


# ─── In-memory caches and locks (process-local state) ───────────────────────

_cache_lock = threading.Lock()
_cache: dict = {}
_seen_jobs: dict = {}
_last_polled: dict = {}

_ssh_pool_lock = threading.Lock()
_ssh_pool: dict = {}
_ssh_cluster_locks: dict = {}

_warm_lock = threading.Lock()
_log_index_cache: dict = {}
_log_content_cache: dict = {}
_stats_cache: dict = {}
_dir_list_cache: dict = {}
_progress_cache: dict = {}
_progress_source_cache: dict = {}
_crash_cache: dict = {}
_est_start_cache: dict = {}
_team_usage_cache: dict = {}
_prefetch_last: dict = {}

# Aggregator-level cache for slow multi-cluster routes that fan out
# SSH work (where_to_submit primarily). Multiple Cursor agents tend to
# call the same tool within seconds of each other; a 30 s TTL lets them
# share one result instead of duplicating the underlying SSH wave.
_aggregator_cache: dict = {}
AGGREGATOR_CACHE_TTL_SEC = 30


# ─── Live proxies for DB-backed values ──────────────────────────────────────
#
# These wrap a callable that returns the current snapshot. Each access
# checks an in-memory TTL cache (default 1 s) before hitting the loader,
# so a request handler that does ``for c in CLUSTERS:`` followed by a
# few ``CLUSTERS.get(name)`` calls only triggers one DB query, not a
# fresh query per access. The TTL is short enough that any settings or
# CRUD write becomes visible to other readers within a second.
#
# Writes that need to be visible IMMEDIATELY (e.g. the Settings UI
# saving a cluster row and then re-rendering) should call
# ``invalidate_live_caches()`` from the module API below — that drops
# every live proxy's snapshot and forces a fresh load on next access.

# Per-instance cache TTL. Override via env for tests that want to pin
# behaviour (``CLAUSIUS_LIVE_TTL_SEC=0`` disables the cache entirely).
_LIVE_TTL_SEC = float(os.environ.get("CLAUSIUS_LIVE_TTL_SEC", "1.0"))


class _LiveMapping(dict):
    """Read-only ``dict`` subclass with a TTL-cached fresh-load fallback.

    Subclasses ``dict`` (instead of just ``Mapping``) so the standard
    ``json.dumps`` C encoder — which walks the underlying hash table
    directly via ``PyDict_Next`` and bypasses Python-level
    ``__iter__``/``__getitem__`` overrides — sees the cached snapshot.
    Every read method (``__getitem__``, ``__iter__``, ``items``, …)
    triggers ``_refresh`` first, which is a no-op as long as the cached
    snapshot is younger than ``_LIVE_TTL_SEC``.

    Mutation methods (``__setitem__``, ``update``, ``pop``, …) are
    intentionally no-ops — call the appropriate CRUD function instead.
    The legacy v3 ``reload_config`` did call ``CLUSTERS.clear()`` /
    ``.update()`` during settings reloads; we silently swallow those to
    keep any in-transition code paths happy.
    """

    def __init__(self, loader: Callable[[], dict], name: str = ""):
        super().__init__()
        # Use object.__setattr__ to bypass any future overrides; keep these
        # attributes on the instance dict, NOT in the (dict) data area.
        object.__setattr__(self, "_loader", loader)
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_lock", threading.Lock())
        # ``-inf`` so the first access always misses and loads fresh
        # data, but subsequent accesses inside ``_LIVE_TTL_SEC`` reuse it.
        object.__setattr__(self, "_cache_ts", float("-inf"))

    def _refresh(self) -> None:
        """Reload the snapshot if the cached copy is older than ``_LIVE_TTL_SEC``."""
        now = time.monotonic()
        with self._lock:
            if now - self._cache_ts < _LIVE_TTL_SEC:
                return
            fresh = self._loader()
            # Use dict.* directly so we don't recurse into our own overrides.
            dict.clear(self)
            dict.update(self, fresh)
            object.__setattr__(self, "_cache_ts", now)

    def invalidate(self) -> None:
        """Drop the cached snapshot. The next access reloads from the DB."""
        with self._lock:
            object.__setattr__(self, "_cache_ts", float("-inf"))

    def __getitem__(self, key):
        self._refresh()
        return dict.__getitem__(self, key)

    def __iter__(self):
        self._refresh()
        return dict.__iter__(self)

    def __len__(self):
        self._refresh()
        return dict.__len__(self)

    def __contains__(self, key):
        self._refresh()
        return dict.__contains__(self, key)

    def get(self, key, default=None):
        self._refresh()
        return dict.get(self, key, default)

    def keys(self):
        self._refresh()
        return dict.keys(self)

    def values(self):
        self._refresh()
        return dict.values(self)

    def items(self):
        self._refresh()
        return dict.items(self)

    def copy(self):
        self._refresh()
        return dict(self)

    def __repr__(self) -> str:
        return f"_LiveMapping({self._name!r})"

    def __eq__(self, other) -> bool:
        self._refresh()
        return dict.__eq__(self, other)

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        return id(self)

    # ── Legacy compatibility no-ops ─────────────────────────────────────
    def clear(self) -> None:  # noqa: D401 — see class docstring
        """No-op (compat shim for v3 ``reload_config`` callers)."""

    def update(self, *args, **kwargs) -> None:  # noqa: D401
        """No-op (compat shim for v3 ``reload_config`` callers)."""

    def __setitem__(self, key, value) -> None:
        # Silently drop — caller should use the CRUD layer.
        pass

    def __delitem__(self, key) -> None:
        pass

    def pop(self, *args, **kwargs):
        return None

    def setdefault(self, key, default=None):
        return self.get(key, default)


class _LiveSequence(list):
    """Read-only ``list`` subclass with a TTL-cached fresh-load fallback.

    Subclasses ``list`` for the same reason :class:`_LiveMapping`
    subclasses ``dict``: the C-level json encoder accesses list elements
    through ``PyList_GET_ITEM``, bypassing any Python-side overrides.
    Each read triggers ``_refresh`` first, which is a no-op as long as
    the cached snapshot is younger than ``_LIVE_TTL_SEC``.
    """

    def __init__(self, loader: Callable[[], list], name: str = ""):
        super().__init__()
        object.__setattr__(self, "_loader", loader)
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_lock", threading.Lock())
        object.__setattr__(self, "_cache_ts", float("-inf"))

    def _refresh(self) -> None:
        now = time.monotonic()
        with self._lock:
            if now - self._cache_ts < _LIVE_TTL_SEC:
                return
            fresh = self._loader()
            list.clear(self)
            list.extend(self, fresh)
            object.__setattr__(self, "_cache_ts", now)

    def invalidate(self) -> None:
        """Drop the cached snapshot. The next access reloads from the DB."""
        with self._lock:
            object.__setattr__(self, "_cache_ts", float("-inf"))

    def __getitem__(self, index):
        self._refresh()
        return list.__getitem__(self, index)

    def __iter__(self):
        self._refresh()
        return list.__iter__(self)

    def __len__(self):
        self._refresh()
        return list.__len__(self)

    def __contains__(self, item):
        self._refresh()
        return list.__contains__(self, item)

    def __repr__(self) -> str:
        return f"_LiveSequence({self._name!r})"

    def __eq__(self, other) -> bool:
        self._refresh()
        return list.__eq__(self, other)

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __hash__(self):
        return id(self)

    # ── Legacy mutation shims (no-op, route writes through CRUD) ─────
    def append(self, _item) -> None:
        pass

    def extend(self, _items) -> None:
        pass

    def insert(self, _index, _item) -> None:
        pass

    def remove(self, _item) -> None:
        pass

    def pop(self, *args, **kwargs):
        return None

    def clear(self) -> None:
        pass

    def __setitem__(self, _index, _value) -> None:
        pass

    def __delitem__(self, _index) -> None:
        pass


# Loader shims — defined as nested functions to defer the import of the
# CRUD modules until first use. Otherwise we'd hit circular imports
# (db.py imports from this module to grab DB_PATH).

def _clusters_loader() -> dict:
    from . import clusters as _c
    return _c.cluster_map(include_local=True, only_enabled=False)


def _team_gpu_alloc_loader() -> dict:
    from . import clusters as _c
    return _c.build_team_gpu_allocations()


def _ppps_loader() -> dict:
    from . import team as _t
    return _t.ppp_id_map()


def _ppp_accounts_loader() -> list:
    from . import team as _t
    return _t.list_ppp_account_names()


def _team_members_loader() -> list:
    from . import team as _t
    return _t.list_team_usernames()


def _log_search_bases_loader() -> list:
    from . import paths as _p
    return _p.list_paths("log_search")


def _nemo_run_bases_loader() -> list:
    from . import paths as _p
    return _p.list_paths("nemo_run")


def _mount_lustre_prefixes_loader() -> list:
    from . import paths as _p
    return _p.list_paths("mount_lustre_prefix")


def _local_proc_include_loader() -> list:
    from . import paths as _p
    return _p.list_patterns("include")


def _local_proc_exclude_loader() -> list:
    from . import paths as _p
    return _p.list_patterns("exclude")


def _mount_map_loader() -> dict:
    from . import clusters as _c
    return _c.build_mount_map()


def _mount_remote_map_loader() -> dict:
    from . import clusters as _c
    return _c.build_mount_remote_map(default_user=DEFAULT_USER)


def _mount_aliases_loader() -> dict:
    from . import clusters as _c
    return _c.build_mount_aliases(default_user=DEFAULT_USER)


CLUSTERS = _LiveMapping(_clusters_loader, "CLUSTERS")
TEAM_GPU_ALLOC = _LiveMapping(_team_gpu_alloc_loader, "TEAM_GPU_ALLOC")
PPPS = _LiveMapping(_ppps_loader, "PPPS")
PPP_ACCOUNTS = _LiveSequence(_ppp_accounts_loader, "PPP_ACCOUNTS")
TEAM_MEMBERS = _LiveSequence(_team_members_loader, "TEAM_MEMBERS")
LOG_SEARCH_BASES = _LiveSequence(_log_search_bases_loader, "LOG_SEARCH_BASES")
NEMO_RUN_BASES = _LiveSequence(_nemo_run_bases_loader, "NEMO_RUN_BASES")
MOUNT_LUSTRE_PREFIXES = _LiveSequence(_mount_lustre_prefixes_loader, "MOUNT_LUSTRE_PREFIXES")
LOCAL_PROC_INCLUDE = _LiveSequence(_local_proc_include_loader, "LOCAL_PROC_INCLUDE")
LOCAL_PROC_EXCLUDE = _LiveSequence(_local_proc_exclude_loader, "LOCAL_PROC_EXCLUDE")
MOUNT_MAP = _LiveMapping(_mount_map_loader, "MOUNT_MAP")
MOUNT_REMOTE_MAP = _LiveMapping(_mount_remote_map_loader, "MOUNT_REMOTE_MAP")
MOUNT_ALIASES = _LiveMapping(_mount_aliases_loader, "MOUNT_ALIASES")


_LIVE_PROXIES = (
    CLUSTERS, TEAM_GPU_ALLOC, PPPS,
    PPP_ACCOUNTS, TEAM_MEMBERS,
    LOG_SEARCH_BASES, NEMO_RUN_BASES, MOUNT_LUSTRE_PREFIXES,
    LOCAL_PROC_INCLUDE, LOCAL_PROC_EXCLUDE,
    MOUNT_MAP, MOUNT_REMOTE_MAP, MOUNT_ALIASES,
)


def invalidate_live_caches() -> None:
    """Drop every live proxy's cached snapshot.

    Call this after a CRUD write that needs to be visible to other
    readers immediately (e.g. the Settings UI saving a cluster row).
    Without this, callers might see stale data for up to
    ``_LIVE_TTL_SEC`` seconds after the write.
    """
    for proxy in _LIVE_PROXIES:
        proxy.invalidate()


# ─── Legacy scalar names via PEP 562 ────────────────────────────────────────
#
# These resolve fresh on every ``server.config.NAME`` access. Old code
# that did ``from server.config import TEAM_NAME`` will still see the
# value at import time (no live updates) — those callers should migrate
# to ``server.settings.get_team_name()``.

_SETTINGS_ALIASES = {
    "TEAM_NAME": "team_name",
    "AIHUB_OPENSEARCH_URL": "aihub_opensearch_url",
    "DASHBOARD_URL": "dashboard_url",
    "AIHUB_CACHE_TTL": "aihub_cache_ttl_sec",
    "WDS_SNAPSHOT_INTERVAL": "wds_snapshot_interval_sec",
    "SSH_TIMEOUT": "ssh_timeout",
    "CACHE_FRESH_SEC": "cache_fresh_sec",
    "STATS_INTERVAL_SEC": "stats_interval_sec",
    "BACKUP_INTERVAL_HOURS": "backup_interval_hours",
    "BACKUP_MAX_KEEP": "backup_max_keep",
    "SDK_INGEST_TOKEN": "sdk_ingest_token",
}


def __getattr__(name: str):
    """PEP 562 module-level attribute access fallback.

    Resolves the legacy scalar config names (``TEAM_NAME``, ``SSH_TIMEOUT``,
    ...) against the live ``app_settings`` table. Raises ``AttributeError``
    for anything else so import-time typos still fail loudly.
    """
    if name in _SETTINGS_ALIASES:
        from .settings import get_setting
        return get_setting(_SETTINGS_ALIASES[name])
    raise AttributeError(f"module 'server.config' has no attribute {name!r}")


# ─── Projects (DB-backed cache, unchanged from v3) ──────────────────────────

PROJECTS: dict = {}

_PROJECT_PALETTE = [
    "#e8f4fd", "#fef3e2", "#e8f5e9", "#fce4ec",
    "#ede7f6", "#fff8e1", "#e0f2f1", "#fbe9e7",
    "#e3f2fd", "#f3e5f5", "#e8eaf6", "#fff3e0",
]

_PROJECT_EMOJIS = [
    "🔬", "🧪", "🚀", "⚡", "🎯", "🔮", "🌊", "🔥",
    "💎", "🧬", "🏗️", "🎨",
]


def reload_projects_cache() -> None:
    """Refresh the in-process ``PROJECTS`` dict from the SQLite ``projects`` table.

    Imported lazily to avoid a circular import with ``server.db``. Safe
    to call repeatedly; replaces the dict's contents in-place so existing
    references (e.g. captured in tests) keep seeing live state.
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

    Supports both the legacy singular ``{"prefix": "..."}`` and the
    multi-prefix ``{"prefixes": [...]}`` forms. Empty prefixes are
    dropped.
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
    """Return the project key for ``job_name`` based on registered prefixes."""
    if not job_name:
        return ""
    candidates = []
    for name, cfg in PROJECTS.items():
        for prefix, _default in _project_prefix_entries(cfg):
            candidates.append((len(prefix), name, prefix))
    candidates.sort(key=lambda x: x[0], reverse=True)
    for _, name, prefix in candidates:
        if job_name.startswith(prefix):
            return name
    return ""


def extract_campaign(job_name, project=""):
    """Return the campaign key from a job name (see project-logbook docs)."""
    if not job_name:
        return ""
    project_cfg = PROJECTS.get(project) if project else None
    delimiter = "_"
    matched_prefix = ""
    matched_default = None
    if project_cfg:
        delimiter = project_cfg.get("campaign_delimiter") or "_"
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
    """Return the color for a registered project, or ``""`` if not registered."""
    if not project_name:
        return ""
    cfg = PROJECTS.get(project_name)
    if not cfg:
        return ""
    return cfg.get("color") or ""


def get_project_emoji(project_name):
    """Return the emoji for a registered project, or ``""`` if not registered."""
    if not project_name:
        return ""
    cfg = PROJECTS.get(project_name)
    if not cfg:
        return ""
    return cfg.get("emoji") or ""


# ─── Cache helpers (unchanged from v3) ──────────────────────────────────────

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
    """Evict entries older than 2x their TTL from all TTL caches."""
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


# ─── Back-compat shims (slated for removal once routes.py is rewired) ───────
#
# The v3 ``POST /api/settings`` handler in ``server/routes.py`` still
# imports these names. They become unused once that endpoint is
# replaced by the per-namespace endpoints (see todo ``rest_api``).

_CONFIG: dict = {}
"""Deprecated: legacy in-memory copy of the v3 ``conf/config.json`` blob.
Always empty in v4 because the JSON file no longer exists."""

CONFIG_PATH = os.path.join(PROJECT_ROOT, "conf", "config.json")
"""Deprecated: legacy path for the v3 JSON config. Kept as a stable
attribute so old test fixtures can still ``monkeypatch.setattr`` against
it without import errors. The file at this path is NOT read by v4."""


def reload_config(_new_cfg) -> None:
    """Deprecated no-op kept for back-compat with the old POST /api/settings
    handler. v4 manages config through per-namespace CRUD modules; calling
    this function does nothing."""
    return None


# ─── Settings response (back-compat aggregate view for the UI) ──────────────

def settings_response():
    """Build the settings payload for ``GET /api/settings``.

    Aggregates every v4 namespace into the legacy single-blob shape the
    Settings page consumes. The frontend will be rewired to per-namespace
    endpoints separately (see todo ``frontend``); until then this view
    keeps the existing UI working.
    """
    from . import clusters as _clusters_mod
    from . import paths as _paths_mod
    from . import team as _team_mod
    from .settings import list_settings

    settings = list_settings()
    payload: dict = {}

    # Scalar settings: flatten so the frontend reads them like before.
    for key, entry in settings.items():
        payload[key] = entry["value"]

    payload["port"] = APP_PORT
    payload["projects"] = {k: dict(v) for k, v in PROJECTS.items()}
    payload["clusters"] = {
        c["name"]: c
        for c in _clusters_mod.list_clusters(include_local=False, only_enabled=False)
    }
    payload["team_gpu_allocations"] = _clusters_mod.build_team_gpu_allocations()
    payload["team_members"] = _team_mod.list_team_usernames()
    payload["ppp_accounts"] = _team_mod.list_ppp_account_names()
    payload["ppps"] = _team_mod.ppp_id_map()
    payload["log_search_bases"] = _paths_mod.list_paths("log_search")
    payload["nemo_run_bases"] = _paths_mod.list_paths("nemo_run")
    payload["mount_lustre_prefixes"] = _paths_mod.list_paths("mount_lustre_prefix")
    payload["local_process_filters"] = {
        "include": _paths_mod.list_patterns("include"),
        "exclude": _paths_mod.list_patterns("exclude"),
    }
    return payload
