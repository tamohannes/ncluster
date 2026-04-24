"""Bootstrap configuration loader for clausius v4.

The bootstrap config is the small set of values that MUST be known before
the database is reachable: where the data directory lives, what TCP port
the UI listens on, and which SSH user/key to fall back to for clusters
that don't override them.

Everything else (clusters, team members, PPP accounts, search paths,
process filters, runtime tunables) lives in the SQLite database and is
managed through the Settings UI, the ``python -m server.cli`` command,
or the MCP tools. This module deliberately knows nothing about those
values.

Resolution order for every field (highest precedence first):

  1. Environment variable (``CLAUSIUS_DATA_DIR``, ``CLAUSIUS_PORT``,
     ``CLAUSIUS_SSH_USER``, ``CLAUSIUS_SSH_KEY``).
  2. The TOML file at ``CLAUSIUS_BOOTSTRAP_FILE`` (default
     ``conf/clausius.toml`` relative to the project root).
  3. Built-in defaults (see ``DEFAULTS`` below).

The TOML file itself is optional — clausius boots cleanly with no file
and no env vars, falling back to the defaults. That makes ``python -m
server.cli setup`` work on a fresh clone without manual configuration.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from typing import Optional


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DEFAULTS = {
    "data_dir": "./data",
    "port": 7272,
    "ssh_user": "$USER",
    "ssh_key": "~/.ssh/id_ed25519",
}


@dataclass(frozen=True)
class Bootstrap:
    """Resolved bootstrap configuration.

    All paths are absolute and ``$USER`` placeholders / ``~`` prefixes are
    fully expanded. Designed to be read once at process start and treated
    as immutable for the process lifetime; restart the service to pick up
    changes.
    """

    data_dir: str
    port: int
    ssh_user: str
    ssh_key: str
    source_file: Optional[str]

    @property
    def db_path(self) -> str:
        """Absolute path to the primary SQLite database file."""
        return os.path.join(self.data_dir, "history.db")

    @property
    def backups_dir(self) -> str:
        return os.path.join(self.data_dir, "backups")

    @property
    def logbook_images_dir(self) -> str:
        return os.path.join(self.data_dir, "logbook_images")

    @property
    def log_path(self) -> str:
        return os.path.join(self.data_dir, "clausius.log")


def _default_bootstrap_path() -> str:
    return os.path.join(PROJECT_ROOT, "conf", "clausius.toml")


def _read_toml(path: str) -> dict:
    """Read a TOML bootstrap file, returning an empty dict if missing.

    Malformed TOML raises ``tomllib.TOMLDecodeError`` so misconfiguration
    is loud, not silent. A missing file is fine — the caller will fall
    back to env vars + defaults.
    """
    if not os.path.isfile(path):
        return {}
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def _expand_user(value: str, *, fallback_user: str) -> str:
    """Expand ``$USER`` and ``~`` in a path/string.

    ``fallback_user`` is the resolved SSH user name; using ``os.environ``
    here directly would be wrong for callers like the SSH key path, where
    the placeholder should track whatever user the bootstrap layer chose.
    """
    if not isinstance(value, str):
        return value
    expanded = value.replace("$USER", fallback_user)
    return os.path.expanduser(expanded)


def _resolve_data_dir(raw: str) -> str:
    """Return an absolute data_dir, resolving relative paths against PROJECT_ROOT."""
    expanded = os.path.expanduser(raw)
    if os.path.isabs(expanded):
        return os.path.normpath(expanded)
    return os.path.normpath(os.path.join(PROJECT_ROOT, expanded))


def _coerce_port(value) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"bootstrap.port must be an integer, got {value!r}") from exc
    if not (1 <= port <= 65535):
        raise ValueError(f"bootstrap.port must be between 1 and 65535, got {port}")
    return port


def _pick(env_name: str, file_value, default):
    """Resolve one field: env var beats file beats default."""
    env_value = os.environ.get(env_name)
    if env_value not in (None, ""):
        return env_value
    if file_value not in (None, ""):
        return file_value
    return default


def load_bootstrap(*, path: Optional[str] = None) -> Bootstrap:
    """Resolve bootstrap config from env + TOML file + defaults.

    ``path`` overrides the TOML file location (otherwise read from
    ``CLAUSIUS_BOOTSTRAP_FILE`` env var, falling back to
    ``conf/clausius.toml``).

    The returned :class:`Bootstrap` is fully resolved: ``$USER`` is
    substituted, ``~`` is expanded, relative ``data_dir`` is anchored at
    the project root, and the port is range-checked.
    """
    if path is None:
        path = os.environ.get("CLAUSIUS_BOOTSTRAP_FILE") or _default_bootstrap_path()

    file_data = _read_toml(path)
    bootstrap_section = file_data.get("bootstrap", {}) or {}
    ssh_section = file_data.get("ssh", {}) or {}

    data_dir_raw = _pick("CLAUSIUS_DATA_DIR", bootstrap_section.get("data_dir"), DEFAULTS["data_dir"])
    port_raw = _pick("CLAUSIUS_PORT", bootstrap_section.get("port"), DEFAULTS["port"])
    ssh_user_raw = _pick("CLAUSIUS_SSH_USER", ssh_section.get("user"), DEFAULTS["ssh_user"])
    ssh_key_raw = _pick("CLAUSIUS_SSH_KEY", ssh_section.get("key"), DEFAULTS["ssh_key"])

    resolved_user = _expand_user(ssh_user_raw, fallback_user=os.environ.get("USER", "user"))
    if not resolved_user:
        resolved_user = os.environ.get("USER", "user")

    return Bootstrap(
        data_dir=_resolve_data_dir(data_dir_raw),
        port=_coerce_port(port_raw),
        ssh_user=resolved_user,
        ssh_key=_expand_user(ssh_key_raw, fallback_user=resolved_user),
        source_file=path if os.path.isfile(path) else None,
    )


_cached: Optional[Bootstrap] = None


def get_bootstrap() -> Bootstrap:
    """Return the process-wide bootstrap config (cached after first load).

    Tests that need to swap the bootstrap can call :func:`reset_bootstrap`
    or pass an override into :func:`load_bootstrap` directly.
    """
    global _cached
    if _cached is None:
        _cached = load_bootstrap()
    return _cached


def reset_bootstrap() -> None:
    """Drop the cached bootstrap so the next ``get_bootstrap()`` re-resolves."""
    global _cached
    _cached = None
