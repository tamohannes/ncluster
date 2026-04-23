"""clausius — entry point.

Run standalone:  python app.py
Run production:  gunicorn -c gunicorn.conf.py app:app
"""

import logging
import logging.config
import os
import threading
import time

from flask import Flask

from server.config import APP_PORT, PROJECT_ROOT
from server.routes import api


def _configure_logging():
    log_dir = os.path.join(PROJECT_ROOT, "data")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "clausius.log")
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "filename": log_path,
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 3,
                "formatter": "standard",
                "encoding": "utf-8",
            },
            "stderr": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
            },
        },
        "loggers": {
            "server": {
                "level": "INFO",
                "handlers": ["file", "stderr"],
                "propagate": False,
            },
        },
        "root": {
            "level": "WARNING",
            "handlers": ["file", "stderr"],
        },
    })


_configure_logging()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024
app.register_blueprint(api)

_BOOT_TS = str(int(time.time()))


@app.context_processor
def _inject_static_version():
    return {"v": _BOOT_TS}


def _sd_notify(state):
    """Send a notification to systemd if NOTIFY_SOCKET is set."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False
    import socket as _sock
    sock = _sock.socket(_sock.AF_UNIX, _sock.SOCK_DGRAM)
    try:
        if addr.startswith("@"):
            addr = "\0" + addr[1:]
        sock.sendto(state.encode(), addr)
        return True
    except Exception:
        return False
    finally:
        sock.close()


def _watchdog_notify_loop():
    """Notify systemd watchdog periodically with an in-process liveness check.

    Previous approach used an HTTP probe to /api/health — but when all
    gunicorn threads are blocked on SSH, the probe can't get a thread,
    times out, and systemd kills the process.  Now we check liveness
    directly: if the watchdog daemon thread itself can run and the worker
    process is responsive, we notify.  Thread exhaustion is handled
    separately by the SSH semaphore and per-cluster caps.
    """
    log = logging.getLogger("server.watchdog")
    _sd_notify("READY=1")
    time.sleep(10)
    while True:
        try:
            from server.routes import _active_request_count, _MAX_ACTIVE
            from server.ssh import get_circuit_breaker_status
            active = _active_request_count()
            if active > _MAX_ACTIVE * 3:
                log.warning(
                    "watchdog: active_requests=%d dangerously high, skipping notify",
                    active,
                )
            else:
                _sd_notify("WATCHDOG=1")
        except Exception as exc:
            log.warning("watchdog: health check failed: %s", exc)
        time.sleep(30)


_MIGRATION_PROJECTS_NAMED = {"artsiv", "hle", "n3ue", "profiling"}


def _migrate_projects_v1():
    """One-time migration: seed the SQLite ``projects`` table from
    ``conf/config.json`` and re-extract the ``project`` field on every
    historical job/run row using the new prefix table.

    Idempotent: skips entirely once the table contains any row. Runs once
    inside ``_shared_init()`` after ``init_db()``.

    Behaviour:
      1. If the ``projects`` table is empty AND ``conf/config.json`` has a
         ``projects`` key, insert exactly the four named projects (artsiv,
         hle, n3ue, profiling) by copying their existing color/emoji/prefix
         metadata. Drop every other entry (those were auto-detected from
         campaigns and are not real projects).
      2. Re-extract ``project`` for every row in ``job_history`` and ``runs``
         using the freshly-loaded prefix table. Rows whose ``job_name`` does
         not match any registered prefix get ``project = ''``.
      3. Strip the now-stale ``projects`` key from ``conf/config.json``.
    """
    import json
    log = logging.getLogger("server.migration")
    from server.db import get_db, db_write, db_create_project
    from server.config import (
        CONFIG_PATH,
        PROJECTS,
        extract_project,
        reload_projects_cache,
    )

    con = get_db()
    has_projects = con.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
    if has_projects:
        return

    legacy_cfg = None
    legacy_projects = {}
    try:
        if os.path.isfile(CONFIG_PATH):
            with open(CONFIG_PATH) as fh:
                legacy_cfg = json.load(fh)
            legacy_projects = legacy_cfg.get("projects") or {}
    except Exception as exc:
        log.warning("project migration: could not read config.json: %s", exc)
        legacy_cfg = None

    seeded = 0
    for name in _MIGRATION_PROJECTS_NAMED:
        cfg = legacy_projects.get(name) or {}
        prefixes = []
        for entry in cfg.get("prefixes") or []:
            if isinstance(entry, dict) and entry.get("prefix"):
                norm = {"prefix": entry["prefix"]}
                if entry.get("default_campaign"):
                    norm["default_campaign"] = entry["default_campaign"]
                prefixes.append(norm)
        if not prefixes and cfg.get("prefix"):
            prefixes.append({"prefix": cfg["prefix"]})
        if not prefixes:
            prefixes.append({"prefix": f"{name}_"})
        result = db_create_project(
            name=name,
            color=cfg.get("color") or None,
            emoji=cfg.get("emoji") or None,
            prefixes=prefixes,
            default_campaign=cfg.get("default_campaign") or None,
            campaign_delimiter=cfg.get("campaign_delimiter") or "_",
        )
        if result.get("status") == "ok":
            seeded += 1
        else:
            log.warning("project migration: failed to seed %s: %s", name, result.get("error"))
    log.info("project migration: seeded %d projects (target=%d)", seeded, len(_MIGRATION_PROJECTS_NAMED))

    reload_projects_cache()
    if not PROJECTS:
        log.warning("project migration: PROJECTS cache empty after seeding — skipping re-extract")
        return

    rows = con.execute("SELECT id, job_name FROM job_history").fetchall()
    updates = [
        (extract_project(r["job_name"] or ""), r["id"])
        for r in rows
    ]
    if updates:
        with db_write() as wcon:
            wcon.executemany("UPDATE job_history SET project=? WHERE id=?", updates)
        log.info("project migration: re-extracted project for %d job_history rows", len(updates))

    run_rows = con.execute("SELECT id, run_name FROM runs").fetchall()
    run_updates = [
        (extract_project(r["run_name"] or ""), r["id"])
        for r in run_rows
    ]
    if run_updates:
        with db_write() as wcon:
            wcon.executemany("UPDATE runs SET project=? WHERE id=?", run_updates)
        log.info("project migration: re-extracted project for %d runs rows", len(run_updates))

    if isinstance(legacy_cfg, dict) and "projects" in legacy_cfg:
        try:
            legacy_cfg.pop("projects", None)
            with open(CONFIG_PATH, "w") as fh:
                json.dump(legacy_cfg, fh, indent=2)
                fh.write("\n")
            log.info("project migration: stripped 'projects' key from config.json")
        except Exception as exc:
            log.warning("project migration: could not rewrite config.json: %s", exc)


def _shared_init():
    """Initialisation common to gunicorn and the in-process MCP server.

    Both processes need a usable DB schema and the cheap shared housekeeping
    threads. They MUST NOT both run anything that owns external state
    (backups, mount remounts, WDS snapshots, the progress scraper) — those
    stay leader-only in `_run_init`.
    """
    from server.db import init_db, cleanup_local_on_startup
    from server.logbooks import migrate_legacy_files
    from server.ssh import ssh_pool_gc_loop
    from server.config import cache_gc_loop, reload_projects_cache

    init_db()
    _migrate_projects_v1()
    reload_projects_cache()
    migrate_legacy_files()
    cleanup_local_on_startup()
    threading.Thread(target=ssh_pool_gc_loop, daemon=True).start()
    threading.Thread(target=cache_gc_loop, daemon=True).start()


def _run_init():
    """Full gunicorn init: shared bits plus everything that owns external
    state (backups, mounts, WDS, progress scraper, the cluster poller, and
    the systemd watchdog notifier)."""
    from server.backup import backup_loop
    from server.mounts import mount_health_loop
    from server.wds import wds_snapshot_loop
    from server.poller import start_poller
    from server.progress_scraper import start_progress_scraper

    _shared_init()
    threading.Thread(target=backup_loop, daemon=True).start()
    threading.Thread(target=mount_health_loop, daemon=True).start()
    threading.Thread(target=wds_snapshot_loop, daemon=True).start()
    threading.Thread(target=_watchdog_notify_loop, daemon=True).start()
    start_poller()
    start_progress_scraper()


def mcp_init():
    """Lean init for the in-process MCP server.

    Brings up just the bits MCP needs to serve tool calls against the shared
    SQLite DB without colliding with gunicorn:
      - DB schema + migrations (idempotent)
      - SSH subprocess GC (in-process bookkeeping only)
      - In-memory cache GC (in-process bookkeeping only)

    Deliberately omitted:
      - backup / mount-health / WDS / progress scraper — single-writer
      - cluster poller — started lazily by the follower-poller in
        mcp_server.py only when gunicorn is unreachable
      - systemd watchdog — gunicorn owns the unit
    """
    _shared_init()


if __name__ == "__main__":
    _run_init()
    app.run(host="0.0.0.0", port=APP_PORT, debug=False, threaded=True)
