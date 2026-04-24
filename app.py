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
