"""Daily SQLite backup with rotation.

Uses SQLite's online backup API so backups are safe even while
the database is being written to. Keeps the last N daily backups
and cleans up older ones automatically.
"""

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

from .config import DB_PATH, PROJECT_ROOT, BACKUP_INTERVAL_HOURS, BACKUP_MAX_KEEP

log = logging.getLogger(__name__)

BACKUP_DIR = os.path.join(PROJECT_ROOT, "data", "backups")


def _backup_path_for_today():
    date_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(BACKUP_DIR, f"history-{date_str}.db")


def _run_backup():
    """Perform a single backup using SQLite online backup API."""
    dest_path = _backup_path_for_today()
    if os.path.exists(dest_path):
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)

    try:
        src = sqlite3.connect(DB_PATH)
        dst = sqlite3.connect(dest_path)
        src.backup(dst)
        dst.close()
        src.close()
        size_kb = os.path.getsize(dest_path) // 1024
        log.info("DB backup created: %s (%d KB)", dest_path, size_kb)
        return True
    except Exception as e:
        log.warning("DB backup failed: %s", e)
        try:
            os.remove(dest_path)
        except OSError:
            pass
        return False


def _cleanup_old_backups():
    """Remove backups older than the configured retention period."""
    if not os.path.isdir(BACKUP_DIR):
        return
    from .config import BACKUP_MAX_KEEP
    cutoff = datetime.now() - timedelta(days=BACKUP_MAX_KEEP)
    for fname in os.listdir(BACKUP_DIR):
        if not fname.startswith("history-") or not fname.endswith(".db"):
            continue
        try:
            date_str = fname[len("history-"):-len(".db")]
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
            if file_date < cutoff:
                os.remove(os.path.join(BACKUP_DIR, fname))
                log.info("Removed old backup: %s", fname)
        except (ValueError, OSError):
            pass


def backup_loop():
    """Background loop: check periodically, backup at configured interval."""
    time.sleep(30)
    while True:
        try:
            created = _run_backup()
            if created:
                _cleanup_old_backups()
        except Exception as e:
            log.warning("Backup loop error: %s", e)
        from .config import BACKUP_INTERVAL_HOURS
        sleep_sec = max(600, BACKUP_INTERVAL_HOURS * 3600 // 4)
        time.sleep(sleep_sec)
