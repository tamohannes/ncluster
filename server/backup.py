"""Daily SQLite + logbook-image backup with rotation.

Uses SQLite's online backup API so DB backups are safe even while
the database is being written to. Logbook images are archived into
a dated tarball alongside the DB snapshot. Keeps the last N daily
backups and cleans up older ones automatically.
"""

import logging
import os
import shutil
import sqlite3
import tarfile
import time
from datetime import datetime, timedelta

from .config import DB_PATH, PROJECT_ROOT, BACKUP_INTERVAL_HOURS, BACKUP_MAX_KEEP

log = logging.getLogger(__name__)

BACKUP_DIR = os.path.join(PROJECT_ROOT, "data", "backups")
LOGBOOK_IMAGES_DIR = os.path.join(PROJECT_ROOT, "data", "logbook_images")


def _date_str_today():
    return datetime.now().strftime("%Y-%m-%d")


def _backup_path_for_today():
    return os.path.join(BACKUP_DIR, f"history-{_date_str_today()}.db")


def _images_backup_path_for_today():
    return os.path.join(BACKUP_DIR, f"logbook-images-{_date_str_today()}.tar.gz")


def _run_backup():
    """Perform a single DB backup using SQLite online backup API."""
    dest_path = _backup_path_for_today()
    if os.path.exists(dest_path):
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)

    try:
        from .db import get_db
        src = get_db()
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


def _run_images_backup():
    """Archive logbook_images/ into a dated .tar.gz alongside the DB backup."""
    dest_path = _images_backup_path_for_today()
    if os.path.exists(dest_path):
        return False

    if not os.path.isdir(LOGBOOK_IMAGES_DIR):
        return False

    file_count = sum(len(files) for _, _, files in os.walk(LOGBOOK_IMAGES_DIR))
    if file_count == 0:
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)
    tmp_path = dest_path + ".tmp"

    try:
        with tarfile.open(tmp_path, "w:gz") as tar:
            tar.add(LOGBOOK_IMAGES_DIR, arcname="logbook_images")
        shutil.move(tmp_path, dest_path)
        size_kb = os.path.getsize(dest_path) // 1024
        log.info("Logbook images backup created: %s (%d KB, %d files)",
                 dest_path, size_kb, file_count)
        return True
    except Exception as e:
        log.warning("Logbook images backup failed: %s", e)
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return False


def _cleanup_old_backups():
    """Remove backups older than the configured retention period."""
    if not os.path.isdir(BACKUP_DIR):
        return
    from .config import BACKUP_MAX_KEEP
    cutoff = datetime.now() - timedelta(days=BACKUP_MAX_KEEP)

    prefixes = ("history-", "logbook-images-")
    suffixes = (".db", ".tar.gz")

    for fname in os.listdir(BACKUP_DIR):
        for prefix, suffix in zip(prefixes, suffixes):
            if fname.startswith(prefix) and fname.endswith(suffix):
                try:
                    date_str = fname[len(prefix):-len(suffix)]
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
            images_created = _run_images_backup()
            if created or images_created:
                _cleanup_old_backups()
        except Exception as e:
            log.warning("Backup loop error: %s", e)
        from .config import BACKUP_INTERVAL_HOURS
        sleep_sec = max(600, BACKUP_INTERVAL_HOURS * 3600 // 4)
        time.sleep(sleep_sec)
