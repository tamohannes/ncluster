"""File-based logbook operations for per-project notes."""

import os
import time
from datetime import datetime

from .config import PROJECT_ROOT

LOGBOOKS_DIR = os.path.join(PROJECT_ROOT, "data", "logbooks")
ENTRY_SEPARATOR = "\n---\n"


def _project_dir(project):
    return os.path.join(LOGBOOKS_DIR, project)


def _logbook_path(project, name):
    if not name.endswith(".md"):
        name += ".md"
    return os.path.join(_project_dir(project), name)


def _sanitize_name(name):
    """Strip extension and disallow path traversal."""
    name = os.path.basename(name)
    if name.endswith(".md"):
        name = name[:-3]
    return name


def _split_entries(content):
    """Split markdown content into entries by --- separator."""
    if not content.strip():
        return []
    parts = content.split(ENTRY_SEPARATOR)
    return [p.strip() for p in parts if p.strip()]


def _join_entries(entries):
    return ENTRY_SEPARATOR.join(entries) + "\n"


def list_logbooks(project):
    """List .md logbooks for a project."""
    d = _project_dir(project)
    if not os.path.isdir(d):
        return []
    result = []
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".md"):
            continue
        fpath = os.path.join(d, fname)
        name = fname[:-3]
        try:
            with open(fpath, "r", encoding="utf-8") as fh:
                content = fh.read()
            entry_count = len(_split_entries(content))
            mtime = os.path.getmtime(fpath)
        except Exception:
            entry_count = 0
            mtime = 0
        result.append({
            "name": name,
            "entry_count": entry_count,
            "last_modified": datetime.fromtimestamp(mtime).isoformat(timespec="seconds") if mtime else "",
        })
    return result


def read_logbook(project, name):
    """Read full logbook content and return entries."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    if not os.path.isfile(path):
        return {"name": name, "content": "", "entries": [], "error": "Logbook not found"}
    with open(path, "r", encoding="utf-8") as fh:
        content = fh.read()
    entries = _split_entries(content)
    return {"name": name, "content": content, "entries": entries}


def add_entry(project, name, content):
    """Prepend a new entry to a logbook. Creates the logbook if needed."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    os.makedirs(_project_dir(project), exist_ok=True)

    existing = ""
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as fh:
            existing = fh.read().strip()

    new_content = content.strip()
    if existing:
        full = new_content + ENTRY_SEPARATOR + existing
    else:
        full = new_content
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(full + "\n")
    return {"status": "ok", "entry_count": len(_split_entries(full))}


def update_entry(project, name, index, content):
    """Replace an entry at the given index (0 = newest)."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    if not os.path.isfile(path):
        return {"status": "error", "error": "Logbook not found"}
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()
    entries = _split_entries(raw)
    if index < 0 or index >= len(entries):
        return {"status": "error", "error": f"Entry index {index} out of range (0-{len(entries)-1})"}
    entries[index] = content.strip()
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(_join_entries(entries))
    return {"status": "ok", "entry_count": len(entries)}


def create_logbook(project, name):
    """Create an empty logbook file."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    os.makedirs(_project_dir(project), exist_ok=True)
    if os.path.isfile(path):
        return {"status": "ok", "message": "Already exists", "name": name}
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("")
    return {"status": "ok", "name": name}


def delete_entry(project, name, index):
    """Delete an entry at the given index (0 = newest)."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    if not os.path.isfile(path):
        return {"status": "error", "error": "Logbook not found"}
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()
    entries = _split_entries(raw)
    if index < 0 or index >= len(entries):
        return {"status": "error", "error": f"Entry index {index} out of range (0-{len(entries)-1})"}
    entries.pop(index)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(_join_entries(entries) if entries else "")
    return {"status": "ok", "entry_count": len(entries)}


def rename_logbook(project, old_name, new_name):
    """Rename a logbook file."""
    old_name = _sanitize_name(old_name)
    new_name = _sanitize_name(new_name)
    if not new_name:
        return {"status": "error", "error": "New name is empty"}
    old_path = _logbook_path(project, old_name)
    new_path = _logbook_path(project, new_name)
    if not os.path.isfile(old_path):
        return {"status": "error", "error": "Logbook not found"}
    if os.path.isfile(new_path):
        return {"status": "error", "error": f"Logbook '{new_name}' already exists"}
    os.rename(old_path, new_path)
    return {"status": "ok", "name": new_name}


def delete_logbook(project, name):
    """Delete a logbook file."""
    name = _sanitize_name(name)
    path = _logbook_path(project, name)
    if not os.path.isfile(path):
        return {"status": "error", "error": "Logbook not found"}
    os.remove(path)
    return {"status": "ok"}
