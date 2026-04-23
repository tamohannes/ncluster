"""SQLite+FTS5-backed logbook with structured entries and BM25 search.

Each entry has: project, title, body (markdown), entry_type, created_at, edited_at.
entry_type is "note" (experiments, debugging, findings) or "plan" (implementation/research plans).
Full-text search via FTS5 with porter stemming and BM25 ranking.
Entries can reference each other with #<entry_id> syntax.
"""

import glob
import logging
import os
import re
from datetime import datetime

from .config import PROJECT_ROOT
from .db import get_db, db_write

log = logging.getLogger(__name__)

BODY_PREVIEW_LEN = 200
_LEGACY_DIR = os.path.join(PROJECT_ROOT, "data", "logbooks")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "data", "logbook_images")
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".html", ".htm"}


def list_logbook_projects():
    """Return all distinct project names that have logbook entries."""
    con = get_db()
    rows = con.execute(
        "SELECT DISTINCT project FROM logbook_entries WHERE project != '' ORDER BY project"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _row_to_dict(row, preview=False):
    d = dict(row)
    if preview and "body" in d:
        body = d["body"]
        d["body_preview"] = body[:BODY_PREVIEW_LEN] + ("…" if len(body) > BODY_PREVIEW_LEN else "")
        del d["body"]
    return d


def list_entries(project, query=None, sort="edited_at", limit=50, offset=0, entry_type=None):
    con = get_db()
    allowed_sorts = {"edited_at", "created_at", "title"}
    sort_col = sort if sort in allowed_sorts else "edited_at"
    sort_dir = "ASC" if sort_col == "title" else "DESC"

    if query and query.strip():
        conditions = ["f.logbook_fts MATCH ?", "e.project = ?"]
        params = [query.strip(), project]
        if entry_type:
            conditions.append("e.entry_type = ?")
            params.append(entry_type)
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        rows = con.execute(
            f"""SELECT e.id, e.project, e.title, e.body, e.created_at, e.edited_at, e.entry_type, e.pinned
                FROM logbook_entries e
                JOIN logbook_fts f ON e.id = f.rowid
                WHERE {where}
                ORDER BY rank
                LIMIT ? OFFSET ?""",
            params,
        ).fetchall()
    else:
        conditions = ["project = ?"]
        params = [project]
        if entry_type:
            conditions.append("entry_type = ?")
            params.append(entry_type)
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        rows = con.execute(
            f"""SELECT id, project, title, body, created_at, edited_at, entry_type, pinned
                FROM logbook_entries
                WHERE {where}
                ORDER BY pinned DESC, {sort_col} {sort_dir}
                LIMIT ? OFFSET ?""",
            params,
        ).fetchall()
    con.close()
    return [_row_to_dict(r, preview=True) for r in rows]


def get_entry(project, entry_id):
    con = get_db()
    row = con.execute(
        "SELECT id, project, title, body, created_at, edited_at, entry_type, pinned FROM logbook_entries WHERE id = ? AND project = ?",
        (entry_id, project),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "error", "error": "Entry not found"}
    return _row_to_dict(row)


def resolve_entry_refs(entry_ids):
    """Resolve entry IDs to {id, project, title} without project constraint.

    Used for rendering cross-project #N references in entry bodies.
    """
    if not entry_ids:
        return []
    con = get_db()
    placeholders = ",".join("?" for _ in entry_ids)
    rows = con.execute(
        f"SELECT id, project, title FROM logbook_entries WHERE id IN ({placeholders})",
        list(entry_ids),
    ).fetchall()
    con.close()
    return [{"id": r["id"], "project": r["project"], "title": r["title"]} for r in rows]


def _extract_entry_refs(body):
    """Extract #<id> references from body text."""
    return list(set(int(m) for m in re.findall(r'#(\d+)', body or "")))


def _update_links(con, entry_id, body):
    """Parse #id refs from body and update logbook_links table."""
    refs = _extract_entry_refs(body)
    con.execute("DELETE FROM logbook_links WHERE source_id=?", (entry_id,))
    for target_id in refs:
        if target_id != entry_id:
            try:
                con.execute(
                    "INSERT OR IGNORE INTO logbook_links (source_id, target_id) VALUES (?, ?)",
                    (entry_id, target_id),
                )
            except Exception:
                pass



def create_entry(project, title, body="", entry_type="note"):
    if entry_type not in ("note", "plan"):
        entry_type = "note"
    now = _now_iso()
    with db_write() as con:
        cur = con.execute(
            "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, entry_type) VALUES (?, ?, ?, ?, ?, ?)",
            (project, title, body, now, now, entry_type),
        )
        entry_id = cur.lastrowid
        _update_links(con, entry_id, body)
    return {"status": "ok", "id": entry_id, "created_at": now}


def update_entry(
    project,
    entry_id,
    title=None,
    body=None,
    entry_type=None,
    pinned=None,
    new_project=None,
):
    """Mutate a logbook entry. Any subset of fields may be updated.

    `new_project`, when provided, moves the entry to a different project.
    Entry IDs are globally unique, so cross-project ``#N`` references keep
    working after a move and ``logbook_links`` rows do not need to change.
    """
    moved_to = None
    if new_project is not None:
        target = new_project.strip()
        if not target:
            return {"status": "error_validation", "error": "new_project must be non-empty"}
        moved_to = target

    with db_write() as con:
        row = con.execute(
            "SELECT id FROM logbook_entries WHERE id = ? AND project = ?",
            (entry_id, project),
        ).fetchone()
        if not row:
            return {"status": "error", "error": "Entry not found"}

        now = _now_iso()
        sets, params = ["edited_at = ?"], [now]
        if title is not None:
            sets.append("title = ?")
            params.append(title)
        if body is not None:
            sets.append("body = ?")
            params.append(body)
        if entry_type is not None and entry_type in ("note", "plan"):
            sets.append("entry_type = ?")
            params.append(entry_type)
        if pinned is not None:
            sets.append("pinned = ?")
            params.append(1 if pinned else 0)
        if moved_to is not None:
            sets.append("project = ?")
            params.append(moved_to)
        params.extend([entry_id, project])
        con.execute(
            f"UPDATE logbook_entries SET {', '.join(sets)} WHERE id = ? AND project = ?",
            params,
        )
        if body is not None:
            _update_links(con, entry_id, body)
    result = {"status": "ok", "id": entry_id, "edited_at": now}
    if moved_to:
        result["project"] = moved_to
    return result


def delete_entry(project, entry_id):
    with db_write() as con:
        cur = con.execute(
            "DELETE FROM logbook_entries WHERE id = ? AND project = ?",
            (entry_id, project),
        )
        deleted = cur.rowcount
    if not deleted:
        return {"status": "error", "error": "Entry not found"}
    return {"status": "ok"}


def search_entries(query, project=None, date_from=None, date_to=None, limit=50):
    if not query or not query.strip():
        return []

    con = get_db()
    conditions = ["f.logbook_fts MATCH ?"]
    params = [query.strip()]

    if project:
        conditions.append("e.project = ?")
        params.append(project)
    if date_from:
        conditions.append("e.created_at >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("e.created_at <= ?")
        params.append(date_to)

    where = " AND ".join(conditions)
    params.extend([limit])

    rows = con.execute(
        f"""SELECT e.id, e.project, e.title, e.body, e.created_at, e.edited_at, e.entry_type
            FROM logbook_entries e
            JOIN logbook_fts f ON e.id = f.rowid
            WHERE {where}
            ORDER BY rank
            LIMIT ?""",
        params,
    ).fetchall()
    con.close()
    return [_row_to_dict(r, preview=True) for r in rows]


def _images_dir(project):
    return os.path.join(IMAGES_DIR, project)


def save_image(project, filename, data):
    """Save image bytes to disk. Returns the serving URL path."""
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_IMAGE_EXTS:
        return {"status": "error", "error": f"Unsupported image type: {ext}"}
    safe_name = os.path.basename(filename)
    dest_dir = _images_dir(project)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, safe_name)
    if os.path.exists(dest):
        base, ext = os.path.splitext(safe_name)
        i = 1
        while os.path.exists(os.path.join(dest_dir, f"{base}_{i}{ext}")):
            i += 1
        safe_name = f"{base}_{i}{ext}"
        dest = os.path.join(dest_dir, safe_name)
    with open(dest, "wb") as fh:
        fh.write(data)
    url = f"/api/logbook/{project}/images/{safe_name}"
    return {"status": "ok", "url": url, "filename": safe_name}


def get_image_path(project, filename):
    """Return the filesystem path for a stored image, or None."""
    safe_name = os.path.basename(filename)
    path = os.path.join(_images_dir(project), safe_name)
    if os.path.isfile(path):
        return path
    return None


def migrate_legacy_files():
    """Import .md files from the old file-based logbook into the DB (one-time)."""
    if not os.path.isdir(_LEGACY_DIR):
        return

    with db_write() as con:
        existing = con.execute("SELECT COUNT(*) FROM logbook_entries").fetchone()[0]
        if existing > 0:
            return

        count = 0
        for project_dir in sorted(glob.glob(os.path.join(_LEGACY_DIR, "*"))):
            if not os.path.isdir(project_dir):
                continue
            project = os.path.basename(project_dir)
            for md_file in sorted(glob.glob(os.path.join(project_dir, "*.md"))):
                fname = os.path.basename(md_file)
                title = fname[:-3] if fname.endswith(".md") else fname
                try:
                    with open(md_file, "r", encoding="utf-8") as fh:
                        body = fh.read().strip()
                    if not body:
                        continue
                    mtime = os.path.getmtime(md_file)
                    ts = datetime.fromtimestamp(mtime).isoformat(timespec="seconds")
                    con.execute(
                        "INSERT INTO logbook_entries (project, title, body, created_at, edited_at) VALUES (?, ?, ?, ?, ?)",
                        (project, title, body, ts, ts),
                    )
                    count += 1
                except Exception as exc:
                    log.warning("logbook migration: failed to import %s: %s", md_file, exc)
    if count:
        log.info("logbook migration: imported %d entries from legacy .md files", count)
