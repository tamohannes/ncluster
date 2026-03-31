"""SQLite+FTS5-backed logbook with structured entries and BM25 search.

Each entry has: project, title, body (markdown), entry_type, created_at, edited_at.
entry_type is "note" (experiments, debugging, findings) or "plan" (implementation/research plans).
Full-text search via FTS5 with porter stemming and BM25 ranking.
"""

import glob
import logging
import os
from datetime import datetime

from .config import PROJECT_ROOT
from .db import get_db

log = logging.getLogger(__name__)

BODY_PREVIEW_LEN = 200
_LEGACY_DIR = os.path.join(PROJECT_ROOT, "data", "logbooks")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "data", "logbook_images")
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".html", ".htm"}


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
            f"""SELECT e.id, e.project, e.title, e.body, e.created_at, e.edited_at, e.entry_type
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
            f"""SELECT id, project, title, body, created_at, edited_at, entry_type
                FROM logbook_entries
                WHERE {where}
                ORDER BY {sort_col} {sort_dir}
                LIMIT ? OFFSET ?""",
            params,
        ).fetchall()
    con.close()
    return [_row_to_dict(r, preview=True) for r in rows]


def get_entry(project, entry_id):
    con = get_db()
    row = con.execute(
        "SELECT id, project, title, body, created_at, edited_at, entry_type FROM logbook_entries WHERE id = ? AND project = ?",
        (entry_id, project),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "error", "error": "Entry not found"}
    return _row_to_dict(row)


def create_entry(project, title, body="", entry_type="note"):
    if entry_type not in ("note", "plan"):
        entry_type = "note"
    now = _now_iso()
    con = get_db()
    cur = con.execute(
        "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, entry_type) VALUES (?, ?, ?, ?, ?, ?)",
        (project, title, body, now, now, entry_type),
    )
    entry_id = cur.lastrowid
    con.commit()
    con.close()
    return {"status": "ok", "id": entry_id, "created_at": now}


def update_entry(project, entry_id, title=None, body=None, entry_type=None):
    con = get_db()
    row = con.execute(
        "SELECT id FROM logbook_entries WHERE id = ? AND project = ?",
        (entry_id, project),
    ).fetchone()
    if not row:
        con.close()
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
    params.extend([entry_id, project])
    con.execute(
        f"UPDATE logbook_entries SET {', '.join(sets)} WHERE id = ? AND project = ?",
        params,
    )
    con.commit()
    con.close()
    return {"status": "ok", "id": entry_id, "edited_at": now}


def delete_entry(project, entry_id):
    con = get_db()
    cur = con.execute(
        "DELETE FROM logbook_entries WHERE id = ? AND project = ?",
        (entry_id, project),
    )
    con.commit()
    deleted = cur.rowcount
    con.close()
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

    con = get_db()
    existing = con.execute("SELECT COUNT(*) FROM logbook_entries").fetchone()[0]
    if existing > 0:
        con.close()
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

    con.commit()
    con.close()
    if count:
        log.info("logbook migration: imported %d entries from legacy .md files", count)
