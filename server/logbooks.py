"""SQLite+FTS5-backed logbook with structured entries and BM25 search.

Each entry has: project, title, body (markdown), entry_type, created_at, edited_at.
entry_type is "note", "plan", or "campaign_board" (singleton per project+campaign;
structured grids live in board_json JSON). Optional ``campaign_goal`` (short
prose) is stored only for ``campaign_board`` rows.
Full-text search via FTS5 with porter stemming and BM25 ranking.
Entries can reference each other with #<entry_id> syntax.
"""

import glob
import json
import logging
import os
import re
from datetime import datetime

from .config import PROJECT_ROOT
from .db import get_db, db_write
from .logbook_board_runtime import attach_board_runtime

log = logging.getLogger(__name__)

BODY_PREVIEW_LEN = 200
_CAMPAIGN_PREFIX_RE = re.compile(r'^\[([^\]]+)\]\s*')
_ENTRY_ID_QUERY_RE = re.compile(r'^\s*(?:#|id:)\s*(\d+)\s*$', re.IGNORECASE)
_BARE_ENTRY_ID_QUERY_RE = re.compile(r'^\s*(\d+)\s*$')
_LEGACY_DIR = os.path.join(PROJECT_ROOT, "data", "logbooks")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "data", "logbook_images")
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".html", ".htm"}

ENTRY_TYPES = ("note", "plan", "campaign_board")
_LOGBOOK_ROW_SELECT = (
    "id, project, title, body, created_at, edited_at, entry_type, pinned, campaign, board_json, campaign_goal"
)
BOARD_JSON_MAX_BYTES = 512 * 1024
BOARD_MAX_SECTIONS = 48
BOARD_MAX_COLS = 64
BOARD_MAX_ROWS_PER_SECTION = 2000
BOARD_MAX_GRID_CELLS = BOARD_MAX_COLS * BOARD_MAX_ROWS_PER_SECTION
CAMPAIGN_GOAL_MAX_CHARS = 8000
BOARD_COLUMN_TYPES = frozenset({"string", "run_status"})
BOARD_SECTION_TYPES = frozenset({"table", "run_metric_grid"})
_COL_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")


def _default_board_json():
    return json.dumps({"version": 1, "sections": []}, separators=(",", ":"))


def validate_campaign_goal(raw):
    """Normalize campaign goal text for ``campaign_board`` entries."""
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise ValueError("campaign_goal must be a string")
    s = raw.strip()
    if len(s) > CAMPAIGN_GOAL_MAX_CHARS:
        raise ValueError(f"campaign_goal must be at most {CAMPAIGN_GOAL_MAX_CHARS} characters")
    return s


def _normalize_table_section(si: int, sec: dict) -> dict:
    """Classic board: rows carry cells + optional row-level cluster/run_hash."""
    title = sec.get("title", "")
    if title is None:
        title = ""
    if not isinstance(title, str):
        raise ValueError(f"section {si} title must be a string")
    cols = sec.get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f"section {si} must have a non-empty columns array")
    if len(cols) > BOARD_MAX_COLS:
        raise ValueError(f"section {si} allows at most {BOARD_MAX_COLS} columns")
    col_ids = []
    norm_cols = []
    seen_ids = set()
    run_status_columns = 0
    for ci, c in enumerate(cols):
        if not isinstance(c, dict):
            raise ValueError(f"section {si} column {ci} must be an object")
        cid = c.get("id")
        if not isinstance(cid, str) or not _COL_ID_RE.match(cid):
            raise ValueError(
                f"section {si} column {ci} needs a valid id "
                "(start with letter, alphanumeric+underscore, max 64 chars)"
            )
        if cid in seen_ids:
            raise ValueError(f"section {si} duplicate column id {cid!r}")
        seen_ids.add(cid)
        col_ids.append(cid)
        lab = c.get("label", cid)
        if lab is None:
            lab = cid
        if not isinstance(lab, str):
            raise ValueError(f"section {si} column {cid!r} label must be a string")
        col_type = c.get("type", "string")
        if col_type is None or col_type == "":
            col_type = "string"
        if not isinstance(col_type, str):
            raise ValueError(f"section {si} column {cid!r} type must be a string")
        col_type = col_type.strip().lower()
        if col_type not in BOARD_COLUMN_TYPES:
            raise ValueError(
                f"section {si} column {cid!r} has unknown type {col_type!r} "
                f"(allowed: string, run_status)"
            )
        if col_type == "run_status":
            run_status_columns += 1
            if run_status_columns > 1:
                raise ValueError(f"section {si} allows at most one run_status column")
        norm_cols.append({"id": cid, "label": lab, "type": col_type})

    rows = sec.get("rows")
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise ValueError(f"section {si} rows must be a list")
    if len(rows) > BOARD_MAX_ROWS_PER_SECTION:
        raise ValueError(
            f"section {si} allows at most {BOARD_MAX_ROWS_PER_SECTION} rows"
        )
    norm_rows = []
    for ri, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"section {si} row {ri} must be an object")
        cells = row.get("cells")
        if cells is None:
            cells = {}
        if not isinstance(cells, dict):
            raise ValueError(f"section {si} row {ri} cells must be an object")
        for k in cells:
            if k not in seen_ids:
                raise ValueError(
                    f"section {si} row {ri} has unknown cell key {k!r} "
                    f"(not in columns)"
                )
        norm_cells = {cid: str(cells.get(cid, "") if cells.get(cid) is not None else "") for cid in col_ids}
        cluster = row.get("cluster", "") or ""
        run_hash = row.get("run_hash", "") or ""
        if not isinstance(cluster, str) or not isinstance(run_hash, str):
            raise ValueError(f"section {si} row {ri} cluster and run_hash must be strings")
        cluster = cluster.strip()
        run_hash = run_hash.strip()
        if run_hash and not cluster:
            raise ValueError(
                f"section {si} row {ri}: cluster is required when run_hash is set"
            )
        norm_rows.append(
            {"cells": norm_cells, "cluster": cluster, "run_hash": run_hash}
        )
    return {"title": title, "columns": norm_cols, "rows": norm_rows}


def _normalize_run_metric_grid_section(si: int, sec: dict) -> dict:
    """Matrix where each cell is its own run (+ optional SDK scalar key)."""
    title = sec.get("title", "")
    if title is None:
        title = ""
    if not isinstance(title, str):
        raise ValueError(f"section {si} title must be a string")
    cols = sec.get("columns")
    if not isinstance(cols, list) or not cols:
        raise ValueError(f"section {si} must have a non-empty columns array")
    if len(cols) > BOARD_MAX_COLS:
        raise ValueError(f"section {si} allows at most {BOARD_MAX_COLS} columns")
    seen_col: set[str] = set()
    norm_cols: list[dict] = []
    for ci, c in enumerate(cols):
        if not isinstance(c, dict):
            raise ValueError(f"section {si} column {ci} must be an object")
        if set(c.keys()) - {"id", "label", "scalar"}:
            raise ValueError(
                f"section {si} run_metric_grid column {ci} only allows id, label, and optional scalar"
            )
        cid = c.get("id")
        if not isinstance(cid, str) or not _COL_ID_RE.match(cid):
            raise ValueError(
                f"section {si} column {ci} needs a valid id "
                "(start with letter, alphanumeric+underscore, max 64 chars)"
            )
        if cid in seen_col:
            raise ValueError(f"section {si} duplicate column id {cid!r}")
        seen_col.add(cid)
        lab = c.get("label", cid)
        if lab is None:
            lab = cid
        if not isinstance(lab, str):
            raise ValueError(f"section {si} column {cid!r} label must be a string")
        col_scalar = c.get("scalar", "") or ""
        if col_scalar is not None and not isinstance(col_scalar, str):
            raise ValueError(f"section {si} column {cid!r} scalar must be a string")
        col_scalar = col_scalar.strip()
        norm_col: dict = {"id": cid, "label": lab}
        if col_scalar:
            norm_col["scalar"] = col_scalar
        norm_cols.append(norm_col)

    rows_raw = sec.get("rows")
    if rows_raw is None:
        rows_raw = []
    if not isinstance(rows_raw, list):
        raise ValueError(f"section {si} rows must be a list")
    if len(rows_raw) > BOARD_MAX_ROWS_PER_SECTION:
        raise ValueError(
            f"section {si} allows at most {BOARD_MAX_ROWS_PER_SECTION} rows"
        )
    if not rows_raw:
        raise ValueError(f"section {si} run_metric_grid must have at least one row")
    seen_row: set[str] = set()
    norm_rows: list[dict] = []
    for ri, row in enumerate(rows_raw):
        if not isinstance(row, dict):
            raise ValueError(f"section {si} row {ri} must be an object")
        if set(row.keys()) - {"id", "label"}:
            raise ValueError(
                f"section {si} run_metric_grid row {ri} only allows id and label"
            )
        rid = row.get("id")
        if not isinstance(rid, str) or not _COL_ID_RE.match(rid):
            raise ValueError(
                f"section {si} row {ri} needs a valid id "
                "(start with letter, alphanumeric+underscore, max 64 chars)"
            )
        if rid in seen_row:
            raise ValueError(f"section {si} duplicate row id {rid!r}")
        seen_row.add(rid)
        rlab = row.get("label", rid)
        if rlab is None:
            rlab = rid
        if not isinstance(rlab, str):
            raise ValueError(f"section {si} row {rid!r} label must be a string")
        norm_rows.append({"id": rid, "label": rlab})

    cells_raw = sec.get("cells")
    if cells_raw is None:
        cells_raw = {}
    if not isinstance(cells_raw, dict):
        raise ValueError(f"section {si} cells must be an object keyed as row_id:col_id")
    if len(cells_raw) > BOARD_MAX_GRID_CELLS:
        raise ValueError(f"section {si} has too many cells (max {BOARD_MAX_GRID_CELLS})")

    expected_pairs: set[str] = {f"{r['id']}:{c['id']}" for r in norm_rows for c in norm_cols}
    col_default_scalar: dict[str, str] = {
        c["id"]: c["scalar"] for c in norm_cols if c.get("scalar")
    }
    norm_cells: dict[str, dict] = {}
    for key, spec in cells_raw.items():
        if not isinstance(key, str) or ":" not in key:
            raise ValueError(
                f"section {si} invalid cells key {key!r} (expected row_id:col_id)"
            )
        rk, ck = key.split(":", 1)
        if rk not in seen_row or ck not in seen_col:
            raise ValueError(
                f"section {si} cells key {key!r} must reference declared row and column ids"
            )
        if not isinstance(spec, dict):
            raise ValueError(f"section {si} cells[{key!r}] must be an object")
        extra = set(spec.keys()) - {"cluster", "run_hash", "scalar"}
        if extra:
            raise ValueError(f"section {si} cells[{key!r}] unknown keys {extra}")
        cluster = spec.get("cluster", "") or ""
        run_hash = spec.get("run_hash", "") or ""
        if not isinstance(cluster, str) or not isinstance(run_hash, str):
            raise ValueError(f"section {si} cells[{key!r}] cluster and run_hash must be strings")
        cluster = cluster.strip()
        run_hash = run_hash.strip()
        if not cluster or not run_hash:
            raise ValueError(f"section {si} cells[{key!r}] requires cluster and run_hash")
        scalar = spec.get("scalar", "") or ""
        if scalar is not None and not isinstance(scalar, str):
            raise ValueError(f"section {si} cells[{key!r}] scalar must be a string")
        scalar = scalar.strip()
        fallback = (col_default_scalar.get(ck) or "").strip()
        effective = scalar or fallback
        norm_cells[f"{rk}:{ck}"] = {
            "cluster": cluster.lower(),
            "run_hash": run_hash.lower(),
            **({"scalar": effective} if effective else {}),
        }

    missing = expected_pairs - set(norm_cells.keys())
    if missing:
        sample = ", ".join(sorted(missing)[:6])
        more = f" (+{len(missing) - 6} more)" if len(missing) > 6 else ""
        raise ValueError(
            f"section {si} run_metric_grid is missing cells for: {sample}{more}"
        )

    return {
        "type": "run_metric_grid",
        "title": title,
        "columns": norm_cols,
        "rows": norm_rows,
        "cells": norm_cells,
    }


def validate_board_json(raw):
    """Normalize and validate board_json payload. Returns compact JSON string.

    Raises ValueError with a short user-facing message on invalid input.
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return _default_board_json()
    if isinstance(raw, (bytes, bytearray)):
        raise ValueError("board_json must be JSON text or a dict")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"board_json is not valid JSON: {e}") from e
    elif isinstance(raw, dict):
        data = raw
    else:
        raise ValueError("board_json must be a dict or JSON string")

    if not isinstance(data, dict):
        raise ValueError("board_json root must be an object")
    ver = data.get("version", 1)
    if ver != 1:
        raise ValueError("board_json version must be 1")
    sections = data.get("sections")
    if sections is None:
        sections = []
    if not isinstance(sections, list):
        raise ValueError("board_json.sections must be a list")
    if len(sections) > BOARD_MAX_SECTIONS:
        raise ValueError(f"board_json allows at most {BOARD_MAX_SECTIONS} sections")

    out_sections = []
    for si, sec in enumerate(sections):
        if not isinstance(sec, dict):
            raise ValueError(f"section {si} must be an object")
        sec_type = sec.get("type", "table")
        if sec_type is None or sec_type == "":
            sec_type = "table"
        if not isinstance(sec_type, str):
            raise ValueError(f"section {si} type must be a string")
        sec_type = sec_type.strip().lower()
        if sec_type not in BOARD_SECTION_TYPES:
            raise ValueError(
                f"section {si} has unknown type {sec_type!r} "
                f"(allowed: table, run_metric_grid)"
            )
        if sec_type == "run_metric_grid":
            out_sections.append(_normalize_run_metric_grid_section(si, sec))
        else:
            out_sections.append(_normalize_table_section(si, sec))

    normalized = {"version": 1, "sections": out_sections}
    blob = json.dumps(normalized, separators=(",", ":"))
    if len(blob.encode("utf-8")) > BOARD_JSON_MAX_BYTES:
        raise ValueError("board_json is too large")
    return blob


def _other_campaign_board_id(con, project, campaign, exclude_id):
    r = con.execute(
        "SELECT id FROM logbook_entries WHERE project = ? AND campaign = ? "
        "AND entry_type = 'campaign_board' AND id != ?",
        (project, campaign, exclude_id),
    ).fetchone()
    return int(r["id"]) if r else None


def get_campaign_board(project, campaign):
    """Return the full campaign_board entry for project+campaign or a not_found dict."""
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error", "error": "campaign is required"}
    con = get_db()
    row = con.execute(
        f"SELECT {_LOGBOOK_ROW_SELECT} FROM logbook_entries "
        "WHERE project = ? AND campaign = ? AND entry_type = 'campaign_board'",
        (project, camp),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "not_found", "project": project, "campaign": camp}
    d = _row_to_dict(row)
    attach_board_runtime(d)
    return d


def list_campaign_boards(project):
    """List all campaign_board rows for a project (lightweight)."""
    con = get_db()
    rows = con.execute(
        "SELECT id, campaign, title, edited_at FROM logbook_entries "
        "WHERE project = ? AND entry_type = 'campaign_board' ORDER BY edited_at DESC, campaign",
        (project,),
    ).fetchall()
    con.close()
    return [
        {
            "entry_id": int(r["id"]),
            "campaign": r["campaign"],
            "title": r["title"],
            "edited_at": r["edited_at"],
        }
        for r in rows
    ]


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


def _extract_campaign_from_title(title):
    """Parse ``[campaign] Rest of title`` and return ``(campaign, stripped)``.

    Returns ``("", title)`` when no bracket prefix is found.
    """
    m = _CAMPAIGN_PREFIX_RE.match(title or "")
    if m:
        return m.group(1).strip().lower(), title[m.end():]
    return "", title or ""


def list_campaigns(project):
    """Return distinct non-empty campaigns for a project with entry counts."""
    con = get_db()
    rows = con.execute(
        "SELECT campaign, COUNT(*) AS cnt, MAX(edited_at) AS last_edited "
        "FROM logbook_entries WHERE project = ? AND campaign != '' "
        "GROUP BY campaign ORDER BY last_edited DESC, campaign",
        (project,),
    ).fetchall()
    con.close()
    return [{"name": r["campaign"], "count": r["cnt"]} for r in rows]


def _row_to_dict(row, preview=False):
    d = dict(row)
    if preview and "body" in d:
        body = d["body"]
        d["body_preview"] = body[:BODY_PREVIEW_LEN] + ("…" if len(body) > BODY_PREVIEW_LEN else "")
        del d["body"]
    if preview and "board_json" in d:
        del d["board_json"]
    if preview and "campaign_goal" in d:
        del d["campaign_goal"]
    if "_snippet" in d:
        d["snippet"] = d.pop("_snippet")
    return d


_SNIPPET_MARKER_L = "\x02"
_SNIPPET_MARKER_R = "\x03"


def _fts_safe_query(raw):
    """Wrap each token in double-quotes so FTS5 treats them as literals.

    This prevents user input like ``OR``, ``NOT``, bare ``*``, or unmatched
    quotes from raising an fts5 syntax error.  Quoted tokens still benefit
    from the porter stemmer configured on the table.
    """
    tokens = raw.strip().split()
    if not tokens:
        return raw.strip()
    return " ".join(f'"{t}"' for t in tokens)


def _entry_id_from_query(query):
    """Return an exact entry id for queries like ``#123`` or ``id:123``."""
    m = _ENTRY_ID_QUERY_RE.match(query or "")
    if not m:
        return None
    return int(m.group(1))


def _bare_entry_id_from_query(query):
    """Return an entry id for bare numeric queries, while still allowing text search."""
    m = _BARE_ENTRY_ID_QUERY_RE.match(query or "")
    if not m:
        return None
    return int(m.group(1))


def _id_search(
    con,
    entry_id,
    project=None,
    entry_type=None,
    limit=50,
    offset=0,
    campaign=None,
    date_from=None,
    date_to=None,
):
    """Exact id lookup that preserves the same filters as list/search."""
    conditions = ["id = ?"]
    params = [entry_id]
    if project:
        conditions.append("project = ?")
        params.append(project)
    if entry_type:
        conditions.append("entry_type = ?")
        params.append(entry_type)
    if campaign:
        conditions.append("campaign = ?")
        params.append(campaign)
    if date_from:
        conditions.append("created_at >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("created_at <= ?")
        params.append(date_to)
    where = " AND ".join(conditions)
    params.extend([limit, offset])
    return con.execute(
        f"""SELECT {_LOGBOOK_ROW_SELECT}
            FROM logbook_entries
            WHERE {where}
            ORDER BY pinned DESC, edited_at DESC
            LIMIT ? OFFSET ?""",
        params,
    ).fetchall()


def _merge_id_first(id_rows, text_rows, limit, offset):
    merged = []
    seen = set()
    for row in list(id_rows) + list(text_rows):
        row_id = row["id"]
        if row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row)
    return merged[offset:offset + limit]


def _fts_search(con, project, query, entry_type, limit, offset, campaign=None):
    """FTS5 search with snippet extraction, falling back to LIKE."""
    fts_q = _fts_safe_query(query)
    conditions = ["f.logbook_fts MATCH ?", "e.project = ?"]
    params = [fts_q, project]
    if entry_type:
        conditions.append("e.entry_type = ?")
        params.append(entry_type)
    if campaign:
        conditions.append("e.campaign = ?")
        params.append(campaign)
    where = " AND ".join(conditions)
    params.extend([limit, offset])
    try:
        return con.execute(
            f"""SELECT e.id, e.project, e.title, e.body, e.created_at, e.edited_at,
                       e.entry_type, e.pinned, e.campaign, e.board_json, e.campaign_goal,
                       snippet(logbook_fts, 1, '{_SNIPPET_MARKER_L}', '{_SNIPPET_MARKER_R}', '…', 48) AS _snippet
                FROM logbook_entries e
                JOIN logbook_fts f ON e.id = f.rowid
                WHERE {where}
                ORDER BY rank
                LIMIT ? OFFSET ?""",
            params,
        ).fetchall()
    except Exception:
        log.debug("FTS MATCH failed for %r, falling back to LIKE", query)
        return _like_search(con, project, query, entry_type, limit, offset, campaign=campaign)


def _like_search(con, project, query, entry_type, limit, offset, campaign=None):
    """Substring fallback when FTS5 MATCH fails."""
    conditions = ["project = ?", "(title LIKE ? OR body LIKE ?)"]
    like = f"%{query}%"
    params = [project, like, like]
    if entry_type:
        conditions.append("entry_type = ?")
        params.append(entry_type)
    if campaign:
        conditions.append("campaign = ?")
        params.append(campaign)
    where = " AND ".join(conditions)
    params.extend([limit, offset])
    return con.execute(
        f"""SELECT {_LOGBOOK_ROW_SELECT}
            FROM logbook_entries
            WHERE {where}
            ORDER BY edited_at DESC
            LIMIT ? OFFSET ?""",
        params,
    ).fetchall()


def list_entries(project, query=None, sort="edited_at", limit=50, offset=0, entry_type=None, campaign=None):
    con = get_db()
    allowed_sorts = {"edited_at", "created_at", "title"}
    sort_col = sort if sort in allowed_sorts else "edited_at"
    sort_dir = "ASC" if sort_col == "title" else "DESC"

    if query and query.strip():
        query_text = query.strip()
        entry_id = _entry_id_from_query(query_text)
        if entry_id is not None:
            rows = _id_search(
                con,
                entry_id,
                project=project,
                entry_type=entry_type,
                limit=limit,
                offset=offset,
                campaign=campaign,
            )
        elif _bare_entry_id_from_query(query_text) is not None:
            row_limit = limit + offset
            rows = _merge_id_first(
                _id_search(
                    con,
                    _bare_entry_id_from_query(query_text),
                    project=project,
                    entry_type=entry_type,
                    limit=1,
                    offset=0,
                    campaign=campaign,
                ),
                _fts_search(con, project, query_text, entry_type, row_limit, 0, campaign=campaign),
                limit,
                offset,
            )
        else:
            rows = _fts_search(con, project, query_text, entry_type, limit, offset, campaign=campaign)
    else:
        conditions = ["project = ?"]
        params = [project]
        if entry_type:
            conditions.append("entry_type = ?")
            params.append(entry_type)
        if campaign:
            conditions.append("campaign = ?")
            params.append(campaign)
        where = " AND ".join(conditions)
        params.extend([limit, offset])
        rows = con.execute(
            f"""SELECT {_LOGBOOK_ROW_SELECT}
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
        f"SELECT {_LOGBOOK_ROW_SELECT} FROM logbook_entries WHERE id = ? AND project = ?",
        (entry_id, project),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "error", "error": "Entry not found"}
    d = _row_to_dict(row)
    attach_board_runtime(d)
    return d


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



def create_entry(
    project, title, body="", entry_type="note", campaign=None, board_json=None, campaign_goal=None
):
    if entry_type not in ENTRY_TYPES:
        entry_type = "note"
    if campaign is None:
        campaign, title = _extract_campaign_from_title(title)
    else:
        campaign = (campaign or "").strip().lower()

    if entry_type == "campaign_board":
        if not campaign:
            return {
                "status": "error_validation",
                "error": "campaign is required for campaign_board entries",
            }
        try:
            bj = validate_board_json(board_json)
        except ValueError as e:
            return {"status": "error_validation", "error": str(e)}
        try:
            cg = validate_campaign_goal(campaign_goal if campaign_goal is not None else "")
        except ValueError as e:
            return {"status": "error_validation", "error": str(e)}
        now = _now_iso()
        with db_write() as con:
            existed = con.execute(
                "SELECT id FROM logbook_entries WHERE project = ? AND campaign = ? "
                "AND entry_type = 'campaign_board'",
                (project, campaign),
            ).fetchone()
            if existed:
                return {
                    "status": "error_validation",
                    "error": "A campaign board already exists for this campaign",
                    "existing_id": int(existed["id"]),
                }
            cur = con.execute(
                "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, "
                "entry_type, campaign, board_json, campaign_goal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (project, title, body, now, now, entry_type, campaign, bj, cg),
            )
            entry_id = cur.lastrowid
            _update_links(con, entry_id, body)
        return {"status": "ok", "id": entry_id, "created_at": now, "campaign": campaign}

    now = _now_iso()
    with db_write() as con:
        cur = con.execute(
            "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, "
            "entry_type, campaign, board_json, campaign_goal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (project, title, body, now, now, entry_type, campaign, "", ""),
        )
        entry_id = cur.lastrowid
        _update_links(con, entry_id, body)
    return {"status": "ok", "id": entry_id, "created_at": now, "campaign": campaign}


def update_entry(
    project,
    entry_id,
    title=None,
    body=None,
    entry_type=None,
    pinned=None,
    new_project=None,
    campaign=None,
    board_json=None,
    campaign_goal=None,
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
            f"SELECT {_LOGBOOK_ROW_SELECT} FROM logbook_entries WHERE id = ? AND project = ?",
            (entry_id, project),
        ).fetchone()
        if not row:
            return {"status": "error", "error": "Entry not found"}
        old = dict(row)

        eff_type = (
            entry_type
            if entry_type is not None and entry_type in ENTRY_TYPES
            else old["entry_type"]
        )
        if campaign is not None:
            eff_campaign = campaign.strip().lower()
        else:
            eff_campaign = (old["campaign"] or "")

        final_project = moved_to if moved_to is not None else project

        if board_json is not None:
            if eff_type != "campaign_board" and old["entry_type"] != "campaign_board":
                return {
                    "status": "error_validation",
                    "error": "board_json is only valid for campaign_board entries",
                }
            try:
                bj = validate_board_json(board_json)
            except ValueError as e:
                return {"status": "error_validation", "error": str(e)}
        else:
            bj = None

        if campaign_goal is not None:
            if eff_type != "campaign_board" and old["entry_type"] != "campaign_board":
                return {
                    "status": "error_validation",
                    "error": "campaign_goal is only valid for campaign_board entries",
                }
            try:
                cg = validate_campaign_goal(campaign_goal)
            except ValueError as e:
                return {"status": "error_validation", "error": str(e)}
        else:
            cg = None

        if eff_type == "campaign_board":
            if not eff_campaign:
                return {
                    "status": "error_validation",
                    "error": "campaign is required for campaign_board entries",
                }
            other = _other_campaign_board_id(con, final_project, eff_campaign, entry_id)
            if other is not None:
                return {
                    "status": "error_validation",
                    "error": "Another campaign board already uses this campaign",
                    "existing_id": other,
                }

        clear_board = (
            entry_type is not None
            and entry_type in ENTRY_TYPES
            and entry_type != "campaign_board"
            and old["entry_type"] == "campaign_board"
        )
        if clear_board and bj is not None:
            return {
                "status": "error_validation",
                "error": "cannot update board_json while changing type away from campaign_board",
            }
        if clear_board and cg is not None:
            return {
                "status": "error_validation",
                "error": "cannot update campaign_goal while changing type away from campaign_board",
            }

        now = _now_iso()
        sets, params = ["edited_at = ?"], [now]
        if title is not None:
            sets.append("title = ?")
            params.append(title)
        if body is not None:
            sets.append("body = ?")
            params.append(body)
        if entry_type is not None and entry_type in ENTRY_TYPES:
            sets.append("entry_type = ?")
            params.append(entry_type)
        if pinned is not None:
            sets.append("pinned = ?")
            params.append(1 if pinned else 0)
        if campaign is not None:
            sets.append("campaign = ?")
            params.append(campaign.strip().lower())
        if moved_to is not None:
            sets.append("project = ?")
            params.append(moved_to)
        if clear_board:
            sets.append("board_json = ?")
            params.append("")
            sets.append("campaign_goal = ?")
            params.append("")
        elif bj is not None:
            sets.append("board_json = ?")
            params.append(bj)
        if not clear_board and cg is not None:
            sets.append("campaign_goal = ?")
            params.append(cg)

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


def update_campaign_board_by_campaign(
    project, campaign, title=None, body=None, board_json=None, campaign_goal=None
):
    """Update the singleton campaign_board for ``project`` + ``campaign`` (by entry id)."""
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error_validation", "error": "campaign is required"}
    eid = find_campaign_board_entry_id(project, camp)
    if eid is None:
        return {"status": "not_found", "project": project, "campaign": camp}
    return update_entry(
        project, eid, title=title, body=body, board_json=board_json, campaign_goal=campaign_goal
    )


def find_campaign_board_entry_id(project, campaign):
    camp = (campaign or "").strip().lower()
    if not camp:
        return None
    con = get_db()
    row = con.execute(
        "SELECT id FROM logbook_entries WHERE project = ? AND campaign = ? AND entry_type = 'campaign_board'",
        (project, camp),
    ).fetchone()
    con.close()
    return int(row["id"]) if row else None


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
    query_text = query.strip()
    entry_id = _entry_id_from_query(query_text)
    if entry_id is not None:
        rows = _id_search(
            con,
            entry_id,
            project=project,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
        )
        con.close()
        return [_row_to_dict(r, preview=True) for r in rows]

    bare_entry_id = _bare_entry_id_from_query(query_text)
    fts_q = _fts_safe_query(query_text)
    conditions = ["f.logbook_fts MATCH ?"]
    params = [fts_q]

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

    try:
        rows = con.execute(
            f"""SELECT e.id, e.project, e.title, e.body, e.created_at, e.edited_at, e.entry_type, e.pinned, e.campaign, e.board_json, e.campaign_goal,
                       snippet(logbook_fts, 1, '{_SNIPPET_MARKER_L}', '{_SNIPPET_MARKER_R}', '…', 48) AS _snippet
                FROM logbook_entries e
                JOIN logbook_fts f ON e.id = f.rowid
                WHERE {where}
                ORDER BY rank
                LIMIT ?""",
            params,
        ).fetchall()
    except Exception:
        log.debug("FTS MATCH failed for %r in search_entries, returning empty", query)
        rows = []
    if bare_entry_id is not None:
        id_rows = _id_search(
            con,
            bare_entry_id,
            project=project,
            limit=1,
            date_from=date_from,
            date_to=date_to,
        )
        rows = _merge_id_first(id_rows, rows, limit, 0)
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
