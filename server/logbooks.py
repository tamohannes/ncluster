"""SQLite+FTS5-backed logbook with structured entries and BM25 search.

Each entry has: project, title, body (markdown), entry_type, created_at, edited_at.
entry_type is "note", "plan", "campaign_board" (legacy; singleton per project+campaign;
static tables in board_json JSON), or "mind_map" (singleton per project+campaign;
static DAG of tasks/experiments/bugs/decisions stored in graph_json JSON).
Optional ``campaign_goal`` (short prose) is stored for ``campaign_board`` and
``mind_map`` rows. Full-text search via FTS5 with porter stemming and BM25 ranking.
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

log = logging.getLogger(__name__)

BODY_PREVIEW_LEN = 200
_CAMPAIGN_PREFIX_RE = re.compile(r'^\[([^\]]+)\]\s*')
_ENTRY_ID_QUERY_RE = re.compile(r'^\s*(?:#|id:)\s*(\d+)\s*$', re.IGNORECASE)
_BARE_ENTRY_ID_QUERY_RE = re.compile(r'^\s*(\d+)\s*$')
_LEGACY_DIR = os.path.join(PROJECT_ROOT, "data", "logbooks")
IMAGES_DIR = os.path.join(PROJECT_ROOT, "data", "logbook_images")
ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".html", ".htm"}

ENTRY_TYPES = ("note", "plan", "campaign_board", "mind_map")
_LOGBOOK_ROW_SELECT = (
    "id, project, title, body, created_at, edited_at, entry_type, pinned, "
    "campaign, board_json, campaign_goal, graph_json"
)
BOARD_JSON_MAX_BYTES = 512 * 1024
BOARD_MAX_SECTIONS = 48
BOARD_MAX_COLS = 64
BOARD_MAX_ROWS_PER_SECTION = 2000
CAMPAIGN_GOAL_MAX_CHARS = 8000
BOARD_COLUMN_TYPES = frozenset({"string"})
BOARD_SECTION_TYPES = frozenset({"table"})
_COL_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")

GRAPH_JSON_MAX_BYTES = 512 * 1024
GRAPH_MAX_NODES = 500
GRAPH_MAX_EDGES = 2000
GRAPH_NODE_TITLE_MAX = 240
GRAPH_NODE_SUMMARY_MAX = 400
GRAPH_NODE_DESCRIPTION_MAX = 32 * 1024
GRAPH_EDGE_LABEL_MAX = 120
GRAPH_NODE_STATUSES = frozenset({
    "planned", "active", "blocked", "done", "failed", "abandoned"
})
GRAPH_EDGE_KINDS = frozenset({
    "default", "success", "failure", "branch", "blocker", "verification",
})
_NODE_ID_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_\-]{0,63}$")
_EDGE_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,63}$")


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
    """Classic board section: static columns and rows of string cells."""
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
                "(only static string columns are supported)"
            )
        norm_cols.append({"id": cid, "label": lab})

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
        norm_rows.append({"cells": norm_cells})
    return {"title": title, "columns": norm_cols, "rows": norm_rows}


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
                "(only static table sections are supported)"
            )
        out_sections.append(_normalize_table_section(si, sec))

    normalized = {"version": 1, "sections": out_sections}
    blob = json.dumps(normalized, separators=(",", ":"))
    if len(blob.encode("utf-8")) > BOARD_JSON_MAX_BYTES:
        raise ValueError("board_json is too large")
    return blob


def _default_graph_json():
    return json.dumps({"version": 1, "nodes": [], "edges": []}, separators=(",", ":"))


def _validate_graph_node(ni: int, node, seen_ids: set):
    """Normalize a single mind_map node. Raises ValueError on bad input."""
    if not isinstance(node, dict):
        raise ValueError(f"node {ni} must be an object")
    nid = node.get("id")
    if not isinstance(nid, str) or not _NODE_ID_RE.match(nid):
        raise ValueError(
            f"node {ni} needs a valid id "
            "(start with letter, alphanumeric/underscore/hyphen, max 64 chars)"
        )
    if nid in seen_ids:
        raise ValueError(f"duplicate node id {nid!r}")
    seen_ids.add(nid)
    title = node.get("title", "")
    if title is None:
        title = ""
    if not isinstance(title, str):
        raise ValueError(f"node {nid!r} title must be a string")
    title = title.strip()
    if not title:
        raise ValueError(f"node {nid!r} title is required")
    if len(title) > GRAPH_NODE_TITLE_MAX:
        raise ValueError(
            f"node {nid!r} title must be at most {GRAPH_NODE_TITLE_MAX} chars"
        )
    status = node.get("status", "planned")
    if status is None or status == "":
        status = "planned"
    if not isinstance(status, str):
        raise ValueError(f"node {nid!r} status must be a string")
    status = status.strip().lower()
    if status not in GRAPH_NODE_STATUSES:
        allowed = ", ".join(sorted(GRAPH_NODE_STATUSES))
        raise ValueError(
            f"node {nid!r} has unknown status {status!r} (allowed: {allowed})"
        )
    summary = node.get("summary", "")
    if summary is None:
        summary = ""
    if not isinstance(summary, str):
        raise ValueError(f"node {nid!r} summary must be a string")
    summary = summary.strip()
    if len(summary) > GRAPH_NODE_SUMMARY_MAX:
        raise ValueError(
            f"node {nid!r} summary must be at most {GRAPH_NODE_SUMMARY_MAX} chars"
        )
    description = node.get("description", "")
    if description is None:
        description = ""
    if not isinstance(description, str):
        raise ValueError(f"node {nid!r} description must be a string")
    if len(description) > GRAPH_NODE_DESCRIPTION_MAX:
        raise ValueError(
            f"node {nid!r} description must be at most {GRAPH_NODE_DESCRIPTION_MAX} chars"
        )
    out = {"id": nid, "title": title, "status": status}
    if summary:
        out["summary"] = summary
    if description:
        out["description"] = description
    return out


def _validate_graph_edge(ei: int, edge, node_ids: set, seen_edge_ids: set):
    """Normalize a single mind_map edge. Raises ValueError on bad input."""
    if not isinstance(edge, dict):
        raise ValueError(f"edge {ei} must be an object")
    eid = edge.get("id")
    if eid is None or eid == "":
        eid = f"e{ei + 1}"
    if not isinstance(eid, str) or not _EDGE_ID_RE.match(eid):
        raise ValueError(
            f"edge {ei} needs a valid id "
            "(alphanumeric/underscore/hyphen, max 64 chars)"
        )
    if eid in seen_edge_ids:
        raise ValueError(f"duplicate edge id {eid!r}")
    seen_edge_ids.add(eid)
    src = edge.get("from")
    dst = edge.get("to")
    if not isinstance(src, str) or src not in node_ids:
        raise ValueError(f"edge {eid!r} 'from' must reference an existing node id")
    if not isinstance(dst, str) or dst not in node_ids:
        raise ValueError(f"edge {eid!r} 'to' must reference an existing node id")
    if src == dst:
        raise ValueError(f"edge {eid!r} cannot connect a node to itself")
    kind = edge.get("kind", "default")
    if kind is None or kind == "":
        kind = "default"
    if not isinstance(kind, str):
        raise ValueError(f"edge {eid!r} kind must be a string")
    kind = kind.strip().lower()
    if kind not in GRAPH_EDGE_KINDS:
        allowed = ", ".join(sorted(GRAPH_EDGE_KINDS))
        raise ValueError(
            f"edge {eid!r} has unknown kind {kind!r} (allowed: {allowed})"
        )
    label = edge.get("label", "")
    if label is None:
        label = ""
    if not isinstance(label, str):
        raise ValueError(f"edge {eid!r} label must be a string")
    label = label.strip()
    if len(label) > GRAPH_EDGE_LABEL_MAX:
        raise ValueError(
            f"edge {eid!r} label must be at most {GRAPH_EDGE_LABEL_MAX} chars"
        )
    out = {"id": eid, "from": src, "to": dst, "kind": kind}
    if label:
        out["label"] = label
    return out


def validate_graph_json(raw):
    """Normalize and validate a mind_map ``graph_json`` payload.

    Returns a compact JSON string ready for storage. Empty / falsy input
    yields the default empty graph. Raises ``ValueError`` with a short
    user-facing message on invalid input.
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return _default_graph_json()
    if isinstance(raw, (bytes, bytearray)):
        raise ValueError("graph_json must be JSON text or a dict")
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"graph_json is not valid JSON: {e}") from e
    elif isinstance(raw, dict):
        data = raw
    else:
        raise ValueError("graph_json must be a dict or JSON string")

    if not isinstance(data, dict):
        raise ValueError("graph_json root must be an object")
    ver = data.get("version", 1)
    if ver != 1:
        raise ValueError("graph_json version must be 1")

    nodes_raw = data.get("nodes")
    if nodes_raw is None:
        nodes_raw = []
    if not isinstance(nodes_raw, list):
        raise ValueError("graph_json.nodes must be a list")
    if len(nodes_raw) > GRAPH_MAX_NODES:
        raise ValueError(f"graph_json allows at most {GRAPH_MAX_NODES} nodes")

    edges_raw = data.get("edges")
    if edges_raw is None:
        edges_raw = []
    if not isinstance(edges_raw, list):
        raise ValueError("graph_json.edges must be a list")
    if len(edges_raw) > GRAPH_MAX_EDGES:
        raise ValueError(f"graph_json allows at most {GRAPH_MAX_EDGES} edges")

    seen_node_ids: set = set()
    out_nodes = []
    for ni, n in enumerate(nodes_raw):
        out_nodes.append(_validate_graph_node(ni, n, seen_node_ids))

    seen_edge_ids: set = set()
    out_edges = []
    for ei, e in enumerate(edges_raw):
        out_edges.append(_validate_graph_edge(ei, e, seen_node_ids, seen_edge_ids))

    normalized = {"version": 1, "nodes": out_nodes, "edges": out_edges}
    blob = json.dumps(normalized, separators=(",", ":"))
    if len(blob.encode("utf-8")) > GRAPH_JSON_MAX_BYTES:
        raise ValueError("graph_json is too large")
    return blob


_PATCH_OPS_ALLOWED = frozenset({
    "add_node", "update_node", "remove_node",
    "add_edge", "update_edge", "remove_edge",
    "set_status",
})


def apply_graph_patch_ops(current_graph_json: str, ops):
    """Apply structured patch ops to a stored ``graph_json`` blob.

    Returns the new compact JSON string after applying every op in order
    and revalidating. Raises ``ValueError`` on bad ops, unknown ids, or
    invalid resulting graph state.
    """
    if not isinstance(ops, list):
        raise ValueError("ops must be a list of patch operations")
    if not ops:
        raise ValueError("ops must contain at least one operation")

    blob = current_graph_json or _default_graph_json()
    try:
        data = json.loads(blob) if isinstance(blob, str) else blob
    except json.JSONDecodeError as e:
        raise ValueError(f"stored graph_json is not valid JSON: {e}") from e
    if not isinstance(data, dict):
        data = {"version": 1, "nodes": [], "edges": []}
    nodes = list(data.get("nodes") or [])
    edges = list(data.get("edges") or [])
    by_id = {n["id"]: n for n in nodes if isinstance(n, dict) and "id" in n}
    edges_by_id = {e["id"]: e for e in edges if isinstance(e, dict) and "id" in e}

    for oi, op in enumerate(ops):
        if not isinstance(op, dict):
            raise ValueError(f"op {oi} must be an object")
        kind = op.get("op")
        if kind not in _PATCH_OPS_ALLOWED:
            allowed = ", ".join(sorted(_PATCH_OPS_ALLOWED))
            raise ValueError(f"op {oi} has unknown op {kind!r} (allowed: {allowed})")

        if kind == "add_node":
            node = op.get("node")
            if not isinstance(node, dict):
                raise ValueError(f"op {oi} add_node requires a node object")
            nid = node.get("id")
            if not isinstance(nid, str) or not nid:
                raise ValueError(f"op {oi} add_node.node.id is required")
            if nid in by_id:
                raise ValueError(f"op {oi} add_node: node {nid!r} already exists")
            nodes.append(node)
            by_id[nid] = node

        elif kind == "update_node":
            nid = op.get("id")
            if not isinstance(nid, str) or nid not in by_id:
                raise ValueError(f"op {oi} update_node: unknown node id {nid!r}")
            patch = op.get("patch")
            if not isinstance(patch, dict):
                raise ValueError(f"op {oi} update_node requires a patch object")
            if "id" in patch and patch["id"] != nid:
                raise ValueError(f"op {oi} update_node: cannot rename node via patch")
            target = by_id[nid]
            target.update({k: v for k, v in patch.items() if k != "id"})

        elif kind == "remove_node":
            nid = op.get("id")
            if not isinstance(nid, str) or nid not in by_id:
                raise ValueError(f"op {oi} remove_node: unknown node id {nid!r}")
            del by_id[nid]
            nodes = [n for n in nodes if n.get("id") != nid]
            kept_edges = []
            for e in edges:
                if e.get("from") == nid or e.get("to") == nid:
                    edges_by_id.pop(e.get("id"), None)
                else:
                    kept_edges.append(e)
            edges = kept_edges

        elif kind == "add_edge":
            edge = op.get("edge")
            if not isinstance(edge, dict):
                raise ValueError(f"op {oi} add_edge requires an edge object")
            eid = edge.get("id")
            if isinstance(eid, str) and eid:
                if eid in edges_by_id:
                    raise ValueError(f"op {oi} add_edge: edge {eid!r} already exists")
            edges.append(edge)
            if isinstance(eid, str) and eid:
                edges_by_id[eid] = edge

        elif kind == "update_edge":
            eid = op.get("id")
            if not isinstance(eid, str) or eid not in edges_by_id:
                raise ValueError(f"op {oi} update_edge: unknown edge id {eid!r}")
            patch = op.get("patch")
            if not isinstance(patch, dict):
                raise ValueError(f"op {oi} update_edge requires a patch object")
            if "id" in patch and patch["id"] != eid:
                raise ValueError(f"op {oi} update_edge: cannot rename edge via patch")
            target = edges_by_id[eid]
            target.update({k: v for k, v in patch.items() if k != "id"})

        elif kind == "remove_edge":
            eid = op.get("id")
            if not isinstance(eid, str) or eid not in edges_by_id:
                raise ValueError(f"op {oi} remove_edge: unknown edge id {eid!r}")
            del edges_by_id[eid]
            edges = [e for e in edges if e.get("id") != eid]

        elif kind == "set_status":
            nid = op.get("id")
            if not isinstance(nid, str) or nid not in by_id:
                raise ValueError(f"op {oi} set_status: unknown node id {nid!r}")
            new_status = op.get("status")
            if not isinstance(new_status, str):
                raise ValueError(f"op {oi} set_status requires a status string")
            by_id[nid]["status"] = new_status

    return validate_graph_json({"version": 1, "nodes": nodes, "edges": edges})


def _other_mind_map_id(con, project, campaign, exclude_id):
    r = con.execute(
        "SELECT id FROM logbook_entries WHERE project = ? AND campaign = ? "
        "AND entry_type = 'mind_map' AND id != ?",
        (project, campaign, exclude_id),
    ).fetchone()
    return int(r["id"]) if r else None


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


def get_mind_map(project, campaign):
    """Return the full mind_map entry for project+campaign or a not_found dict."""
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error", "error": "campaign is required"}
    con = get_db()
    row = con.execute(
        f"SELECT {_LOGBOOK_ROW_SELECT} FROM logbook_entries "
        "WHERE project = ? AND campaign = ? AND entry_type = 'mind_map'",
        (project, camp),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "not_found", "project": project, "campaign": camp}
    return _row_to_dict(row)


def list_mind_maps(project):
    """List all mind_map rows for a project (lightweight)."""
    con = get_db()
    rows = con.execute(
        "SELECT id, campaign, title, edited_at FROM logbook_entries "
        "WHERE project = ? AND entry_type = 'mind_map' ORDER BY edited_at DESC, campaign",
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


def find_mind_map_entry_id(project, campaign):
    camp = (campaign or "").strip().lower()
    if not camp:
        return None
    con = get_db()
    row = con.execute(
        "SELECT id FROM logbook_entries WHERE project = ? AND campaign = ? AND entry_type = 'mind_map'",
        (project, camp),
    ).fetchone()
    con.close()
    return int(row["id"]) if row else None


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
    if preview and "graph_json" in d:
        del d["graph_json"]
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
                       e.graph_json,
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
    project, title, body="", entry_type="note", campaign=None,
    board_json=None, campaign_goal=None, graph_json=None,
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
                "entry_type, campaign, board_json, campaign_goal, graph_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (project, title, body, now, now, entry_type, campaign, bj, cg, ""),
            )
            entry_id = cur.lastrowid
            _update_links(con, entry_id, body)
        return {"status": "ok", "id": entry_id, "created_at": now, "campaign": campaign}

    if entry_type == "mind_map":
        if not campaign:
            return {
                "status": "error_validation",
                "error": "campaign is required for mind_map entries",
            }
        try:
            gj = validate_graph_json(graph_json)
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
                "AND entry_type = 'mind_map'",
                (project, campaign),
            ).fetchone()
            if existed:
                return {
                    "status": "error_validation",
                    "error": "A mind map already exists for this campaign",
                    "existing_id": int(existed["id"]),
                }
            cur = con.execute(
                "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, "
                "entry_type, campaign, board_json, campaign_goal, graph_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (project, title, body, now, now, entry_type, campaign, "", cg, gj),
            )
            entry_id = cur.lastrowid
            _update_links(con, entry_id, body)
        return {"status": "ok", "id": entry_id, "created_at": now, "campaign": campaign}

    now = _now_iso()
    with db_write() as con:
        cur = con.execute(
            "INSERT INTO logbook_entries (project, title, body, created_at, edited_at, "
            "entry_type, campaign, board_json, campaign_goal, graph_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (project, title, body, now, now, entry_type, campaign, "", "", ""),
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
    graph_json=None,
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

        goal_supported_types = {"campaign_board", "mind_map"}

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

        if graph_json is not None:
            if eff_type != "mind_map" and old["entry_type"] != "mind_map":
                return {
                    "status": "error_validation",
                    "error": "graph_json is only valid for mind_map entries",
                }
            try:
                gj = validate_graph_json(graph_json)
            except ValueError as e:
                return {"status": "error_validation", "error": str(e)}
        else:
            gj = None

        if campaign_goal is not None:
            if (
                eff_type not in goal_supported_types
                and old["entry_type"] not in goal_supported_types
            ):
                return {
                    "status": "error_validation",
                    "error": "campaign_goal is only valid for campaign_board or mind_map entries",
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
        elif eff_type == "mind_map":
            if not eff_campaign:
                return {
                    "status": "error_validation",
                    "error": "campaign is required for mind_map entries",
                }
            other = _other_mind_map_id(con, final_project, eff_campaign, entry_id)
            if other is not None:
                return {
                    "status": "error_validation",
                    "error": "Another mind map already uses this campaign",
                    "existing_id": other,
                }

        clear_board = (
            entry_type is not None
            and entry_type in ENTRY_TYPES
            and entry_type != "campaign_board"
            and old["entry_type"] == "campaign_board"
        )
        clear_graph = (
            entry_type is not None
            and entry_type in ENTRY_TYPES
            and entry_type != "mind_map"
            and old["entry_type"] == "mind_map"
        )
        if clear_board and bj is not None:
            return {
                "status": "error_validation",
                "error": "cannot update board_json while changing type away from campaign_board",
            }
        if clear_graph and gj is not None:
            return {
                "status": "error_validation",
                "error": "cannot update graph_json while changing type away from mind_map",
            }
        clearing_goal_holder = (
            entry_type is not None
            and entry_type in ENTRY_TYPES
            and entry_type not in goal_supported_types
            and old["entry_type"] in goal_supported_types
        )
        if clearing_goal_holder and cg is not None:
            return {
                "status": "error_validation",
                "error": "cannot update campaign_goal while changing type away from campaign_board/mind_map",
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
        elif bj is not None:
            sets.append("board_json = ?")
            params.append(bj)
        if clear_graph:
            sets.append("graph_json = ?")
            params.append("")
        elif gj is not None:
            sets.append("graph_json = ?")
            params.append(gj)
        if clearing_goal_holder:
            sets.append("campaign_goal = ?")
            params.append("")
        elif cg is not None:
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


def update_mind_map_by_campaign(
    project, campaign, title=None, body=None, graph_json=None, campaign_goal=None
):
    """Update the singleton mind_map for ``project`` + ``campaign`` (by entry id)."""
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error_validation", "error": "campaign is required"}
    eid = find_mind_map_entry_id(project, camp)
    if eid is None:
        return {"status": "not_found", "project": project, "campaign": camp}
    return update_entry(
        project, eid, title=title, body=body, graph_json=graph_json, campaign_goal=campaign_goal
    )


def patch_mind_map_by_campaign(project, campaign, ops):
    """Apply structured patch ops to the singleton mind_map for ``project`` + ``campaign``."""
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error_validation", "error": "campaign is required"}
    eid = find_mind_map_entry_id(project, camp)
    if eid is None:
        return {"status": "not_found", "project": project, "campaign": camp}
    con = get_db()
    row = con.execute(
        "SELECT graph_json FROM logbook_entries WHERE id = ? AND project = ?",
        (eid, project),
    ).fetchone()
    con.close()
    if not row:
        return {"status": "not_found", "project": project, "campaign": camp}
    try:
        new_blob = apply_graph_patch_ops(row["graph_json"] or "", ops)
    except ValueError as e:
        return {"status": "error_validation", "error": str(e)}
    return update_entry(project, eid, graph_json=new_blob)


def convert_campaign_board_to_mind_map(project, campaign):
    """Create a mind_map seeded from an existing campaign_board's body + campaign_goal.

    Idempotent: returns ``{"status": "exists", "existing_id": …}`` if a
    mind_map already exists for that campaign, and ``{"status": "not_found"}``
    if there is no campaign_board to convert from. The original
    campaign_board is left untouched — the user is expected to migrate
    structured table content into the mind_map graph manually and then
    delete the legacy board.
    """
    camp = (campaign or "").strip().lower()
    if not camp:
        return {"status": "error_validation", "error": "campaign is required"}
    board = get_campaign_board(project, camp)
    if board.get("status") in ("not_found", "error"):
        return {"status": "not_found", "project": project, "campaign": camp}
    existing = find_mind_map_entry_id(project, camp)
    if existing is not None:
        return {
            "status": "exists",
            "existing_id": existing,
            "project": project,
            "campaign": camp,
        }
    title = (board.get("title") or "").strip() or f"Mind map: {camp}"
    if title.lower().startswith("campaign board"):
        title = title.replace("Campaign board", "Mind map", 1).replace(
            "campaign board", "mind map", 1
        )
    return create_entry(
        project,
        title,
        body=board.get("body") or "",
        entry_type="mind_map",
        campaign=camp,
        graph_json=None,
        campaign_goal=board.get("campaign_goal") or "",
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
                       e.graph_json,
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
