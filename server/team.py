"""Team-roster and PPP-account CRUD.

Replaces ``team_members`` / ``ppps`` / ``ppp_accounts`` from the legacy
``config.json``. Two tables, two parallel CRUD APIs — kept in one
module because both are tiny and conceptually owned by the same
"team identity" concern.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Optional

from .db import db_write, get_db


_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9._-]*$")
_ACCOUNT_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


# ─── Team members ───────────────────────────────────────────────────────────

def _member_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "username": row["username"],
        "display_name": row["display_name"] or "",
        "email": row["email"] or "",
        "notes": row["notes"] or "",
        "position": int(row["position"]),
        "created_at": row["created_at"],
    }


def list_team_members() -> List[Dict[str, Any]]:
    """Return every team member ordered by ``position`` then username."""
    con = get_db()
    rows = con.execute(
        "SELECT * FROM team_members ORDER BY position, username"
    ).fetchall()
    return [_member_row(r) for r in rows]


def list_team_usernames() -> List[str]:
    """Return just usernames — cheaper than :func:`list_team_members` for
    the common case where the caller only needs the membership set."""
    con = get_db()
    rows = con.execute(
        "SELECT username FROM team_members ORDER BY position, username"
    ).fetchall()
    return [r["username"] for r in rows]


def get_team_member(username: str) -> Optional[Dict[str, Any]]:
    if not username:
        return None
    con = get_db()
    row = con.execute(
        "SELECT * FROM team_members WHERE username=?", (username,)
    ).fetchone()
    return _member_row(row) if row else None


def add_team_member(
    username: str,
    *,
    display_name: str = "",
    email: str = "",
    notes: str = "",
    position: Optional[int] = None,
) -> Dict[str, Any]:
    """Insert a new team member.

    Usernames are case-sensitive (Slurm/Linux usernames typically are).
    Auto-appends to the end of the list when ``position`` is omitted.
    """
    username = (username or "").strip()
    if not username:
        return {"status": "error", "error": "username is required"}
    if not _USERNAME_RE.match(username):
        return {
            "status": "error",
            "error": "username must start with a letter and contain only letters, digits, dots, underscores, hyphens",
        }

    if position is None:
        con = get_db()
        row = con.execute("SELECT COALESCE(MAX(position), -1) AS m FROM team_members").fetchone()
        position = (row["m"] if row else -1) + 1

    try:
        with db_write() as con:
            con.execute(
                "INSERT INTO team_members (username, display_name, email, notes, position) "
                "VALUES (?,?,?,?,?)",
                (username, display_name or "", email or "", notes or "", int(position)),
            )
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"team member {username!r} already exists"}

    return {"status": "ok", "member": get_team_member(username)}


def update_team_member(
    username: str,
    *,
    display_name: Optional[str] = None,
    email: Optional[str] = None,
    notes: Optional[str] = None,
    position: Optional[int] = None,
) -> Dict[str, Any]:
    """Update mutable fields on an existing member. Username itself is the
    PK and cannot be changed (delete + re-add to rename)."""
    existing = get_team_member(username)
    if existing is None:
        return {"status": "error", "error": f"team member {username!r} not found"}

    cols, vals = [], []
    if display_name is not None:
        cols.append("display_name=?"); vals.append(display_name)
    if email is not None:
        cols.append("email=?"); vals.append(email)
    if notes is not None:
        cols.append("notes=?"); vals.append(notes)
    if position is not None:
        cols.append("position=?"); vals.append(int(position))
    if not cols:
        return {"status": "ok", "member": existing}
    vals.append(username)
    with db_write() as con:
        con.execute(f"UPDATE team_members SET {', '.join(cols)} WHERE username=?", vals)
    return {"status": "ok", "member": get_team_member(username)}


def remove_team_member(username: str) -> Dict[str, Any]:
    if get_team_member(username) is None:
        return {"status": "error", "error": f"team member {username!r} not found"}
    with db_write() as con:
        con.execute("DELETE FROM team_members WHERE username=?", (username,))
    return {"status": "ok", "removed": username}


def reorder_team_members(usernames: List[str]) -> Dict[str, Any]:
    """Persist a new display order (drag-and-drop in the UI)."""
    if not isinstance(usernames, list):
        return {"status": "error", "error": "usernames must be a list"}
    existing = {m["username"] for m in list_team_members()}
    unknown = [u for u in usernames if u not in existing]
    if unknown:
        return {"status": "error", "error": f"unknown team members: {unknown}"}
    seen = set(usernames)
    tail = [u for u in existing if u not in seen]
    ordered = list(usernames) + tail
    with db_write() as con:
        con.executemany(
            "UPDATE team_members SET position=? WHERE username=?",
            [(i, u) for i, u in enumerate(ordered)],
        )
    return {"status": "ok", "order": ordered}


# ─── PPP accounts ───────────────────────────────────────────────────────────

def _account_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "name": row["name"],
        "ppp_id": row["ppp_id"] or "",
        "description": row["description"] or "",
        "position": int(row["position"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def list_ppp_accounts() -> List[Dict[str, Any]]:
    """Return every PPP account ordered by ``position`` then name."""
    con = get_db()
    rows = con.execute(
        "SELECT * FROM ppp_accounts ORDER BY position, name"
    ).fetchall()
    return [_account_row(r) for r in rows]


def list_ppp_account_names() -> List[str]:
    con = get_db()
    rows = con.execute(
        "SELECT name FROM ppp_accounts ORDER BY position, name"
    ).fetchall()
    return [r["name"] for r in rows]


def ppp_id_map() -> Dict[str, str]:
    """Return ``{account_name: ppp_id}`` for accounts with a non-empty id.

    Drop-in replacement for the legacy ``PPPS`` global. Account names
    without an id are intentionally dropped — the consumer code always
    treated them as "missing" anyway.
    """
    return {a["name"]: a["ppp_id"] for a in list_ppp_accounts() if a["ppp_id"]}


def get_ppp_account(name: str) -> Optional[Dict[str, Any]]:
    if not name:
        return None
    con = get_db()
    row = con.execute("SELECT * FROM ppp_accounts WHERE name=?", (name,)).fetchone()
    return _account_row(row) if row else None


def add_ppp_account(
    name: str,
    *,
    ppp_id: str = "",
    description: str = "",
    position: Optional[int] = None,
) -> Dict[str, Any]:
    """Insert a new PPP account. ``ppp_id`` is optional but recommended —
    AI Hub queries that need the numeric id will skip accounts without
    one."""
    name = (name or "").strip()
    if not name:
        return {"status": "error", "error": "name is required"}
    if not _ACCOUNT_RE.match(name):
        return {
            "status": "error",
            "error": "name must start with a letter and contain only letters, digits, underscores, hyphens",
        }

    if position is None:
        con = get_db()
        row = con.execute("SELECT COALESCE(MAX(position), -1) AS m FROM ppp_accounts").fetchone()
        position = (row["m"] if row else -1) + 1

    try:
        with db_write() as con:
            con.execute(
                "INSERT INTO ppp_accounts (name, ppp_id, description, position) "
                "VALUES (?,?,?,?)",
                (name, str(ppp_id or ""), description or "", int(position)),
            )
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"PPP account {name!r} already exists"}

    return {"status": "ok", "account": get_ppp_account(name)}


def update_ppp_account(
    name: str,
    *,
    ppp_id: Optional[str] = None,
    description: Optional[str] = None,
    position: Optional[int] = None,
) -> Dict[str, Any]:
    existing = get_ppp_account(name)
    if existing is None:
        return {"status": "error", "error": f"PPP account {name!r} not found"}

    cols, vals = [], []
    if ppp_id is not None:
        cols.append("ppp_id=?"); vals.append(str(ppp_id))
    if description is not None:
        cols.append("description=?"); vals.append(description)
    if position is not None:
        cols.append("position=?"); vals.append(int(position))
    if not cols:
        return {"status": "ok", "account": existing}
    cols.append("updated_at=datetime('now')")
    vals.append(name)
    with db_write() as con:
        con.execute(f"UPDATE ppp_accounts SET {', '.join(cols)} WHERE name=?", vals)
    return {"status": "ok", "account": get_ppp_account(name)}


def remove_ppp_account(name: str) -> Dict[str, Any]:
    if get_ppp_account(name) is None:
        return {"status": "error", "error": f"PPP account {name!r} not found"}
    with db_write() as con:
        con.execute("DELETE FROM ppp_accounts WHERE name=?", (name,))
    return {"status": "ok", "removed": name}


def reorder_ppp_accounts(names: List[str]) -> Dict[str, Any]:
    if not isinstance(names, list):
        return {"status": "error", "error": "names must be a list"}
    existing = {a["name"] for a in list_ppp_accounts()}
    unknown = [n for n in names if n not in existing]
    if unknown:
        return {"status": "error", "error": f"unknown PPP accounts: {unknown}"}
    seen = set(names)
    tail = [n for n in existing if n not in seen]
    ordered = list(names) + tail
    with db_write() as con:
        con.executemany(
            "UPDATE ppp_accounts SET position=? WHERE name=?",
            [(i, n) for i, n in enumerate(ordered)],
        )
    return {"status": "ok", "order": ordered}
