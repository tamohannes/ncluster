"""Per-job persistent state for the WasteWatcher.

Lives in the ``waste_watcher_state`` table (one row per ``(cluster, job_id)``).
The watcher loop maintains a small adaptive-sampling state machine here so
that crashed/restarted gunicorn processes resume mid-flight without losing
the "this job has been idle for 12 minutes" context.

State machine:
  ``cold``       - newly seen RUNNING GPU job, polled every ``cold_probe_sec``
  ``warm``       - has shown healthy GPU usage, polled every ``warm_probe_sec``
  ``suspicious`` - went idle while running, polled every ``suspicious_probe_sec``
  ``wasteful``   - confirmed wasted (queued for verification + cancel/flag)
  ``exempt``     - user-pinned or matched ``exempt_name_regex``; skipped entirely

This module is intentionally thin: only DB CRUD + a dataclass. All control
flow lives in ``waste_watcher.py``; all detection logic in
``waste_watcher_rules.py``. That separation keeps the state layer trivial
to unit-test in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

from .db import db_write, get_db


# ─── State constants ────────────────────────────────────────────────────────

STATE_COLD = "cold"
STATE_WARM = "warm"
STATE_SUSPICIOUS = "suspicious"
STATE_WASTEFUL = "wasteful"
STATE_EXEMPT = "exempt"

ALL_STATES = (STATE_COLD, STATE_WARM, STATE_SUSPICIOUS, STATE_WASTEFUL, STATE_EXEMPT)


def _now() -> datetime:
    """Wallclock now (UTC-naive); centralised so tests can monkeypatch."""
    return datetime.now()


def _iso(dt: Optional[datetime]) -> str:
    """ISO-8601 string for ``dt``, or empty string if ``None``."""
    return dt.isoformat(timespec="seconds") if dt else ""


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Inverse of :func:`_iso`. Returns ``None`` on empty/garbage."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


# ─── State row ──────────────────────────────────────────────────────────────

@dataclass
class WatcherState:
    """In-memory mirror of one ``waste_watcher_state`` row."""

    cluster: str
    job_id: str
    state: str = STATE_COLD
    state_entered_at: datetime = field(default_factory=_now)
    last_probe_at: Optional[datetime] = None
    next_probe_due: datetime = field(default_factory=_now)
    consecutive_zero_util_samples: int = 0
    last_log_hash: str = ""
    last_log_change_at: Optional[datetime] = None
    last_sdk_heartbeat_at: Optional[datetime] = None
    suspected_reason: str = ""
    suspected_confidence: str = ""
    exempt_until: Optional[datetime] = None
    last_notes: str = ""

    def is_exempt_now(self) -> bool:
        """True when the exempt-until window is still active."""
        return self.exempt_until is not None and self.exempt_until > _now()

    def time_in_state(self) -> timedelta:
        """How long since the row entered its current state."""
        return _now() - self.state_entered_at


# ─── CRUD ────────────────────────────────────────────────────────────────────

def _row_to_state(row) -> WatcherState:
    return WatcherState(
        cluster=row["cluster"],
        job_id=row["job_id"],
        state=row["state"],
        state_entered_at=_parse_iso(row["state_entered_at"]) or _now(),
        last_probe_at=_parse_iso(row["last_probe_at"]),
        next_probe_due=_parse_iso(row["next_probe_due"]) or _now(),
        consecutive_zero_util_samples=int(row["consecutive_zero_util_samples"] or 0),
        last_log_hash=row["last_log_hash"] or "",
        last_log_change_at=_parse_iso(row["last_log_change_at"]),
        last_sdk_heartbeat_at=_parse_iso(row["last_sdk_heartbeat_at"]),
        suspected_reason=row["suspected_reason"] or "",
        suspected_confidence=row["suspected_confidence"] or "",
        exempt_until=_parse_iso(row["exempt_until"]),
        last_notes=row["last_notes"] or "",
    )


def load_state(cluster: str, job_id: str) -> Optional[WatcherState]:
    """Return the persisted state for one job, or ``None`` if absent."""
    con = get_db()
    row = con.execute(
        "SELECT * FROM waste_watcher_state WHERE cluster=? AND job_id=?",
        (cluster, str(job_id)),
    ).fetchone()
    con.close()
    return _row_to_state(row) if row else None


def load_states_by_state(state: str) -> List[WatcherState]:
    """Return every row currently in the given ``state`` (e.g. all suspicious)."""
    con = get_db()
    rows = con.execute(
        "SELECT * FROM waste_watcher_state WHERE state=?", (state,),
    ).fetchall()
    con.close()
    return [_row_to_state(r) for r in rows]


def load_states_due(limit: int = 500) -> List[WatcherState]:
    """Return jobs whose ``next_probe_due`` has elapsed (oldest first).

    Used by the watcher loop to find work without scanning every row.
    """
    now_iso = _iso(_now())
    con = get_db()
    rows = con.execute(
        "SELECT * FROM waste_watcher_state WHERE next_probe_due <= ? "
        "ORDER BY next_probe_due ASC LIMIT ?",
        (now_iso, int(limit)),
    ).fetchall()
    con.close()
    return [_row_to_state(r) for r in rows]


def upsert_state(state: WatcherState) -> None:
    """Insert or update a state row in one statement."""
    with db_write() as con:
        con.execute(
            """
            INSERT INTO waste_watcher_state (
                cluster, job_id, state, state_entered_at, last_probe_at,
                next_probe_due, consecutive_zero_util_samples,
                last_log_hash, last_log_change_at, last_sdk_heartbeat_at,
                suspected_reason, suspected_confidence, exempt_until,
                last_notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(cluster, job_id) DO UPDATE SET
                state                         = excluded.state,
                state_entered_at              = excluded.state_entered_at,
                last_probe_at                 = excluded.last_probe_at,
                next_probe_due                = excluded.next_probe_due,
                consecutive_zero_util_samples = excluded.consecutive_zero_util_samples,
                last_log_hash                 = excluded.last_log_hash,
                last_log_change_at            = excluded.last_log_change_at,
                last_sdk_heartbeat_at         = excluded.last_sdk_heartbeat_at,
                suspected_reason              = excluded.suspected_reason,
                suspected_confidence          = excluded.suspected_confidence,
                exempt_until                  = excluded.exempt_until,
                last_notes                    = excluded.last_notes,
                updated_at                    = datetime('now')
            """,
            (
                state.cluster, str(state.job_id), state.state,
                _iso(state.state_entered_at), _iso(state.last_probe_at),
                _iso(state.next_probe_due), int(state.consecutive_zero_util_samples),
                state.last_log_hash, _iso(state.last_log_change_at),
                _iso(state.last_sdk_heartbeat_at),
                state.suspected_reason, state.suspected_confidence,
                _iso(state.exempt_until), state.last_notes,
            ),
        )


def delete_state(cluster: str, job_id: str) -> None:
    """Hard-delete one state row (used when a job leaves the live board)."""
    with db_write() as con:
        con.execute(
            "DELETE FROM waste_watcher_state WHERE cluster=? AND job_id=?",
            (cluster, str(job_id)),
        )


def prune_terminal_states(known_live: Iterable[tuple[str, str]]) -> int:
    """Delete state rows for jobs no longer present in the live view.

    ``known_live`` is an iterable of ``(cluster, job_id)`` tuples. Any state
    row not in that set is removed (the job has reached a terminal state
    and we no longer need to track its sampling cadence). Returns the
    number of rows deleted.

    The poller is the source of truth for what's RUNNING; we lazy-prune
    here rather than maintain a parallel job-tracker.
    """
    keep = {(c, str(j)) for c, j in known_live}
    con = get_db()
    rows = con.execute(
        "SELECT cluster, job_id FROM waste_watcher_state",
    ).fetchall()
    con.close()
    stale = [(r["cluster"], r["job_id"]) for r in rows if (r["cluster"], r["job_id"]) not in keep]
    if not stale:
        return 0
    with db_write() as con:
        con.executemany(
            "DELETE FROM waste_watcher_state WHERE cluster=? AND job_id=?",
            stale,
        )
    return len(stale)


def list_candidates(min_states: Iterable[str] = (STATE_SUSPICIOUS, STATE_WASTEFUL)) -> List[WatcherState]:
    """Return all rows in any of ``min_states`` (for the candidates API)."""
    placeholders = ",".join("?" * len(tuple(min_states)))
    states_tuple = tuple(min_states)
    if not states_tuple:
        return []
    con = get_db()
    rows = con.execute(
        f"SELECT * FROM waste_watcher_state WHERE state IN ({placeholders}) "
        f"ORDER BY state_entered_at ASC",
        states_tuple,
    ).fetchall()
    con.close()
    return [_row_to_state(r) for r in rows]


# ─── Convenience helpers used by the loop ───────────────────────────────────

def set_exempt(cluster: str, job_id: str, duration_min: int, note: str = "") -> WatcherState:
    """Mark a job exempt for the next ``duration_min`` minutes.

    Used by ``POST /api/waste/exempt_job`` (manual override after a false
    positive) and internally when verification demotes a candidate back to
    warm (with a short cooldown so we don't immediately re-flag it).
    """
    until = _now() + timedelta(minutes=max(1, int(duration_min)))
    state = load_state(cluster, str(job_id)) or WatcherState(cluster=cluster, job_id=str(job_id))
    state.state = STATE_EXEMPT
    state.state_entered_at = _now()
    state.next_probe_due = until
    state.exempt_until = until
    state.suspected_reason = ""
    state.suspected_confidence = ""
    state.last_notes = note or state.last_notes
    upsert_state(state)
    return state
