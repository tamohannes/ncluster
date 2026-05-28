"""WasteWatcher daemon: detect and (optionally) cancel wasted GPU compute.

Architecture
------------

This module owns the **gunicorn-only** background thread that periodically
walks every RUNNING GPU job, samples its utilisation at an adaptive
cadence (faster while cold/suspicious, slower while warm), runs the
detection rules from :mod:`server.waste_watcher_rules`, and — when a
high-confidence pattern fires AND ``waste_watcher_cancel_enabled`` is on
— issues a structured ``scancel`` plus an audit logbook entry.

Single-writer placement
~~~~~~~~~~~~~~~~~~~~~~~

WasteWatcher mutates the ``waste_watcher_state`` and ``job_history``
tables and calls ``scancel`` over SSH. Both of those are leader-only
operations; running them in both the gunicorn worker and the standalone
MCP process would lead to interleaved scancels and split-brain state.
Therefore the watcher is started from :func:`app._run_init` only — the
MCP follower-poller never starts it.

Phase rollout
~~~~~~~~~~~~~

``waste_watcher_cancel_enabled`` is the global auto-cancel switch; keep
``waste_watcher_cancel_disabled_clusters`` as the per-cluster kill switch
when a cluster or experiment pattern needs flag-only mode. The watcher
always verifies high-confidence candidates before issuing ``scancel``.

State machine
~~~~~~~~~~~~~

The per-job state machine lives in :mod:`server.waste_watcher_state` and
is updated here in :func:`_transition_after_probe`. Probe cadence is
keyed off the state:

    cold       → probe every ``waste_watcher_cold_probe_sec`` (default 60s)
    warm       → probe every ``waste_watcher_warm_probe_sec`` (default 900s)
    suspicious → probe every ``waste_watcher_suspicious_probe_sec`` (default 30s)
    wasteful   → no further probing; queued for verification + cancel
    exempt     → skipped entirely until ``exempt_until`` elapses

Verification
~~~~~~~~~~~~

Before any scancel, :func:`verify_wasteful` runs a 3-probe / 30s burst,
checks the log-tail hash for ongoing writes, and checks SDK heartbeat
freshness when available. If verification fails the job is demoted back
to ``warm`` with a short cooldown so we don't immediately re-flag it.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import List, Mapping, Optional, Sequence

from .db import (
    get_db,
    get_run_with_jobs,
    update_job_waste_fields,
    update_run_fields,
)
from .jobs import get_job_stats, get_stats_snapshots
from .settings import get_setting
from .ssh import cancel_jobs_with_report
from . import waste_watcher_rules as rules
from .waste_watcher_rules import (
    CONFIDENCE_HIGH,
    WASTE_REASON_DEAD_SERVER,
    Detection,
)
from .waste_watcher_state import (
    STATE_COLD,
    STATE_EXEMPT,
    STATE_SUSPICIOUS,
    STATE_WARM,
    STATE_WASTEFUL,
    WatcherState,
    load_state,
    prune_terminal_states,
    set_exempt,
    upsert_state,
)

log = logging.getLogger("server.waste_watcher")


# ─── Module-level shutdown coordination ─────────────────────────────────────

_shutdown_event = threading.Event()
_thread: Optional[threading.Thread] = None
# Per-cluster sleep windows used by the ``POST /api/waste/pause`` route.
_pause_until: dict[str, datetime] = {}
_pause_lock = threading.Lock()


def request_shutdown() -> None:
    """Signal the watcher loop to exit at the next tick (used by tests)."""
    _shutdown_event.set()


def is_paused(cluster: str) -> bool:
    """True when the cluster is in a manual pause window."""
    with _pause_lock:
        until = _pause_until.get(cluster) or _pause_until.get("__all__")
    return until is not None and until > datetime.now()


def pause(cluster: Optional[str], duration_min: int) -> dict:
    """Pause the watcher for ``duration_min`` minutes on one (or all) clusters.

    ``cluster=None`` pauses globally. Returns the resolved pause window.
    """
    until = datetime.now() + timedelta(minutes=max(1, int(duration_min)))
    key = cluster or "__all__"
    with _pause_lock:
        _pause_until[key] = until
    return {"cluster": key, "until": until.isoformat(timespec="seconds")}


def clear_pause(cluster: Optional[str]) -> None:
    """Drop a pause entry (called by tests/admin to resume early)."""
    key = cluster or "__all__"
    with _pause_lock:
        _pause_until.pop(key, None)


# ─── Live job iteration ─────────────────────────────────────────────────────

def _enumerate_live_gpu_jobs() -> List[dict]:
    """Return RUNNING jobs with GPU allocations across all clusters.

    Reads from the in-memory poller cache (``server.config._cache``) so
    we don't issue any SSH while building the work list — the cache is
    refreshed every 15s by the poller. Each job dict is annotated with
    ``cluster`` so downstream code doesn't have to track context.
    """
    from .config import CLUSTERS, _cache, _cache_lock

    jobs: List[dict] = []
    with _cache_lock:
        for cluster in CLUSTERS:
            if cluster == "local":
                continue
            data = _cache.get(cluster) or {}
            for job in (data.get("jobs") or []):
                if str(job.get("state", "")).upper() != "RUNNING":
                    continue
                if "gpu" not in str(job.get("gres", "")).lower():
                    continue
                jid = str(job.get("jobid") or job.get("job_id") or "")
                if not jid or jid.startswith("sdk-"):
                    continue
                jobs.append({**job, "cluster": cluster, "jobid": jid})
    return jobs


def _enumerate_all_jobs_for_cluster(cluster: str) -> List[dict]:
    """Return every cached job for ``cluster`` (RUNNING + PENDING + recent
    terminal) so detection rules can reason about dependents and
    parent/child relationships within the same run.
    """
    from .config import _cache, _cache_lock

    with _cache_lock:
        data = _cache.get(cluster) or {}
        return list(data.get("jobs") or [])


# ─── Sampling & log helpers ─────────────────────────────────────────────────

def _probe_job(cluster: str, job_id: str) -> Optional[dict]:
    """Run a one-shot GPU/CPU probe over SSH and persist a snapshot row.

    Uses the existing ``get_job_stats`` (which itself parallelises
    sstat + nvidia-smi over SSH). The snapshot is recorded via the
    public helper so the standard table-write throttle and DB locking
    apply. Returns the stats dict for the caller to inspect, or None
    when the probe failed (job vanished, SSH unreachable, etc.).
    """
    try:
        stats = get_job_stats(cluster, job_id)
    except Exception as exc:
        log.debug("waste-watcher probe %s/%s failed: %s", cluster, job_id, exc)
        return None
    if not stats or stats.get("status") != "ok":
        return None

    # Lazy import to avoid circular: jobs.py imports from us only inside
    # the future MCP wrappers, but we already import from it at module
    # level for ``get_job_stats``/``get_stats_snapshots``. The
    # ``_save_stats_snapshot`` helper writes to ``job_stats_snapshots``
    # and is the canonical way to bypass the global throttle interval
    # only when the caller has a reason to sample faster.
    from .jobs import _save_stats_snapshot

    try:
        _save_stats_snapshot(cluster, job_id, stats)
    except Exception as exc:
        log.debug("waste-watcher snapshot persist failed %s/%s: %s", cluster, job_id, exc)
    return stats


def _stats_show_idle(stats: Mapping, busy_threshold_pct: float) -> bool:
    """Translate a stats dict into a single 'is this idle?' bool."""
    gpus = stats.get("gpus") or []
    if not gpus:
        # No per-GPU rows; fall back to sstat average.
        try:
            avg = float(str(stats.get("gpuutil_ave", "")).rstrip("%").strip())
        except (ValueError, TypeError):
            return True
        return avg <= busy_threshold_pct
    for g in gpus:
        try:
            util = float(str(g.get("util", "0")).rstrip("%").strip())
        except (ValueError, TypeError):
            util = 0.0
        if util > busy_threshold_pct:
            return False
    return True


def _hash_log_tail(text: str) -> str:
    """Stable short hash of a log tail (used to detect 'log is still growing')."""
    if not text:
        return ""
    return hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:16]


def _read_log_tail(cluster: str, job_id: str, lines: int = 80) -> str:
    """Best-effort tail read for one job; returns '' when no log discoverable.

    Reuses the existing log-discovery helpers so we benefit from the
    mount-vs-SSH fallback they already implement.
    """
    from .logs import _db_log_context, fetch_log_tail, tail_local_file
    from .mounts import resolve_mounted_path

    db_path = _db_log_context(cluster, job_id).get("log_path", "")
    if not db_path:
        return ""
    db_path = db_path.replace("%j", str(job_id))

    try:
        mt = resolve_mounted_path(cluster, db_path, want_dir=False)
    except Exception:
        mt = None
    if mt:
        try:
            import os
            if os.path.isfile(mt):
                return tail_local_file(mt, lines=lines) or ""
        except Exception:
            pass
    try:
        return fetch_log_tail(cluster, db_path, lines=lines) or ""
    except Exception:
        return ""


def _sdk_heartbeat_age_min(job: Mapping) -> Optional[float]:
    """Minutes since the last SDK heartbeat for a job's run, or None.

    Returns None when the job is not SDK-tracked. We treat SDK telemetry
    freshness as positive evidence the job is alive even if the GPU is
    idle (e.g. a long sandbox tool-call). Reads ``sdk_events`` directly
    rather than going through any aggregator to keep the watcher loop
    fast.
    """
    run_uuid = (job.get("run_uuid") or "").strip()
    if not run_uuid:
        return None
    con = get_db()
    row = con.execute(
        "SELECT MAX(ts) AS last_ts FROM sdk_events WHERE run_uuid = ?",
        (run_uuid,),
    ).fetchone()
    con.close()
    last_ts = row["last_ts"] if row else None
    if not last_ts:
        return None
    try:
        # sdk_events.ts is a unix-epoch float
        age_sec = time.time() - float(last_ts)
    except (TypeError, ValueError):
        return None
    return max(0.0, age_sec / 60.0)


# ─── State-machine transitions ──────────────────────────────────────────────

def _next_probe_due(state: str, cfg: dict) -> datetime:
    """Compute the next probe timestamp for a state, applying cadence config."""
    cadence_map = {
        STATE_COLD: cfg["cold_probe_sec"],
        STATE_WARM: cfg["warm_probe_sec"],
        STATE_SUSPICIOUS: cfg["suspicious_probe_sec"],
        # Wasteful jobs don't get re-probed; cancellation flow takes over.
        STATE_WASTEFUL: cfg["suspicious_probe_sec"],
        STATE_EXEMPT: cfg["warm_probe_sec"],
    }
    return datetime.now() + timedelta(seconds=int(cadence_map[state]))


def _transition_after_probe(
    state: WatcherState, *,
    is_idle: bool,
    log_age_min: Optional[float],
    cfg: dict,
) -> WatcherState:
    """Update the per-job state machine given the latest probe outcome.

    Pure-ish: mutates and returns the same dataclass. No DB or network
    side-effects here — the caller persists the result and decides
    whether to fire detection rules.
    """
    now = datetime.now()
    state.last_probe_at = now

    if is_idle:
        state.consecutive_zero_util_samples += 1
    else:
        # Activity observed → reset streak and promote toward warm.
        state.consecutive_zero_util_samples = 0
        if state.state != STATE_WARM:
            state.state = STATE_WARM
            state.state_entered_at = now
            state.suspected_reason = ""
            state.suspected_confidence = ""
        state.next_probe_due = _next_probe_due(STATE_WARM, cfg)
        return state

    # Idle path: maybe escalate.
    if state.state == STATE_COLD:
        cold_min = (now - state.state_entered_at).total_seconds() / 60.0
        if cold_min >= cfg["cold_grace_min"]:
            state.state = STATE_SUSPICIOUS
            state.state_entered_at = now
    elif state.state == STATE_WARM:
        # The state's entered_at is the last time we observed activity.
        warm_idle_min = (now - state.state_entered_at).total_seconds() / 60.0
        if warm_idle_min >= cfg["warm_idle_min"]:
            state.state = STATE_SUSPICIOUS
            state.state_entered_at = now
    elif state.state == STATE_SUSPICIOUS:
        idle_min = (now - state.state_entered_at).total_seconds() / 60.0
        log_quiet_ok = (
            log_age_min is None or log_age_min >= cfg["log_quiet_min"]
        )
        if idle_min >= cfg["suspicious_confirm_min"] and log_quiet_ok:
            state.state = STATE_WASTEFUL
            state.state_entered_at = now

    state.next_probe_due = _next_probe_due(state.state, cfg)
    return state


# ─── Verification burst ─────────────────────────────────────────────────────

def verify_wasteful(
    *,
    cluster: str,
    job_id: str,
    reason: str,
    cfg: dict,
    sleep_sec: float = 10.0,
) -> tuple[bool, str]:
    """Confirm a candidate is genuinely idle before issuing scancel.

    Runs:
      1. Three GPU probes spaced ``sleep_sec`` apart; abort if *any* GPU
         shows utilisation > ``util_busy_threshold``.
      2. Log-tail hash check; abort if the tail has changed within
         ``log_quiet_min`` minutes.
      3. SDK heartbeat freshness (when SDK-tracked); abort if heartbeat
         is fresher than ``sdk_heartbeat_stale_min``.
      4. Reason-specific extra check for ``WASTE_REASON_DEAD_SERVER``
         (re-confirm parent exited before child started).

    Returns ``(verified, note)`` where ``note`` describes what aborted
    the cancel (used in the audit logbook entry).
    """
    busy_threshold = float(cfg["util_busy_threshold"])

    # 1. GPU probes
    for attempt in range(3):
        stats = _probe_job(cluster, job_id)
        if stats is None:
            return False, "probe_failed"
        if not _stats_show_idle(stats, busy_threshold):
            return False, f"gpu_now_busy_attempt_{attempt+1}"
        if attempt < 2:
            time.sleep(sleep_sec)

    # 2. Log tail freshness
    tail_now = _read_log_tail(cluster, job_id, lines=80)
    state = load_state(cluster, job_id)
    if state and state.last_log_hash and tail_now:
        new_hash = _hash_log_tail(tail_now)
        if new_hash != state.last_log_hash:
            return False, "log_still_growing"

    # 3. SDK heartbeat (best-effort; absence is not a veto)
    job_row = None
    try:
        con = get_db()
        job_row = con.execute(
            "SELECT * FROM job_history WHERE cluster=? AND job_id=?",
            (cluster, str(job_id)),
        ).fetchone()
        con.close()
    except Exception:
        pass
    if job_row:
        run_uuid = ""
        if job_row["run_id"]:
            con = get_db()
            run_row = con.execute(
                "SELECT run_uuid FROM runs WHERE id=?",
                (job_row["run_id"],),
            ).fetchone()
            con.close()
            run_uuid = (run_row["run_uuid"] or "") if run_row else ""
        if run_uuid:
            age = _sdk_heartbeat_age_min({"run_uuid": run_uuid})
            if age is not None and age < float(cfg["sdk_heartbeat_stale_min"]):
                return False, f"sdk_heartbeat_fresh_{age:.1f}min"

    # 4. Reason-specific
    if reason == WASTE_REASON_DEAD_SERVER and job_row and job_row["run_id"]:
        # Re-confirm at least one parent server is terminal and ended
        # before this job started.
        con = get_db()
        run_row = con.execute(
            "SELECT cluster, root_job_id FROM runs WHERE id=?",
            (job_row["run_id"],),
        ).fetchone()
        con.close()
        if run_row:
            run = get_run_with_jobs(run_row["cluster"], run_row["root_job_id"])
            if run:
                client = next(
                    (j for j in run["jobs"] if str(j.get("jobid", "")) == str(job_id)),
                    None,
                )
                if client and not rules.detect_dead_server_before_client(
                    job={**client, "cluster": cluster}, run_jobs=run["jobs"],
                ):
                    return False, "parent_recovered"

    return True, "confirmed"


# ─── Cancel + audit ─────────────────────────────────────────────────────────

def cancel_with_reason(
    *,
    cluster: str,
    job_ids: Sequence[str],
    reason: str,
    confidence: str,
    summary: str,
    evidence: Mapping,
    by_watcher: bool,
    audit_project: str,
) -> dict:
    """Issue scancel for ``job_ids`` and write the structured audit trail.

    Steps:
      1. Call :func:`server.ssh.cancel_jobs_with_report` (no-op for sdk-*
         job IDs which can't be scancelled anyway).
      2. For each cancelled job, stamp ``job_history.waste_reason`` and
         ``waste_cancelled_at``.
      3. Walk up to the parent run(s) and stamp
         ``runs.wasteful``, ``runs.waste_reason``,
         ``runs.waste_detected_at``, ``runs.waste_cancelled_by_watcher``.
      4. Append a structured logbook entry in the ``compute`` project
         (configurable via ``waste_watcher_audit_project``).

    Returns ``{"cancelled": [...], "errors": [...], "run_ids": [...]}``.
    """
    sanitized = [str(j) for j in job_ids if str(j).strip() and not str(j).startswith("sdk-")]
    cancel_result = {"cancelled_ids": [], "errors": []}
    if sanitized:
        try:
            cancel_result = cancel_jobs_with_report(cluster, sanitized, timeout_sec=20, chunk_size=25)
        except Exception as exc:
            log.exception("WasteWatcher cancel failed for %s/%s: %s", cluster, sanitized, exc)
            cancel_result = {"cancelled_ids": [], "errors": [
                {"job_id": j, "error": str(exc), "exit_code": None} for j in sanitized
            ]}

    now_iso = datetime.now().isoformat(timespec="seconds")
    affected_run_ids: set[int] = set()
    for jid in cancel_result["cancelled_ids"]:
        try:
            update_job_waste_fields(cluster, jid, waste_reason=reason, waste_cancelled_at=now_iso)
        except Exception as exc:
            log.warning("waste-watcher: failed to stamp job %s/%s: %s", cluster, jid, exc)
        # Look up run for the cancelled job
        try:
            con = get_db()
            row = con.execute(
                "SELECT run_id FROM job_history WHERE cluster=? AND job_id=?",
                (cluster, str(jid)),
            ).fetchone()
            con.close()
            if row and row["run_id"]:
                affected_run_ids.add(int(row["run_id"]))
        except Exception:
            pass

    for run_id in affected_run_ids:
        try:
            update_run_fields(
                run_id,
                wasteful=True,
                waste_reason=reason,
                waste_detected_at=now_iso,
                waste_cancelled_by_watcher=by_watcher,
            )
        except Exception as exc:
            log.warning("waste-watcher: failed to stamp run %s: %s", run_id, exc)

    # Build the audit logbook entry. Lazy import to avoid pulling the
    # whole logbook module on every import of waste_watcher.
    try:
        from .logbooks import create_entry

        body_lines = [
            f"**Cluster:** `{cluster}`",
            f"**Reason:** `{reason}` (confidence: `{confidence}`)",
            f"**Cancelled by watcher:** {'yes' if by_watcher else 'no'}",
            "",
            f"**Summary:** {summary}",
            "",
            "## Targets",
            "",
        ]
        for jid in sanitized:
            ok = jid in cancel_result["cancelled_ids"]
            body_lines.append(f"- `{jid}` — {'cancelled' if ok else 'cancel failed'}")
        if cancel_result["errors"]:
            body_lines.append("")
            body_lines.append("## Errors")
            body_lines.append("")
            for err in cancel_result["errors"]:
                body_lines.append(f"- `{err['job_id']}`: {err['error']}")

        body_lines.extend([
            "",
            "## Evidence",
            "",
            "```json",
            _safe_json(evidence),
            "```",
        ])
        title = f"WasteWatcher: {reason} on {cluster} ({len(cancel_result['cancelled_ids'])} cancelled)"
        create_entry(audit_project, title, body="\n".join(body_lines), entry_type="note")
    except Exception as exc:
        log.warning("waste-watcher: audit logbook entry failed: %s", exc)

    return {
        "cancelled": cancel_result["cancelled_ids"],
        "errors": cancel_result["errors"],
        "run_ids": sorted(affected_run_ids),
    }


def _safe_json(obj) -> str:
    """``json.dumps`` with a default that converts datetimes to ISO strings."""
    import json as _json

    def _default(value):
        if isinstance(value, datetime):
            return value.isoformat(timespec="seconds")
        return str(value)
    try:
        return _json.dumps(obj, indent=2, default=_default)
    except Exception:
        return str(obj)


def flag_only(
    *,
    cluster: str,
    job_ids: Sequence[str],
    reason: str,
    confidence: str,
    summary: str,
    evidence: Mapping,
    audit_project: str,
) -> dict:
    """Like :func:`cancel_with_reason` but never calls scancel.

    Used when ``cancel_enabled=False`` and for the ``manifest_only_failure``
    rule where nothing is left to cancel.
    """
    now_iso = datetime.now().isoformat(timespec="seconds")
    affected_runs: set[int] = set()
    for jid in job_ids:
        try:
            con = get_db()
            row = con.execute(
                "SELECT run_id FROM job_history WHERE cluster=? AND job_id=?",
                (cluster, str(jid)),
            ).fetchone()
            con.close()
            if row and row["run_id"]:
                affected_runs.add(int(row["run_id"]))
        except Exception:
            pass
        try:
            update_job_waste_fields(cluster, jid, waste_reason=reason)
        except Exception as exc:
            log.warning("waste-watcher flag-only: stamp job %s/%s failed: %s", cluster, jid, exc)
    for run_id in affected_runs:
        try:
            update_run_fields(
                run_id,
                wasteful=True,
                waste_reason=reason,
                waste_detected_at=now_iso,
                waste_cancelled_by_watcher=False,
            )
        except Exception as exc:
            log.warning("waste-watcher flag-only: stamp run %s failed: %s", run_id, exc)
    try:
        from .logbooks import create_entry
        body = (
            f"**Cluster:** `{cluster}`\n"
            f"**Reason:** `{reason}` (confidence: `{confidence}`)\n"
            f"**Cancelled by watcher:** no (cancel disabled or no target)\n\n"
            f"**Summary:** {summary}\n\n"
            f"```json\n{_safe_json(evidence)}\n```"
        )
        title = f"WasteWatcher: flag-only {reason} on {cluster}"
        create_entry(audit_project, title, body=body, entry_type="note")
    except Exception as exc:
        log.warning("waste-watcher flag-only: audit log failed: %s", exc)
    return {"flagged": list(job_ids), "run_ids": sorted(affected_runs)}


# ─── Detection orchestration ────────────────────────────────────────────────

def _settings_snapshot() -> dict:
    """Single dict bundling every WasteWatcher tunable read once per tick.

    Reading every setting up-front avoids racing with operators who flip
    flags mid-tick and keeps the call tree pure (no live ``get_setting``
    calls inside detection helpers).
    """
    return {
        "enabled": bool(get_setting("waste_watcher_enabled")),
        "cancel_enabled": bool(get_setting("waste_watcher_cancel_enabled")),
        "cancel_disabled_clusters": [
            c.strip() for c in str(get_setting("waste_watcher_cancel_disabled_clusters", "")).split(",") if c.strip()
        ],
        "tick_sec": int(get_setting("waste_watcher_tick_sec")),
        "cold_probe_sec": int(get_setting("waste_watcher_cold_probe_sec")),
        "warm_probe_sec": int(get_setting("waste_watcher_warm_probe_sec")),
        "suspicious_probe_sec": int(get_setting("waste_watcher_suspicious_probe_sec")),
        "cold_grace_min": int(get_setting("waste_watcher_cold_grace_min")),
        "warm_idle_min": int(get_setting("waste_watcher_warm_idle_min")),
        "suspicious_confirm_min": int(get_setting("waste_watcher_suspicious_confirm_min")),
        "log_quiet_min": int(get_setting("waste_watcher_log_quiet_min")),
        "sdk_heartbeat_stale_min": int(get_setting("waste_watcher_sdk_heartbeat_stale_min")),
        "util_busy_threshold": float(get_setting("waste_watcher_util_busy_threshold")),
        "exempt_name_regex": str(get_setting("waste_watcher_exempt_name_regex")),
        "min_runtime_min": int(get_setting("waste_watcher_min_runtime_min")),
        "audit_project": str(get_setting("waste_watcher_audit_project")),
    }


def _ensure_state(cluster: str, job: Mapping, cfg: dict) -> WatcherState:
    """Load or create the per-job watcher state row.

    A brand-new job starts in ``cold`` with ``next_probe_due == now``.
    """
    jid = str(job.get("jobid") or "")
    state = load_state(cluster, jid)
    if state is None:
        state = WatcherState(
            cluster=cluster,
            job_id=jid,
            state=STATE_COLD,
            state_entered_at=datetime.now(),
            next_probe_due=datetime.now(),
        )
        if rules.is_exempt_name(str(job.get("name") or ""), cfg["exempt_name_regex"]):
            return set_exempt(cluster, jid, duration_min=cfg["warm_probe_sec"] // 60, note="exempt_name_regex")
        upsert_state(state)
    return state


def _evaluate_job(job: dict, cfg: dict) -> Optional[Detection]:
    """Probe one job, update its state row, return a Detection if it fired.

    Walks the state machine, persists the new state, then runs every
    applicable detection rule against the latest snapshots/log tail. The
    first rule that fires wins (high-confidence ones are tried first).
    """
    cluster = job["cluster"]
    jid = str(job["jobid"])
    state = _ensure_state(cluster, job, cfg)
    if state.state == STATE_EXEMPT and state.is_exempt_now():
        return None
    if state.state == STATE_EXEMPT and not state.is_exempt_now():
        # Exemption expired → drop back to cold.
        state.state = STATE_COLD
        state.state_entered_at = datetime.now()
        state.exempt_until = None

    stats = _probe_job(cluster, jid)
    if stats is None:
        # Probe failed; back off and try again next tick.
        state.next_probe_due = datetime.now() + timedelta(seconds=cfg["cold_probe_sec"])
        upsert_state(state)
        return None

    is_idle = _stats_show_idle(stats, cfg["util_busy_threshold"])

    # Update log-hash cursor (used by verification + idle-gpu rule)
    log_age_min: Optional[float] = None
    try:
        tail = _read_log_tail(cluster, jid, lines=80)
        if tail:
            new_hash = _hash_log_tail(tail)
            if not state.last_log_hash:
                state.last_log_hash = new_hash
                state.last_log_change_at = datetime.now()
            elif new_hash != state.last_log_hash:
                state.last_log_hash = new_hash
                state.last_log_change_at = datetime.now()
                log_age_min = 0.0
            elif state.last_log_change_at:
                log_age_min = max(0.0, (datetime.now() - state.last_log_change_at).total_seconds() / 60.0)
    except Exception as exc:
        log.debug("waste-watcher log check failed %s/%s: %s", cluster, jid, exc)

    # Update SDK heartbeat cursor for visibility on candidates API
    heartbeat = _sdk_heartbeat_age_min(job)
    if heartbeat is not None:
        state.last_sdk_heartbeat_at = datetime.now() - timedelta(minutes=heartbeat)

    # Walk the state machine.
    state = _transition_after_probe(state, is_idle=is_idle, log_age_min=log_age_min, cfg=cfg)

    detection: Optional[Detection] = None

    # High-confidence rules run on any RUNNING GPU job; we don't gate
    # them on the state machine because their preconditions are stronger.
    snapshots = []
    try:
        snapshots = get_stats_snapshots(cluster, jid)
    except Exception:
        pass

    # Port-mismatch needs the log to confirm the server actually bound,
    # so read the tail unconditionally — but cap the line count to keep
    # the SSH/tail call cheap for cold-state jobs.
    tail_lines = 160 if state.state in (STATE_SUSPICIOUS, STATE_WASTEFUL) else 60
    tail = _read_log_tail(cluster, jid, lines=tail_lines)
    detection = rules.detect_port_mismatch_hang(
        job={**job, "cluster": cluster},
        snapshots=snapshots,
        log_tail=tail,
        min_runtime_min=cfg["min_runtime_min"],
        busy_threshold_pct=cfg["util_busy_threshold"],
    )

    if not detection:
        # dead-server-before-client requires sibling info; fetch run jobs
        run_jobs = _enumerate_all_jobs_for_cluster(cluster)
        # only include same-run jobs when possible
        same_run_jobs = [j for j in run_jobs if j.get("run_root_job_id") == job.get("run_root_job_id")]
        if not same_run_jobs:
            same_run_jobs = run_jobs
        detection = rules.detect_dead_server_before_client(
            job={**job, "cluster": cluster},
            run_jobs=same_run_jobs,
        )

    if not detection:
        # QoS deadlock heuristic
        run_jobs = _enumerate_all_jobs_for_cluster(cluster)
        same_run_jobs = [j for j in run_jobs if j.get("run_root_job_id") == job.get("run_root_job_id")] or run_jobs
        detection = rules.detect_qos_self_deadlock(
            running_job={**job, "cluster": cluster},
            run_jobs=same_run_jobs,
        )

    if not detection:
        detection = rules.detect_gpu_allocation_mismatch(
            job={**job, "cluster": cluster},
            snapshots=snapshots,
            log_tail=tail,
            min_runtime_min=cfg["min_runtime_min"],
            busy_threshold_pct=cfg["util_busy_threshold"],
        )

    if not detection and state.state == STATE_WASTEFUL:
        detection = rules.detect_idle_gpu_sustained(
            job={**job, "cluster": cluster},
            snapshots=snapshots,
            min_runtime_min=cfg["min_runtime_min"],
            suspicious_confirm_min=cfg["suspicious_confirm_min"],
            busy_threshold_pct=cfg["util_busy_threshold"],
            log_quiet_min=cfg["log_quiet_min"],
            log_age_min=log_age_min,
        )

    if detection:
        state.suspected_reason = detection.reason
        state.suspected_confidence = detection.confidence
        state.last_notes = detection.summary[:280]
        if state.state != STATE_WASTEFUL:
            state.state = STATE_WASTEFUL
            state.state_entered_at = datetime.now()
    upsert_state(state)
    return detection


def _can_cancel(cluster: str, confidence: str, cfg: dict) -> bool:
    """Gate auto-cancel by cluster kill-switch + confidence + global flag."""
    if not cfg["cancel_enabled"]:
        return False
    if cluster in cfg["cancel_disabled_clusters"]:
        return False
    return confidence == CONFIDENCE_HIGH


def _act_on_detection(
    job: dict, detection: Detection, cfg: dict,
) -> None:
    """Either auto-cancel (with verification) or just flag the detection."""
    cluster = job["cluster"]
    if not detection.target_jobs:
        # No-target detections (e.g. manifest_only_failure) are
        # informational; always flag-only.
        flag_only(
            cluster=cluster,
            job_ids=[],
            reason=detection.reason,
            confidence=detection.confidence,
            summary=detection.summary,
            evidence=detection.evidence,
            audit_project=cfg["audit_project"],
        )
        return

    if not _can_cancel(cluster, detection.confidence, cfg):
        flag_only(
            cluster=cluster,
            job_ids=[jid for _, jid in detection.target_jobs],
            reason=detection.reason,
            confidence=detection.confidence,
            summary=detection.summary,
            evidence=detection.evidence,
            audit_project=cfg["audit_project"],
        )
        return

    # Auto-cancel path: verify each target before issuing scancel.
    verified_targets: List[str] = []
    verification_notes: dict[str, str] = {}
    for tgt_cluster, tgt_job in detection.target_jobs:
        ok, note = verify_wasteful(
            cluster=tgt_cluster,
            job_id=tgt_job,
            reason=detection.reason,
            cfg=cfg,
        )
        verification_notes[tgt_job] = note
        if ok:
            verified_targets.append(tgt_job)
        else:
            # Demote so we don't immediately re-flag.
            set_exempt(tgt_cluster, tgt_job, duration_min=30,
                       note=f"verify_failed:{note}")
    if not verified_targets:
        flag_only(
            cluster=cluster,
            job_ids=[jid for _, jid in detection.target_jobs],
            reason=detection.reason,
            confidence=detection.confidence,
            summary=f"{detection.summary} (verification rejected all targets)",
            evidence={**detection.evidence, "verification": verification_notes},
            audit_project=cfg["audit_project"],
        )
        return

    cancel_with_reason(
        cluster=cluster,
        job_ids=verified_targets,
        reason=detection.reason,
        confidence=detection.confidence,
        summary=detection.summary,
        evidence={**detection.evidence, "verification": verification_notes},
        by_watcher=True,
        audit_project=cfg["audit_project"],
    )


# ─── Loop entry point ───────────────────────────────────────────────────────

def _tick_once(cfg: Optional[dict] = None) -> int:
    """Run one watcher tick.

    Returns the number of jobs evaluated. Broken out of the loop so
    tests can drive a single iteration deterministically.
    """
    cfg = cfg or _settings_snapshot()
    if not cfg["enabled"]:
        return 0
    jobs = _enumerate_live_gpu_jobs()
    evaluated = 0
    for job in jobs:
        cluster = job["cluster"]
        if is_paused(cluster):
            continue
        try:
            state = load_state(cluster, str(job["jobid"]))
            now = datetime.now()
            if state and state.next_probe_due > now and state.state != STATE_EXEMPT:
                continue
            detection = _evaluate_job(job, cfg)
            evaluated += 1
            if detection:
                _act_on_detection(job, detection, cfg)
        except Exception as exc:
            log.exception("waste-watcher: tick failed for %s/%s: %s",
                          cluster, job.get("jobid"), exc)
    # Lazy housekeeping: drop state rows for jobs no longer in the live view.
    try:
        live_ids = [(j["cluster"], str(j["jobid"])) for j in jobs]
        prune_terminal_states(live_ids)
    except Exception as exc:
        log.debug("waste-watcher prune failed: %s", exc)
    return evaluated


def _watcher_loop() -> None:
    """Forever-loop body. Sleeps between ticks; exits on ``request_shutdown``."""
    log.info("WasteWatcher loop started")
    while not _shutdown_event.is_set():
        cfg = _settings_snapshot()
        try:
            # Skip work when the poller has paused (no live data to act on)
            try:
                from .poller import get_poller
                if getattr(get_poller(), "_idle", False):
                    _shutdown_event.wait(cfg["tick_sec"])
                    continue
            except Exception:
                pass
            _tick_once(cfg)
        except Exception:
            log.exception("WasteWatcher loop tick raised")
        _shutdown_event.wait(cfg["tick_sec"])
    log.info("WasteWatcher loop exited")


def start_waste_watcher() -> threading.Thread:
    """Start the watcher daemon thread (idempotent).

    Returns the running ``threading.Thread`` so callers (and tests) can
    inspect/join it. Calling start a second time is a no-op.
    """
    global _thread
    if _thread is not None and _thread.is_alive():
        return _thread
    _shutdown_event.clear()
    _thread = threading.Thread(
        target=_watcher_loop, daemon=True, name="waste_watcher",
    )
    _thread.start()
    log.info("waste watcher started")
    return _thread
