"""Resubmit SDK-tracked runs by re-executing their captured submit_command.

The SDK already stores everything we need to reproduce a submission: the
full multi-line shell command (``runs.submit_command``), the working
directory (``runs.submit_cwd``), and the original ``run_started`` event
payload (``sdk_events``) which carries ``env_subset.CONDA_PREFIX`` and
``python_executable``. This module ties those together so the UI can offer
a one-click "Resubmit" action.

Only SDK runs are eligible — legacy runs (5k+ rows) never captured a
submit_command. Eligibility also requires every job to be in a terminal
state so a click can't accidentally double-submit while jobs are still
RUNNING or PENDING.

The resubmit itself runs *locally* (clausius is assumed to live on the
same machine as the original launcher, which is the normal setup).
``subprocess.Popen`` spawns ``bash -c <cmd>`` with ``start_new_session``
so the child survives gunicorn restarts; stdout/stderr stream into
``data/resubmit_logs/<run_uuid>__<ts>.log`` for after-the-fact viewing.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import time
from typing import Any, Dict, Tuple

from .bootstrap import get_bootstrap
from .db import get_db

log = logging.getLogger(__name__)


ACTIVE_JOB_STATES = {"RUNNING", "COMPLETING", "PENDING", "SUBMITTING"}
"""Mirror of ``_isActivelyCancelableState`` in ``static/js/jobs.js``."""

_FALLBACK_CONDA_ROOTS = [
    "~/miniconda3",
    "~/anaconda3",
    "~/miniforge3",
    "~/mambaforge",
]

_LOG_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]+\.log$")


def eligibility(run: Dict[str, Any]) -> Tuple[bool, str]:
    """Return ``(can_resubmit, reason)`` for a run dict from ``_run_info_response``.

    ``reason`` is empty on success and a short human-readable explanation
    when blocked, suitable for surfacing in a tooltip.
    """
    if not isinstance(run, dict):
        return False, "Run not loaded"
    if (run.get("source") or "") != "sdk":
        return False, "Only SDK-tracked runs can be resubmitted"
    if not str(run.get("submit_command") or "").strip():
        return False, "No captured submit command for this run"
    jobs = run.get("jobs") or []
    if not jobs:
        return False, "Run has no jobs yet"
    for j in jobs:
        st = str(j.get("state") or "").upper().split()[0] if j.get("state") else ""
        if st in ACTIVE_JOB_STATES:
            return False, "Cannot resubmit while jobs are still active"
    return True, ""


def _conda_root_from_payload(payload: Dict[str, Any]) -> str:
    """Derive conda root (the directory containing ``etc/profile.d/conda.sh``).

    Looks at ``env_subset.CONDA_PREFIX`` first (most reliable when the
    user was in an activated env at launch time), then falls back to
    parsing ``python_executable``.
    """
    env = payload.get("env_subset") or {}
    conda_prefix = str(env.get("CONDA_PREFIX") or "")
    if conda_prefix:
        m = re.match(r"^(?P<root>.+?)/envs/[^/]+/?$", conda_prefix)
        if m:
            return m.group("root")
        if os.path.isfile(os.path.join(conda_prefix, "etc", "profile.d", "conda.sh")):
            return conda_prefix
    python_exe = str(payload.get("python_executable") or "")
    if python_exe:
        m = re.match(r"^(?P<root>.+?)/envs/[^/]+/bin/python(?:\d.*)?$", python_exe)
        if m:
            return m.group("root")
    return ""


def _fallback_conda_root() -> str:
    """Last-resort search for a conda install on disk."""
    conda_exe = os.environ.get("CONDA_EXE", "")
    if conda_exe:
        root = os.path.dirname(os.path.dirname(conda_exe))
        if os.path.isfile(os.path.join(root, "etc", "profile.d", "conda.sh")):
            return root
    for raw in _FALLBACK_CONDA_ROOTS:
        root = os.path.expanduser(raw)
        if os.path.isfile(os.path.join(root, "etc", "profile.d", "conda.sh")):
            return root
    return ""


def derive_conda_init(run_uuid: str) -> str:
    """Return a shell prefix that initialises conda for ``bash -c``.

    The captured ``submit_command`` typically starts with
    ``conda activate <env>``, which fails in a non-interactive bash
    invocation unless conda's profile script has been sourced first.
    """
    if not run_uuid:
        return ""
    row = None
    try:
        con = get_db()
        row = con.execute(
            """SELECT payload_json FROM sdk_events
               WHERE run_uuid=? AND event_type='run_started'
               ORDER BY id DESC LIMIT 1""",
            (run_uuid,),
        ).fetchone()
    except Exception:
        log.exception("resubmit: failed to read run_started event for %s", run_uuid)
    root = ""
    if row:
        try:
            payload = json.loads(row["payload_json"])
            if isinstance(payload, dict):
                root = _conda_root_from_payload(payload)
        except (ValueError, TypeError):
            log.warning("resubmit: malformed run_started payload for %s", run_uuid)
    if not root:
        root = _fallback_conda_root()
    if not root:
        return ""
    return f"source {os.path.join(root, 'etc/profile.d/conda.sh')} && "


def _resubmit_logs_dir() -> str:
    return os.path.join(get_bootstrap().data_dir, "resubmit_logs")


def _log_filename(run_uuid: str) -> str:
    safe_uuid = re.sub(r"[^A-Za-z0-9]", "_", str(run_uuid or "run"))[:32] or "run"
    ts = time.strftime("%Y%m%dT%H%M%S")
    return f"{safe_uuid}__{ts}.log"


def _build_header(run: Dict[str, Any], cwd: str | None, submit_cmd: str) -> str:
    return (
        "# clausius resubmit\n"
        f"# run_hash: {run.get('run_hash') or ''}\n"
        f"# run_uuid: {run.get('run_uuid') or ''}\n"
        f"# cluster:  {run.get('cluster') or ''}\n"
        f"# cwd:      {cwd or '(unset)'}\n"
        f"# started:  {time.strftime('%Y-%m-%dT%H:%M:%S%z')}\n"
        "# command:\n"
        f"{submit_cmd}\n"
        "# --- output ---\n"
    )


def spawn(run: Dict[str, Any]) -> Dict[str, Any]:
    """Spawn the captured submit_command as a detached child process.

    Returns ``{pid, log_path, log_name, log_url, had_conda_prefix}`` on
    success. Raises on filesystem or spawn failures so the caller can
    surface an error.
    """
    submit_cmd = str(run.get("submit_command") or "").strip()
    if not submit_cmd:
        raise ValueError("submit_command is empty")
    cwd = str(run.get("submit_cwd") or "").strip() or None
    if cwd and not os.path.isdir(cwd):
        log.warning("resubmit: submit_cwd %r is not a directory; running from $HOME", cwd)
        cwd = None
    run_uuid = str(run.get("run_uuid") or "")
    prefix = derive_conda_init(run_uuid)
    full_cmd = f"{prefix}{submit_cmd}" if prefix else submit_cmd

    logs_dir = _resubmit_logs_dir()
    os.makedirs(logs_dir, exist_ok=True)
    log_name = _log_filename(run_uuid)
    log_path = os.path.join(logs_dir, log_name)
    header = _build_header(run, cwd, submit_cmd)
    log_fd = open(log_path, "ab", buffering=0)
    try:
        log_fd.write(header.encode("utf-8"))
        proc = subprocess.Popen(
            ["bash", "-c", full_cmd],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        log_fd.close()
    return {
        "pid": proc.pid,
        "log_path": log_path,
        "log_name": log_name,
        "log_url": f"/api/resubmit_log/{log_name}",
        "had_conda_prefix": bool(prefix),
    }


def read_log(filename: str, *, max_bytes: int = 1_000_000) -> Tuple[bool, str, int]:
    """Return ``(ok, content_or_error, status_code)`` for a resubmit log file.

    Filename is validated against ``_LOG_NAME_RE`` so callers can't
    traverse outside ``data/resubmit_logs/``. Output is capped at
    ``max_bytes`` with a ``... (truncated)`` marker appended.
    """
    name = str(filename or "")
    if not _LOG_NAME_RE.match(name):
        return False, "Invalid log filename", 400
    full = os.path.join(_resubmit_logs_dir(), name)
    if not os.path.isfile(full):
        return False, "Log not found", 404
    try:
        with open(full, "rb") as fh:
            data = fh.read(max_bytes + 1)
    except OSError as exc:
        return False, f"Failed to read log: {exc}", 500
    truncated = len(data) > max_bytes
    if truncated:
        data = data[:max_bytes]
    text = data.decode("utf-8", errors="replace")
    if truncated:
        text += "\n... (truncated)"
    return True, text, 200
