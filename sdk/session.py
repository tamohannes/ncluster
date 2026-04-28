# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Clausius SDK session: owns run identity, event sequencing, and delivery."""

from __future__ import annotations

import atexit
import logging
import os
import platform
import subprocess
import sys
import threading
import time
import uuid
from typing import Any

from nemo_skills.clausius_sdk.events import (
    Event,
    EventType,
    JobInfo,
    RunProvenance,
)
from nemo_skills.clausius_sdk.transports.base import Transport

LOG = logging.getLogger(__name__)

_BATCH_SIZE = 20
_FLUSH_INTERVAL_SEC = 10.0

_ENV_DENYLIST_SUBSTRINGS = {
    "SECRET", "TOKEN", "PASSWORD", "PASSWD", "CREDENTIAL", "API_KEY",
    "PRIVATE_KEY", "AUTH",
}
_ENV_DENYLIST_EXACT = {
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "GH_TOKEN", "GITHUB_TOKEN", "GITLAB_TOKEN", "HF_TOKEN",
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "NGC_API_KEY",
    "CLAUSIUS_TOKEN",
    "LS_COLORS", "LSCOLORS",
    "SSH_AUTH_SOCK", "SSH_AGENT_PID", "GPG_AGENT_INFO",
}
_ENV_MAX_VALUE_LEN = 1024


def _git_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            cwd=os.getcwd(),
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception:
        return ""


def _is_env_safe(key: str) -> bool:
    upper = key.upper()
    if upper in _ENV_DENYLIST_EXACT:
        return False
    return not any(s in upper for s in _ENV_DENYLIST_SUBSTRINGS)


def _safe_env_subset() -> dict[str, str]:
    out = {}
    for k, v in sorted(os.environ.items()):
        if not _is_env_safe(k):
            continue
        if len(v) > _ENV_MAX_VALUE_LEN:
            v = v[:_ENV_MAX_VALUE_LEN] + "…"
        out[k] = v
    return out


def _detect_conda_env() -> str:
    """Return active conda/venv environment name, or empty string."""
    if os.environ.get("CONDA_DEFAULT_ENV"):
        return os.environ["CONDA_DEFAULT_ENV"]
    venv = os.environ.get("VIRTUAL_ENV", "")
    if venv:
        return os.path.basename(venv)
    return ""


_SUBMIT_ENV_VARS = {
    "CLAUSIUS_URL",
    "CLAUSIUS_TOKEN",
    "NEMO_SKILLS_DISABLE_UNCOMMITTED_CHANGES_CHECK",
    "CUDA_VISIBLE_DEVICES",
    "NEMO_SKILLS_CONFIG",
    "HF_HOME",
    "WANDB_PROJECT",
}


def _detect_env_vars_set() -> list[str]:
    """Return env var assignments that were set at launch time (for reproducing)."""
    return [f"{k}={os.environ[k]}" for k in sorted(_SUBMIT_ENV_VARS) if k in os.environ]


_PARAMS_MAX_DEPTH = 3
_PARAMS_MAX_STR_LEN = 2048
_PARAMS_MAX_ITEMS = 64


def _sanitize_params(raw: Any, depth: int = 0) -> Any:
    """Coerce pipeline kwargs into a JSON-safe shape with size limits.

    Drops non-primitive values that can't be represented (e.g. live objects,
    callables), truncates very long strings, and caps container sizes so a
    misbehaving caller can't blow up the event payload.
    """
    if raw is None or isinstance(raw, (bool, int, float)):
        return raw
    if isinstance(raw, str):
        if len(raw) > _PARAMS_MAX_STR_LEN:
            return raw[:_PARAMS_MAX_STR_LEN] + "…"
        return raw
    if depth >= _PARAMS_MAX_DEPTH:
        return str(raw)[:_PARAMS_MAX_STR_LEN]
    if isinstance(raw, dict):
        out: dict[str, Any] = {}
        for k, v in list(raw.items())[:_PARAMS_MAX_ITEMS]:
            if not isinstance(k, str):
                k = str(k)
            out[k] = _sanitize_params(v, depth + 1)
        return out
    if isinstance(raw, (list, tuple, set)):
        return [_sanitize_params(v, depth + 1) for v in list(raw)[:_PARAMS_MAX_ITEMS]]
    try:
        return str(raw)[:_PARAMS_MAX_STR_LEN]
    except Exception:
        return None


class ClausiusSession:
    """Tracks a single NeMo-Skills run from launch through completion.

    Thread-safe. Events are buffered in memory and flushed periodically
    or on shutdown. Delivery failures never propagate exceptions to the
    caller; the worst case is silent event loss.
    """

    _instance: ClausiusSession | None = None
    _instance_lock = threading.Lock()

    def __init__(self, transports: list[Transport], run_uuid: str | None = None, seq_start: int = 0):
        self._run_uuid = run_uuid or uuid.uuid4().hex
        self._transports = list(transports)
        self._seq = max(0, int(seq_start or 0))
        self._lock = threading.Lock()
        self._buffer: list[Event] = []
        self._closed = False
        self._finished = False

        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        atexit.register(self._shutdown)

    @property
    def run_uuid(self) -> str:
        return self._run_uuid

    @property
    def active(self) -> bool:
        return not self._closed

    def _next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    _FLUSH_IMMEDIATELY = frozenset({"run_started", "run_finished", "run_failed"})

    def _emit(self, event_type: str | EventType, payload: dict[str, Any] | None = None):
        if self._closed:
            return
        etype = event_type.value if isinstance(event_type, EventType) else event_type
        ev = Event(
            run_uuid=self._run_uuid,
            event_type=etype,
            event_seq=self._next_seq(),
            payload=payload or {},
        )
        with self._lock:
            self._buffer.append(ev)
            if len(self._buffer) >= _BATCH_SIZE or etype in self._FLUSH_IMMEDIATELY:
                self._drain_buffer()

    def _drain_buffer(self):
        """Send buffered events to all transports. Called with self._lock held."""
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._buffer.clear()
        for transport in self._transports:
            try:
                transport.send(batch)
            except Exception as exc:
                LOG.debug("clausius: transport %s failed: %s", type(transport).__name__, exc)

    def _flush_loop(self):
        while not self._closed:
            try:
                time.sleep(_FLUSH_INTERVAL_SEC)
            except Exception:
                return
            try:
                with self._lock:
                    self._drain_buffer()
            except Exception:
                return

    def _shutdown(self):
        if self._closed:
            return
        if not self._finished:
            try:
                self._emit(EventType.RUN_FAILED, {"error": "submission interrupted", "status": "submit_failed"})
            except Exception:
                pass
        self._closed = True
        try:
            with self._lock:
                self._drain_buffer()
            for t in self._transports:
                try:
                    t.flush()
                    t.close()
                except Exception:
                    pass
        except Exception:
            pass

    # ── Public API ────────────────────────────────────────────────────

    def emit_run_started(self, provenance: RunProvenance):
        self._emit(EventType.RUN_STARTED, provenance.to_dict())

    def emit_job_prepared(self, job: JobInfo):
        self._emit(EventType.JOB_PREPARED, job.to_dict())

    def emit_job_submitted(self, job: JobInfo):
        self._emit(EventType.JOB_SUBMITTED, job.to_dict())

    def emit_job_state(self, slurm_job_id: str, state: str, **extra):
        self._emit(EventType.JOB_STATE, {"slurm_job_id": slurm_job_id, "state": state, **extra})

    def log_metric(self, key: str, value: Any, step: int | None = None, **context):
        payload: dict[str, Any] = {"key": key, "value": value}
        if step is not None:
            payload["step"] = step
        if context:
            payload["context"] = context
        self._emit(EventType.METRIC_LOGGED, payload)

    def log_params(self, params: dict[str, Any]):
        for k, v in params.items():
            self.log_metric(f"param.{k}", v)

    def log_metadata(self, metadata: dict[str, Any]):
        self._emit(EventType.METADATA_LOGGED, {"metadata": _sanitize_params(metadata)})

    def log_artifact(self, name: str, path: str, **metadata):
        self._emit(EventType.ARTIFACT_LOGGED, {"name": name, "path": path, **metadata})

    def finish(self, status: str = "completed", **extra):
        self._finished = True
        self._emit(EventType.RUN_FINISHED, {"status": status, **extra})
        self._shutdown()

    def fail(self, error: str = "", **extra):
        self._finished = True
        self._emit(EventType.RUN_FAILED, {"error": error, **extra})
        self._shutdown()

    def close(self):
        """Flush and close the session without emitting a terminal event."""
        self._finished = True
        self._shutdown()

    # ── Factory / singleton ───────────────────────────────────────────

    @classmethod
    def start_from_cli(
        cls,
        expname: str,
        command: str = "",
        output_dir: str = "",
        cluster: str = "",
        config_overrides: dict | None = None,
        params: dict | None = None,
    ) -> ClausiusSession:
        """Create or return the global session, capturing launch provenance."""
        with cls._instance_lock:
            if cls._instance is not None and cls._instance.active:
                return cls._instance

        transports = _build_transports(output_dir)
        session = cls(transports)
        cls._instance = session

        full_command = " ".join(sys.argv)
        provenance = RunProvenance(
            argv=sys.argv[:],
            command=full_command,
            cwd=os.getcwd(),
            expname=expname,
            output_dir=output_dir,
            git_commit=_git_sha(),
            hostname=platform.node(),
            env_subset=_safe_env_subset(),
            cluster=cluster,
            config_overrides=config_overrides or {},
            conda_env=_detect_conda_env(),
            python_executable=sys.executable,
            env_vars_set=_detect_env_vars_set(),
            params=_sanitize_params(params or {}),
        )
        session.emit_run_started(provenance)
        return session

    @classmethod
    def get(cls) -> ClausiusSession | None:
        """Return the current session or None if tracking is disabled."""
        return cls._instance

    @classmethod
    def reset(cls):
        """Tear down the global session. Primarily for testing."""
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance._shutdown()
            cls._instance = None


def _build_transports(output_dir: str = "") -> list[Transport]:
    """Construct transports from environment variables."""
    transports: list[Transport] = []

    url = os.environ.get("CLAUSIUS_URL", "")
    token = os.environ.get("CLAUSIUS_TOKEN", "")
    if url:
        from nemo_skills.clausius_sdk.transports.http import HttpTransport

        transports.append(HttpTransport(url=url, token=token))

    spool_dir = os.environ.get("CLAUSIUS_SPOOL_DIR", "")
    if not spool_dir and output_dir:
        spool_dir = os.path.join(output_dir, ".clausius")
    if spool_dir:
        try:
            from nemo_skills.clausius_sdk.transports.file_spool import FileSpoolTransport

            transports.append(FileSpoolTransport(os.path.join(spool_dir, "events.jsonl")))
        except OSError:
            LOG.debug("clausius: spool dir %s not writable, skipping file transport", spool_dir)

    if not transports:
        LOG.debug("clausius: no transports configured; tracking is a no-op")

    return transports
