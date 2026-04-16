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

_ENV_ALLOWLIST = {
    "USER",
    "HOME",
    "HOSTNAME",
    "SLURM_JOB_ID",
    "SLURM_JOB_NAME",
    "SLURM_JOB_NODELIST",
    "SLURM_NNODES",
    "CUDA_VISIBLE_DEVICES",
    "NEMO_SKILLS_CONFIG",
    "HF_HOME",
    "WANDB_PROJECT",
    "WANDB_RUN_ID",
}


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


def _safe_env_subset() -> dict[str, str]:
    return {k: os.environ[k] for k in _ENV_ALLOWLIST if k in os.environ}


class ClausiusSession:
    """Tracks a single NeMo-Skills run from launch through completion.

    Thread-safe. Events are buffered in memory and flushed periodically
    or on shutdown. Delivery failures never propagate exceptions to the
    caller; the worst case is silent event loss.
    """

    _instance: ClausiusSession | None = None
    _instance_lock = threading.Lock()

    def __init__(self, transports: list[Transport], run_uuid: str | None = None):
        self._run_uuid = run_uuid or uuid.uuid4().hex
        self._transports = list(transports)
        self._seq = 0
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

    # ── Factory / singleton ───────────────────────────────────────────

    @classmethod
    def start_from_cli(
        cls,
        expname: str,
        command: str = "",
        output_dir: str = "",
        cluster: str = "",
        config_overrides: dict | None = None,
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
