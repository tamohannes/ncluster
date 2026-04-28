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

"""Aim-style manual run facade for Clausius SDK telemetry."""

from __future__ import annotations

import os
import platform
import sys
import time
from typing import Any

from nemo_skills.clausius_sdk.events import RunProvenance
from nemo_skills.clausius_sdk.session import (
    ClausiusSession,
    _build_transports,
    _detect_conda_env,
    _detect_env_vars_set,
    _git_sha,
    _safe_env_subset,
    _sanitize_params,
)


def _attached_seq_start() -> int:
    """Pick a high event_seq base so attached processes avoid early SDK events."""
    return int(time.time() * 1_000_000) + (os.getpid() % 1000)


class Run:
    """Manual experiment run handle.

    A new ``Run(run_name=..., cluster=...)`` emits ``run_started`` and owns the
    run lifecycle. ``Run(run_uuid=..., connect=True)`` attaches to an existing
    run and only emits future metric/metadata events.
    """

    def __init__(
        self,
        run_name: str = "",
        cluster: str = "",
        run_uuid: str | None = None,
        connect: bool = False,
        metadata: dict[str, Any] | None = None,
        output_dir: str = "",
        command: str = "",
        params: dict[str, Any] | None = None,
    ):
        if connect and not run_uuid:
            raise ValueError("connect=True requires run_uuid")

        self._attached = bool(run_uuid)
        self._closed = False

        if self._attached:
            self._session = ClausiusSession(
                _build_transports(output_dir),
                run_uuid=run_uuid,
                seq_start=_attached_seq_start(),
            )
            # Attached handles should never mark an existing NeMo run failed
            # merely because the manual logging process exits.
            self._session._finished = True
        else:
            if not run_name:
                raise ValueError("run_name is required when creating a new Clausius Run")
            if not cluster:
                raise ValueError("cluster is required when creating a new Clausius Run")
            self._session = ClausiusSession(_build_transports(output_dir))
            full_command = command or " ".join(sys.argv)
            provenance = RunProvenance(
                argv=sys.argv[:],
                command=full_command,
                cwd=os.getcwd(),
                expname=run_name,
                output_dir=output_dir,
                git_commit=_git_sha(),
                hostname=platform.node(),
                env_subset=_safe_env_subset(),
                cluster=cluster,
                config_overrides={},
                conda_env=_detect_conda_env(),
                python_executable=sys.executable,
                env_vars_set=_detect_env_vars_set(),
                params=_sanitize_params(params or {}),
            )
            self._session.emit_run_started(provenance)

        if metadata:
            self.set_metadata(metadata)

    @property
    def run_uuid(self) -> str:
        return self._session.run_uuid

    @property
    def active(self) -> bool:
        return not self._closed and self._session.active

    def track(
        self,
        key: str,
        value: Any,
        step: int | None = None,
        context: dict[str, Any] | None = None,
        **context_kwargs: Any,
    ) -> "Run":
        merged_context: dict[str, Any] = {}
        if context is not None:
            if not isinstance(context, dict):
                raise TypeError("context must be a dict when provided")
            merged_context.update(context)
        merged_context.update(context_kwargs)
        self._session.log_metric(key, value, step=step, **merged_context)
        return self

    def set_metadata(self, metadata: dict[str, Any]) -> "Run":
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be a dict")
        self._session.log_metadata(metadata)
        return self

    def log_artifact(self, name: str, path: str, **metadata: Any) -> "Run":
        self._session.log_artifact(name, path, **metadata)
        return self

    def finish(self, status: str = "completed", **extra: Any) -> None:
        self._closed = True
        self._session.finish(status=status, **extra)

    def fail(self, error: str = "", **extra: Any) -> None:
        self._closed = True
        self._session.fail(error=error, **extra)

    def close(self, status: str = "completed") -> None:
        if self._closed:
            return
        self._closed = True
        if self._attached:
            self._session.close()
        else:
            self._session.finish(status=status)

    def __enter__(self) -> "Run":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc is None:
            self.close()
        else:
            self.fail(error=str(exc))
        return False
