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

"""NeMo-Skills pipeline integration hooks for the Clausius SDK.

These functions are called from the shared pipeline utilities
(add_task, run_exp, top-level CLI commands) so every run gets
automatic tracking without per-recipe instrumentation.
"""

from __future__ import annotations

import logging
import os
import re

from clausius_sdk.events import JobInfo
from clausius_sdk.session import ClausiusSession

LOG = logging.getLogger(__name__)

# A run is auto-tagged ``smoke`` when ``smoke`` appears as a delimited token
# anywhere in the expname or output_dir (e.g. ``mcp_smoke_...``,
# ``...-smoke-r3``, ``.../smoke-test/...``). This catches launchers that name
# their runs after the smoke convention without needing CLAUSIUS_TAGS.
_SMOKE_TOKEN_RE = re.compile(r"(?:^|[-_/.])smoke(?:[-_/.]|$)", re.IGNORECASE)


def _tracking_enabled() -> bool:
    """Check if Clausius tracking is configured."""
    return bool(
        os.environ.get("CLAUSIUS_URL")
        or os.environ.get("CLAUSIUS_SPOOL_DIR")
    )


def _resolve_launch_tags(
    expname: str = "",
    output_dir: str = "",
    explicit: list[str] | str | None = None,
) -> list[str]:
    """Collect launch-time tags from explicit args, env, and naming heuristics.

    Sources, in order:
      1. ``explicit`` tags passed by the caller.
      2. The ``CLAUSIUS_TAGS`` env var (comma/space-separated). This is the
         reliable cross-launcher knob — e.g. ``CLAUSIUS_TAGS=smoke ns eval ...``
         tags any submission as a smoke run regardless of its name.
      3. An auto-detected ``smoke`` tag when ``smoke`` appears as a delimited
         token in the expname or output_dir.

    The returned list is passed through ``_sanitize_tags`` downstream (in
    ``start_from_cli``), which lowercases, de-dupes, and validates each tag, so
    duplicates and casing here are harmless.
    """
    tags: list[str] = []
    if explicit:
        if isinstance(explicit, str):
            tags.append(explicit)
        else:
            tags.extend(str(t) for t in explicit)

    env_tags = os.environ.get("CLAUSIUS_TAGS", "")
    if env_tags:
        tags.extend(re.split(r"[,\s]+", env_tags.strip()))

    if _SMOKE_TOKEN_RE.search(expname or "") or _SMOKE_TOKEN_RE.search(output_dir or ""):
        tags.append("smoke")

    return [t for t in (t.strip() for t in tags) if t]


def maybe_start_session(
    expname: str,
    command: str = "",
    output_dir: str = "",
    cluster: str = "",
    config_overrides: dict | None = None,
    params: dict | None = None,
    tags: list[str] | str | None = None,
) -> ClausiusSession | None:
    """Start or return the global Clausius session if tracking is configured.

    Returns None if CLAUSIUS_URL and CLAUSIUS_SPOOL_DIR are both unset.
    Safe to call multiple times; only the first call creates the session.

    ``params`` is a free-form dict of pipeline kwargs (model, benchmarks,
    num_samples, judge_model, ...) captured at the hook call site. It is
    serialised into the run_started event payload and rendered in the
    "Run Parameters" block in the UI.
    ``tags`` marks runs such as ``smoke`` at launch. On top of any explicit
    tags, the ``CLAUSIUS_TAGS`` env var and a ``smoke``-token heuristic on the
    expname/output_dir are folded in (see ``_resolve_launch_tags``), so smoke
    submissions get tagged without per-launcher wiring.
    """
    if not _tracking_enabled():
        return None
    try:
        return ClausiusSession.start_from_cli(
            expname=expname,
            command=command,
            output_dir=output_dir,
            cluster=cluster,
            config_overrides=config_overrides,
            params=params,
            tags=_resolve_launch_tags(expname, output_dir, tags),
        )
    except Exception as exc:
        LOG.debug("clausius: failed to start session: %s", exc)
        return None


def on_task_prepared(
    task_name: str,
    cluster: str = "",
    partition: str = "",
    account: str = "",
    num_nodes: int = 0,
    num_gpus: int | None = None,
    num_tasks: int = 0,
    container: str = "",
    dependencies: list[str] | None = None,
    role: str = "main",
) -> None:
    """Emit a job_prepared event. Called from add_task()."""
    session = ClausiusSession.get()
    if not session:
        return
    try:
        session.emit_job_prepared(
            JobInfo(
                job_local_name=task_name,
                task_name=task_name,
                cluster=cluster,
                partition=partition,
                account=account,
                num_nodes=num_nodes,
                num_gpus=num_gpus,
                num_tasks=num_tasks,
                container=container,
                dependencies=dependencies or [],
                role=role,
            )
        )
    except Exception as exc:
        LOG.debug("clausius: job_prepared failed: %s", exc)


def on_run_submitted(cluster: str = "", dry_run: bool = False) -> None:
    """Emit a job_submitted event after run_exp(). Called from run_exp().

    Also marks the session as finished since the local launcher's job is
    done once Slurm has accepted the submission.
    """
    if dry_run:
        return
    session = ClausiusSession.get()
    if not session:
        return
    try:
        session.emit_job_submitted(
            JobInfo(
                job_local_name="experiment",
                cluster=cluster,
                role="root",
            )
        )
        session.finish(status="submitted")
    except Exception as exc:
        LOG.debug("clausius: job_submitted failed: %s", exc)


def on_run_finished(status: str = "completed") -> None:
    """Finalize the session. Called when the top-level command returns."""
    session = ClausiusSession.get()
    if not session:
        return
    try:
        session.finish(status=status)
    except Exception:
        pass


def on_run_failed(error: str = "") -> None:
    """Mark the run as failed. Called on unhandled exceptions."""
    session = ClausiusSession.get()
    if not session:
        return
    try:
        session.fail(error=error)
    except Exception:
        pass
