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

"""Clausius SDK event schema and serialization."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    RUN_STARTED = "run_started"
    JOB_PREPARED = "job_prepared"
    JOB_SUBMITTED = "job_submitted"
    JOB_STATE = "job_state"
    METRIC_LOGGED = "metric_logged"
    ARTIFACT_LOGGED = "artifact_logged"
    RUN_FINISHED = "run_finished"
    RUN_FAILED = "run_failed"


@dataclass
class Event:
    run_uuid: str
    event_type: str
    event_seq: int
    ts: float = field(default_factory=time.time)
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        if isinstance(d.get("event_type"), EventType):
            d["event_type"] = d["event_type"].value
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, d: dict) -> Event:
        return cls(**d)


@dataclass
class RunProvenance:
    """Captured at run start before any jobs are submitted."""

    argv: list[str]
    command: str
    cwd: str
    expname: str
    output_dir: str = ""
    git_commit: str = ""
    hostname: str = ""
    env_subset: dict[str, str] = field(default_factory=dict)
    cluster: str = ""
    config_overrides: dict[str, Any] = field(default_factory=dict)
    conda_env: str = ""
    python_executable: str = ""
    env_vars_set: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class JobInfo:
    """Per-job lineage metadata emitted with job_prepared / job_submitted."""

    job_local_name: str
    task_name: str = ""
    slurm_job_id: str = ""
    cluster: str = ""
    partition: str = ""
    account: str = ""
    num_nodes: int = 0
    num_gpus: int | None = None
    num_tasks: int = 0
    dependencies: list[str] = field(default_factory=list)
    container: str = ""
    role: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
