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

"""Aim-style convenience wrappers that delegate to the active ClausiusSession.

These are safe to call even when no session is active: they silently no-op.

Usage from evaluation / training code:

    from nemo_skills.clausius_sdk.metrics import log_metric, log_artifact

    log_metric("accuracy", 0.84, step=100)
    log_artifact("metrics.json", "/path/to/metrics.json")
"""

from __future__ import annotations

from typing import Any


def log_metric(key: str, value: Any, step: int | None = None, **context) -> None:
    from nemo_skills.clausius_sdk.session import ClausiusSession

    s = ClausiusSession.get()
    if s:
        if step is None:
            s.log_scalar(key, value, **context)
        else:
            s.log_metric(key, value, step=step, **context)


def log_scalar(key: str, value: Any, **context) -> None:
    from nemo_skills.clausius_sdk.session import ClausiusSession

    s = ClausiusSession.get()
    if s:
        s.log_scalar(key, value, **context)


def log_params(params: dict[str, Any]) -> None:
    from nemo_skills.clausius_sdk.session import ClausiusSession

    s = ClausiusSession.get()
    if s:
        s.log_params(params)


def log_artifact(name: str, path: str, **metadata) -> None:
    from nemo_skills.clausius_sdk.session import ClausiusSession

    s = ClausiusSession.get()
    if s:
        s.log_artifact(name, path, **metadata)
