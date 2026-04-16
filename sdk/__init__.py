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

"""Clausius SDK — structured experiment telemetry for NeMo-Skills.

Quick start (automatic — no user code needed):
    Set CLAUSIUS_URL and/or CLAUSIUS_SPOOL_DIR env vars before running
    any `ns` command.  The SDK hooks into the shared pipeline utilities
    and emits run/job/metric events automatically.

Manual metric logging from eval/training code:
    from nemo_skills.clausius_sdk.metrics import log_metric, log_artifact
    log_metric("accuracy", 0.84, step=100)
    log_artifact("metrics.json", "/path/to/metrics.json")
"""

from nemo_skills.clausius_sdk.session import ClausiusSession

__all__ = ["ClausiusSession"]
