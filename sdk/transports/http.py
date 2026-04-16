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

"""HTTP transport: posts batched JSON events to a Clausius ingest endpoint."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

from nemo_skills.clausius_sdk.events import Event
from nemo_skills.clausius_sdk.transports.base import Transport

LOG = logging.getLogger(__name__)

_DEFAULT_TIMEOUT_SEC = 5
_MAX_RETRIES = 2


class HttpTransport(Transport):
    """Sends event batches via HTTP POST with bearer-token auth.

    Uses only stdlib (urllib) so no extra dependencies are needed inside
    NeMo-Skills containers.
    """

    def __init__(self, url: str, token: str = "", timeout: int = _DEFAULT_TIMEOUT_SEC):
        self._url = url.rstrip("/")
        self._token = token
        self._timeout = timeout

    def send(self, events: list[Event]) -> bool:
        if not events:
            return True
        body = json.dumps([e.to_dict() for e in events], default=str).encode()
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        req = urllib.request.Request(
            f"{self._url}/api/sdk/events",
            data=body,
            headers=headers,
            method="POST",
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    if resp.status < 300:
                        return True
                    LOG.warning("clausius http: status %d on attempt %d", resp.status, attempt)
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                LOG.debug("clausius http: attempt %d failed: %s", attempt, exc)
        return False
