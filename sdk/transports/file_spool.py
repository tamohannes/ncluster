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

"""File-spool transport: appends JSONL events to a local file for later replay."""

from __future__ import annotations

import logging
import os

from nemo_skills.clausius_sdk.events import Event
from nemo_skills.clausius_sdk.transports.base import Transport

LOG = logging.getLogger(__name__)


class FileSpoolTransport(Transport):
    """Appends events as newline-delimited JSON to a spool file.

    Always succeeds unless the filesystem is broken; acts as the durable
    fallback when HTTP is unavailable.
    """

    def __init__(self, path: str):
        self._path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._fh = None

    def _ensure_open(self):
        if self._fh is None:
            self._fh = open(self._path, "a", encoding="utf-8")

    def send(self, events: list[Event]) -> bool:
        if not events:
            return True
        try:
            self._ensure_open()
            for ev in events:
                self._fh.write(ev.to_json() + "\n")
            self._fh.flush()
            return True
        except OSError as exc:
            LOG.warning("clausius spool: write failed: %s", exc)
            return False

    def flush(self) -> None:
        if self._fh:
            try:
                self._fh.flush()
            except OSError:
                pass

    def close(self) -> None:
        if self._fh:
            try:
                self._fh.close()
            except OSError:
                pass
            self._fh = None
