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

"""Abstract transport interface for Clausius SDK event delivery."""

from __future__ import annotations

from abc import ABC, abstractmethod

from nemo_skills.clausius_sdk.events import Event


class Transport(ABC):
    """Base class for event delivery backends."""

    @abstractmethod
    def send(self, events: list[Event]) -> bool:
        """Send a batch of events. Returns True on success."""

    def flush(self) -> None:
        """Flush any buffered events. Called on session shutdown."""

    def close(self) -> None:
        """Release resources. Called once when the session ends."""
