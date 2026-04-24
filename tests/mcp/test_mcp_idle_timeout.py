"""Tests for the MCP idle-timeout watchdog.

Even with the singleton lock, an MCP process can end up idle indefinitely
if Cursor crashes without a clean shutdown. The idle watchdog measures
the time since the last successful tool call and self-terminates the
process via os._exit(0) when it crosses ``CLAUSIUS_MCP_IDLE_SHUTDOWN_SEC``.
"""

import time

import pytest

import mcp_server
from mcp_server import (
    _record_activity,
    _idle_shutdown_step,
    _api_async,
)


@pytest.mark.mcp
class TestIdleShutdownStep:
    def test_returns_false_immediately_after_activity(self):
        _record_activity()
        assert not _idle_shutdown_step(idle_threshold_sec=60.0)

    def test_returns_true_when_threshold_exceeded(self, monkeypatch):
        # Force last_activity backwards so the step thinks we've been
        # idle for a long time without sleeping.
        with mcp_server._activity_lock:
            mcp_server._last_activity_ts = time.monotonic() - 100.0
        assert _idle_shutdown_step(idle_threshold_sec=10.0)

    def test_record_activity_resets_to_now(self, monkeypatch):
        with mcp_server._activity_lock:
            mcp_server._last_activity_ts = time.monotonic() - 100.0
        _record_activity()
        # After recording, we are no longer idle even with a tight
        # threshold.
        assert not _idle_shutdown_step(idle_threshold_sec=1.0)


@pytest.mark.mcp
class TestApiAsyncRecordsActivity:
    async def test_api_call_marks_activity(self, monkeypatch):
        # Force last_activity into the past so we can detect _api_async
        # bumping it forward.
        with mcp_server._activity_lock:
            mcp_server._last_activity_ts = time.monotonic() - 500.0

        # Patch the underlying sync _api so we don't hit the real Flask
        # app during a unit test.
        from unittest.mock import patch

        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = await _api_async("GET", "/api/anything")

        assert result == {"status": "ok"}
        # Threshold of 60 s must NOT trip — the call we just made
        # bumped the activity timestamp to "now".
        assert not _idle_shutdown_step(idle_threshold_sec=60.0)
