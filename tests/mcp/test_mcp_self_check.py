"""Tests for the mcp_self_check tool, per-tool counters, and per-PID
log tagging.

These give agents and operators a way to tell 'MCP dead' from 'MCP
slow' from 'cluster slow' without grepping the shared log file.
"""

import logging
import os
import time
from unittest.mock import patch

import pytest

import mcp_server
from mcp_server import _PidTagFilter, _record_tool_call, _TOOL_STATS, _TOOL_STATS_LOCK, mcp_self_check


@pytest.fixture(autouse=True)
def _reset_tool_stats():
    with _TOOL_STATS_LOCK:
        _TOOL_STATS.clear()
    yield
    with _TOOL_STATS_LOCK:
        _TOOL_STATS.clear()


@pytest.mark.mcp
class TestSelfCheck:
    async def test_returns_expected_fields(self):
        with patch("mcp_server._probe_leader", return_value=True):
            result = await mcp_self_check()
        for key in ("pid", "uptime_sec", "last_activity_sec_ago",
                    "follower_active", "leader_url",
                    "leader_reachable", "tool_stats"):
            assert key in result, f"missing key {key!r}"
        assert result["pid"] == os.getpid()
        assert result["uptime_sec"] >= 0
        assert isinstance(result["tool_stats"], dict)

    async def test_reports_leader_unreachable(self):
        with patch("mcp_server._probe_leader", return_value=False):
            result = await mcp_self_check()
        assert result["leader_reachable"] is False

    async def test_tool_stats_includes_per_tool_counters(self):
        _record_tool_call("alpha", 12.0, errored=False)
        _record_tool_call("alpha", 18.0, errored=False)
        _record_tool_call("alpha", 80.0, errored=True)
        _record_tool_call("beta", 5.0, errored=False)

        with patch("mcp_server._probe_leader", return_value=True):
            result = await mcp_self_check()

        stats = result["tool_stats"]
        assert stats["alpha"]["calls"] == 3
        assert stats["alpha"]["errors"] == 1
        assert stats["alpha"]["p50_ms"] is not None
        assert stats["alpha"]["p99_ms"] is not None
        assert stats["beta"]["calls"] == 1
        assert stats["beta"]["errors"] == 0


@pytest.mark.mcp
class TestRecordToolCall:
    def test_samples_are_capped(self):
        for i in range(150):
            _record_tool_call("loadtest", float(i), errored=False)
        with _TOOL_STATS_LOCK:
            samples = _TOOL_STATS["loadtest"]["samples_ms"]
        # The cap is 100; the most recent samples are kept.
        assert len(samples) == 100
        assert samples[-1] == 149.0

    def test_error_count_increments(self):
        _record_tool_call("err", 1.0, errored=True)
        _record_tool_call("err", 1.0, errored=True)
        _record_tool_call("err", 1.0, errored=False)
        with _TOOL_STATS_LOCK:
            rec = _TOOL_STATS["err"]
        assert rec["calls"] == 3
        assert rec["errors"] == 2


@pytest.mark.mcp
class TestPidLogTag:
    def test_filter_prepends_pid_to_message(self):
        # Test the filter directly because server.mcp has propagate=False
        # (set in app._configure_logging), so caplog can't see the
        # records — but we don't need to: the filter mutates record.msg
        # in place before any handler runs.
        flt = _PidTagFilter()
        rec = logging.LogRecord(
            "server.mcp", logging.INFO, "x.py", 1, "raw test msg", None, None,
        )
        flt.filter(rec)
        assert rec.msg.startswith(f"[mcp:pid={os.getpid()}]")
        assert "raw test msg" in rec.msg

    def test_filter_attached_to_module_logger(self):
        log = logging.getLogger("server.mcp")
        assert any(isinstance(f, _PidTagFilter) for f in log.filters), \
            "_PidTagFilter must be attached to the server.mcp logger"
