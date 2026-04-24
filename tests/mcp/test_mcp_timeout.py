"""Regression tests for the off-thread + timeout dispatch in mcp_server.

These exist because of a real outage: a slow ``where_to_submit`` followed
by an SSH circuit breaker tripping caused Cursor's MCP stdio transport to
close mid-conversation, and Cursor did not auto-reconnect. The root cause
was that FastMCP runs sync tool handlers on its asyncio event loop — so
a blocking SSH call inside a handler stalled the loop, the heartbeat
backed up, and the transport died.

The fix:

  * Every tool is ``async def`` and routes through ``_api_async``.
  * ``_api_async`` off-loads the synchronous Flask call to a worker
    thread via ``anyio.to_thread.run_sync`` and wraps it in
    ``asyncio.wait_for`` with a wall-clock timeout.

The tests below pin both halves of that contract.
"""

import asyncio
import threading
import time
from unittest.mock import patch

import pytest

import mcp_server
from mcp_server import _api_async, _api_text_async, get_partitions


@pytest.mark.mcp
class TestApiAsyncTimeout:
    async def test_returns_structured_error_when_blocking_call_exceeds_timeout(self):
        """A handler that blocks past the timeout must surface a structured
        error rather than letting ``asyncio.wait_for`` raise into FastMCP."""

        def slow_call(*_args, **_kwargs):
            # Simulate a stuck SSH call. The wrapper must give up via the
            # explicit timeout, not by waiting for this to return.
            time.sleep(2.0)
            return {"status": "ok"}

        start = time.monotonic()
        with patch("mcp_server._api", side_effect=slow_call):
            result = await _api_async("GET", "/api/anything", timeout=0.2)
        elapsed = time.monotonic() - start

        assert isinstance(result, dict)
        assert result["status"] == "error"
        assert "timed out" in result["error"]
        # The whole call should return well before the underlying ``slow_call``
        # would have finished, proving the wrapper didn't sit blocked.
        assert elapsed < 1.5, f"wrapper waited {elapsed:.2f}s, expected <1.5s"

    async def test_text_variant_returns_error_string_on_timeout(self):
        def slow_call(*_args, **_kwargs):
            time.sleep(2.0)
            return "fresh log content"

        with patch("mcp_server._api_text", side_effect=slow_call):
            result = await _api_text_async("GET", "/api/log/...", timeout=0.2)

        assert isinstance(result, str)
        assert result.startswith("Error")
        assert "timed out" in result

    async def test_fast_call_returns_normally(self):
        """Sanity check: a fast call returns the underlying value untouched."""
        with patch("mcp_server._api", return_value={"status": "ok", "x": 1}):
            result = await _api_async("GET", "/api/x", timeout=0.5)
        assert result == {"status": "ok", "x": 1}


@pytest.mark.mcp
class TestEventLoopStaysResponsive:
    async def test_concurrent_slow_call_does_not_block_other_tools(self):
        """Two concurrent tool calls — one slow, one fast — must both make
        progress. Pre-fix, the slow one would block the loop and starve the
        fast one. With ``anyio.to_thread.run_sync`` they share the worker
        pool and the fast one returns immediately.

        We share one ``_api`` patch across both tasks (mock.patch is
        process-wide, not thread-local) and branch on the request path so
        each task gets its own response shape. ``threading.Event`` is used
        instead of ``asyncio.Event`` because the dispatcher runs in a
        worker thread, and ``asyncio.Event.set`` is not thread-safe.
        """

        slow_started = threading.Event()

        def dispatch(method, path, **_kwargs):
            if path == "/api/slow":
                slow_started.set()
                time.sleep(1.5)
                return {"status": "ok", "slow": True}
            return {"status": "ok", "fast": True}

        async def run_slow():
            return await _api_async("GET", "/api/slow", timeout=5.0)

        async def run_fast():
            # Wait until the slow call is actually running in a worker
            # thread so we're measuring real concurrency, not serial
            # dispatch. ``threading.Event.wait`` is blocking, so off-load
            # it so the asyncio loop keeps spinning.
            await asyncio.to_thread(slow_started.wait)
            return await _api_async("GET", "/api/fast", timeout=5.0)

        with patch("mcp_server._api", side_effect=dispatch):
            slow_task = asyncio.create_task(run_slow())
            fast_task = asyncio.create_task(run_fast())

            # The fast call must complete well before the slow one — within
            # a small fraction of the slow call's wall time.
            fast_start = time.monotonic()
            fast_result = await fast_task
            fast_elapsed = time.monotonic() - fast_start

            slow_result = await slow_task

        assert fast_result["fast"] is True
        assert fast_elapsed < 1.0, f"fast call took {fast_elapsed:.2f}s; loop was blocked"
        assert slow_result["slow"] is True


@pytest.mark.mcp
class TestToolTimeoutPropagation:
    async def test_tool_returns_timeout_error_dict_when_route_hangs(self):
        """End-to-end: a tool call that hits a hung route returns the
        structured timeout error from the wrapper instead of raising."""

        def slow_call(*_args, **_kwargs):
            time.sleep(2.0)
            return {"status": "ok"}

        # Shrink the wrapper's default budget to keep the test fast.
        with patch.object(mcp_server, "_DEFAULT_TIMEOUT_SEC", 0.2), \
             patch("mcp_server._api", side_effect=slow_call):
            result = await get_partitions(cluster="eos")

        assert isinstance(result, dict)
        assert result["status"] == "error"
        assert "timed out" in result["error"]
