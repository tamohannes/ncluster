"""Test that any exception raised by a tool body is converted to a
structured error envelope before reaching FastMCP.

A bare exception from a tool handler is a worst-case failure mode for
the MCP transport: FastMCP turns it into a JSON-RPC error response, but
if the exception happens after we've started writing or while the
message frame is mid-flight, the stdio stream can desync and Cursor
closes the connection. The ``_isolate_tool`` wrapper catches every
Exception and returns a structured ``{"status": "error", ...}`` dict.
"""

import io
import sys

import pytest

from mcp_server import _isolate_tool, _install_stdout_safety, _restore_stdout


@pytest.mark.mcp
class TestIsolateTool:
    async def test_passes_normal_return_through(self):
        @_isolate_tool
        async def ok_tool():
            return {"status": "ok", "x": 1}

        result = await ok_tool()
        assert result == {"status": "ok", "x": 1}

    async def test_catches_exception_and_returns_structured_error(self):
        @_isolate_tool
        async def bad_tool():
            raise RuntimeError("boom")

        result = await bad_tool()
        assert isinstance(result, dict)
        assert result["status"] == "error"
        assert "RuntimeError" in result["error"]
        assert "boom" in result["error"]

    async def test_includes_exception_class_in_error(self):
        @_isolate_tool
        async def value_error_tool():
            raise ValueError("nope")

        result = await value_error_tool()
        assert "ValueError: nope" in result["error"]


@pytest.mark.mcp
class TestStdoutSafety:
    def test_install_redirects_print_to_stderr(self, capsys):
        _install_stdout_safety()
        try:
            print("from-tool-handler")
            captured = capsys.readouterr()
            assert captured.out == ""
            assert "from-tool-handler" in captured.err
        finally:
            _restore_stdout()

    def test_restore_brings_back_normal_print(self, capsys):
        _install_stdout_safety()
        _restore_stdout()
        print("normal again")
        captured = capsys.readouterr()
        assert "normal again" in captured.out


@pytest.mark.mcp
class TestEveryRegisteredToolIsIsolated:
    """Smoke check: registering a tool through the patched ``mcp.tool``
    should auto-isolate the function so a runtime exception can't leak."""

    async def test_registered_tool_is_wrapped(self):
        # Re-import to grab the patched mcp.tool used in production.
        import mcp_server

        @mcp_server.mcp.tool()
        async def _bad_smoke_tool():
            raise KeyError("bad-key")

        # The wrapper used to register stores the wrapped function on the
        # tool registry; calling the underlying coroutine directly should
        # surface the structured error envelope.
        result = await _bad_smoke_tool()
        assert isinstance(result, dict)
        assert result["status"] == "error"
        assert "KeyError" in result["error"]
