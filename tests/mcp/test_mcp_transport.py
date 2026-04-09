"""MCP server import and health check smoke tests.

The old HTTP transport layer (_api_get, _api_post) no longer exists.
This file verifies the MCP server module loads and the health_check tool works.
"""

import pytest

from mcp_server import health_check


@pytest.mark.mcp
class TestMcpImport:
    def test_module_imports(self):
        import mcp_server
        assert hasattr(mcp_server, "mcp")
        assert hasattr(mcp_server, "health_check")

    def test_health_check_returns_ok(self):
        result = health_check()
        assert result["status"] == "ok"
        assert isinstance(result["clusters"], list)
        assert isinstance(result["db"], bool)
