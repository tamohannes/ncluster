"""MCP server import and health check smoke tests (in-process Flask architecture)."""

import pytest
from unittest.mock import patch

from mcp_server import health_check


@pytest.mark.mcp
class TestMcpImport:
    def test_module_imports(self):
        import mcp_server
        assert hasattr(mcp_server, "mcp")
        assert hasattr(mcp_server, "health_check")

    async def test_health_check_returns_ok(self):
        """Healthy in-process Flask response."""
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 42}):
            result = await health_check()
        assert result["status"] == "ok"
        assert result["service"] == "in-process"
        assert result["board_version"] == 42
        assert "follower_active" in result

    async def test_health_check_service_degraded(self):
        """If the in-process API itself returns an error envelope, MCP labels
        the service 'degraded' rather than 'unreachable' — there is no remote
        service to be unreachable any more, just an internal error."""
        with patch("mcp_server._api", return_value={"status": "error", "error": "boom"}):
            result = await health_check()
        assert result["status"] == "ok"
        assert result["service"] == "degraded"
