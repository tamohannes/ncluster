"""MCP edge case tests for boundary inputs and error handling (in-process Flask architecture)."""

import pytest
from unittest.mock import patch

from mcp_server import list_jobs, get_job_log, get_history, jobs_summary, mount_cluster, clear_failed, clear_completed


@pytest.mark.mcp
class TestBoundaryParameters:
    async def test_zero_lines_log(self):
        with patch("mcp_server._api", return_value={"status": "ok", "content": ""}):
            result = await get_job_log("dfw", "1", lines=0)
        assert isinstance(result, str)

    async def test_negative_history_limit(self):
        with patch("mcp_server._api", return_value=[]):
            result = await get_history(limit=-1)
        assert isinstance(result, list)

    async def test_large_history_limit(self):
        with patch("mcp_server._api", return_value=[]):
            result = await get_history(limit=999999)
        assert isinstance(result, list)


@pytest.mark.mcp
class TestEmptyData:
    async def test_list_jobs_empty_cluster(self):
        with patch("mcp_server._api", return_value={"status": "ok", "jobs": []}):
            result = await list_jobs(cluster="dfw")
        assert result == []

    async def test_list_jobs_all_empty(self):
        with patch("mcp_server._api", return_value={"dfw": {"status": "ok", "jobs": []}}):
            result = await list_jobs()
        assert result == []

    async def test_summary_with_unreachable_only(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 0 running, 0 pending, 0 failed\nc1: unreachable"}):
            result = await jobs_summary()
        assert "unreachable" in result
        assert "Total: 0" in result


@pytest.mark.mcp
class TestMountEdgeCases:
    async def test_invalid_action_no_api_call(self):
        result = await mount_cluster("c1", "bad")
        assert result["status"] == "error"

    async def test_valid_mount_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "OK", "mounts": {}}) as mock:
            result = await mount_cluster("c1", "mount")
        assert result["status"] == "ok"
        mock.assert_called_once()


@pytest.mark.mcp
class TestClearEdgeCases:
    async def test_clear_failed_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            result = await clear_failed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()

    async def test_clear_completed_calls_api(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            result = await clear_completed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()
