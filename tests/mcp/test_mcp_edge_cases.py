"""MCP edge case tests for boundary inputs and error handling."""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import list_jobs, get_job_log, get_history, jobs_summary, mount_cluster, clear_failed


@pytest.mark.mcp
class TestBoundaryParameters:
    def test_zero_lines_log(self):
        with patch.object(mcp_server, "get_job_log_files_cached",
                          return_value={"files": [{"label": "main", "path": "/a.log"}]}), \
             patch.object(mcp_server, "resolve_mounted_path", return_value=None), \
             patch.object(mcp_server, "fetch_log_tail", return_value=""):
            result = get_job_log("dfw", "1", lines=0)
        assert isinstance(result, str)

    def test_negative_history_limit(self):
        with patch("server.db.get_history", return_value=[]):
            result = get_history(limit=-1)
        assert isinstance(result, list)

    def test_large_history_limit(self):
        with patch("server.db.get_history", return_value=[]):
            result = get_history(limit=999999)
        assert isinstance(result, list)


@pytest.mark.mcp
class TestEmptyData:
    def test_list_jobs_empty_cluster(self):
        with patch.object(mcp_server, "_get_cluster_jobs",
                          return_value={"status": "ok", "jobs": []}):
            result = list_jobs(cluster="dfw")
        assert result == []

    def test_list_jobs_all_empty(self):
        with patch.object(mcp_server, "_get_all_jobs_snapshot",
                          return_value={"dfw": {"status": "ok", "jobs": []}}):
            result = list_jobs()
        assert result == []

    def test_summary_with_unreachable_only(self):
        with patch.object(mcp_server, "_get_all_jobs_snapshot",
                          return_value={"c1": {"status": "error"}}):
            result = jobs_summary()
        assert "unreachable" in result
        assert "Total: 0" in result


@pytest.mark.mcp
class TestMountEdgeCases:
    def test_invalid_action_no_api_call(self):
        result = mount_cluster("c1", "bad")
        assert result["status"] == "error"

    def test_valid_mount_calls_script(self):
        with patch.object(mcp_server, "run_mount_script", return_value=(True, "OK")), \
             patch.object(mcp_server, "all_mount_status", return_value={}):
            result = mount_cluster("c1", "mount")
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestClearEdgeCases:
    def test_clear_failed_calls_dismiss(self):
        with patch.object(mcp_server, "dismiss_by_state_prefix") as mock:
            result = clear_failed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()

    def test_clear_completed_calls_dismiss(self):
        from mcp_server import clear_completed
        with patch.object(mcp_server, "dismiss_by_state_prefix") as mock:
            result = clear_completed("c1")
        assert result["status"] == "ok"
        mock.assert_called_once()
