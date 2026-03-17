"""MCP edge case tests for parameter encoding and boundary inputs."""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import list_jobs, get_job_log, get_history, jobs_summary, mount_cluster, clear_failed


@pytest.mark.mcp
class TestUrlEncoding:
    def test_cluster_with_slash(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = {"status": "ok", "jobs": []}
            list_jobs(cluster="cluster/name")
            url = mock.call_args[0][0]
            # urllib.parse.quote encodes / to %2F
            assert "cluster%2Fname" in url or "cluster/name" in url

    def test_job_id_with_special_chars(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = {"status": "ok", "content": "x"}
            get_job_log("c1", "job 123", path="/path with spaces/file.log")
            url = mock.call_args[0][0]
            assert "job%20123" in url
            assert "path+with+spaces" in url or "path%20with%20spaces" in url


@pytest.mark.mcp
class TestBoundaryParameters:
    def test_zero_lines(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = {"status": "ok", "content": ""}
            get_job_log("c1", "1", lines=0)
            url = mock.call_args[0][0]
            assert "lines=0" in url

    def test_negative_limit(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = []
            get_history(limit=-1)
            url = mock.call_args[0][0]
            assert "limit=-1" in url

    def test_very_large_limit(self):
        with patch.object(mcp_server, "_api_get") as mock:
            mock.return_value = []
            get_history(limit=999999)
            url = mock.call_args[0][0]
            assert "limit=999999" in url


@pytest.mark.mcp
class TestEmptyData:
    def test_list_jobs_empty_cluster(self):
        with patch.object(mcp_server, "_api_get",
                          return_value={"status": "ok", "jobs": []}):
            result = list_jobs(cluster="c1")
        assert result == []

    def test_list_jobs_all_empty(self):
        with patch.object(mcp_server, "_api_get",
                          return_value={"c1": {"status": "ok", "jobs": []}}):
            result = list_jobs()
        assert result == []

    def test_summary_with_unreachable_only(self):
        with patch.object(mcp_server, "_api_get",
                          return_value={"c1": {"status": "error"}}):
            result = jobs_summary()
        assert "unreachable" in result
        assert "Total: 0" in result


@pytest.mark.mcp
class TestMountEdgeCases:
    def test_cluster_with_special_chars(self):
        with patch.object(mcp_server, "_api_post") as mock:
            mock.return_value = {"status": "ok"}
            mount_cluster("cluster/name", "mount")
            url = mock.call_args[0][0]
            assert "cluster%2Fname" in url or "cluster/name" in url

    def test_invalid_action_no_api_call(self):
        with patch.object(mcp_server, "_api_post") as mock:
            result = mount_cluster("c1", "bad")
            mock.assert_not_called()
            assert result["status"] == "error"


@pytest.mark.mcp
class TestClearEdgeCases:
    def test_clear_failed_special_cluster(self):
        with patch.object(mcp_server, "_api_post") as mock:
            mock.return_value = {"status": "ok"}
            clear_failed("cluster/name")
            url = mock.call_args[0][0]
            assert "cluster%2Fname" in url or "cluster/name" in url
