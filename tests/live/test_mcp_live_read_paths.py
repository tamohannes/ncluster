"""Live MCP read-path tests against running app.

Run with: pytest tests/live/test_mcp_live_read_paths.py -m live -v
Requires: clausius running at localhost:7272.
"""

import pytest

from mcp_server import (
    list_jobs, list_log_files, get_job_log,
    get_job_stats, get_history, jobs_summary,
)
from tests.live.helpers.slurm_fixture import LIVE_CLUSTER

pytestmark = [pytest.mark.live, pytest.mark.mcp]


def _skip_if_no_cluster():
    if not LIVE_CLUSTER:
        pytest.skip("No live cluster configured")


class TestMcpLiveRead:
    def test_list_jobs_all(self):
        result = list_jobs()
        assert isinstance(result, list)

    def test_list_jobs_single_cluster(self):
        _skip_if_no_cluster()
        result = list_jobs(cluster=LIVE_CLUSTER)
        assert isinstance(result, list)
        if result and "error" not in result[0]:
            assert all(r.get("cluster") == LIVE_CLUSTER for r in result)

    def test_get_history(self):
        _skip_if_no_cluster()
        result = get_history(cluster=LIVE_CLUSTER, limit=5)
        assert isinstance(result, list)

    def test_get_job_stats_local_error(self):
        result = get_job_stats("local", "99999")
        assert result["status"] == "error"

    def test_jobs_summary(self):
        result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result

    def test_list_log_files_nonexistent(self):
        _skip_if_no_cluster()
        result = list_log_files(LIVE_CLUSTER, "99999999")
        assert "files" in result or "error" in result

    def test_get_job_log_nonexistent(self):
        _skip_if_no_cluster()
        result = get_job_log(LIVE_CLUSTER, "99999999")
        assert isinstance(result, str)
