"""MCP tool/resource contract tests — HTTP proxy architecture.

Mocks mcp_server._api to verify each tool sends the right HTTP call
and returns the right shape.
"""

import pytest
from unittest.mock import patch, call

from mcp_server import (
    health_check,
    list_jobs, list_log_files, get_job_log,
    get_job_stats, get_history, cancel_job, cancel_jobs,
    jobs_summary, _slim_job,
    get_mounts, mount_cluster, clear_failed, clear_completed,
    run_script,
)


# ── _slim_job ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestSlimJob:
    def test_includes_cluster(self):
        result = _slim_job("cluster-a", {"jobid": "1", "name": "eval"})
        assert result["cluster"] == "cluster-a"
        assert result["jobid"] == "1"

    def test_strips_empty_fields(self):
        result = _slim_job("c", {"jobid": "1", "name": "", "reason": None, "depends_on": []})
        assert "name" not in result
        assert "reason" not in result
        assert "depends_on" not in result

    def test_keeps_nonempty_fields(self):
        result = _slim_job("c", {"jobid": "1", "progress": 45, "state": "RUNNING"})
        assert result["progress"] == 45
        assert result["state"] == "RUNNING"


@pytest.mark.mcp
class TestSlimJobCrashFields:
    def test_preserves_crash_detected(self):
        result = _slim_job("c1", {"jobid": "1", "state": "RUNNING", "crash_detected": "OOM killed"})
        assert result["crash_detected"] == "OOM killed"

    def test_preserves_exit_code(self):
        result = _slim_job("c1", {"jobid": "1", "state": "FAILED", "exit_code": "1:0"})
        assert result["exit_code"] == "1:0"

    def test_omits_when_absent(self):
        result = _slim_job("c1", {"jobid": "1", "state": "RUNNING"})
        assert "crash_detected" not in result
        assert "exit_code" not in result

    def test_omits_when_empty(self):
        result = _slim_job("c1", {"jobid": "1", "crash_detected": "", "exit_code": ""})
        assert "crash_detected" not in result
        assert "exit_code" not in result


# ── health_check ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestHealthCheck:
    def test_returns_ok(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 1}):
            result = health_check()
        assert result["status"] == "ok"

    def test_reports_service_status(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 1}):
            result = health_check()
        assert "service" in result


# ── list_jobs ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListJobs:
    def test_all_clusters_flattened(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]},
            "c2": {"status": "ok", "jobs": [{"jobid": "2", "state": "PENDING"}]},
        }
        with patch("mcp_server._api", return_value=snapshot):
            result = list_jobs()
        assert len(result) == 2
        assert {r["cluster"] for r in result} == {"c1", "c2"}

    def test_single_cluster(self):
        cdata = {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]}
        with patch("mcp_server._api", return_value=cdata):
            result = list_jobs(cluster="c1")
        assert len(result) == 1
        assert result[0]["cluster"] == "c1"

    def test_cluster_error_propagated(self):
        cdata = {"status": "error", "error": "unreachable"}
        with patch("mcp_server._api", return_value=cdata):
            result = list_jobs(cluster="bad")
        assert len(result) == 1
        assert "error" in result[0]

    def test_project_filter(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [
                {"jobid": "1", "state": "RUNNING", "project": "alpha"},
                {"jobid": "2", "state": "RUNNING", "project": "beta"},
            ]},
        }
        with patch("mcp_server._api", return_value=snapshot):
            result = list_jobs(project="alpha")
        assert len(result) == 1
        assert result[0]["jobid"] == "1"

    def test_returns_list(self):
        with patch("mcp_server._api", return_value={}):
            result = list_jobs()
        assert isinstance(result, list)


# ── list_log_files ───────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListLogFiles:
    def test_passthrough(self):
        resp = {"status": "ok", "files": [{"label": "main", "path": "/x"}], "dirs": []}
        with patch("mcp_server._api", return_value=resp):
            result = list_log_files("c1", "123")
        assert result["status"] == "ok"
        assert len(result["files"]) == 1


# ── get_job_log ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobLog:
    def test_returns_content(self):
        resp = {"status": "ok", "content": "log line 1\nlog line 2"}
        with patch("mcp_server._api", return_value=resp):
            result = get_job_log("c1", "123")
        assert isinstance(result, str)
        assert "log line 1" in result

    def test_error_returned(self):
        resp = {"status": "error", "error": "No log files found"}
        with patch("mcp_server._api", return_value=resp):
            result = get_job_log("c1", "123")
        assert "Error" in result


# ── get_job_stats ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobStats:
    def test_returns_dict(self):
        resp = {"status": "ok", "job_id": "1", "state": "RUNNING", "gpus": []}
        with patch("mcp_server._api", return_value=resp):
            result = get_job_stats("c1", "1")
        assert isinstance(result, dict)
        assert result["status"] == "ok"


# ── get_history ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetHistory:
    def test_returns_list(self):
        rows = [{"job_id": "1", "job_name": "alpha_x"}, {"job_id": "2", "job_name": "beta_y"}]
        with patch("mcp_server._api", return_value=rows):
            result = get_history()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_passes_params(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            get_history(cluster="c1", project="alpha", state="FAILED", limit=10)
        mock.assert_called_once()
        _, kwargs = mock.call_args
        # Now uses Werkzeug's test client, which takes `query_string=` instead
        # of httpx's `params=`.
        qs = kwargs["query_string"]
        assert qs["cluster"] == "c1"
        assert qs["project"] == "alpha"
        assert qs["state"] == "FAILED"
        assert qs["limit"] == "10"


# ── cancel_job ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJob:
    def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = cancel_job("c1", "123")
        assert result["status"] == "ok"

    def test_error(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "refused"}):
            result = cancel_job("c1", "123")
        assert result["status"] == "error"


# ── cancel_jobs ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJobs:
    def test_batch_success(self):
        with patch("mcp_server._api", return_value={"status": "ok", "cancelled": 3, "cancelled_ids": ["100", "200", "300"]}):
            result = cancel_jobs("c1", ["100", "200", "300"])
        assert result["status"] == "ok"

    def test_batch_partial(self):
        with patch("mcp_server._api", return_value={
            "status": "partial", "cancelled": 1, "cancelled_ids": ["100"],
            "failed_ids": ["200"], "errors": ["200: already gone"],
        }):
            result = cancel_jobs("c1", ["100", "200"])
        assert result["status"] == "partial"


# ── jobs_summary ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestJobsSummary:
    def test_summary_format(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 1 running, 1 pending, 0 failed\nc1: 1 running, 1 pending\nc2: unreachable"}):
            result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result
        assert "1 running" in result

    def test_all_idle(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 0 running, 0 pending, 0 failed\nc1: idle"}):
            result = jobs_summary()
        assert "idle" in result


# ── get_mounts ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetMounts:
    def test_returns_dict(self):
        with patch("mcp_server._api", return_value={"status": "ok", "mounts": {"c1": {"mounted": True}}}):
            result = get_mounts()
        assert result["status"] == "ok"
        assert "mounts" in result


# ── mount_cluster ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestMountCluster:
    def test_mount(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "Mounted", "mounts": {}}):
            result = mount_cluster("c1", "mount")
        assert result["status"] == "ok"

    def test_unmount(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "Unmounted", "mounts": {}}):
            result = mount_cluster("c1", "unmount")
        assert result["status"] == "ok"

    def test_invalid_action(self):
        result = mount_cluster("c1", "restart")
        assert result["status"] == "error"
        assert "mount" in result["error"]

    def test_script_failure(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "fuse error"}):
            result = mount_cluster("c1", "mount")
        assert result["status"] == "error"


# ── clear_failed ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearFailed:
    def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = clear_failed("c1")
        assert result["status"] == "ok"


# ── clear_completed ──────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearCompleted:
    def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = clear_completed("c1")
        assert result["status"] == "ok"


# ── run_script ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestRunScript:
    def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok", "stdout": "hello\n", "stderr": ""}):
            result = run_script("c1", "print('hello')")
        assert result["status"] == "ok"
        assert result["stdout"] == "hello\n"

    def test_error(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "Unknown cluster"}):
            result = run_script("nonexistent", "print(1)")
        assert result["status"] == "error"

    def test_invalid_interpreter(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "interpreter must be one of: bash, python, python3, sh"}):
            result = run_script("c1", "print(1)", interpreter="ruby")
        assert result["status"] == "error"
