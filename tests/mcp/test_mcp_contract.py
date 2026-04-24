"""MCP tool/resource contract tests — in-process Flask architecture.

Mocks ``mcp_server._api`` (the synchronous Flask test_client wrapper) to
verify each tool sends the right HTTP call and returns the right shape.
``_api_async`` runs the lambda in a worker thread, so the patch on the
underlying sync ``_api`` still intercepts every call.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    health_check,
    list_jobs, list_log_files, get_job_log,
    get_job_stats, get_history, cancel_job, cancel_jobs,
    jobs_summary, _slim_job,
    get_mounts, mount_cluster, clear_failed, clear_completed,
    run_script,
)


# ── _slim_job ────────────────────────────────────────────────────────────────
#
# ``_slim_job`` is a pure transform — no async path involved.

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
    async def test_returns_ok(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 1}):
            result = await health_check()
        assert result["status"] == "ok"

    async def test_reports_service_status(self):
        with patch("mcp_server._api", return_value={"status": "ok", "board_version": 1}):
            result = await health_check()
        assert "service" in result


# ── list_jobs ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListJobs:
    async def test_all_clusters_flattened(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]},
            "c2": {"status": "ok", "jobs": [{"jobid": "2", "state": "PENDING"}]},
        }
        with patch("mcp_server._api", return_value=snapshot):
            result = await list_jobs()
        assert len(result) == 2
        assert {r["cluster"] for r in result} == {"c1", "c2"}

    async def test_single_cluster(self):
        cdata = {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]}
        with patch("mcp_server._api", return_value=cdata):
            result = await list_jobs(cluster="c1")
        assert len(result) == 1
        assert result[0]["cluster"] == "c1"

    async def test_cluster_error_propagated(self):
        cdata = {"status": "error", "error": "unreachable"}
        with patch("mcp_server._api", return_value=cdata):
            result = await list_jobs(cluster="bad")
        assert len(result) == 1
        assert "error" in result[0]

    async def test_project_filter(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [
                {"jobid": "1", "state": "RUNNING", "project": "alpha"},
                {"jobid": "2", "state": "RUNNING", "project": "beta"},
            ]},
        }
        with patch("mcp_server._api", return_value=snapshot):
            result = await list_jobs(project="alpha")
        assert len(result) == 1
        assert result[0]["jobid"] == "1"

    async def test_returns_list(self):
        with patch("mcp_server._api", return_value={}):
            result = await list_jobs()
        assert isinstance(result, list)


# ── list_log_files ───────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListLogFiles:
    async def test_passthrough(self):
        resp = {"status": "ok", "files": [{"label": "main", "path": "/x"}], "dirs": []}
        with patch("mcp_server._api", return_value=resp):
            result = await list_log_files("c1", "123")
        assert result["status"] == "ok"
        assert len(result["files"]) == 1


# ── get_job_log ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobLog:
    async def test_returns_content(self):
        resp = {"status": "ok", "content": "log line 1\nlog line 2"}
        with patch("mcp_server._api", return_value=resp):
            result = await get_job_log("c1", "123")
        assert isinstance(result, str)
        assert "log line 1" in result

    async def test_error_returned(self):
        resp = {"status": "error", "error": "No log files found"}
        with patch("mcp_server._api", return_value=resp):
            result = await get_job_log("c1", "123")
        assert "Error" in result


# ── get_job_stats ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobStats:
    async def test_returns_dict(self):
        resp = {"status": "ok", "job_id": "1", "state": "RUNNING", "gpus": []}
        with patch("mcp_server._api", return_value=resp):
            result = await get_job_stats("c1", "1")
        assert isinstance(result, dict)
        assert result["status"] == "ok"


# ── get_history ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetHistory:
    async def test_returns_list(self):
        rows = [{"job_id": "1", "job_name": "alpha_x"}, {"job_id": "2", "job_name": "beta_y"}]
        with patch("mcp_server._api", return_value=rows):
            result = await get_history()
        assert isinstance(result, list)
        assert len(result) == 2

    async def test_passes_params(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            await get_history(cluster="c1", project="alpha", state="FAILED", limit=10)
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
    async def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = await cancel_job("c1", "123")
        assert result["status"] == "ok"

    async def test_error(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "refused"}):
            result = await cancel_job("c1", "123")
        assert result["status"] == "error"


# ── cancel_jobs ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJobs:
    async def test_batch_success(self):
        with patch("mcp_server._api", return_value={"status": "ok", "cancelled": 3, "cancelled_ids": ["100", "200", "300"]}):
            result = await cancel_jobs("c1", ["100", "200", "300"])
        assert result["status"] == "ok"

    async def test_batch_partial(self):
        with patch("mcp_server._api", return_value={
            "status": "partial", "cancelled": 1, "cancelled_ids": ["100"],
            "failed_ids": ["200"], "errors": ["200: already gone"],
        }):
            result = await cancel_jobs("c1", ["100", "200"])
        assert result["status"] == "partial"


# ── jobs_summary ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestJobsSummary:
    async def test_summary_format(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 1 running, 1 pending, 0 failed\nc1: 1 running, 1 pending\nc2: unreachable"}):
            result = await jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result
        assert "1 running" in result

    async def test_all_idle(self):
        with patch("mcp_server._api", return_value={"status": "ok", "summary": "Total: 0 running, 0 pending, 0 failed\nc1: idle"}):
            result = await jobs_summary()
        assert "idle" in result


# ── get_mounts ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetMounts:
    async def test_returns_dict(self):
        with patch("mcp_server._api", return_value={"status": "ok", "mounts": {"c1": {"mounted": True}}}):
            result = await get_mounts()
        assert result["status"] == "ok"
        assert "mounts" in result


# ── mount_cluster ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestMountCluster:
    async def test_mount(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "Mounted", "mounts": {}}):
            result = await mount_cluster("c1", "mount")
        assert result["status"] == "ok"

    async def test_unmount(self):
        with patch("mcp_server._api", return_value={"status": "ok", "message": "Unmounted", "mounts": {}}):
            result = await mount_cluster("c1", "unmount")
        assert result["status"] == "ok"

    async def test_invalid_action(self):
        result = await mount_cluster("c1", "restart")
        assert result["status"] == "error"
        assert "mount" in result["error"]

    async def test_script_failure(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "fuse error"}):
            result = await mount_cluster("c1", "mount")
        assert result["status"] == "error"


# ── clear_failed ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearFailed:
    async def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = await clear_failed("c1")
        assert result["status"] == "ok"


# ── clear_completed ──────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearCompleted:
    async def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = await clear_completed("c1")
        assert result["status"] == "ok"


# ── run_script ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestRunScript:
    async def test_success(self):
        with patch("mcp_server._api", return_value={"status": "ok", "stdout": "hello\n", "stderr": ""}):
            result = await run_script("c1", "print('hello')")
        assert result["status"] == "ok"
        assert result["stdout"] == "hello\n"

    async def test_error(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "Unknown cluster"}):
            result = await run_script("nonexistent", "print(1)")
        assert result["status"] == "error"

    async def test_invalid_interpreter(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "interpreter must be one of: bash, python, python3, sh"}):
            result = await run_script("c1", "print(1)", interpreter="ruby")
        assert result["status"] == "error"
