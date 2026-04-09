"""MCP tool/resource contract tests — direct-import architecture.

Mocks server modules (server.config, server.jobs, server.ssh, etc.)
at the mcp_server helper level. No HTTP mocks.
"""

import pytest
from unittest.mock import patch, MagicMock

import mcp_server
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


# ── _slim_job crash_detected / exit_code ─────────────────────────────────────

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
        result = health_check()
        assert result["status"] == "ok"
        assert "clusters" in result
        assert isinstance(result["clusters"], list)

    def test_reports_db_existence(self):
        result = health_check()
        assert "db" in result
        assert isinstance(result["db"], bool)


# ── list_jobs ────────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListJobs:
    def test_all_clusters_flattened(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]},
            "c2": {"status": "ok", "jobs": [{"jobid": "2", "state": "PENDING"}]},
        }
        with patch.object(mcp_server, "_get_all_jobs_snapshot", return_value=snapshot):
            result = list_jobs()
        assert len(result) == 2
        assert {r["cluster"] for r in result} == {"c1", "c2"}

    def test_single_cluster(self):
        cdata = {"status": "ok", "jobs": [{"jobid": "1", "state": "RUNNING"}]}
        with patch.object(mcp_server, "_get_cluster_jobs", return_value=cdata), \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = list_jobs(cluster="c1")
        assert len(result) == 1
        assert result[0]["cluster"] == "c1"

    def test_unknown_cluster_returns_error(self):
        result = list_jobs(cluster="nonexistent-xyz")
        assert len(result) == 1
        assert "error" in result[0]

    def test_cluster_error_propagated(self):
        cdata = {"status": "error", "error": "unreachable"}
        with patch.object(mcp_server, "_get_cluster_jobs", return_value=cdata), \
             patch.dict(mcp_server.CLUSTERS, {"bad": {}}):
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
        with patch.object(mcp_server, "_get_all_jobs_snapshot", return_value=snapshot):
            result = list_jobs(project="alpha")
        assert len(result) == 1
        assert result[0]["jobid"] == "1"

    def test_returns_list(self):
        with patch.object(mcp_server, "_get_all_jobs_snapshot", return_value={}):
            result = list_jobs()
        assert isinstance(result, list)


# ── list_log_files ───────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestListLogFiles:
    def test_passthrough(self):
        resp = {"status": "ok", "files": [{"label": "main", "path": "/x"}], "dirs": []}
        with patch.object(mcp_server, "get_job_log_files_cached", return_value=resp):
            result = list_log_files("c1", "123")
        assert result["status"] == "ok"
        assert len(result["files"]) == 1


# ── get_job_log ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobLog:
    def test_returns_content(self):
        files = {"files": [{"label": "main", "path": "/log.out"}], "dirs": []}
        with patch.object(mcp_server, "get_job_log_files_cached", return_value=files), \
             patch.object(mcp_server, "fetch_log_tail", return_value="log line 1\nlog line 2"):
            result = get_job_log("c1", "123")
        assert isinstance(result, str)
        assert "log line 1" in result

    def test_no_files_returns_error(self):
        with patch.object(mcp_server, "get_job_log_files_cached", return_value={"files": [], "dirs": []}):
            result = get_job_log("c1", "123")
        assert "Error" in result

    def test_with_explicit_path(self):
        with patch.object(mcp_server, "resolve_mounted_path", return_value=None), \
             patch.object(mcp_server, "fetch_log_tail", return_value="content") as mock_tail:
            result = get_job_log("c1", "1", path="/a/b.log", lines=50)
        mock_tail.assert_called_once_with("c1", "/a/b.log", 50)
        assert result == "content"


# ── get_job_stats ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetJobStats:
    def test_returns_dict(self):
        resp = {"status": "ok", "job_id": "1", "state": "RUNNING", "gpus": []}
        with patch.object(mcp_server, "get_job_stats_cached", return_value=resp):
            result = get_job_stats("c1", "1")
        assert isinstance(result, dict)
        assert result["status"] == "ok"


# ── get_history ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetHistory:
    def test_returns_list(self):
        rows = [{"job_id": "1", "job_name": "alpha_x"}, {"job_id": "2", "job_name": "beta_y"}]
        with patch("server.db.get_history", return_value=rows):
            result = get_history()
        assert isinstance(result, list)
        assert len(result) == 2

    def test_enriches_project(self):
        rows = [{"job_id": "1", "job_name": "alpha_eval", "project": ""}]
        with patch("server.db.get_history", return_value=rows):
            result = get_history()
        assert result[0]["project"] == "alpha"

    def test_passes_filters(self):
        with patch("server.db.get_history", return_value=[]) as mock_hist:
            get_history(cluster="c1", project="alpha", limit=10)
        mock_hist.assert_called_once_with("c1", 10, project="alpha")


# ── cancel_job ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJob:
    def test_success(self):
        with patch.object(mcp_server, "ssh_run_with_timeout", return_value=("", "")), \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = cancel_job("c1", "123")
        assert result["status"] == "ok"

    def test_unknown_cluster(self):
        result = cancel_job("nonexistent-xyz", "123")
        assert result["status"] == "error"

    def test_ssh_error(self):
        with patch.object(mcp_server, "ssh_run_with_timeout", side_effect=RuntimeError("refused")), \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = cancel_job("c1", "123")
        assert result["status"] == "error"
        assert "refused" in result["error"]

    def test_local_kill(self):
        with patch("os.kill") as mock_kill:
            result = cancel_job("local", "12345")
        mock_kill.assert_called_once_with(12345, 15)
        assert result["status"] == "ok"


# ── cancel_jobs ──────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestCancelJobs:
    def test_batch_success(self):
        with patch.object(mcp_server, "ssh_run_with_timeout", return_value=("", "")), \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = cancel_jobs("c1", ["100", "200", "300"])
        assert result["status"] == "ok"
        assert result["cancelled"] == 3

    def test_no_valid_ids(self):
        with patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = cancel_jobs("c1", ["bad", ""])
        assert result["status"] == "error"

    def test_unknown_cluster(self):
        result = cancel_jobs("nonexistent-xyz", ["100"])
        assert result["status"] == "error"


# ── jobs_summary ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestJobsSummary:
    def test_summary_format(self):
        snapshot = {
            "c1": {"status": "ok", "jobs": [
                {"state": "RUNNING"}, {"state": "PENDING"},
            ]},
            "c2": {"status": "error"},
        }
        with patch.object(mcp_server, "_get_all_jobs_snapshot", return_value=snapshot):
            result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result
        assert "1 running" in result
        assert "1 pending" in result
        assert "c2: unreachable" in result

    def test_all_idle(self):
        with patch.object(mcp_server, "_get_all_jobs_snapshot", return_value={"c1": {"status": "ok", "jobs": []}}):
            result = jobs_summary()
        assert "idle" in result
        assert "Total: 0 running" in result


# ── get_mounts ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestGetMounts:
    def test_returns_dict(self):
        with patch.object(mcp_server, "all_mount_status", return_value={"c1": {"mounted": True}}):
            result = get_mounts()
        assert result["status"] == "ok"
        assert "mounts" in result


# ── mount_cluster ────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestMountCluster:
    def test_mount(self):
        with patch.object(mcp_server, "run_mount_script", return_value=(True, "Mounted")), \
             patch.object(mcp_server, "all_mount_status", return_value={}):
            result = mount_cluster("c1", "mount")
        assert result["status"] == "ok"

    def test_unmount(self):
        with patch.object(mcp_server, "run_mount_script", return_value=(True, "Unmounted")), \
             patch.object(mcp_server, "all_mount_status", return_value={}):
            result = mount_cluster("c1", "unmount")
        assert result["status"] == "ok"

    def test_invalid_action(self):
        result = mount_cluster("c1", "restart")
        assert result["status"] == "error"
        assert "mount" in result["error"]

    def test_script_failure(self):
        with patch.object(mcp_server, "run_mount_script", return_value=(False, "fuse error")):
            result = mount_cluster("c1", "mount")
        assert result["status"] == "error"
        assert "fuse" in result["error"]


# ── clear_failed ─────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearFailed:
    def test_success(self):
        with patch.object(mcp_server, "dismiss_by_state_prefix"):
            result = clear_failed("c1")
        assert result["status"] == "ok"


# ── clear_completed ──────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestClearCompleted:
    def test_success(self):
        with patch.object(mcp_server, "dismiss_by_state_prefix"):
            result = clear_completed("c1")
        assert result["status"] == "ok"


# ── run_script ───────────────────────────────────────────────────────────────

@pytest.mark.mcp
class TestRunScript:
    def test_success(self):
        with patch.object(mcp_server, "ssh_run_with_timeout", return_value=("hello\n", "")), \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = run_script("c1", "print('hello')")
        assert result["status"] == "ok"
        assert result["stdout"] == "hello\n"

    def test_unknown_cluster(self):
        result = run_script("nonexistent-xyz", "print(1)")
        assert result["status"] == "error"

    def test_local_not_supported(self):
        result = run_script("local", "print(1)")
        assert result["status"] == "error"

    def test_invalid_interpreter(self):
        with patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            result = run_script("c1", "print(1)", interpreter="ruby")
        assert result["status"] == "error"

    def test_timeout_clamped(self):
        with patch.object(mcp_server, "ssh_run_with_timeout", return_value=("", "")) as mock_ssh, \
             patch.dict(mcp_server.CLUSTERS, {"c1": {}}):
            run_script("c1", "x", timeout=999)
        _, kwargs = mock_ssh.call_args
        assert kwargs["timeout_sec"] == 300
