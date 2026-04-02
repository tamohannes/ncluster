"""Integration tests for cancel, mount, and stats routes."""

import json
import pytest


@pytest.mark.integration
class TestApiCancel:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.post("/api/cancel/nonexistent/123")
        assert resp.status_code == 404

    def test_remote_cancel_ok(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "scancel", ("", ""))
        resp = client.post(f"/api/cancel/{mock_cluster}/12345")
        data = resp.get_json()
        assert data["status"] == "ok"

    def test_local_cancel_invalid_pid(self, client, mock_ssh):
        resp = client.post("/api/cancel/local/99999999")
        data = resp.get_json()
        assert data["status"] == "error"



@pytest.mark.integration
class TestApiStats:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/stats/nonexistent/123")
        assert resp.status_code == 404

    def test_local_unsupported(self, client, mock_ssh):
        resp = client.get("/api/stats/local/123")
        data = resp.get_json()
        assert data["status"] == "error"
        assert "Slurm" in data["error"]

    def test_remote_stats_squeue_path(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "squeue", ("RUNNING|2|16|gpu:8|node01|01:30:00", ""))
        mock_ssh.set(mock_cluster, "sstat", ("", ""))
        resp = client.get(f"/api/stats/{mock_cluster}/12345")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["state"] == "RUNNING"

    def test_remote_stats_scontrol_fallback(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "squeue", ("", ""))
        mock_ssh.set(mock_cluster, "scontrol", (
            "JobId=12345 JobState=COMPLETED NumNodes=1 NumCPUs=8 NodeList=node01 RunTime=02:00:00", ""
        ))
        mock_ssh.set(mock_cluster, "sstat", ("", ""))
        resp = client.get(f"/api/stats/{mock_cluster}/12345")
        data = resp.get_json()
        assert data["status"] == "ok"


@pytest.mark.integration
class TestApiMounts:
    def test_get_all_mounts(self, client, mock_ssh):
        resp = client.get("/api/mounts")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "mounts" in data

    def test_get_single_mount(self, client, mock_ssh, mock_cluster):
        resp = client.get(f"/api/mounts?cluster={mock_cluster}")
        assert resp.status_code == 200

    def test_unknown_cluster_mount(self, client, mock_ssh):
        resp = client.get("/api/mounts?cluster=nonexistent")
        assert resp.status_code == 404

    def test_local_mount_rejected(self, client, mock_ssh):
        resp = client.get("/api/mounts?cluster=local")
        assert resp.status_code == 404


@pytest.mark.integration
class TestApiClearFailed:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.post("/api/clear_failed/nonexistent")
        assert resp.status_code == 404

    def test_clear_failed_ok(self, client, mock_ssh, mock_cluster, db_path):
        from server.db import upsert_job
        upsert_job(mock_cluster, {"jobid": "1", "state": "FAILED"}, terminal=True)
        resp = client.post(f"/api/clear_failed/{mock_cluster}")
        assert resp.get_json()["status"] == "ok"

    def test_clear_completed_ok(self, client, mock_ssh, mock_cluster, db_path):
        from server.db import upsert_job
        upsert_job(mock_cluster, {"jobid": "2", "state": "COMPLETED"}, terminal=True)
        resp = client.post(f"/api/clear_completed/{mock_cluster}")
        assert resp.get_json()["status"] == "ok"

    def test_clear_single_job_ok(self, client, mock_ssh, mock_cluster, db_path):
        from server.db import upsert_job
        upsert_job(mock_cluster, {"jobid": "3", "state": "FAILED"}, terminal=True)
        resp = client.post(f"/api/clear_failed_job/{mock_cluster}/3")
        assert resp.get_json()["status"] == "ok"


@pytest.mark.integration
class TestApiRunScript:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.post("/api/run_script/nonexistent", json={"script": "print(1)"})
        assert resp.status_code == 404

    def test_local_not_supported(self, client, mock_ssh):
        resp = client.post("/api/run_script/local", json={"script": "print(1)"})
        assert resp.status_code == 400
        assert resp.get_json()["status"] == "error"

    def test_missing_script(self, client, mock_ssh, mock_cluster):
        resp = client.post(f"/api/run_script/{mock_cluster}", json={})
        assert resp.status_code == 400
        assert "No script" in resp.get_json()["error"]

    def test_invalid_interpreter(self, client, mock_ssh, mock_cluster):
        resp = client.post(f"/api/run_script/{mock_cluster}",
                           json={"script": "echo hi", "interpreter": "ruby"})
        assert resp.status_code == 400
        assert "interpreter" in resp.get_json()["error"]

    def test_python3_success(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "base64", ("hello world\n", ""))
        resp = client.post(f"/api/run_script/{mock_cluster}",
                           json={"script": "print('hello world')"})
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["interpreter"] == "python3"
        assert data["cluster"] == mock_cluster
        assert "stdout" in data
        assert "stderr" in data

    def test_bash_interpreter(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "base64", ("hello\n", ""))
        resp = client.post(f"/api/run_script/{mock_cluster}",
                           json={"script": "echo hello", "interpreter": "bash"})
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["interpreter"] == "bash"

    def test_timeout_clamped(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "base64", ("", ""))
        resp = client.post(f"/api/run_script/{mock_cluster}",
                           json={"script": "print(1)", "timeout": 9999})
        assert resp.get_json()["status"] == "ok"

    def test_script_base64_encoded_in_command(self, client, mock_ssh, mock_cluster):
        """Verify the script is sent base64-encoded so special chars work."""
        import base64
        script = "print('hello \"world\"')\nprint(1+1)"
        mock_ssh.set(mock_cluster, "base64", ("hello \"world\"\n2\n", ""))
        resp = client.post(f"/api/run_script/{mock_cluster}", json={"script": script})
        assert resp.get_json()["status"] == "ok"
        # Verify the SSH command contained base64-encoded content
        calls = mock_ssh._calls
        assert any("base64" in cmd for _, cmd in calls)
