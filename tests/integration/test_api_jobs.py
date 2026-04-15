"""Integration tests for /api/jobs and /api/jobs/<cluster> routes."""

import json
import pytest


@pytest.mark.integration
class TestApiJobs:
    def test_get_jobs_returns_all_clusters(self, client, mock_ssh):
        resp = client.get("/api/jobs")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "local" in data

    def test_get_jobs_with_refresh(self, client, mock_ssh):
        resp = client.get("/api/jobs?refresh=1")
        assert resp.status_code == 200

    def test_get_jobs_honors_etag(self, client, mock_ssh):
        first = client.get("/api/jobs")
        assert first.status_code == 200
        etag = first.headers.get("ETag")
        second = client.get("/api/jobs", headers={"If-None-Match": etag})
        assert second.status_code == 304

    def test_get_jobs_cluster_unknown(self, client, mock_ssh):
        resp = client.get("/api/jobs/nonexistent")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "error"

    def test_get_jobs_cluster_returns_data(self, client, mock_ssh):
        mock_ssh.set("local", "ps", ("", ""))
        resp = client.get("/api/jobs/local")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "jobs" in data

    def test_get_jobs_mock_cluster(self, client, mock_ssh, mock_cluster):
        mock_ssh.set(mock_cluster, "squeue", ("", ""))
        resp = client.get(f"/api/jobs/{mock_cluster}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "jobs" in data

    def test_pinned_jobs_merged(self, client, mock_ssh, mock_cluster, db_path):
        from server.db import upsert_job
        job = {"jobid": "999", "name": "old-eval", "state": "FAILED",
               "ended_at": "2026-03-13T12:00:00"}
        upsert_job(mock_cluster, job, terminal=True)
        resp = client.get("/api/jobs")
        data = resp.get_json()
        if mock_cluster in data and data[mock_cluster].get("status") == "ok":
            jobs = data[mock_cluster].get("jobs", [])
            pinned = [j for j in jobs if j.get("_pinned")]
            assert len(pinned) >= 1

    def test_jobs_response_includes_mount_status(self, client, mock_ssh, mock_cluster):
        resp = client.get("/api/jobs")
        data = resp.get_json()
        if mock_cluster in data and data[mock_cluster].get("status") == "ok":
            assert "mount" in data[mock_cluster]

    def test_force_poll_queues_priority_refresh(self, client, monkeypatch, mock_cluster):
        class _Poller:
            def __init__(self):
                self.queued = []

            def request_priority(self, cluster):
                assert cluster == mock_cluster
                self.queued.append(cluster)

            def get_status(self):
                return {mock_cluster: {"state": "healthy", "inflight": False}}

        poller = _Poller()
        monkeypatch.setattr("server.routes.get_poller", lambda: poller)

        resp = client.post(f"/api/force_poll/{mock_cluster}")
        assert resp.status_code == 202
        data = resp.get_json()
        assert data["status"] == "queued"
        assert data["cluster"] == mock_cluster
        assert poller.queued == [mock_cluster]


@pytest.mark.integration
class TestApiPrefetchVisible:
    def test_prefetch_returns_ok(self, client, mock_ssh):
        payload = {"jobs": [{"cluster": "local", "job_id": "123"}]}
        resp = client.post("/api/prefetch_visible",
                           data=json.dumps(payload),
                           content_type="application/json")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"

    def test_prefetch_empty_payload(self, client, mock_ssh):
        resp = client.post("/api/prefetch_visible",
                           data=json.dumps({}),
                           content_type="application/json")
        assert resp.status_code == 200
        assert resp.get_json()["jobs"] == 0

    def test_prefetch_unknown_cluster_filtered(self, client, mock_ssh):
        payload = {"jobs": [{"cluster": "nonexistent", "job_id": "1"}]}
        resp = client.post("/api/prefetch_visible",
                           data=json.dumps(payload),
                           content_type="application/json")
        assert resp.get_json()["jobs"] == 0


@pytest.mark.integration
class TestApiProgress:
    def test_progress_response_includes_board_version(self, client, mock_ssh):
        resp = client.post(
            "/api/progress",
            data=json.dumps({"jobs": []}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert "board_version" in data
