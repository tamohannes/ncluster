"""Integration tests for log, ls, log_full, jsonl_index, jsonl_record routes."""

import json
import os
import threading
import time
import pytest

from server.db import (
    get_custom_metrics_config,
    get_db,
    set_custom_metrics_config,
    upsert_job,
)


@pytest.mark.integration
class TestApiLogFiles:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log_files/nonexistent/123")
        assert resp.status_code == 404

    def test_local_cluster(self, client, mock_ssh):
        resp = client.get("/api/log_files/local/99999")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "files" in data
        assert "dirs" in data


@pytest.mark.integration
class TestApiLs:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/ls/nonexistent?path=/tmp")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/ls/local")
        assert resp.status_code == 400

    def test_local_dir_listing(self, client, mock_ssh, tmp_path):
        (tmp_path / "file.txt").write_text("x")
        resp = client.get(f"/api/ls/local?path={tmp_path}")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["source"] == "local"
        names = [e["name"] for e in data["entries"]]
        assert "file.txt" in names

    def test_force_bypasses_cache(self, client, mock_ssh, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        client.get(f"/api/ls/local?path={tmp_path}")
        (tmp_path / "b.txt").write_text("y")
        resp = client.get(f"/api/ls/local?path={tmp_path}&force=1")
        names = [e["name"] for e in resp.get_json()["entries"]]
        assert "b.txt" in names


@pytest.mark.integration
class TestApiLog:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log/nonexistent/123")
        assert resp.status_code == 404

    def test_local_file_read(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("line1\nline2\nline3\n")
        resp = client.get(f"/api/log/local/1?path={f}&lines=10")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "line1" in data["content"]
        assert data["source"] == "local"

    def test_cache_hit(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("cached content")
        client.get(f"/api/log/local/1?path={f}")
        resp = client.get(f"/api/log/local/1?path={f}")
        data = resp.get_json()
        assert data["source"] == "cache"

    def test_force_bypasses_cache(self, client, mock_ssh, tmp_path):
        f = tmp_path / "test.log"
        f.write_text("original")
        client.get(f"/api/log/local/1?path={f}")
        f.write_text("updated")
        resp = client.get(f"/api/log/local/1?path={f}&force=1")
        data = resp.get_json()
        assert data["source"] == "local"


@pytest.mark.integration
class TestApiLogFull:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/log_full/nonexistent/1?path=/x")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/log_full/local/1")
        assert resp.status_code == 400

    def test_local_pagination(self, client, mock_ssh, tmp_path):
        f = tmp_path / "big.log"
        f.write_text("\n".join(f"line {i}" for i in range(1000)))
        resp = client.get(f"/api/log_full/local/1?path={f}&page=0&page_size=100")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["total_pages"] == 10
        assert data["page"] == 0
        assert "line 0" in data["content"]


@pytest.mark.integration
class TestApiJsonlIndex:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/jsonl_index/nonexistent/1?path=/x")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/jsonl_index/local/1")
        assert resp.status_code == 400

    def test_local_jsonl_index(self, client, mock_ssh, tmp_path):
        f = tmp_path / "data.jsonl"
        f.write_text('{"id": 1}\n{"id": 2}\n')
        resp = client.get(f"/api/jsonl_index/local/1?path={f}&mode=all")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["count"] == 2


@pytest.mark.integration
class TestApiJsonlRecord:
    def test_unknown_cluster_404(self, client, mock_ssh):
        resp = client.get("/api/jsonl_record/nonexistent/1?path=/x&line=0")
        assert resp.status_code == 404

    def test_no_path_400(self, client, mock_ssh):
        resp = client.get("/api/jsonl_record/local/1?line=0")
        assert resp.status_code == 400

    def test_local_record_fetch(self, client, mock_ssh, tmp_path):
        f = tmp_path / "data.jsonl"
        f.write_text('{"id": 0}\n{"id": 1}\n')
        resp = client.get(f"/api/jsonl_record/local/1?path={f}&line=1")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert '"id": 1' in data["content"]


@pytest.mark.integration
class TestCustomMetricsRoutes:
    def test_copy_metrics_config_does_not_fall_back_to_sibling_source(self, client, mock_cluster):
        upsert_job(mock_cluster, {"jobid": "100", "name": "src", "state": "RUNNING"})
        upsert_job(mock_cluster, {"jobid": "101", "name": "src-sibling", "state": "RUNNING"})
        upsert_job(mock_cluster, {"jobid": "200", "name": "dest", "state": "RUNNING"})

        con = get_db()
        try:
            con.execute(
                "UPDATE job_history SET run_id=? WHERE cluster=? AND job_id IN (?, ?)",
                (77, mock_cluster, "100", "101"),
            )
            con.commit()
        finally:
            con.close()

        set_custom_metrics_config(mock_cluster, "101", json.dumps({
            "extractors": [{"name": "loss", "regex": r"loss=(\d+)", "group": 1}],
        }))

        resp = client.post(
            f"/api/copy_metrics_config/{mock_cluster}/200",
            json={"src_cluster": mock_cluster, "src_job_id": "100"},
        )

        assert resp.status_code == 400
        assert resp.get_json()["error"] == f"No custom config found on source job {mock_cluster}/100"
        assert get_custom_metrics_config(mock_cluster, "200") == ""

    def test_custom_metrics_run_parallelizes_extraction(self, client, mock_cluster, monkeypatch):
        jobs = [
            {"job_id": "100", "job_name": "job-100", "state": "RUNNING"},
            {"job_id": "101", "job_name": "job-101", "state": "RUNNING"},
            {"job_id": "102", "job_name": "job-102", "state": "RUNNING"},
            {"job_id": "103", "job_name": "job-103", "state": "RUNNING"},
        ]
        state = {"active": 0, "max_active": 0}
        lock = threading.Lock()

        monkeypatch.setattr("server.routes.get_jobs_in_run", lambda cluster, run_id: jobs)
        monkeypatch.setattr("server.routes.enable_standalone_ssh", lambda: None)

        def fake_get_run(cluster, root_job_id):
            return {"id": 77}

        def fake_extract(cluster, job_id):
            with lock:
                state["active"] += 1
                state["max_active"] = max(state["max_active"], state["active"])
            time.sleep(0.05)
            with lock:
                state["active"] -= 1
            return {"status": "ok", "metrics": [{"name": "loss", "value": "1", "match_count": 1}]}

        monkeypatch.setattr("server.db.get_run", fake_get_run)
        monkeypatch.setattr("server.routes.extract_custom_metrics", fake_extract)

        resp = client.get(f"/api/custom_metrics_run/{mock_cluster}/100")

        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data["jobs"]) == 4
        assert state["max_active"] > 1
