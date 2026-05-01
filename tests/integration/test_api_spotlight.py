"""Integration tests for /api/spotlight unified search endpoint."""

import json
import pytest

from server.db import db_write, get_run_hash, upsert_job


@pytest.mark.integration
class TestSpotlightEmpty:
    def test_empty_query_returns_empty(self, client, db_path):
        resp = client.get("/api/spotlight?q=")
        data = resp.get_json()
        assert data == {"projects": [], "logbook": [], "history": []}

    def test_no_query_param(self, client, db_path):
        resp = client.get("/api/spotlight")
        data = resp.get_json()
        assert data == {"projects": [], "logbook": [], "history": []}


@pytest.mark.integration
class TestSpotlightProjects:
    def test_finds_project_by_name(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "artsiv", {"prefix": "artsiv_", "color": "#aabb00", "emoji": "🔬"})
        upsert_job("c", {"jobid": "1", "name": "artsiv_eval", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=artsiv")
        data = resp.get_json()
        assert len(data["projects"]) >= 1
        p = data["projects"][0]
        assert p["project"] == "artsiv"
        assert p["emoji"] == "🔬"
        assert p["job_count"] >= 1

    def test_no_match(self, client, db_path):
        resp = client.get("/api/spotlight?q=zzzznonexistent")
        data = resp.get_json()
        assert data["projects"] == []

    def test_case_insensitive(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "myproj", {"prefix": "myproj_"})
        upsert_job("c", {"jobid": "1", "name": "myproj_run", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=MyPROJ")
        data = resp.get_json()
        assert len(data["projects"]) >= 1

    def test_unregistered_project_not_in_spotlight_projects(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "spotkeep", {"prefix": "spotkeep_"})
        upsert_job("c", {"jobid": "1", "name": "", "project": "spotzombie", "state": "COMPLETED"})
        upsert_job("c", {"jobid": "2", "name": "spotkeep_run", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=spot")
        data = resp.get_json()
        names = [p["project"] for p in data["projects"]]
        assert "spotzombie" not in names
        assert "spotkeep" in names


@pytest.mark.integration
class TestSpotlightLogbook:
    def test_finds_logbook_entry(self, client, db_path):
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "GPU benchmark results", "body": "CUDA 12 performance"}),
                     content_type="application/json")
        resp = client.get("/api/spotlight?q=benchmark")
        data = resp.get_json()
        assert len(data["logbook"]) >= 1
        assert "benchmark" in data["logbook"][0]["title"].lower()

    def test_logbook_no_match(self, client, db_path):
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "Some note", "body": "content"}),
                     content_type="application/json")
        resp = client.get("/api/spotlight?q=zzzznothing")
        data = resp.get_json()
        assert data["logbook"] == []


@pytest.mark.integration
class TestSpotlightHistory:
    def test_finds_job_by_name(self, client, db_path):
        upsert_job("eos", {"jobid": "123", "name": "hle_eval-math", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=eval-math")
        data = resp.get_json()
        assert len(data["history"]) >= 1
        h = data["history"][0]
        assert "eval-math" in h["job_name"]
        assert h["cluster"] == "eos"
        assert h["job_id"] == "123"

    def test_history_limit(self, client, db_path):
        for i in range(20):
            upsert_job("c", {"jobid": str(i), "name": f"test_run_{i}", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=test_run")
        data = resp.get_json()
        assert len(data["history"]) <= 8

    def test_history_no_match(self, client, db_path):
        upsert_job("c", {"jobid": "1", "name": "real_job", "state": "COMPLETED"})
        resp = client.get("/api/spotlight?q=zzzznothing")
        data = resp.get_json()
        assert data["history"] == []


@pytest.mark.integration
class TestSpotlightRuns:
    def test_finds_old_run_by_exact_run_hash(self, client, db_path, mock_cluster):
        target_root = "target-root"
        target_hash = get_run_hash(mock_cluster, target_root)
        with db_write() as con:
            con.execute(
                """INSERT INTO runs (cluster, root_job_id, run_name, project, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (mock_cluster, target_root, "target_run", "demo", "2025-01-01T00:00:00"),
            )
            for idx in range(2001):
                con.execute(
                    """INSERT INTO runs (cluster, root_job_id, run_name, project, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (mock_cluster, f"newer-{idx}", f"newer_run_{idx}", "demo", f"2026-01-01T00:{idx % 60:02d}:00"),
                )

        resp = client.get(f"/api/spotlight?q={target_hash}")
        data = resp.get_json()

        assert data["runs"][0]["run_hash"] == target_hash
        assert data["runs"][0]["run_name"] == "target_run"


@pytest.mark.integration
class TestSpotlightFiles:
    def test_finds_known_log_path_by_filename(self, client, db_path, mock_cluster):
        upsert_job(mock_cluster, {
            "jobid": "12345",
            "name": "demo_eval_math",
            "state": "COMPLETED",
            "log_path": "/runs/demo/eval-logs/worker-12345.out",
        })

        resp = client.get("/api/spotlight?q=worker-12345.out")
        data = resp.get_json()

        assert len(data["files"]) >= 1
        result = data["files"][0]
        assert result["cluster"] == mock_cluster
        assert result["job_id"] == "12345"
        assert result["path"] == "/runs/demo/eval-logs/worker-12345.out"
        assert result["root_dir"] == "/runs/demo"

    def test_opens_direct_file_under_known_run_root(self, client, db_path, mock_cluster):
        upsert_job(mock_cluster, {
            "jobid": "12345",
            "name": "demo_eval_math",
            "state": "COMPLETED",
            "log_path": "/runs/demo/eval-logs/worker-12345.out",
        })

        target = "/runs/demo/eval-results/math/metrics.json"
        resp = client.get(f"/api/spotlight?q={target}")
        data = resp.get_json()

        match = next(r for r in data["files"] if r["path"] == target)
        assert match["job_id"] == "12345"
        assert match["root_dir"] == "/runs/demo"


@pytest.mark.integration
class TestSpotlightCombined:
    def test_all_sources_returned(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "demo", {"prefix": "demo_"})
        upsert_job("c", {"jobid": "1", "name": "demo_train", "state": "COMPLETED"})
        client.post("/api/logbook/demo/entries",
                     data=json.dumps({"title": "demo results", "body": "demo experiment"}),
                     content_type="application/json")
        resp = client.get("/api/spotlight?q=demo")
        data = resp.get_json()
        assert len(data["projects"]) >= 1
        assert len(data["logbook"]) >= 1
        assert len(data["history"]) >= 1

    def test_response_structure(self, client, db_path):
        resp = client.get("/api/spotlight?q=x")
        data = resp.get_json()
        assert "projects" in data
        assert "logbook" in data
        assert "files" in data
        assert "history" in data
        assert isinstance(data["projects"], list)
        assert isinstance(data["logbook"], list)
        assert isinstance(data["files"], list)
        assert isinstance(data["history"], list)
