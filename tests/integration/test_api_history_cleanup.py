"""Integration tests for /api/history and /api/cleanup routes."""

import json
import pytest

from server.db import upsert_job


@pytest.mark.integration
class TestApiHistory:
    def test_get_history_empty(self, client, db_path):
        resp = client.get("/api/history")
        assert resp.status_code == 200
        data = resp.get_json()
        assert isinstance(data, list)

    def test_get_history_cluster_filter(self, client, db_path):
        upsert_job("c1", {"jobid": "1", "state": "COMPLETED"})
        upsert_job("c2", {"jobid": "2", "state": "COMPLETED"})
        resp = client.get("/api/history?cluster=c1")
        data = resp.get_json()
        assert all(r["cluster"] == "c1" for r in data)

    def test_get_history_limit(self, client, db_path):
        for i in range(10):
            upsert_job("c", {"jobid": str(i), "state": "COMPLETED"})
        resp = client.get("/api/history?limit=3")
        assert len(resp.get_json()) == 3

    def test_get_history_project_filter(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "testproj", {"prefix": "testproj_"})
        upsert_job("c", {"jobid": "1", "name": "testproj_eval", "state": "COMPLETED"})
        upsert_job("c", {"jobid": "2", "name": "other_eval", "state": "COMPLETED"})
        resp = client.get("/api/history?project=testproj")
        data = resp.get_json()
        assert all(r.get("project") == "testproj" for r in data)
        assert len(data) == 1

    def test_history_includes_project_color(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "colorproj", {"prefix": "colorproj_", "color": "#aabbcc"})
        upsert_job("c", {"jobid": "1", "name": "colorproj_eval", "state": "COMPLETED"})
        resp = client.get("/api/history")
        data = resp.get_json()
        matching = [r for r in data if r.get("project") == "colorproj"]
        assert len(matching) >= 1
        assert matching[0].get("project_color") == "#aabbcc"


@pytest.mark.integration
class TestApiProjects:
    def test_get_projects_empty(self, client, db_path):
        resp = client.get("/api/projects")
        assert resp.status_code == 200
        assert isinstance(resp.get_json(), list)

    def test_get_projects_with_data(self, client, db_path, monkeypatch):
        from server.config import PROJECTS
        monkeypatch.setitem(PROJECTS, "myproj", {"prefix": "myproj_", "color": "#e8f4fd"})
        upsert_job("c", {"jobid": "1", "name": "myproj_eval", "state": "COMPLETED"})
        upsert_job("c", {"jobid": "2", "name": "myproj_judge", "state": "FAILED"})
        resp = client.get("/api/projects")
        data = resp.get_json()
        matching = [p for p in data if p["project"] == "myproj"]
        assert len(matching) == 1
        assert matching[0]["job_count"] == 2
        assert matching[0]["color"] == "#e8f4fd"


@pytest.mark.integration
class TestApiCleanup:
    def test_cleanup_dry_run(self, client, db_path):
        upsert_job("c", {"jobid": "1", "state": "COMPLETED",
                         "ended_at": "2020-01-01T00:00:00"})
        resp = client.post("/api/cleanup",
                           data=json.dumps({"days": 1, "dry_run": True}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["dry_run"] is True
        assert data["deleted_records"] >= 1

    def test_cleanup_real_deletes(self, client, db_path):
        upsert_job("c", {"jobid": "1", "state": "COMPLETED",
                         "ended_at": "2020-01-01T00:00:00"})
        resp = client.post("/api/cleanup",
                           data=json.dumps({"days": 1}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["deleted_records"] >= 1
        from server.db import get_history
        assert len([r for r in get_history("c") if r["job_id"] == "1"]) == 0

    def test_cleanup_invalid_days(self, client, db_path):
        resp = client.post("/api/cleanup",
                           data=json.dumps({"days": 0}),
                           content_type="application/json")
        assert resp.status_code == 400

    def test_cleanup_no_matches(self, client, db_path):
        resp = client.post("/api/cleanup",
                           data=json.dumps({"days": 1}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["deleted_records"] == 0
