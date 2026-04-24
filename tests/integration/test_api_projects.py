"""Integration tests for the dedicated project CRUD endpoints + migration."""

import json

import pytest


@pytest.mark.integration
class TestApiProjectsAll:
    def test_empty_when_no_projects(self, client):
        resp = client.get("/api/projects/all")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_returns_full_records(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({
                "name": "alpha",
                "color": "#abcdef",
                "emoji": "🧪",
                "prefixes": ["alpha_"],
            }),
            content_type="application/json",
        )
        resp = client.get("/api/projects/all")
        rows = resp.get_json()
        assert len(rows) == 1
        assert rows[0]["name"] == "alpha"
        assert rows[0]["color"] == "#abcdef"
        assert rows[0]["prefixes"] == [{"prefix": "alpha_"}]


@pytest.mark.integration
class TestApiProjectCreate:
    def test_creates_project(self, client):
        resp = client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["project"]["name"] == "alpha"
        assert "reassigned" in data
        assert data["reassigned"]["jobs_updated"] == 0

    def test_duplicate_returns_400(self, client):
        body = {"name": "alpha", "prefixes": ["alpha_"]}
        client.post("/api/projects", data=json.dumps(body), content_type="application/json")
        resp = client.post("/api/projects", data=json.dumps(body), content_type="application/json")
        assert resp.status_code == 400
        assert resp.get_json()["status"] == "error"

    def test_invalid_name_returns_400(self, client):
        resp = client.post(
            "/api/projects",
            data=json.dumps({"name": "Bad Name!", "prefixes": ["x_"]}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_create_re_tags_unmatched_jobs(self, client):
        from server.db import get_db
        con = get_db()
        con.execute(
            "INSERT INTO job_history (cluster, job_id, job_name, state, project) "
            "VALUES (?, ?, ?, ?, ?)",
            ("c", "j1", "newproj_eval-r1", "COMPLETED", ""),
        )
        con.commit()
        resp = client.post(
            "/api/projects",
            data=json.dumps({"name": "newproj", "prefixes": ["newproj_"]}),
            content_type="application/json",
        )
        data = resp.get_json()
        assert data["reassigned"]["jobs_updated"] == 1
        row = get_db().execute(
            "SELECT project FROM job_history WHERE job_id=?", ("j1",)
        ).fetchone()
        assert row["project"] == "newproj"


@pytest.mark.integration
class TestApiProjectUpdate:
    def test_update_color(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "color": "#aaaaaa", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        resp = client.put(
            "/api/projects/alpha",
            data=json.dumps({"color": "#bbbbbb"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert resp.get_json()["project"]["color"] == "#bbbbbb"

    def test_missing_returns_404(self, client):
        resp = client.put(
            "/api/projects/nope",
            data=json.dumps({"color": "#000000"}),
            content_type="application/json",
        )
        assert resp.status_code == 404


@pytest.mark.integration
class TestApiProjectDelete:
    def test_deletes_project(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        resp = client.delete("/api/projects/alpha")
        assert resp.status_code == 200
        assert resp.get_json()["deleted"] == "alpha"
        listing = client.get("/api/projects/all").get_json()
        assert listing == []

    def test_missing_returns_404(self, client):
        resp = client.delete("/api/projects/nope")
        assert resp.status_code == 404


# NOTE: tests for the legacy ``_migrate_projects_v1`` and the legacy
# ``POST /api/settings`` ``projects`` field were removed when v4 dropped
# the JSON config and the one-shot project migration. Project CRUD is
# covered by tests/unit/test_projects_db.py and the rest of this file.
