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

    def test_update_prefix_reassigns_more_specific_matches(self, client):
        from server.db import get_db, upsert_run

        client.post(
            "/api/projects",
            data=json.dumps({"name": "hle", "prefixes": ["hle_"]}),
            content_type="application/json",
        )
        client.post(
            "/api/projects",
            data=json.dumps({"name": "mcp", "prefixes": ["mcp_"]}),
            content_type="application/json",
        )
        con = get_db()
        con.execute(
            "INSERT INTO job_history (cluster, job_id, job_name, state, project) "
            "VALUES (?, ?, ?, ?, ?)",
            ("c", "j1", "hle_mcpablation_qwen35-r1", "COMPLETED", "hle"),
        )
        con.commit()
        upsert_run("c", "j1", "hle_mcpablation_qwen35-r1", "hle")

        resp = client.put(
            "/api/projects/mcp",
            data=json.dumps({
                "prefixes": [
                    {"prefix": "hle_mcpablation_", "default_campaign": "mcpablation"},
                    "mcp_",
                ],
            }),
            content_type="application/json",
        )

        assert resp.status_code == 200
        assert resp.get_json()["reassigned"] == {"jobs_updated": 1, "runs_updated": 1}
        con = get_db()
        job = con.execute("SELECT project FROM job_history WHERE job_id='j1'").fetchone()
        run = con.execute("SELECT project FROM runs WHERE root_job_id='j1'").fetchone()
        assert job["project"] == "mcp"
        assert run["project"] == "mcp"

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


@pytest.mark.integration
class TestApiProjectStatus:
    """Sidebar-visibility status field: active vs backlog."""

    def _seed_history(self, project_name):
        """Insert a job_history row so the activity-based ``/api/projects``
        endpoint actually returns the project."""
        from server.db import get_db
        con = get_db()
        con.execute(
            "INSERT INTO job_history (cluster, job_id, job_name, state, project) "
            "VALUES (?, ?, ?, ?, ?)",
            ("c", f"j-{project_name}", f"{project_name}_eval-r1", "COMPLETED", project_name),
        )
        con.commit()

    def test_default_create_is_active(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        proj = client.get("/api/projects/all").get_json()[0]
        assert proj["status"] == "active"

    def test_create_with_backlog(self, client):
        resp = client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"], "status": "backlog"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert resp.get_json()["project"]["status"] == "backlog"

    def test_update_status_to_backlog(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        resp = client.put(
            "/api/projects/alpha",
            data=json.dumps({"status": "backlog"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert resp.get_json()["project"]["status"] == "backlog"

    def test_update_status_invalid_returns_400(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        resp = client.put(
            "/api/projects/alpha",
            data=json.dumps({"status": "archived"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_sidebar_excludes_backlog_by_default(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        client.post(
            "/api/projects",
            data=json.dumps({"name": "beta", "prefixes": ["beta_"], "status": "backlog"}),
            content_type="application/json",
        )
        self._seed_history("alpha")
        self._seed_history("beta")
        rows = client.get("/api/projects").get_json()
        names = sorted(r["project"] for r in rows)
        assert names == ["alpha"]

    def test_sidebar_include_all_returns_backlog(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        client.post(
            "/api/projects",
            data=json.dumps({"name": "beta", "prefixes": ["beta_"], "status": "backlog"}),
            content_type="application/json",
        )
        self._seed_history("alpha")
        self._seed_history("beta")
        rows = client.get("/api/projects?include=all").get_json()
        names = sorted(r["project"] for r in rows)
        assert names == ["alpha", "beta"]

    def test_sidebar_status_backlog_filter_only(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        client.post(
            "/api/projects",
            data=json.dumps({"name": "beta", "prefixes": ["beta_"], "status": "backlog"}),
            content_type="application/json",
        )
        self._seed_history("alpha")
        self._seed_history("beta")
        rows = client.get("/api/projects?status=backlog").get_json()
        names = sorted(r["project"] for r in rows)
        assert names == ["beta"]

    def test_sidebar_includes_status_field(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        self._seed_history("alpha")
        rows = client.get("/api/projects").get_json()
        assert rows[0]["status"] == "active"

    def test_projects_all_includes_backlog(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "beta", "prefixes": ["beta_"], "status": "backlog"}),
            content_type="application/json",
        )
        rows = client.get("/api/projects/all").get_json()
        assert len(rows) == 1
        assert rows[0]["status"] == "backlog"

    def test_spotlight_excludes_backlog(self, client):
        client.post(
            "/api/projects",
            data=json.dumps({"name": "alpha", "prefixes": ["alpha_"]}),
            content_type="application/json",
        )
        client.post(
            "/api/projects",
            data=json.dumps({"name": "beta", "prefixes": ["beta_"], "status": "backlog"}),
            content_type="application/json",
        )
        self._seed_history("alpha")
        self._seed_history("beta")
        # Match both project names with a single common substring.
        result = client.get("/api/spotlight?q=a").get_json()
        names = sorted(p["project"] for p in result.get("projects", []))
        assert names == ["alpha"]


# NOTE: tests for the legacy ``_migrate_projects_v1`` and the legacy
# ``POST /api/settings`` ``projects`` field were removed when v4 dropped
# the JSON config and the one-shot project migration. Project CRUD is
# covered by tests/unit/test_projects_db.py and the rest of this file.
