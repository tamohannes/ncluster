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


@pytest.mark.integration
class TestApiSettingsDoesNotAcceptProjects:
    """POST /api/settings stops accepting the legacy ``projects`` key."""

    def test_projects_field_silently_ignored(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        (tmp_path / "config.json").write_text(json.dumps({"port": 7272}))

        resp = client.post(
            "/api/settings",
            data=json.dumps({
                "projects": {"sneak": {"prefix": "sneak_", "color": "#ff0000"}},
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        listing = client.get("/api/projects/all").get_json()
        assert listing == []


@pytest.mark.integration
class TestProjectMigration:
    """``_migrate_projects_v1`` seeds 4 named projects and re-extracts job rows."""

    def test_seeds_named_projects_only(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        legacy_cfg = {
            "port": 7272,
            "projects": {
                "artsiv": {"prefix": "artsiv_", "color": "#9effbb", "emoji": "🦅"},
                "hle": {"prefix": "hle_", "color": "#9ed5ff", "emoji": "🧪"},
                "n3ue": {
                    "color": "#ff8e8e",
                    "emoji": "💎",
                    "prefixes": [
                        {"prefix": "hle_chem", "default_campaign": "chem"},
                        {"prefix": "n3ue_"},
                    ],
                },
                "profiling": {"prefix": "profiling_", "color": "#f0f8ff", "emoji": "🔬"},
                "sandboxabc": {"prefix": "sandboxabc_"},
                "eval": {"prefix": "eval_"},
                "compute": {},
            },
        }
        (tmp_path / "config.json").write_text(json.dumps(legacy_cfg))

        from server.db import db_list_projects, get_db
        for name, prefix in [
            ("art1", "artsiv_eval-r1"),
            ("hle1", "hle_mpsf_run-1"),
            ("chem1", "hle_chem-omesilver"),
            ("n31", "n3ue_rprof_run-1"),
            ("eval1", "eval_test-r1"),
            ("comp1", "compute_thing"),
        ]:
            con = get_db()
            con.execute(
                "INSERT INTO job_history (cluster, job_id, job_name, state, project) "
                "VALUES (?, ?, ?, ?, ?)",
                ("c", name, prefix, "COMPLETED", "wrong-old-value"),
            )
            con.commit()

        from app import _migrate_projects_v1
        from server.config import reload_projects_cache
        _migrate_projects_v1()
        reload_projects_cache()

        names = sorted(p["name"] for p in db_list_projects())
        assert names == ["artsiv", "hle", "n3ue", "profiling"]

        con = get_db()
        rows = {r["job_id"]: r["project"] for r in
                con.execute("SELECT job_id, project FROM job_history").fetchall()}
        assert rows["art1"] == "artsiv"
        assert rows["hle1"] == "hle"
        assert rows["chem1"] == "n3ue"
        assert rows["n31"] == "n3ue"
        assert rows["eval1"] == ""
        assert rows["comp1"] == ""

        rewritten = json.loads((tmp_path / "config.json").read_text())
        assert "projects" not in rewritten

    def test_migration_is_idempotent(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        (tmp_path / "config.json").write_text(json.dumps({
            "port": 7272,
            "projects": {"artsiv": {"prefix": "artsiv_"}},
        }))
        from app import _migrate_projects_v1
        from server.db import db_list_projects, db_create_project
        _migrate_projects_v1()
        first = sorted(p["name"] for p in db_list_projects())

        db_create_project("manual", prefixes=["manual_"])
        _migrate_projects_v1()
        second = sorted(p["name"] for p in db_list_projects())
        assert "manual" in second
        assert all(p in second for p in first)
