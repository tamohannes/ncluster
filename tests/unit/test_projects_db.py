"""Unit tests for the SQLite ``projects`` table CRUD helpers in server/db.py.

These exercise the underlying read/write functions plus the side-effect
contract that mutations refresh the in-process ``PROJECTS`` cache via
``server.config.reload_projects_cache``.
"""

import json

import pytest

from server.db import (
    _normalize_prefixes,
    db_create_project,
    db_delete_project,
    db_get_project,
    db_list_projects,
    db_update_project,
    get_db,
    init_db,
    re_extract_unmatched_projects,
)


@pytest.fixture(autouse=True)
def _init_projects_db(_isolate_db):
    """Every test in this module needs the schema and an empty cache."""
    init_db()
    from server import config as cfg
    cfg.PROJECTS.clear()
    yield
    cfg.PROJECTS.clear()


@pytest.mark.unit
class TestNormalizePrefixes:
    def test_none_returns_empty(self):
        assert _normalize_prefixes(None) == []

    def test_string_wraps_in_list(self):
        assert _normalize_prefixes("artsiv_") == [{"prefix": "artsiv_"}]

    def test_list_of_strings(self):
        assert _normalize_prefixes(["a_", "b_"]) == [
            {"prefix": "a_"},
            {"prefix": "b_"},
        ]

    def test_list_of_dicts_preserves_default_campaign(self):
        out = _normalize_prefixes([
            {"prefix": "hle_chem", "default_campaign": "chem"},
            {"prefix": "n3ue_"},
        ])
        assert out == [
            {"prefix": "hle_chem", "default_campaign": "chem"},
            {"prefix": "n3ue_"},
        ]

    def test_drops_empty_prefixes(self):
        assert _normalize_prefixes(["", "ok_", {"prefix": ""}]) == [{"prefix": "ok_"}]

    def test_rejects_invalid_type(self):
        with pytest.raises(ValueError):
            _normalize_prefixes(42)

    def test_rejects_non_string_non_dict_entry(self):
        with pytest.raises(ValueError):
            _normalize_prefixes([123])


@pytest.mark.unit
class TestCreateProject:
    def test_basic_create_round_trip(self):
        result = db_create_project(
            "alpha",
            color="#abcdef",
            emoji="🧪",
            prefixes=["alpha_"],
            description="test project",
        )
        assert result["status"] == "ok"
        proj = result["project"]
        assert proj["name"] == "alpha"
        assert proj["color"] == "#abcdef"
        assert proj["emoji"] == "🧪"
        assert proj["prefixes"] == [{"prefix": "alpha_"}]
        assert proj["description"] == "test project"

    def test_auto_picks_color_and_emoji(self):
        result = db_create_project("beta", prefixes=["beta_"])
        assert result["status"] == "ok"
        proj = result["project"]
        assert proj["color"].startswith("#")
        assert len(proj["color"]) == 7
        assert proj["emoji"]

    def test_default_campaign_attached_to_single_prefix(self):
        result = db_create_project(
            "gamma",
            prefixes=["gamma_"],
            default_campaign="forced",
        )
        proj = result["project"]
        assert proj["prefixes"] == [{"prefix": "gamma_", "default_campaign": "forced"}]

    def test_multi_prefix_with_per_prefix_default(self):
        result = db_create_project(
            "n3ue",
            prefixes=[
                {"prefix": "hle_chem", "default_campaign": "chem"},
                {"prefix": "n3ue_"},
            ],
        )
        proj = result["project"]
        assert proj["prefixes"] == [
            {"prefix": "hle_chem", "default_campaign": "chem"},
            {"prefix": "n3ue_"},
        ]

    def test_lowercases_name(self):
        result = db_create_project("MixedCase", prefixes=["mc_"])
        assert result["project"]["name"] == "mixedcase"

    def test_duplicate_name_errors(self):
        db_create_project("dup", prefixes=["dup_"])
        result = db_create_project("dup", prefixes=["dup_"])
        assert result["status"] == "error"
        assert "already exists" in result["error"]

    def test_rejects_empty_name(self):
        result = db_create_project("", prefixes=["x_"])
        assert result["status"] == "error"

    def test_rejects_invalid_name(self):
        result = db_create_project("bad name!", prefixes=["x_"])
        assert result["status"] == "error"

    def test_no_prefix_is_allowed(self):
        """Manual projects (no auto-routing) are valid too."""
        result = db_create_project("manual")
        assert result["status"] == "ok"
        assert result["project"]["prefixes"] == []

    def test_reloads_cache_after_create(self):
        from server import config as cfg
        assert "alpha" not in cfg.PROJECTS
        db_create_project("alpha", color="#abcdef", emoji="🧪", prefixes=["alpha_"])
        assert "alpha" in cfg.PROJECTS
        assert cfg.PROJECTS["alpha"]["color"] == "#abcdef"


@pytest.mark.unit
class TestListGetProject:
    def test_list_empty_returns_empty(self):
        assert db_list_projects() == []

    def test_list_ordered_by_name(self):
        db_create_project("zeta", prefixes=["z_"])
        db_create_project("alpha", prefixes=["a_"])
        names = [p["name"] for p in db_list_projects()]
        assert names == ["alpha", "zeta"]

    def test_get_returns_none_for_missing(self):
        assert db_get_project("nope") is None

    def test_get_returns_full_record(self):
        db_create_project("xx", color="#111111", emoji="🧪", prefixes=["xx_"], description="d")
        proj = db_get_project("xx")
        assert proj["name"] == "xx"
        assert proj["color"] == "#111111"
        assert proj["description"] == "d"
        assert proj["prefixes"] == [{"prefix": "xx_"}]


@pytest.mark.unit
class TestUpdateProject:
    def test_update_color_only(self):
        db_create_project("alpha", color="#aaaaaa", emoji="🧪", prefixes=["alpha_"])
        result = db_update_project("alpha", color="#bbbbbb")
        assert result["status"] == "ok"
        assert result["project"]["color"] == "#bbbbbb"
        assert result["project"]["emoji"] == "🧪"

    def test_update_prefixes_replaces_list(self):
        db_create_project("alpha", prefixes=["old_"])
        result = db_update_project("alpha", prefixes=["new1_", "new2_"])
        assert result["project"]["prefixes"] == [
            {"prefix": "new1_"},
            {"prefix": "new2_"},
        ]

    def test_update_default_campaign_modifies_single_prefix(self):
        db_create_project("alpha", prefixes=["alpha_"])
        result = db_update_project("alpha", default_campaign="forced")
        assert result["project"]["prefixes"] == [
            {"prefix": "alpha_", "default_campaign": "forced"},
        ]

    def test_update_missing_returns_error(self):
        result = db_update_project("nope", color="#000000")
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_update_no_op_returns_existing(self):
        db_create_project("alpha", prefixes=["a_"])
        result = db_update_project("alpha")
        assert result["status"] == "ok"

    def test_update_reloads_cache(self):
        from server import config as cfg
        db_create_project("alpha", color="#aaaaaa", prefixes=["a_"])
        db_update_project("alpha", color="#cccccc")
        assert cfg.PROJECTS["alpha"]["color"] == "#cccccc"


@pytest.mark.unit
class TestDeleteProject:
    def test_delete_round_trip(self):
        db_create_project("alpha", prefixes=["a_"])
        result = db_delete_project("alpha")
        assert result["status"] == "ok"
        assert result["deleted"] == "alpha"
        assert db_get_project("alpha") is None

    def test_delete_missing_errors(self):
        result = db_delete_project("nope")
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_delete_reloads_cache(self):
        from server import config as cfg
        db_create_project("alpha", prefixes=["a_"])
        assert "alpha" in cfg.PROJECTS
        db_delete_project("alpha")
        assert "alpha" not in cfg.PROJECTS


@pytest.mark.unit
class TestRoundTripWithExtractProject:
    """Verify the cache + extract_project pipeline picks up new projects."""

    def test_create_then_extract(self):
        from server.config import extract_project
        db_create_project(
            "n3ue",
            prefixes=[
                {"prefix": "hle_chem", "default_campaign": "chem"},
                {"prefix": "n3ue_"},
            ],
        )
        assert extract_project("hle_chem-foo") == "n3ue"
        assert extract_project("n3ue_run-1") == "n3ue"

    def test_delete_then_extract_returns_empty(self):
        from server.config import extract_project
        db_create_project("temp", prefixes=["temp_"])
        assert extract_project("temp_run-1") == "temp"
        db_delete_project("temp")
        assert extract_project("temp_run-1") == ""


@pytest.mark.unit
class TestReExtractUnmatchedProjects:
    """Verify the helper re-tags rows whose project was empty before."""

    def _seed_jobs(self, jobs):
        con = get_db()
        for jid, name, project in jobs:
            con.execute(
                "INSERT INTO job_history (cluster, job_id, job_name, state, project) "
                "VALUES (?, ?, ?, ?, ?)",
                ("c", jid, name, "COMPLETED", project),
            )
        con.commit()

    def test_fills_empty_project_for_matching_rows(self):
        self._seed_jobs([
            ("j1", "newproj_eval-r1", ""),
            ("j2", "newproj_train-r1", ""),
            ("j3", "other_run-1", ""),
        ])
        db_create_project("newproj", prefixes=["newproj_"])
        result = re_extract_unmatched_projects()
        assert result["jobs_updated"] == 2

        con = get_db()
        rows = {r["job_id"]: r["project"] for r in
                con.execute("SELECT job_id, project FROM job_history").fetchall()}
        assert rows["j1"] == "newproj"
        assert rows["j2"] == "newproj"
        assert rows["j3"] == ""

    def test_does_not_touch_already_tagged_rows(self):
        self._seed_jobs([
            ("j1", "alpha_run-1", "alpha"),
            ("j2", "alpha_run-2", ""),
        ])
        db_create_project("alpha", prefixes=["alpha_"])
        result = re_extract_unmatched_projects()
        assert result["jobs_updated"] == 1
        con = get_db()
        rows = {r["job_id"]: r["project"] for r in
                con.execute("SELECT job_id, project FROM job_history").fetchall()}
        assert rows["j1"] == "alpha"
        assert rows["j2"] == "alpha"

    def test_returns_zero_when_no_matches(self):
        self._seed_jobs([("j1", "ghost_run", "")])
        db_create_project("alpha", prefixes=["alpha_"])
        result = re_extract_unmatched_projects()
        assert result == {"jobs_updated": 0, "runs_updated": 0}
