"""Unit tests for server/paths.py CRUD (path_bases + process_filters)."""

import pytest

from server.db import init_db
from server.paths import (
    FILTER_MODES,
    PATH_KINDS,
    add_path_base,
    add_process_filter,
    list_path_bases,
    list_paths,
    list_patterns,
    list_process_filters,
    remove_path_base,
    remove_path_base_by_id,
    remove_process_filter,
    remove_process_filter_by_id,
    reorder_path_bases,
    reorder_process_filters,
)


@pytest.fixture(autouse=True)
def _init_paths_db(_isolate_db):
    init_db()


# ─── Path bases ──────────────────────────────────────────────────────────────

@pytest.mark.unit
class TestAddPathBase:
    def test_basic_add(self):
        result = add_path_base("log_search", "/lustre/$USER/logs")
        assert result["status"] == "ok"
        assert result["path"]["path"] == "/lustre/$USER/logs"
        assert result["path"]["position"] == 0

    def test_invalid_kind_rejected(self):
        result = add_path_base("bogus_kind", "/p")
        assert result["status"] == "error"
        assert "kind" in result["error"]

    def test_empty_path_rejected(self):
        assert add_path_base("log_search", "")["status"] == "error"

    def test_duplicate_rejected(self):
        add_path_base("log_search", "/p")
        result = add_path_base("log_search", "/p")
        assert result["status"] == "error"

    def test_same_path_different_kinds_allowed(self):
        add_path_base("log_search", "/p")
        result = add_path_base("nemo_run", "/p")
        assert result["status"] == "ok"

    def test_position_auto_increments_per_kind(self):
        add_path_base("log_search", "/a")
        add_path_base("log_search", "/b")
        add_path_base("nemo_run", "/c")  # independent counter
        assert list_paths("log_search") == ["/a", "/b"]
        assert list_paths("nemo_run") == ["/c"]


@pytest.mark.unit
class TestListPaths:
    def test_empty(self):
        assert list_path_bases() == []
        assert list_paths("log_search") == []

    def test_filter_by_kind(self):
        add_path_base("log_search", "/a")
        add_path_base("nemo_run", "/b")
        ls = list_path_bases(kind="log_search")
        assert len(ls) == 1 and ls[0]["path"] == "/a"

    def test_unknown_kind_returns_empty(self):
        assert list_path_bases(kind="bogus_kind") == []

    def test_ordered_by_position(self):
        add_path_base("log_search", "/c", position=2)
        add_path_base("log_search", "/a", position=0)
        add_path_base("log_search", "/b", position=1)
        assert list_paths("log_search") == ["/a", "/b", "/c"]


@pytest.mark.unit
class TestRemovePathBase:
    def test_remove_round_trip(self):
        add_path_base("log_search", "/p")
        result = remove_path_base("log_search", "/p")
        assert result["status"] == "ok"
        assert list_paths("log_search") == []

    def test_remove_missing_rejected(self):
        assert remove_path_base("log_search", "/ghost")["status"] == "error"

    def test_remove_by_id(self):
        result = add_path_base("log_search", "/p")
        entry_id = result["path"]["id"]
        assert remove_path_base_by_id(entry_id)["status"] == "ok"
        assert list_paths("log_search") == []

    def test_remove_by_id_missing(self):
        assert remove_path_base_by_id(99999)["status"] == "error"


@pytest.mark.unit
class TestReorderPathBases:
    def test_reorder_round_trip(self):
        add_path_base("log_search", "/a")
        add_path_base("log_search", "/b")
        add_path_base("log_search", "/c")
        reorder_path_bases("log_search", ["/c", "/a", "/b"])
        assert list_paths("log_search") == ["/c", "/a", "/b"]

    def test_unknown_path_errors(self):
        add_path_base("log_search", "/a")
        result = reorder_path_bases("log_search", ["/a", "/ghost"])
        assert result["status"] == "error"

    def test_reorder_independent_per_kind(self):
        add_path_base("log_search", "/a")
        add_path_base("nemo_run", "/x")
        reorder_path_bases("log_search", ["/a"])
        assert list_paths("nemo_run") == ["/x"]


# ─── Process filters ─────────────────────────────────────────────────────────

@pytest.mark.unit
class TestAddProcessFilter:
    def test_basic_add(self):
        result = add_process_filter("include", "nemo-skills")
        assert result["status"] == "ok"
        assert result["filter"]["pattern"] == "nemo-skills"

    def test_invalid_mode_rejected(self):
        assert add_process_filter("bogus_mode", "x")["status"] == "error"

    def test_empty_pattern_rejected(self):
        assert add_process_filter("include", "")["status"] == "error"

    def test_duplicate_rejected(self):
        add_process_filter("include", "x")
        assert add_process_filter("include", "x")["status"] == "error"

    def test_same_pattern_in_both_modes_allowed(self):
        add_process_filter("include", "x")
        assert add_process_filter("exclude", "x")["status"] == "ok"


@pytest.mark.unit
class TestListProcessFilters:
    def test_empty(self):
        assert list_process_filters() == []
        assert list_patterns("include") == []

    def test_filter_by_mode(self):
        add_process_filter("include", "a")
        add_process_filter("exclude", "b")
        assert list_patterns("include") == ["a"]
        assert list_patterns("exclude") == ["b"]

    def test_unknown_mode_returns_empty(self):
        assert list_process_filters(mode="bogus_mode") == []


@pytest.mark.unit
class TestRemoveProcessFilter:
    def test_remove_round_trip(self):
        add_process_filter("include", "x")
        assert remove_process_filter("include", "x")["status"] == "ok"
        assert list_patterns("include") == []

    def test_remove_missing_rejected(self):
        assert remove_process_filter("include", "ghost")["status"] == "error"

    def test_remove_by_id(self):
        result = add_process_filter("include", "x")
        entry_id = result["filter"]["id"]
        assert remove_process_filter_by_id(entry_id)["status"] == "ok"

    def test_remove_by_id_missing(self):
        assert remove_process_filter_by_id(99999)["status"] == "error"


@pytest.mark.unit
class TestReorderProcessFilters:
    def test_reorder_round_trip(self):
        add_process_filter("include", "a")
        add_process_filter("include", "b")
        add_process_filter("include", "c")
        reorder_process_filters("include", ["c", "a", "b"])
        assert list_patterns("include") == ["c", "a", "b"]


@pytest.mark.unit
class TestConstants:
    def test_path_kinds_immutable_set(self):
        assert "log_search" in PATH_KINDS
        assert "nemo_run" in PATH_KINDS
        assert "mount_lustre_prefix" in PATH_KINDS

    def test_filter_modes(self):
        assert "include" in FILTER_MODES
        assert "exclude" in FILTER_MODES
