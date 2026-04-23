"""Unit tests for server/config.py helpers."""

import json
import os
import time
import pytest

from server.config import (
    _cache_get, _cache_set, _warm_lock, _load_mount_map, settings_response,
    extract_project, extract_campaign, get_project_color, PROJECTS,
)


class TestCacheGetSet:
    @pytest.mark.unit
    def test_set_and_get(self):
        store = {}
        _cache_set(store, "k1", "val1")
        assert _cache_get(store, "k1", ttl_sec=60) == "val1"

    @pytest.mark.unit
    def test_expired_returns_none(self):
        store = {}
        with _warm_lock:
            store["k1"] = {"ts": time.monotonic() - 100, "value": "old"}
        assert _cache_get(store, "k1", ttl_sec=10) is None

    @pytest.mark.unit
    def test_missing_key_returns_none(self):
        assert _cache_get({}, "nope", ttl_sec=60) is None

    @pytest.mark.unit
    def test_overwrite(self):
        store = {}
        _cache_set(store, "k1", "v1")
        _cache_set(store, "k1", "v2")
        assert _cache_get(store, "k1", ttl_sec=60) == "v2"


class TestLoadMountMap:
    @pytest.mark.unit
    def test_default_when_env_empty(self, monkeypatch):
        monkeypatch.delenv("CLAUSIUS_MOUNT_MAP", raising=False)
        result = _load_mount_map()
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_valid_json_env(self, monkeypatch, mock_cluster):
        monkeypatch.setenv("CLAUSIUS_MOUNT_MAP",
                           json.dumps({mock_cluster: ["~/.jm/mounts/test"]}))
        result = _load_mount_map()
        assert mock_cluster in result

    @pytest.mark.unit
    def test_malformed_json_falls_back(self, monkeypatch):
        monkeypatch.setenv("CLAUSIUS_MOUNT_MAP", "not json{{{")
        result = _load_mount_map()
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_string_root_wrapped_in_list(self, monkeypatch, mock_cluster):
        monkeypatch.setenv("CLAUSIUS_MOUNT_MAP",
                           json.dumps({mock_cluster: "/single/path"}))
        result = _load_mount_map()
        if mock_cluster in result:
            assert isinstance(result[mock_cluster], list)


class TestSettingsResponse:
    @pytest.mark.unit
    def test_contains_expected_keys(self):
        resp = settings_response()
        assert "port" in resp
        assert "ssh_timeout" in resp
        assert "cache_fresh_sec" in resp
        assert "projects" in resp


class TestExtractProject:
    @pytest.mark.unit
    def test_matching_prefix(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "alpha", {"prefix": "alpha_"})
        assert extract_project("alpha_eval-math") == "alpha"

    @pytest.mark.unit
    def test_no_match_without_underscore(self):
        assert extract_project("random-job-name") == ""

    @pytest.mark.unit
    def test_empty_name(self):
        assert extract_project("") == ""

    @pytest.mark.unit
    def test_none_name(self):
        assert extract_project(None) == ""

    @pytest.mark.unit
    def test_multiple_prefixes(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "alpha", {"prefix": "alpha_"})
        monkeypatch.setitem(PROJECTS, "beta", {"prefix": "beta-"})
        assert extract_project("beta-eval-math") == "beta"
        assert extract_project("alpha_eval-code") == "alpha"

    @pytest.mark.unit
    def test_prefix_with_hyphen(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "beta", {"prefix": "beta-"})
        assert extract_project("beta-gpt-large-120b") == "beta"

    @pytest.mark.unit
    def test_unregistered_prefix_returns_empty(self, monkeypatch):
        """extract_project no longer auto-creates projects from unknown prefixes."""
        monkeypatch.setattr("server.config.PROJECTS", {})
        assert extract_project("newproj_eval-math") == ""
        assert extract_project("colortest_job") == ""

    @pytest.mark.unit
    def test_no_match_for_bareword(self):
        assert extract_project("nounderscore") == ""

    @pytest.mark.unit
    def test_does_not_mutate_projects(self, monkeypatch):
        """extract_project must be side-effect free — no implicit registration."""
        monkeypatch.setattr("server.config.PROJECTS", {})
        from server.config import PROJECTS as live
        before = dict(live)
        extract_project("ghostproj_run-1")
        assert dict(live) == before

    @pytest.mark.unit
    def test_longest_prefix_wins(self, monkeypatch):
        """When two prefixes both match, the longer one takes precedence."""
        monkeypatch.setitem(PROJECTS, "parent", {"prefix": "shared_"})
        monkeypatch.setitem(PROJECTS, "child", {"prefix": "shared_sub_"})
        assert extract_project("shared_sub_run-1") == "child"
        assert extract_project("shared_other-run") == "parent"


class TestExtractCampaign:
    @pytest.mark.unit
    def test_default_underscore_split(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "alpha", {"prefix": "alpha_"})
        assert extract_campaign("alpha_eval_math-r1", "alpha") == "eval"

    @pytest.mark.unit
    def test_default_campaign_overrides_derivation(self, monkeypatch):
        monkeypatch.setitem(
            PROJECTS,
            "fixed",
            {"prefix": "shared_sub_", "default_campaign": "myforced"},
        )
        # Even if the remainder would derive a different campaign, the
        # forced value wins.
        assert (
            extract_campaign("shared_sub_omesilver-no-tool-r86-results", "fixed")
            == "myforced"
        )

    @pytest.mark.unit
    def test_custom_campaign_delimiter(self, monkeypatch):
        monkeypatch.setitem(
            PROJECTS,
            "delim",
            {"prefix": "delim_", "campaign_delimiter": "-"},
        )
        assert (
            extract_campaign("delim_omesilver-no-tool-r1", "delim") == "omesilver"
        )

    @pytest.mark.unit
    def test_empty_returns_empty(self):
        assert extract_campaign("", "anything") == ""


class TestGetProjectColor:
    @pytest.mark.unit
    def test_returns_color_for_configured_project(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "test-proj", {"prefix": "test_", "color": "#abcdef"})
        assert get_project_color("test-proj") == "#abcdef"

    @pytest.mark.unit
    def test_returns_empty_when_color_missing(self, monkeypatch):
        """get_project_color is read-only — palette assignment happens at create time."""
        monkeypatch.setitem(PROJECTS, "no-color", {"prefix": "noc_"})
        assert get_project_color("no-color") == ""

    @pytest.mark.unit
    def test_does_not_mutate_projects(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "no-color", {"prefix": "noc_"})
        get_project_color("no-color")
        assert "color" not in PROJECTS["no-color"]

    @pytest.mark.unit
    def test_unknown_project_returns_empty(self):
        assert get_project_color("nonexistent") == ""

    @pytest.mark.unit
    def test_empty_name_returns_empty(self):
        assert get_project_color("") == ""
