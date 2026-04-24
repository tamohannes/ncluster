"""Unit tests for server/config.py helpers (v4: live DB-backed accessors)."""

import time
import pytest

from server.config import (
    _cache_get, _cache_set, _warm_lock, settings_response,
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


class TestMultiPrefixProject:
    """Projects that declare multiple prefixes via the `prefixes` list."""

    @pytest.mark.unit
    def test_either_prefix_resolves_to_project(self, monkeypatch):
        monkeypatch.setitem(
            PROJECTS,
            "n3ue",
            {
                "prefixes": [
                    {"prefix": "hle_chem", "default_campaign": "chem"},
                    {"prefix": "n3ue_"},
                ],
            },
        )
        assert extract_project("hle_chem-omesilver-no-tool-r86") == "n3ue"
        assert extract_project("hle_chem_chem700k-python-r1") == "n3ue"
        assert extract_project("n3ue_rprof_nano-stem-cot-r1") == "n3ue"

    @pytest.mark.unit
    def test_per_prefix_default_campaign(self, monkeypatch):
        monkeypatch.setitem(
            PROJECTS,
            "n3ue",
            {
                "prefixes": [
                    {"prefix": "hle_chem", "default_campaign": "chem"},
                    {"prefix": "n3ue_"},
                ],
            },
        )
        # Legacy hle_chem* prefix forces the campaign label to "chem".
        assert (
            extract_campaign("hle_chem-chem700k-no-tool-r19", "n3ue") == "chem"
        )
        assert (
            extract_campaign("hle_chem_omesilver-no-tool-r86-results", "n3ue")
            == "chem"
        )
        # Modern n3ue_ prefix has no default — campaign derives from the
        # second underscore-delimited segment.
        assert (
            extract_campaign("n3ue_rprof_nano-stem-cot-r1", "n3ue") == "rprof"
        )
        assert (
            extract_campaign("n3ue_chem_chem700k-r1", "n3ue") == "chem"
        )

    @pytest.mark.unit
    def test_longest_prefix_wins_across_projects(self, monkeypatch):
        monkeypatch.setitem(PROJECTS, "hle", {"prefix": "hle_"})
        monkeypatch.setitem(
            PROJECTS,
            "n3ue",
            {
                "prefixes": [
                    {"prefix": "hle_chem", "default_campaign": "chem"},
                    {"prefix": "n3ue_"},
                ],
            },
        )
        # "hle_chem" (8) beats "hle_" (4); other hle_* names still go to hle.
        assert extract_project("hle_chem-foo") == "n3ue"
        assert extract_project("hle_chem_bar") == "n3ue"
        assert extract_project("hle_eval_physics-r1") == "hle"
        assert extract_project("hle_mpsf_run-1") == "hle"

    @pytest.mark.unit
    def test_legacy_singular_prefix_still_works(self, monkeypatch):
        """Pre-existing projects using `prefix` (no `prefixes`) keep working."""
        monkeypatch.setitem(PROJECTS, "old", {"prefix": "old_"})
        assert extract_project("old_eval_x") == "old"
        assert extract_campaign("old_eval_x", "old") == "eval"


class TestLiveProxiesReflectDbChanges:
    """In v4 ``CLUSTERS``, ``TEAM_GPU_ALLOC``, ``PPPS`` are live proxies
    backed by the SQLite tables. Modules that imported them by name in
    v3 (``wds.py``, ``aihub.py``) keep working because the proxy
    re-fetches on every access — the captured reference itself is stable
    but its contents always reflect the current DB state."""

    @pytest.mark.unit
    def test_team_gpu_alloc_reflects_cluster_writes(self):
        from server.clusters import add_cluster, remove_cluster, update_cluster
        from server.config import TEAM_GPU_ALLOC

        captured_ref = TEAM_GPU_ALLOC
        add_cluster("dfwtest", host="x", team_gpu_alloc="999")
        try:
            assert captured_ref["dfwtest"] == 999
            update_cluster("dfwtest", team_gpu_alloc="any")
            assert captured_ref["dfwtest"] == "any"
        finally:
            remove_cluster("dfwtest")

    @pytest.mark.unit
    def test_ppps_reflects_account_writes(self):
        from server.config import PPPS
        from server.team import add_ppp_account, remove_ppp_account

        captured_ref = PPPS
        add_ppp_account("test_acct_a", ppp_id="42")
        try:
            assert captured_ref["test_acct_a"] == "42"
        finally:
            remove_ppp_account("test_acct_a")

    @pytest.mark.unit
    def test_clusters_reflects_add_remove(self):
        from server.clusters import add_cluster, remove_cluster
        from server.config import CLUSTERS

        captured_ref = CLUSTERS
        add_cluster("livetest", host="x")
        try:
            assert "livetest" in captured_ref
            assert captured_ref["livetest"]["host"] == "x"
        finally:
            remove_cluster("livetest")
        assert "livetest" not in captured_ref


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


class TestLiveProxyCache:
    """The live proxies cache their loader result for ``_LIVE_TTL_SEC``
    so a hot loop like ``for c in CLUSTERS: CLUSTERS.get(c, ...)``
    only triggers one DB query, not one per access. Writes through the
    CRUD layer (which uses ``db_write``) auto-invalidate the cache so
    readers still see fresh data immediately."""

    @pytest.mark.unit
    def test_repeated_access_calls_loader_once_within_ttl(self, monkeypatch):
        from server import config as cfg

        # Inflate the TTL so the cache stays warm for the whole test.
        monkeypatch.setattr(cfg, "_LIVE_TTL_SEC", 60.0)

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return {"a": 1, "b": 2}

        proxy = cfg._LiveMapping(loader, "TestProxy")
        for _ in range(20):
            _ = proxy["a"]
            _ = proxy["b"]
            _ = list(proxy.keys())
            _ = "a" in proxy
        assert calls["n"] == 1, f"loader called {calls['n']} times, expected 1"

    @pytest.mark.unit
    def test_loader_reruns_after_ttl_expiry(self, monkeypatch):
        from server import config as cfg

        # 0.0 TTL means every access misses the cache.
        monkeypatch.setattr(cfg, "_LIVE_TTL_SEC", 0.0)

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return {"x": calls["n"]}

        proxy = cfg._LiveMapping(loader, "TestProxy")
        first = proxy["x"]
        second = proxy["x"]
        assert first == 1
        assert second == 2
        assert calls["n"] == 2

    @pytest.mark.unit
    def test_invalidate_forces_reload(self, monkeypatch):
        from server import config as cfg

        monkeypatch.setattr(cfg, "_LIVE_TTL_SEC", 60.0)

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return {"x": calls["n"]}

        proxy = cfg._LiveMapping(loader, "TestProxy")
        assert proxy["x"] == 1
        assert proxy["x"] == 1  # cache hit
        proxy.invalidate()
        assert proxy["x"] == 2
        assert calls["n"] == 2

    @pytest.mark.unit
    def test_sequence_cache_works_too(self, monkeypatch):
        from server import config as cfg

        monkeypatch.setattr(cfg, "_LIVE_TTL_SEC", 60.0)

        calls = {"n": 0}

        def loader():
            calls["n"] += 1
            return ["a", "b", "c"]

        proxy = cfg._LiveSequence(loader, "TestProxy")
        for _ in range(10):
            _ = proxy[0]
            _ = list(proxy)
            _ = len(proxy)
            _ = "b" in proxy
        assert calls["n"] == 1

    @pytest.mark.unit
    def test_invalidate_live_caches_drops_every_proxy(self, monkeypatch):
        from server import config as cfg

        monkeypatch.setattr(cfg, "_LIVE_TTL_SEC", 60.0)

        # Touch every proxy once so each has a populated cache.
        for proxy in cfg._LIVE_PROXIES:
            try:
                _ = list(proxy)
            except Exception:
                # Some loaders may need a populated DB to succeed; fine
                # for this smoke test — we still want to exercise
                # ``invalidate``.
                pass

        cfg.invalidate_live_caches()
        for proxy in cfg._LIVE_PROXIES:
            assert proxy._cache_ts == float("-inf"), \
                f"{proxy._name} cache_ts should be reset, got {proxy._cache_ts}"

    @pytest.mark.unit
    def test_db_write_auto_invalidates_caches(self):
        """End-to-end: a CRUD write through ``db_write`` invalidates
        live caches so the next read sees fresh data without an explicit
        invalidate call."""
        from server.config import CLUSTERS
        from server.clusters import add_cluster, remove_cluster, update_cluster

        # First read populates the cache.
        _ = list(CLUSTERS)

        add_cluster("autoinv", host="x", team_gpu_alloc="100")
        try:
            assert "autoinv" in CLUSTERS  # auto-invalidated by db_write commit
            update_cluster("autoinv", team_gpu_alloc="200")
            assert CLUSTERS["autoinv"].get("team_gpu_alloc") == "200"
        finally:
            remove_cluster("autoinv")
        assert "autoinv" not in CLUSTERS
