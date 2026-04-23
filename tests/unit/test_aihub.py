"""Unit tests for server/aihub.py — AI Hub OpenSearch integration."""

import json
import time
import pytest

from server.aihub import (
    _friendly_cluster,
    _os_cluster_names,
    _pick_best_accounts,
    cluster_name_map,
    cluster_name_rev,
)
from server.jobs import _parse_gres_gpu_count
from server.config import _cache_get, _cache_set


@pytest.fixture
def fake_clusters(monkeypatch):
    """Inject a synthetic CLUSTERS dict so the cluster-name-map tests stay
    independent of the infrastructure identifiers in conf/config.json."""
    fake = {
        "alpha": {"aihub_name": "alpha-os-1"},
        "beta":  {"aihub_name": "beta-os-2"},
        "gamma": {"aihub_name": "gamma-shared"},
        # No aihub_name → cluster has no AI Hub ingestion and is excluded.
        "delta": {},
    }
    monkeypatch.setattr("server.aihub.CLUSTERS", fake)
    return fake


class TestClusterNameMapping:
    @pytest.mark.unit
    def test_forward_mapping_skips_clusters_without_aihub_name(self, fake_clusters):
        m = cluster_name_map()
        assert m == {"alpha": "alpha-os-1", "beta": "beta-os-2", "gamma": "gamma-shared"}
        assert "delta" not in m

    @pytest.mark.unit
    def test_reverse_mapping_round_trips(self, fake_clusters):
        rev = cluster_name_rev()
        assert rev["alpha-os-1"] == "alpha"
        assert rev["beta-os-2"] == "beta"

    @pytest.mark.unit
    def test_friendly_cluster_known(self, fake_clusters):
        assert _friendly_cluster("alpha-os-1") == "alpha"
        assert _friendly_cluster("beta-os-2") == "beta"

    @pytest.mark.unit
    def test_friendly_cluster_unknown_passes_through(self, fake_clusters):
        assert _friendly_cluster("unknown-cluster") == "unknown-cluster"

    @pytest.mark.unit
    def test_os_cluster_names_specific(self, fake_clusters):
        result = _os_cluster_names(["alpha", "beta"])
        assert set(result) == {"alpha-os-1", "beta-os-2"}

    @pytest.mark.unit
    def test_os_cluster_names_all(self, fake_clusters):
        result = _os_cluster_names(None)
        assert set(result) == {"alpha-os-1", "beta-os-2", "gamma-shared"}

    @pytest.mark.unit
    def test_os_cluster_names_filters_unknown(self, fake_clusters):
        result = _os_cluster_names(["alpha", "nonexistent"])
        assert result == ["alpha-os-1"]

    @pytest.mark.unit
    def test_legacy_constant_imports_still_work(self, fake_clusters):
        """`from server.aihub import CLUSTER_NAME_MAP` keeps working through
        the PEP 562 ``__getattr__`` shim so callers don't have to migrate."""
        from server import aihub
        assert aihub.CLUSTER_NAME_MAP == cluster_name_map()
        assert aihub.CLUSTER_NAME_REV == cluster_name_rev()


class TestPickBestAccounts:
    @pytest.mark.unit
    def test_picks_highest_level_fs_for_priority(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 1.5, "headroom": 100, "gpus_allocated": 500},
            "acct_b": {"level_fs": 3.0, "headroom": 50, "gpus_allocated": 100},
        }}
        _pick_best_accounts(cd)
        assert cd["best_priority"]["account"] == "acct_b"
        assert cd["best_priority"]["level_fs"] == 3.0

    @pytest.mark.unit
    def test_picks_highest_headroom_for_capacity(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 1.5, "headroom": 200, "gpus_allocated": 500},
            "acct_b": {"level_fs": 3.0, "headroom": 50, "gpus_allocated": 100},
        }}
        _pick_best_accounts(cd)
        assert cd["best_capacity"]["account"] == "acct_a"
        assert cd["best_capacity"]["headroom"] == 200

    @pytest.mark.unit
    def test_same_account_for_both(self):
        cd = {"accounts": {
            "acct_a": {"level_fs": 5.0, "headroom": 300, "gpus_allocated": 400},
        }}
        _pick_best_accounts(cd)
        assert cd["best_priority"]["account"] == "acct_a"
        assert cd["best_capacity"]["account"] == "acct_a"

    @pytest.mark.unit
    def test_empty_accounts(self):
        cd = {"accounts": {}}
        _pick_best_accounts(cd)
        assert cd["best_priority"] is None
        assert cd["best_capacity"] is None


class TestParseGresGpuCount:
    @pytest.mark.unit
    def test_standard_gres(self):
        assert _parse_gres_gpu_count("gpu:8") == 8

    @pytest.mark.unit
    def test_typed_gres(self):
        assert _parse_gres_gpu_count("gpu:a100:4") == 4

    @pytest.mark.unit
    def test_empty_gres(self):
        assert _parse_gres_gpu_count("") == 0
        assert _parse_gres_gpu_count("N/A") == 0
        assert _parse_gres_gpu_count("(null)") == 0

    @pytest.mark.unit
    def test_multi_gres(self):
        assert _parse_gres_gpu_count("gpu:4,shard:2") == 4

    @pytest.mark.unit
    def test_gres_prefix(self):
        assert _parse_gres_gpu_count("gres/gpu:4") == 4
        assert _parse_gres_gpu_count("gres/gpu:b200:4") == 4

    @pytest.mark.unit
    def test_gres_prefix_with_socket(self):
        assert _parse_gres_gpu_count("gres/gpu:4(S:0-1)") == 4
        assert _parse_gres_gpu_count("gres/gpu:b200:4(S:0-1)") == 4


class TestAihubCaching:
    @pytest.mark.unit
    def test_cache_stores_and_retrieves(self):
        store = {}
        _cache_set(store, "test_key", {"data": 42})
        result = _cache_get(store, "test_key", 300)
        assert result == {"data": 42}

    @pytest.mark.unit
    def test_cache_expires(self):
        from server.config import _warm_lock
        store = {}
        with _warm_lock:
            store["old"] = {"ts": time.monotonic() - 600, "value": "stale"}
        result = _cache_get(store, "old", 300)
        assert result is None
