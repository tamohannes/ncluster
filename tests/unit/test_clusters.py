"""Unit tests for server/clusters.py CRUD."""

import json
import os

import pytest

from server.clusters import (
    LOCAL_CLUSTER,
    add_cluster,
    build_mount_aliases,
    build_mount_map,
    build_mount_remote_map,
    build_team_gpu_allocations,
    cluster_map,
    get_cluster,
    list_cluster_names,
    list_clusters,
    remove_cluster,
    reorder_clusters,
    update_cluster,
)
from server.db import init_db


@pytest.fixture(autouse=True)
def _init_clusters_db(_isolate_db):
    """Clear the auto-injected mock-cluster row so tests start with an
    empty ``clusters`` table. The conftest autouse fixture seeds it for
    every other test suite — these CRUD tests need a clean slate."""
    init_db()
    from server.clusters import remove_cluster
    remove_cluster("mock-cluster")


@pytest.mark.unit
class TestAddCluster:
    def test_basic_add_returns_full_record(self):
        result = add_cluster("dfw", host="login.dfw.example.com", gpu_type="H100", gpus_per_node=8)
        assert result["status"] == "ok"
        c = result["cluster"]
        assert c["name"] == "dfw"
        assert c["host"] == "login.dfw.example.com"
        assert c["gpu_type"] == "H100"
        assert c["gpus_per_node"] == 8
        assert c["enabled"] == 1
        assert c["mount_paths"] == []
        assert c["mount_aliases"] == {}

    def test_full_field_round_trip(self):
        result = add_cluster(
            "hsg",
            host="login.hsg",
            data_host="dc.hsg",
            port=2222,
            ssh_user="alice",
            ssh_key="/keys/alice",
            account="myacct",
            gpu_type="B200",
            gpu_mem_gb=192,
            gpus_per_node=8,
            aihub_name="hsg-cluster",
            mount_paths=["/lustre/$USER", "/scratch/$USER"],
            mount_aliases={"/symlink/path": 0},
            team_gpu_alloc="128",
        )
        c = result["cluster"]
        assert c["port"] == 2222
        assert c["user"] == "alice"
        assert c["key"] == "/keys/alice"
        assert c["mount_paths"] == ["/lustre/$USER", "/scratch/$USER"]
        assert c["mount_aliases"] == {"/symlink/path": 0}
        assert c["team_gpu_alloc"] == "128"

    def test_duplicate_errors(self):
        add_cluster("dup", host="x")
        result = add_cluster("dup", host="x")
        assert result["status"] == "error"
        assert "already exists" in result["error"]

    def test_local_name_rejected(self):
        result = add_cluster("local", host="x")
        assert result["status"] == "error"
        assert "reserved" in result["error"]

    def test_empty_name_rejected(self):
        result = add_cluster("", host="x")
        assert result["status"] == "error"

    def test_missing_host_rejected(self):
        result = add_cluster("alpha", host="")
        assert result["status"] == "error"
        assert "host" in result["error"]

    def test_invalid_name_rejected(self):
        for bad in ["1abc", "with space", "with!bang", "-leadhyphen"]:
            result = add_cluster(bad, host="x")
            assert result["status"] == "error", f"name {bad!r} should be rejected"

    def test_non_list_mount_paths_rejected(self):
        result = add_cluster("alpha", host="x", mount_paths={"not": "a list"})
        assert result["status"] == "error"
        assert "mount_paths" in result["error"]

    def test_string_mount_path_wrapped_in_list(self):
        result = add_cluster("alpha", host="x", mount_paths="/single/path/$USER")
        assert result["cluster"]["mount_paths"] == ["/single/path/$USER"]

    def test_position_auto_increments(self):
        add_cluster("a", host="x")
        add_cluster("b", host="x")
        add_cluster("c", host="x")
        positions = {c["name"]: c["position"] for c in list_clusters(include_local=False)}
        assert positions == {"a": 0, "b": 1, "c": 2}


@pytest.mark.unit
class TestSshDefaults:
    def test_empty_ssh_user_inherits_bootstrap(self, monkeypatch):
        from server import bootstrap as boot_mod
        monkeypatch.setattr(boot_mod, "_cached", boot_mod.Bootstrap(
            data_dir="/tmp", port=7272, ssh_user="bootuser",
            ssh_key="/boot/key", source_file=None,
        ))
        add_cluster("alpha", host="x")
        c = get_cluster("alpha")
        assert c["user"] == "bootuser"
        assert c["key"] == "/boot/key"

    def test_explicit_ssh_user_overrides_bootstrap(self, monkeypatch):
        from server import bootstrap as boot_mod
        monkeypatch.setattr(boot_mod, "_cached", boot_mod.Bootstrap(
            data_dir="/tmp", port=7272, ssh_user="bootuser",
            ssh_key="/boot/key", source_file=None,
        ))
        add_cluster("alpha", host="x", ssh_user="explicit", ssh_key="/k")
        c = get_cluster("alpha")
        assert c["user"] == "explicit"
        assert c["key"] == "/k"


@pytest.mark.unit
class TestListAndGet:
    def test_list_empty_returns_only_local_when_included(self):
        names = list_cluster_names()
        assert names == ["local"]

    def test_list_omits_local_when_requested(self):
        assert list_cluster_names(include_local=False) == []

    def test_list_returns_in_position_order(self):
        add_cluster("aaa", host="x", position=2)
        add_cluster("bbb", host="x", position=0)
        add_cluster("ccc", host="x", position=1)
        names = list_cluster_names(include_local=False)
        assert names == ["bbb", "ccc", "aaa"]

    def test_list_skips_disabled_by_default(self):
        add_cluster("on", host="x")
        add_cluster("off", host="x", enabled=False)
        assert "off" not in list_cluster_names(include_local=False)
        assert "off" in list_cluster_names(include_local=False, only_enabled=False)

    def test_get_returns_none_for_missing(self):
        assert get_cluster("nope") is None

    def test_get_local_returns_synthetic(self):
        c = get_cluster("local")
        assert c["host"] is None
        assert c["gpu_type"] == "local"

    def test_cluster_map_keyed_by_name(self):
        add_cluster("alpha", host="x")
        add_cluster("beta", host="y")
        m = cluster_map(include_local=False)
        assert set(m.keys()) == {"alpha", "beta"}
        assert m["alpha"]["host"] == "x"


@pytest.mark.unit
class TestUpdateCluster:
    def test_update_single_field(self):
        add_cluster("alpha", host="old.host")
        update_cluster("alpha", host="new.host")
        assert get_cluster("alpha")["host"] == "new.host"

    def test_update_multiple_fields(self):
        add_cluster("alpha", host="x", gpu_type="H100", gpus_per_node=8)
        update_cluster("alpha", gpu_type="B200", gpus_per_node=4, gpu_mem_gb=192)
        c = get_cluster("alpha")
        assert c["gpu_type"] == "B200"
        assert c["gpus_per_node"] == 4
        assert c["gpu_mem_gb"] == 192

    def test_update_mount_paths(self):
        add_cluster("alpha", host="x", mount_paths=["/old"])
        update_cluster("alpha", mount_paths=["/new1", "/new2"])
        assert get_cluster("alpha")["mount_paths"] == ["/new1", "/new2"]

    def test_update_mount_aliases(self):
        add_cluster("alpha", host="x")
        update_cluster("alpha", mount_aliases={"/sym": 1})
        assert get_cluster("alpha")["mount_aliases"] == {"/sym": 1}

    def test_update_enabled_flag(self):
        add_cluster("alpha", host="x", enabled=True)
        update_cluster("alpha", enabled=False)
        assert get_cluster("alpha")["enabled"] == 0

    def test_update_local_rejected(self):
        result = update_cluster("local", host="x")
        assert result["status"] == "error"

    def test_update_missing_rejected(self):
        result = update_cluster("nope", host="x")
        assert result["status"] == "error"
        assert "not found" in result["error"]

    def test_update_no_op_returns_existing(self):
        add_cluster("alpha", host="x")
        result = update_cluster("alpha")
        assert result["status"] == "ok"
        assert result["cluster"]["name"] == "alpha"

    def test_update_unknown_field_silently_ignored(self):
        add_cluster("alpha", host="x")
        result = update_cluster("alpha", made_up_field="x")
        assert result["status"] == "ok"

    def test_update_invalid_port_returns_error(self):
        add_cluster("alpha", host="x")
        result = update_cluster("alpha", port="not-a-port")
        assert result["status"] == "error"


@pytest.mark.unit
class TestRemoveCluster:
    def test_remove_round_trip(self):
        add_cluster("alpha", host="x")
        result = remove_cluster("alpha")
        assert result["status"] == "ok"
        assert result["removed"] == "alpha"
        assert get_cluster("alpha") is None

    def test_remove_local_rejected(self):
        result = remove_cluster("local")
        assert result["status"] == "error"

    def test_remove_missing_rejected(self):
        result = remove_cluster("nope")
        assert result["status"] == "error"


@pytest.mark.unit
class TestReorderClusters:
    def test_reorder_round_trip(self):
        add_cluster("a", host="x")
        add_cluster("b", host="x")
        add_cluster("c", host="x")
        reorder_clusters(["c", "a", "b"])
        assert list_cluster_names(include_local=False) == ["c", "a", "b"]

    def test_reorder_unknown_cluster_errors(self):
        add_cluster("a", host="x")
        result = reorder_clusters(["a", "ghost"])
        assert result["status"] == "error"

    def test_reorder_missing_clusters_pushed_to_end(self):
        add_cluster("a", host="x")
        add_cluster("b", host="x")
        add_cluster("c", host="x")
        reorder_clusters(["b"])
        assert list_cluster_names(include_local=False) == ["b", "a", "c"]


@pytest.mark.unit
class TestMountHelpers:
    def test_build_mount_map_uses_indexed_subdirs(self):
        add_cluster("alpha", host="x", mount_paths=["/p1/$USER", "/p2/$USER"])
        m = build_mount_map()
        assert "alpha" in m
        assert len(m["alpha"]) == 2
        assert m["alpha"][0].endswith("/0")
        assert m["alpha"][1].endswith("/1")

    def test_build_mount_map_falls_back_to_base_when_no_paths(self):
        add_cluster("alpha", host="x")
        m = build_mount_map()
        assert len(m["alpha"]) == 1
        assert m["alpha"][0].endswith("/alpha")

    def test_build_mount_map_env_override_wins(self, monkeypatch):
        add_cluster("alpha", host="x", mount_paths=["/x/$USER"])
        monkeypatch.setenv("CLAUSIUS_MOUNT_MAP", json.dumps({"alpha": "/custom/mount"}))
        m = build_mount_map()
        assert m["alpha"] == [os.path.abspath("/custom/mount")]

    def test_build_mount_map_env_skips_unknown_clusters(self, monkeypatch):
        add_cluster("alpha", host="x")
        monkeypatch.setenv(
            "CLAUSIUS_MOUNT_MAP",
            json.dumps({"ghost": "/ignored", "alpha": "/used"}),
        )
        m = build_mount_map()
        assert "ghost" not in m
        assert m["alpha"] == [os.path.abspath("/used")]

    def test_build_mount_remote_map_substitutes_user(self):
        add_cluster("alpha", host="x", mount_paths=["/lustre/$USER/data"])
        out = build_mount_remote_map(default_user="bob")
        assert out["alpha"] == ["/lustre/bob/data"]

    def test_build_mount_aliases_substitutes_user(self):
        add_cluster("alpha", host="x", mount_aliases={"/lustre/$USER/sym": 0})
        out = build_mount_aliases(default_user="alice")
        assert out["alpha"] == [("/lustre/alice/sym", 0)]


@pytest.mark.unit
class TestTeamGpuAllocations:
    def test_int_alloc_round_trip(self):
        add_cluster("alpha", host="x", team_gpu_alloc="128")
        out = build_team_gpu_allocations()
        assert out["alpha"] == 128

    def test_any_alloc_kept_as_string(self):
        add_cluster("alpha", host="x", team_gpu_alloc="any")
        out = build_team_gpu_allocations()
        assert out["alpha"] == "any"

    def test_empty_alloc_omitted(self):
        add_cluster("alpha", host="x")
        out = build_team_gpu_allocations()
        assert "alpha" not in out
