"""Integration tests for the ``python -m server.cli`` command.

Exercises the actual CLI entry point — covers the round-trip from
argparse parsing through the CRUD layer to the SQLite DB.
"""

import json as _json
import os
import sys
import tempfile

import pytest

from server.cli import main
from server.db import init_db


@pytest.fixture
def tmp_data_dir(monkeypatch, tmp_path):
    """Point the bootstrap at a fresh data dir for each test."""
    from server import bootstrap
    bootstrap.reset_bootstrap()

    db_path = tmp_path / "_db" / "history.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    fake = bootstrap.Bootstrap(
        data_dir=str(tmp_path / "_db"),
        port=7272,
        ssh_user="testuser",
        ssh_key="/tmp/testkey",
        source_file=None,
    )
    monkeypatch.setattr(bootstrap, "_cached", fake)
    yield tmp_path
    bootstrap.reset_bootstrap()


@pytest.mark.integration
class TestCliSetup:
    def test_non_interactive_setup_creates_schema(self, tmp_data_dir, capsys):
        # _isolate_db autouse fixture already initialised the schema.
        # Re-running setup must be idempotent.
        rc = main(["setup", "--non-interactive"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "schema initialised" in out
        assert "clausius v4 setup" in out


@pytest.mark.integration
class TestCliClusters:
    def test_add_list_remove_round_trip(self, capsys):
        # The autouse mock-cluster fixture already added 'mock-cluster'.
        # Removing it gives us a clean slate.
        from server.clusters import remove_cluster
        remove_cluster("mock-cluster")
        capsys.readouterr()

        rc = main(["add-cluster", "alpha", "--host", "x.example.com",
                   "--gpu-type", "H100", "--gpus-per-node", "8"])
        assert rc == 0

        capsys.readouterr()
        rc = main(["list-clusters"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "alpha" in out
        assert "x.example.com" in out

        rc = main(["remove-cluster", "alpha"])
        assert rc == 0
        capsys.readouterr()
        rc = main(["list-clusters"])
        out = capsys.readouterr().out
        assert "alpha" not in out

    def test_add_cluster_with_mount_paths(self, capsys):
        rc = main(["add-cluster", "beta", "--host", "y.example.com",
                   "--mount-path", "/lustre/$USER",
                   "--mount-path", "/scratch/$USER"])
        assert rc == 0
        out = capsys.readouterr().out
        payload = _json.loads(out)
        assert payload["cluster"]["mount_paths"] == ["/lustre/$USER", "/scratch/$USER"]

    def test_show_cluster_missing(self, capsys):
        rc = main(["show-cluster", "ghost"])
        assert rc != 0


@pytest.mark.integration
class TestCliTeam:
    def test_team_round_trip(self, capsys):
        rc = main(["add-team-member", "alice", "--display-name", "Alice"])
        assert rc == 0
        capsys.readouterr()

        rc = main(["list-team"])
        out = capsys.readouterr().out
        assert "alice" in out

        rc = main(["remove-team-member", "alice"])
        assert rc == 0
        capsys.readouterr()
        rc = main(["list-team"])
        out = capsys.readouterr().out
        assert "alice" not in out


@pytest.mark.integration
class TestCliPpp:
    def test_ppp_round_trip(self, capsys):
        rc = main(["add-ppp", "my_acct", "--id", "12345"])
        assert rc == 0
        capsys.readouterr()

        rc = main(["list-ppp"])
        out = capsys.readouterr().out
        assert "my_acct" in out
        assert "12345" in out


@pytest.mark.integration
class TestCliPaths:
    def test_path_round_trip(self, capsys):
        rc = main(["add-path", "--kind", "log_search", "/lustre/$USER/logs"])
        assert rc == 0
        capsys.readouterr()

        rc = main(["list-paths", "--kind", "log_search"])
        out = capsys.readouterr().out
        assert "/lustre/$USER/logs" in out


@pytest.mark.integration
class TestCliFilters:
    def test_filter_round_trip(self, capsys):
        rc = main(["add-filter", "--mode", "include", "nemo-skills"])
        assert rc == 0
        capsys.readouterr()

        rc = main(["list-filters", "--mode", "include"])
        out = capsys.readouterr().out
        assert "nemo-skills" in out


@pytest.mark.integration
class TestCliSettings:
    def test_set_get_round_trip(self, capsys):
        rc = main(["set", "ssh_timeout", "42"])
        assert rc == 0
        capsys.readouterr()

        rc = main(["get", "ssh_timeout"])
        out = capsys.readouterr().out.strip()
        assert out == "42"

    def test_unknown_key_returns_default(self, capsys):
        rc = main(["get", "team_name"])
        out = capsys.readouterr().out.strip()
        # default is empty string
        assert out == ""

    def test_unset_returns_to_default(self, capsys):
        main(["set", "ssh_timeout", "99"])
        capsys.readouterr()
        rc = main(["unset", "ssh_timeout"])
        assert rc == 0
        capsys.readouterr()
        rc = main(["get", "ssh_timeout"])
        out = capsys.readouterr().out.strip()
        # default is 5 — confirm we got the registered default back
        assert out == "5"

    def test_settings_listing_includes_descriptions(self, capsys):
        rc = main(["settings"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "ssh_timeout" in out
        assert "team_name" in out


@pytest.mark.integration
class TestCliImportJson:
    def test_imports_v3_config(self, tmp_path, capsys):
        cfg = {
            "port": 7272,
            "team": "myteam",
            "team_members": ["alice", "bob"],
            "ppps": {"acct1": "111", "acct2": "222"},
            "ppp_accounts": ["acct1", "acct2"],
            "log_search_bases": ["/lustre/$USER/logs"],
            "nemo_run_bases": ["/lustre/$USER/nemo-run"],
            "mount_lustre_prefixes": ["lustre/$USER"],
            "local_process_filters": {
                "include": ["nemo-skills"],
                "exclude": ["jupyter"],
            },
            "ssh_timeout": 8,
            "clusters": {
                "alpha": {
                    "host": "alpha.example.com",
                    "gpu_type": "H100",
                    "gpus_per_node": 8,
                    "mount_paths": ["/lustre/$USER/data"],
                },
            },
            "team_gpu_allocations": {"alpha": 256},
        }
        path = tmp_path / "v3.json"
        path.write_text(_json.dumps(cfg))

        # Clean the autouse mock cluster so it doesn't conflict.
        from server.clusters import remove_cluster
        remove_cluster("mock-cluster")

        rc = main(["import-json", str(path)])
        assert rc == 0
        out = _json.loads(capsys.readouterr().out)
        summary = out["imported"]
        assert summary["clusters_added"] == 1
        assert summary["team_members_added"] == 2
        assert summary["ppp_added"] == 2
        assert summary["paths_added"] == 3
        assert summary["filters_added"] == 2
        assert summary["settings_set"] >= 2  # team + ssh_timeout

        # Verify a few round-trips
        from server.clusters import get_cluster
        from server.settings import get_setting, get_team_name
        from server.team import get_ppp_account, get_team_member

        assert get_team_name() == "myteam"
        assert get_setting("ssh_timeout") == 8
        assert get_team_member("alice") is not None
        assert get_ppp_account("acct1")["ppp_id"] == "111"
        c = get_cluster("alpha")
        assert c["host"] == "alpha.example.com"
        assert c["mount_paths"] == ["/lustre/$USER/data"]
        assert c["team_gpu_alloc"] == "256"

    def test_import_missing_file(self, capsys):
        rc = main(["import-json", "/nonexistent/path.json"])
        assert rc != 0


@pytest.mark.integration
class TestCliErrorCodes:
    def test_unknown_subcommand_exits(self):
        with pytest.raises(SystemExit):
            main(["totally-not-a-command"])

    def test_missing_required_arg(self):
        with pytest.raises(SystemExit):
            main(["add-cluster", "alpha"])  # missing --host
