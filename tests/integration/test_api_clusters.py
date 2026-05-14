"""Integration tests for the per-cluster REST endpoints introduced in v4."""

import json

import pytest


@pytest.mark.integration
class TestClustersListGet:
    def test_list_excludes_local(self, client):
        rows = client.get("/api/clusters").get_json()
        names = {c["name"] for c in rows}
        assert "local" not in names

    def test_list_includes_mock_cluster(self, client):
        rows = client.get("/api/clusters").get_json()
        names = {c["name"] for c in rows}
        assert "mock-cluster" in names

    def test_get_existing(self, client):
        resp = client.get("/api/clusters/mock-cluster")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["cluster"]["host"] == "mock-login.example.com"

    def test_get_missing_returns_404(self, client):
        resp = client.get("/api/clusters/nonexistent")
        assert resp.status_code == 404


@pytest.mark.integration
class TestClustersCreate:
    def test_basic_create(self, client):
        resp = client.post("/api/clusters", json={
            "name": "newcluster",
            "host": "new.example.com",
            "gpu_type": "B200",
            "gpus_per_node": 8,
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["cluster"]["host"] == "new.example.com"

    def test_missing_name_400(self, client):
        resp = client.post("/api/clusters", json={"host": "x"})
        assert resp.status_code == 400

    def test_missing_host_400(self, client):
        resp = client.post("/api/clusters", json={"name": "alpha"})
        assert resp.status_code == 400

    def test_duplicate_400(self, client):
        client.post("/api/clusters", json={"name": "dup", "host": "x"})
        resp = client.post("/api/clusters", json={"name": "dup", "host": "y"})
        assert resp.status_code == 400


@pytest.mark.integration
class TestClustersUpdate:
    def test_update_host(self, client):
        client.post("/api/clusters", json={"name": "alpha", "host": "old.example.com"})
        resp = client.put("/api/clusters/alpha", json={"host": "new.example.com"})
        assert resp.status_code == 200
        assert resp.get_json()["cluster"]["host"] == "new.example.com"

    def test_update_mount_paths(self, client):
        client.post("/api/clusters", json={"name": "alpha", "host": "x"})
        resp = client.put("/api/clusters/alpha",
                          json={"mount_paths": ["/lustre/$USER", "/scratch/$USER"]})
        assert resp.status_code == 200
        assert resp.get_json()["cluster"]["mount_paths"] == ["/lustre/$USER", "/scratch/$USER"]

    def test_update_missing_404(self, client):
        resp = client.put("/api/clusters/ghost", json={"host": "x"})
        assert resp.status_code == 404

    def test_update_local_400(self, client):
        resp = client.put("/api/clusters/local", json={"host": "x"})
        assert resp.status_code == 400


@pytest.mark.integration
class TestClustersDelete:
    def test_delete_round_trip(self, client):
        client.post("/api/clusters", json={"name": "doomed", "host": "x"})
        resp = client.delete("/api/clusters/doomed")
        assert resp.status_code == 200
        assert client.get("/api/clusters/doomed").status_code == 404

    def test_delete_missing_404(self, client):
        resp = client.delete("/api/clusters/ghost")
        assert resp.status_code == 404


@pytest.mark.integration
class TestClusterResolveEndpoint:
    """GET /api/cluster_resolve — read-only name normalization."""

    def _seed(self, client):
        client.post("/api/clusters", json={
            "name": "aws-cmh",
            "host": "aws-cmh-slurm-1-login-01.nvidia.com",
            "aliases": ["aws-cmh-science"],
        })

    def test_canonical_lookup(self, client):
        self._seed(client)
        resp = client.get("/api/cluster_resolve?name=aws-cmh")
        assert resp.status_code == 200
        assert resp.get_json() == {"canonical": "aws-cmh", "source": "canonical"}

    def test_alias_lookup(self, client):
        self._seed(client)
        resp = client.get("/api/cluster_resolve?name=aws-cmh-science")
        assert resp.status_code == 200
        assert resp.get_json() == {
            "canonical": "aws-cmh",
            "source": "alias",
            "matched_alias": "aws-cmh-science",
        }

    def test_host_fallback(self, client):
        self._seed(client)
        resp = client.get(
            "/api/cluster_resolve?name=unknown-yaml-name"
            "&host=aws-cmh-slurm-1-login-01.nvidia.com"
        )
        assert resp.status_code == 200
        assert resp.get_json() == {"canonical": "aws-cmh", "source": "host"}

    def test_unknown_returns_404(self, client):
        self._seed(client)
        resp = client.get("/api/cluster_resolve?name=not-a-cluster")
        assert resp.status_code == 404
        body = resp.get_json()
        assert body == {"error": "no_match", "name": "not-a-cluster"}

    def test_missing_name_returns_404(self, client):
        resp = client.get("/api/cluster_resolve")
        assert resp.status_code == 404

    def test_clusters_list_exposes_aliases(self, client):
        self._seed(client)
        rows = client.get("/api/clusters").get_json()
        cmh = next(c for c in rows if c["name"] == "aws-cmh")
        assert cmh["aliases"] == ["aws-cmh-science"]

    def test_post_aliases_collision_400(self, client):
        self._seed(client)
        resp = client.post("/api/clusters", json={
            "name": "other",
            "host": "y",
            "aliases": ["aws-cmh-science"],
        })
        assert resp.status_code == 400


@pytest.mark.integration
class TestSdkIngestClusterNormalization:
    """SDK ingest must store the canonical cluster even when the client
    emits a non-canonical alias for ``run_started`` / ``job_*`` events."""

    def _seed(self, client):
        client.post("/api/clusters", json={
            "name": "aws-cmh",
            "host": "aws-cmh-slurm-1-login-01.nvidia.com",
            "aliases": ["aws-cmh-science"],
        })

    def test_run_started_stores_canonical_cluster(self, client):
        self._seed(client)
        run_uuid = "abc123" * 5 + "ab"
        resp = client.post("/api/sdk/events", json=[{
            "run_uuid": run_uuid,
            "event_type": "run_started",
            "event_seq": 1,
            "ts": 1700000000.0,
            "payload": {
                "expname": "hle_eval_demo",
                "cluster": "aws-cmh-science",
                "output_dir": "/tmp/clausius_test",
                "argv": ["ns", "eval"],
                "command": "ns eval",
                "cwd": "/tmp",
                "git_commit": "deadbeef",
                "hostname": "host",
                "env_subset": {},
                "config_overrides": {},
                "conda_env": "",
                "python_executable": "/usr/bin/python",
                "env_vars_set": [],
                "params": {},
            },
        }])
        assert resp.status_code == 200

        from server.db import get_run_by_uuid, get_db
        run = get_run_by_uuid(run_uuid)
        assert run is not None
        assert run["cluster"] == "aws-cmh"

        con = get_db()
        rows = con.execute(
            "SELECT cluster FROM job_history WHERE job_id=?",
            (f"sdk-{run_uuid[:12]}",),
        ).fetchall()
        assert rows
        assert all(r["cluster"] == "aws-cmh" for r in rows)
