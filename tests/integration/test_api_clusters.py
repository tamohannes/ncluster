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
