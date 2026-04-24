"""MCP tool contract tests for v4 configuration management tools.

Exercises the new cluster/team/ppp/path/filter/setting tools through
the same Flask test client path that the real MCP server uses.
"""

import pytest

from server.db import init_db


@pytest.fixture(autouse=True)
def _init_mcp_config_db(_isolate_db):
    init_db()


@pytest.mark.mcp
class TestMcpClusterTools:
    def test_list_cluster_configs_empty(self, client):
        from server.clusters import remove_cluster
        remove_cluster("mock-cluster")
        resp = client.get("/api/clusters")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_add_get_remove_round_trip(self, client):
        resp = client.post("/api/clusters", json={"name": "testcl", "host": "test.example.com", "gpu_type": "H100"})
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "ok"

        resp = client.get("/api/clusters/testcl")
        assert resp.get_json()["cluster"]["gpu_type"] == "H100"

        resp = client.delete("/api/clusters/testcl")
        assert resp.get_json()["status"] == "ok"
        assert client.get("/api/clusters/testcl").status_code == 404

    def test_update_cluster_config(self, client):
        client.post("/api/clusters", json={"name": "testcl", "host": "old.host"})
        resp = client.put("/api/clusters/testcl", json={"host": "new.host"})
        assert resp.get_json()["cluster"]["host"] == "new.host"


@pytest.mark.mcp
class TestMcpTeamTools:
    def test_add_list_remove_round_trip(self, client):
        resp = client.post("/api/team/members", json={"username": "alice", "display_name": "Alice"})
        assert resp.status_code == 200

        rows = client.get("/api/team/members").get_json()
        assert any(m["username"] == "alice" for m in rows)

        assert client.delete("/api/team/members/alice").status_code == 200
        rows = client.get("/api/team/members").get_json()
        assert not any(m["username"] == "alice" for m in rows)


@pytest.mark.mcp
class TestMcpPppTools:
    def test_add_update_remove_round_trip(self, client):
        resp = client.post("/api/team/ppps", json={"name": "myacct", "ppp_id": "100"})
        assert resp.status_code == 200

        resp = client.put("/api/team/ppps/myacct", json={"ppp_id": "200"})
        assert resp.get_json()["account"]["ppp_id"] == "200"

        assert client.delete("/api/team/ppps/myacct").status_code == 200


@pytest.mark.mcp
class TestMcpPathTools:
    def test_add_list_remove_round_trip(self, client):
        resp = client.post("/api/paths/log_search", json={"path": "/lustre/$USER/logs"})
        assert resp.status_code == 200

        rows = client.get("/api/paths/log_search").get_json()
        assert rows[0]["path"] == "/lustre/$USER/logs"

        resp = client.delete("/api/paths/log_search", json={"path": "/lustre/$USER/logs"})
        assert resp.status_code == 200


@pytest.mark.mcp
class TestMcpFilterTools:
    def test_add_list_remove_round_trip(self, client):
        resp = client.post("/api/process_filters/include", json={"pattern": "nemo-skills"})
        assert resp.status_code == 200

        rows = client.get("/api/process_filters/include").get_json()
        assert rows[0]["pattern"] == "nemo-skills"

        resp = client.delete("/api/process_filters/include", json={"pattern": "nemo-skills"})
        assert resp.status_code == 200


@pytest.mark.mcp
class TestMcpAppSettingTools:
    def test_get_default(self, client):
        resp = client.get("/api/settings/ssh_timeout")
        data = resp.get_json()
        assert data["value"] == 5
        assert data["source"] == "default"

    def test_set_get_delete_round_trip(self, client):
        resp = client.put("/api/settings/ssh_timeout", json={"value": 42})
        assert resp.status_code == 200
        assert resp.get_json()["value"] == 42

        resp = client.get("/api/settings/ssh_timeout")
        assert resp.get_json()["value"] == 42

        client.delete("/api/settings/ssh_timeout")
        resp = client.get("/api/settings/ssh_timeout")
        assert resp.get_json()["value"] == 5  # back to default
