"""Integration tests for the v4 path / process_filter / single-setting endpoints."""

import pytest


@pytest.mark.integration
class TestPathBases:
    def test_list_empty_for_known_kind(self, client):
        assert client.get("/api/paths/log_search").get_json() == []

    def test_unknown_kind_404(self, client):
        assert client.get("/api/paths/bogus_kind").status_code == 404

    def test_add_round_trip(self, client):
        resp = client.post("/api/paths/log_search", json={"path": "/lustre/$USER/logs"})
        assert resp.status_code == 200
        rows = client.get("/api/paths/log_search").get_json()
        assert rows[0]["path"] == "/lustre/$USER/logs"

    def test_add_empty_400(self, client):
        resp = client.post("/api/paths/log_search", json={"path": ""})
        assert resp.status_code == 400

    def test_remove_by_path(self, client):
        client.post("/api/paths/log_search", json={"path": "/p"})
        resp = client.delete("/api/paths/log_search", json={"path": "/p"})
        assert resp.status_code == 200
        assert client.get("/api/paths/log_search").get_json() == []

    def test_remove_by_id(self, client):
        client.post("/api/paths/log_search", json={"path": "/p"})
        rows = client.get("/api/paths/log_search").get_json()
        entry_id = rows[0]["id"]
        resp = client.delete("/api/paths/log_search", json={"id": entry_id})
        assert resp.status_code == 200

    def test_remove_missing_404(self, client):
        resp = client.delete("/api/paths/log_search", json={"path": "/ghost"})
        assert resp.status_code == 404


@pytest.mark.integration
class TestProcessFilters:
    def test_unknown_mode_404(self, client):
        assert client.get("/api/process_filters/bogus_mode").status_code == 404

    def test_add_round_trip(self, client):
        resp = client.post("/api/process_filters/include", json={"pattern": "nemo-skills"})
        assert resp.status_code == 200
        rows = client.get("/api/process_filters/include").get_json()
        assert rows[0]["pattern"] == "nemo-skills"

    def test_remove_round_trip(self, client):
        client.post("/api/process_filters/include", json={"pattern": "p"})
        resp = client.delete("/api/process_filters/include", json={"pattern": "p"})
        assert resp.status_code == 200


@pytest.mark.integration
class TestSingleSettingEndpoint:
    def test_get_default(self, client):
        resp = client.get("/api/settings/ssh_timeout")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["value"] == 5
        assert data["source"] == "default"

    def test_unknown_key_404(self, client):
        resp = client.get("/api/settings/totally_unknown_key")
        assert resp.status_code == 404

    def test_put_value(self, client):
        resp = client.put("/api/settings/ssh_timeout", json={"value": 99})
        assert resp.status_code == 200
        assert resp.get_json()["value"] == 99
        # Re-read
        data = client.get("/api/settings/ssh_timeout").get_json()
        assert data["value"] == 99
        assert data["source"] == "db"

    def test_put_missing_value_400(self, client):
        resp = client.put("/api/settings/ssh_timeout", json={})
        assert resp.status_code == 400

    def test_put_invalid_value_400(self, client):
        resp = client.put("/api/settings/ssh_timeout", json={"value": "not-a-number"})
        assert resp.status_code == 400

    def test_delete_returns_to_default(self, client):
        client.put("/api/settings/ssh_timeout", json={"value": 99})
        client.delete("/api/settings/ssh_timeout")
        data = client.get("/api/settings/ssh_timeout").get_json()
        assert data["value"] == 5
