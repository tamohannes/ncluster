"""Integration tests for the v4 team / PPP REST endpoints."""

import pytest


@pytest.mark.integration
class TestTeamMembers:
    def test_list_empty(self, client):
        rows = client.get("/api/team/members").get_json()
        assert rows == []

    def test_create_round_trip(self, client):
        resp = client.post("/api/team/members", json={"username": "alice", "display_name": "Alice"})
        assert resp.status_code == 200
        rows = client.get("/api/team/members").get_json()
        assert any(m["username"] == "alice" for m in rows)

    def test_missing_username_400(self, client):
        resp = client.post("/api/team/members", json={"display_name": "x"})
        assert resp.status_code == 400

    def test_duplicate_400(self, client):
        client.post("/api/team/members", json={"username": "alice"})
        resp = client.post("/api/team/members", json={"username": "alice"})
        assert resp.status_code == 400

    def test_invalid_username_400(self, client):
        resp = client.post("/api/team/members", json={"username": "1bad"})
        assert resp.status_code == 400

    def test_delete_round_trip(self, client):
        client.post("/api/team/members", json={"username": "alice"})
        resp = client.delete("/api/team/members/alice")
        assert resp.status_code == 200
        rows = client.get("/api/team/members").get_json()
        assert not any(m["username"] == "alice" for m in rows)

    def test_delete_missing_404(self, client):
        resp = client.delete("/api/team/members/ghost")
        assert resp.status_code == 404


@pytest.mark.integration
class TestPppAccounts:
    def test_list_empty(self, client):
        assert client.get("/api/team/ppps").get_json() == []

    def test_create_round_trip(self, client):
        resp = client.post("/api/team/ppps", json={"name": "myppp", "ppp_id": "12345"})
        assert resp.status_code == 200
        rows = client.get("/api/team/ppps").get_json()
        assert rows[0]["ppp_id"] == "12345"

    def test_update_id(self, client):
        client.post("/api/team/ppps", json={"name": "myppp", "ppp_id": "100"})
        resp = client.put("/api/team/ppps/myppp", json={"ppp_id": "200"})
        assert resp.status_code == 200
        rows = client.get("/api/team/ppps").get_json()
        assert rows[0]["ppp_id"] == "200"

    def test_update_missing_404(self, client):
        resp = client.put("/api/team/ppps/ghost", json={"ppp_id": "1"})
        assert resp.status_code == 404

    def test_delete_round_trip(self, client):
        client.post("/api/team/ppps", json={"name": "myppp"})
        assert client.delete("/api/team/ppps/myppp").status_code == 200
        assert client.get("/api/team/ppps").get_json() == []
