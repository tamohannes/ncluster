"""Unit tests for AI Hub API routes."""

import json
import pytest


class TestAihubAllocationsRoute:
    @pytest.mark.unit
    def test_allocations_returns_ok(self, client, monkeypatch):
        monkeypatch.setattr(
            "server.routes._aihub_alloc",
            lambda accounts=None, clusters=None, force=False: {"clusters": {"eos": {"accounts": {}}}}
        )
        resp = client.get("/api/aihub/allocations")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "clusters" in data

    @pytest.mark.unit
    def test_allocations_with_accounts_param(self, client, monkeypatch):
        captured = {}
        def mock_alloc(accounts=None, clusters=None, force=False):
            captured["accounts"] = accounts
            return {"clusters": {}}
        monkeypatch.setattr("server.routes._aihub_alloc", mock_alloc)
        client.get("/api/aihub/allocations?accounts=acct_a,acct_b")
        assert captured["accounts"] == ["acct_a", "acct_b"]


class TestAihubHistoryRoute:
    @pytest.mark.unit
    def test_history_returns_ok(self, client, monkeypatch):
        monkeypatch.setattr(
            "server.routes._aihub_history",
            lambda accounts=None, clusters=None, days=14, interval="1d": {"clusters": {}, "days": days}
        )
        resp = client.get("/api/aihub/history?days=7&cluster=eos")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["days"] == 7


class TestAihubUsersRoute:
    @pytest.mark.unit
    def test_users_requires_params(self, client):
        resp = client.get("/api/aihub/users")
        data = resp.get_json()
        assert data["status"] == "error"
        assert resp.status_code == 400

    @pytest.mark.unit
    def test_users_returns_ok(self, client, monkeypatch):
        monkeypatch.setattr(
            "server.routes._aihub_users",
            lambda account, cluster, days=7: {"users": [], "account": account, "cluster": cluster}
        )
        resp = client.get("/api/aihub/users?account=test&cluster=eos")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["account"] == "test"


class TestTeamJobsRoute:
    @pytest.mark.unit
    def test_team_jobs_returns_ok(self, client, monkeypatch):
        monkeypatch.setattr(
            "server.routes.fetch_team_jobs",
            lambda c: {"jobs": [], "summary": {"by_user": {}, "total_running": 0, "total_pending": 0, "total_dependent": 0}}
        )
        resp = client.get("/api/team_jobs?cluster=mock-cluster")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "clusters" in data


class TestRecommendRoute:
    @pytest.mark.unit
    def test_recommend_accepts_accounts(self, client, monkeypatch):
        captured = {}
        def mock_recommend(**kwargs):
            captured.update(kwargs)
            return []
        monkeypatch.setattr("server.recommendations.recommend", mock_recommend)
        client.post("/api/recommend",
                     data=json.dumps({"nodes": 1, "accounts": ["a", "b"]}),
                     content_type="application/json")
        assert captured.get("accounts") == ["a", "b"]
