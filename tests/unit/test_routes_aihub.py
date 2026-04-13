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

    @pytest.mark.unit
    def test_allocations_with_cluster_param(self, client, monkeypatch):
        captured = {}
        def mock_alloc(accounts=None, clusters=None, force=False):
            captured["clusters"] = clusters
            captured["force"] = force
            return {"clusters": {}}
        monkeypatch.setattr("server.routes._aihub_alloc", mock_alloc)
        client.get("/api/aihub/allocations?cluster=mock-cluster&force=1")
        assert captured["clusters"] == ["mock-cluster"]
        assert captured["force"] is True


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


class TestAihubTeamOverlayRoute:
    @pytest.mark.unit
    def test_team_overlay_with_cluster_param(self, client, monkeypatch):
        captured = {}
        def mock_overlay(clusters=None, force=False):
            captured["clusters"] = clusters
            captured["force"] = force
            return {"clusters": {}, "current_user": "test", "team_members": []}
        monkeypatch.setattr("server.routes._aihub_team_overlay", mock_overlay)
        resp = client.get("/api/aihub/team_overlay?cluster=mock-cluster&force=1")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert captured["clusters"] == ["mock-cluster"]
        assert captured["force"] is True


class TestAihubMyFairshareRoute:
    @pytest.mark.unit
    def test_my_fairshare_with_cluster_param(self, client, monkeypatch):
        captured = {}
        def mock_fairshare(clusters=None, force=False):
            captured["clusters"] = clusters
            captured["force"] = force
            return {"user": "testuser", "clusters": {}}
        monkeypatch.setattr("server.routes._aihub_my_fairshare", mock_fairshare)
        resp = client.get("/api/aihub/my_fairshare?cluster=mock-cluster&force=1")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert captured["clusters"] == ["mock-cluster"]
        assert captured["force"] is True


class TestPartitionSummaryRoute:
    @pytest.mark.unit
    def test_partition_summary_with_cluster_param(self, client, mock_cluster, monkeypatch):
        captured = {}

        def mock_get_partitions(cluster, force=False):
            captured["cluster"] = cluster
            captured["force"] = force
            return [{
                "name": "batch",
                "state": "UP",
                "user_accessible": True,
                "max_time": "4:00:00",
                "priority_tier": 10,
                "total_nodes": 4,
                "idle_nodes": 2,
                "pending_jobs": 3,
                "gpus_per_node": 8,
                "preempt_mode": "OFF",
            }]

        monkeypatch.setattr("server.routes._get_partitions", mock_get_partitions)
        resp = client.get(f"/api/partition_summary?cluster={mock_cluster}&force=1")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert captured == {"cluster": mock_cluster, "force": True}
        assert data["clusters"][mock_cluster]["idle_nodes"] == 2
        assert data["clusters"][mock_cluster]["pending_jobs"] == 3


class TestTeamJobsRoute:
    @pytest.mark.unit
    def test_team_jobs_returns_ok(self, client, monkeypatch):
        from server.config import _cache_set
        from server.jobs import _team_jobs_cache
        _cache_set(_team_jobs_cache, "mock-cluster",
                   {"jobs": [], "summary": {"by_user": {}, "total_running": 0, "total_pending": 0, "total_dependent": 0}})
        resp = client.get("/api/team_jobs?cluster=mock-cluster")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "clusters" in data

    @pytest.mark.unit
    def test_team_jobs_fetches_when_cache_empty(self, client, monkeypatch):
        monkeypatch.setattr(
            "server.routes.fetch_team_jobs",
            lambda cluster: {"jobs": [{"job_name": "x"}], "summary": {"by_user": {}}},
        )
        resp = client.get("/api/team_jobs?cluster=mock-cluster")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "mock-cluster" in data["clusters"]

    @pytest.mark.unit
    def test_team_jobs_force_bypasses_cache(self, client, monkeypatch):
        from server.config import _cache_set
        from server.jobs import _team_jobs_cache
        _cache_set(_team_jobs_cache, "mock-cluster", {"jobs": [], "summary": {"by_user": {"cached": {}}}})
        monkeypatch.setattr(
            "server.routes.fetch_team_jobs",
            lambda cluster: {"jobs": [{"job_name": "fresh"}], "summary": {"by_user": {"fresh": {}}}},
        )
        resp = client.get("/api/team_jobs?cluster=mock-cluster&force=1")
        data = resp.get_json()
        assert "fresh" in data["clusters"]["mock-cluster"]["summary"]["by_user"]


class TestTeamUsageRoute:
    @pytest.mark.unit
    def test_team_usage_fetches_when_cache_empty(self, client, mock_cluster, monkeypatch):
        monkeypatch.setattr(
            "server.routes.fetch_team_usage",
            lambda cluster: {"account": "acct", "users": {"alice": {"running_gpus": 8, "pending_gpus": 0}}},
        )
        resp = client.post(
            "/api/team_usage",
            data=json.dumps({"clusters": [mock_cluster], "force": True}),
            content_type="application/json",
        )
        data = resp.get_json()
        assert data["status"] == "ok"
        assert mock_cluster in data["team_usage"]


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
