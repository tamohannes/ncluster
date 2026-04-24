"""Tests that aggregator routes skip clusters with an open SSH circuit breaker.

Stops a known-broken cluster (SSH auth perm-denied, host down, etc.) from
costing every aggregator call ~3 s of guaranteed-failed SSH retries. The
skipped clusters are surfaced in the response under ``skipped_clusters``
so the UI / agents can show why a cluster is missing from the result.
"""

import time
from unittest.mock import patch

import pytest

from server.clusters import add_cluster
from server.ssh import _cb_failures, _cb_lock


@pytest.fixture(autouse=True)
def _reset_circuit_breakers():
    """Tests in this file mutate the SSH circuit-breaker state directly.
    Restore a clean slate before AND after so they don't leak."""
    with _cb_lock:
        _cb_failures.clear()
    yield
    with _cb_lock:
        _cb_failures.clear()


def _open_breaker(cluster_name: str) -> None:
    """Force a cluster's circuit breaker to open with a long cooldown."""
    with _cb_lock:
        _cb_failures[cluster_name] = {"ts": time.monotonic(), "count": 5}


@pytest.mark.integration
class TestWhereToSubmitSkipsBroken:
    def test_open_breaker_cluster_is_skipped_and_call_stays_fast(self, client):
        # Two test clusters: one healthy, one with the breaker open.
        add_cluster("ok1", host="ok1.example.com", gpu_type="H100", team_gpu_alloc="100")
        add_cluster("broken1", host="broken1.example.com", gpu_type="H100", team_gpu_alloc="100")
        _open_breaker("broken1")

        seen = []

        def slow_team_jobs(cluster):
            seen.append(cluster)
            time.sleep(0.4)
            return {"jobs": [], "total_running_gpus": 0}

        fake_alloc = {"clusters": {"ok1": {}, "broken1": {}}}

        with patch("server.aihub.get_ppp_allocations", return_value=fake_alloc), \
             patch("server.aihub.get_my_fairshare", return_value={"clusters": {}}), \
             patch("server.partitions.get_partition_summary", return_value={}), \
             patch("server.jobs.fetch_team_jobs", side_effect=slow_team_jobs):
            resp = client.post("/api/where_to_submit", json={"nodes": 1})

        assert resp.status_code == 200
        data = resp.get_json()
        assert "broken1" in data["skipped_clusters"], data
        assert "broken1" not in seen, "fetch_team_jobs was called for a CB-open cluster"


@pytest.mark.integration
class TestTeamJobsForceSkipsBroken:
    def test_open_breaker_cluster_is_skipped(self, client):
        add_cluster("ok2", host="ok2.example.com", gpu_type="H100")
        add_cluster("broken2", host="broken2.example.com", gpu_type="H100")
        _open_breaker("broken2")

        seen = []

        def slow_fetch(cluster):
            seen.append(cluster)
            time.sleep(0.3)
            return {"jobs": [], "total_running_gpus": 0}

        with patch("server.jobs.fetch_team_jobs", side_effect=slow_fetch):
            resp = client.get("/api/team_jobs?force=1")

        assert resp.status_code == 200
        data = resp.get_json()
        assert "broken2" in data["skipped_clusters"], data
        assert "broken2" not in seen
