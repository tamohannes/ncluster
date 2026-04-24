"""Test the 30 s aggregator-result cache on /api/where_to_submit.

Multiple Cursor agents tend to call the same MCP tool within seconds
of each other; before this cache, each call triggered an independent
multi-cluster SSH wave. With the cache, the second call inside the
TTL window reuses the first call's result without hitting any of the
slow underlying paths.
"""

from unittest.mock import patch

import pytest

from server.config import _aggregator_cache, _warm_lock


@pytest.fixture(autouse=True)
def _reset_aggregator_cache():
    with _warm_lock:
        _aggregator_cache.clear()
    yield
    with _warm_lock:
        _aggregator_cache.clear()


@pytest.mark.integration
class TestWhereToSubmitAggregatorCache:
    def test_second_call_within_ttl_reuses_result(self, client):
        call_count = {"alloc": 0, "fs": 0, "parts": 0, "tj": 0}

        def fake_alloc():
            call_count["alloc"] += 1
            return {"clusters": {}}

        def fake_fs():
            call_count["fs"] += 1
            return {"clusters": {}}

        def fake_parts():
            call_count["parts"] += 1
            return {}

        def fake_tj(_c):
            call_count["tj"] += 1
            return {"jobs": [], "total_running_gpus": 0}

        with patch("server.aihub.get_ppp_allocations", side_effect=fake_alloc), \
             patch("server.aihub.get_my_fairshare", side_effect=fake_fs), \
             patch("server.partitions.get_partition_summary", side_effect=fake_parts), \
             patch("server.jobs.fetch_team_jobs", side_effect=fake_tj):
            r1 = client.post("/api/where_to_submit", json={"nodes": 1, "gpus_per_node": 8})
            r2 = client.post("/api/where_to_submit", json={"nodes": 1, "gpus_per_node": 8})

        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r1.get_json() == r2.get_json()

        # First call hits the upstream once each; second call must not
        # touch them at all.
        assert call_count["alloc"] == 1
        assert call_count["fs"] == 1
        assert call_count["parts"] == 1

    def test_force_payload_bypasses_cache(self, client):
        calls = {"n": 0}

        def fake_alloc():
            calls["n"] += 1
            return {"clusters": {}}

        with patch("server.aihub.get_ppp_allocations", side_effect=fake_alloc), \
             patch("server.aihub.get_my_fairshare", return_value={"clusters": {}}), \
             patch("server.partitions.get_partition_summary", return_value={}), \
             patch("server.jobs.fetch_team_jobs", return_value={"jobs": []}):
            client.post("/api/where_to_submit", json={"nodes": 1})
            client.post("/api/where_to_submit", json={"nodes": 1, "force": True})

        assert calls["n"] == 2

    def test_different_args_get_separate_cache_entries(self, client):
        calls = {"n": 0}

        def fake_alloc():
            calls["n"] += 1
            return {"clusters": {}}

        with patch("server.aihub.get_ppp_allocations", side_effect=fake_alloc), \
             patch("server.aihub.get_my_fairshare", return_value={"clusters": {}}), \
             patch("server.partitions.get_partition_summary", return_value={}), \
             patch("server.jobs.fetch_team_jobs", return_value={"jobs": []}):
            client.post("/api/where_to_submit", json={"nodes": 1, "gpus_per_node": 8})
            client.post("/api/where_to_submit", json={"nodes": 4, "gpus_per_node": 8})
            client.post("/api/where_to_submit", json={"nodes": 1, "gpu_type": "H100"})

        assert calls["n"] == 3
