"""Regression test for the parallel team_jobs fetch in where_to_submit.

Pre-fix the per-cluster team_jobs fetches inside ``api_where_to_submit``
were dispatched by a single worker that iterated CLUSTERS in a list
comprehension, so the wall time of the whole call was N × per-cluster
SSH time (observed 12-17 s with 9 clusters in production logs). The
fix fans the team_jobs work out alongside alloc / fs / partition_summary
in one shared pool, so total wall time is bounded by the slowest single
cluster.

This test mocks every upstream callable to a controlled sleep and asserts
the route returns in roughly one cluster's worth of time, not N.
"""

import time
from unittest.mock import patch

import pytest

from server.clusters import add_cluster


_SLEEP = 0.4   # per-cluster fetch latency (sleep)
_N_EXTRA_CLUSTERS = 5   # plus the autouse mock-cluster that conftest seeds


@pytest.mark.integration
class TestWhereToSubmitParallel:
    def test_team_jobs_fetched_in_parallel(self, client):
        # Seed enough clusters to make a serial vs parallel difference
        # obvious but keep the test fast.
        for i in range(_N_EXTRA_CLUSTERS):
            add_cluster(f"par{i}", host=f"par{i}.example.com",
                        gpu_type="H100", team_gpu_alloc="100")

        def slow_team_jobs(_cluster):
            time.sleep(_SLEEP)
            return {"jobs": [], "total_running_gpus": 0}

        # _wts_alloc returns a non-empty payload so the route doesn't
        # short-circuit with "could not fetch allocation data".
        fake_alloc = {
            "clusters": {f"par{i}": {} for i in range(_N_EXTRA_CLUSTERS)},
        }

        with patch("server.aihub.get_ppp_allocations", return_value=fake_alloc), \
             patch("server.aihub.get_my_fairshare", return_value={"clusters": {}}), \
             patch("server.partitions.get_partition_summary", return_value={}), \
             patch("server.jobs.fetch_team_jobs", side_effect=slow_team_jobs):
            t0 = time.monotonic()
            resp = client.post("/api/where_to_submit", json={"nodes": 1, "gpus_per_node": 8})
            elapsed = time.monotonic() - t0

        assert resp.status_code == 200, resp.get_data(as_text=True)
        # Serial would have been at least (N_EXTRA + 1) * _SLEEP seconds.
        # With parallel dispatch it should finish in ~1 sleep + a bit of
        # bookkeeping. Allow generous slack for slow CI.
        serial_floor = (_N_EXTRA_CLUSTERS + 1) * _SLEEP
        parallel_ceiling = _SLEEP * 3
        assert elapsed < parallel_ceiling, (
            f"where_to_submit took {elapsed:.2f}s; expected < {parallel_ceiling:.2f}s "
            f"(serial would have been ≥ {serial_floor:.2f}s)"
        )
