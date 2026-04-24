"""Regression test for the parallel force=1 path in /api/team_jobs.

The force=1 branch used to iterate clusters sequentially. In production
this showed up as 158 slow-request log entries at 2-4 s each (one per
slow cluster summed). This test seeds multiple clusters, mocks
``fetch_team_jobs`` to a controlled sleep, and asserts the route
returns in roughly one cluster's worth of time, not N.
"""

import time
from unittest.mock import patch

import pytest

from server.clusters import add_cluster


_SLEEP = 0.4
_N_EXTRA_CLUSTERS = 5


@pytest.mark.integration
class TestTeamJobsForceParallel:
    def test_force_path_is_parallel(self, client):
        for i in range(_N_EXTRA_CLUSTERS):
            add_cluster(f"tjp{i}", host=f"tjp{i}.example.com",
                        gpu_type="H100", team_gpu_alloc="100")

        def slow_fetch(_cluster):
            time.sleep(_SLEEP)
            return {"jobs": [], "total_running_gpus": 0}

        with patch("server.jobs.fetch_team_jobs", side_effect=slow_fetch):
            t0 = time.monotonic()
            resp = client.get("/api/team_jobs?force=1")
            elapsed = time.monotonic() - t0

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"

        serial_floor = (_N_EXTRA_CLUSTERS + 1) * _SLEEP
        parallel_ceiling = _SLEEP * 3
        assert elapsed < parallel_ceiling, (
            f"/api/team_jobs?force=1 took {elapsed:.2f}s; expected < {parallel_ceiling:.2f}s "
            f"(serial would have been ≥ {serial_floor:.2f}s)"
        )
