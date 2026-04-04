"""Live destructive tests with throwaway resources.

Run with: pytest tests/live/test_app_live_destructive.py -m "live and destructive" -v
Requires: clausius running, SSH + Slurm access to a configured cluster.

NOTE: cancel_all is excluded by policy.
"""

import json
import os
import time
import urllib.request
import pytest

from .helpers.slurm_fixture import (
    LIVE_CLUSTER, APP_BASE,
    submit_throwaway_job, cancel_throwaway_job, wait_for_job_state,
)

pytestmark = [pytest.mark.live, pytest.mark.destructive]


def _skip_if_no_cluster():
    if not LIVE_CLUSTER:
        pytest.skip("No live cluster configured (set TEST_CLUSTER or add clusters to config.json)")


def _post(path, data=None):
    url = f"{APP_BASE}{path}"
    body = json.dumps(data).encode() if data else b""
    req = urllib.request.Request(url, method="POST", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


class TestLiveDestructive:
    def test_cancel_throwaway_job(self):
        _skip_if_no_cluster()
        job_id = submit_throwaway_job(LIVE_CLUSTER, duration_sec=120)
        try:
            state = wait_for_job_state(LIVE_CLUSTER, job_id, {"RUNNING", "PENDING"}, timeout=60)
            assert state in {"RUNNING", "PENDING"}, f"Job {job_id} not running/pending: {state}"

            result = _post(f"/api/cancel/{LIVE_CLUSTER}/{job_id}")
            assert result["status"] == "ok"

            final = wait_for_job_state(LIVE_CLUSTER, job_id, {"CANCELLED", "GONE"}, timeout=30)
            assert final in {"CANCELLED", "GONE"}
        finally:
            cancel_throwaway_job(LIVE_CLUSTER, job_id)

    def test_cleanup_dry_run(self):
        result = _post("/api/cleanup", {"days": 365, "dry_run": True})
        assert result["status"] == "ok"
        assert result.get("dry_run") is True or result.get("deleted_records") == 0
