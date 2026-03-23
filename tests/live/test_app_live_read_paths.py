"""Live read-path tests against the running ncluster app.

Run with: pytest tests/live/test_app_live_read_paths.py -m live -v
Requires: ncluster running at localhost:7272, SSH access to a configured cluster.
Set TEST_CLUSTER to override which cluster is tested (default: first in config.json).
"""

import json
import os
import urllib.request
import pytest

from .helpers.slurm_fixture import LIVE_CLUSTER, APP_BASE

pytestmark = pytest.mark.live


def _skip_if_no_cluster():
    if not LIVE_CLUSTER:
        pytest.skip("No live cluster configured (set TEST_CLUSTER or add clusters to config.json)")


def _get(path):
    url = f"{APP_BASE}{path}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _post(path, data=None):
    url = f"{APP_BASE}{path}"
    body = json.dumps(data).encode() if data else b""
    req = urllib.request.Request(url, method="POST", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


class TestLiveReadPaths:
    def test_api_jobs_returns_cluster(self):
        _skip_if_no_cluster()
        data = _get("/api/jobs")
        assert LIVE_CLUSTER in data
        assert data[LIVE_CLUSTER].get("status") in ("ok", "error")

    def test_api_jobs_cluster_returns_jobs_list(self):
        _skip_if_no_cluster()
        data = _get(f"/api/jobs/{LIVE_CLUSTER}")
        assert "jobs" in data

    def test_api_history_returns_list(self):
        _skip_if_no_cluster()
        data = _get(f"/api/history?cluster={LIVE_CLUSTER}&limit=5")
        assert isinstance(data, list)

    def test_api_mounts_returns_status(self):
        _skip_if_no_cluster()
        data = _get(f"/api/mounts?cluster={LIVE_CLUSTER}")
        assert data["status"] == "ok"
        mounts = data["mounts"]
        assert LIVE_CLUSTER in mounts
        assert "mounted" in mounts[LIVE_CLUSTER]

    def test_api_settings_readable(self):
        _skip_if_no_cluster()
        data = _get("/api/settings")
        assert "clusters" in data
        assert LIVE_CLUSTER in data["clusters"]

    def test_api_stats_local_rejects(self):
        data = _get("/api/stats/local/99999")
        assert data["status"] == "error"

    def test_jobs_summary_resource(self):
        data = _get("/api/jobs")
        assert isinstance(data, dict)
        assert len(data) > 0
