"""Integration tests for the read side of /api/settings.

The v3 ``POST /api/settings`` blob handler is removed in v4. Per-namespace
endpoint tests live in ``test_api_clusters.py`` / ``test_api_team.py`` /
``test_api_paths.py`` (added in the ``tests_refactor`` step). The WDS
snapshot trigger now lives on the cluster CRUD endpoints (``team_gpu_alloc``
field on the cluster row).
"""

import json
import pytest


@pytest.mark.integration
class TestApiSettingsGet:
    def test_returns_config(self, client, mock_ssh):
        resp = client.get("/api/settings")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "port" in data
        assert "ssh_timeout" in data
        assert "clusters" in data

    def test_contains_cache_fresh(self, client, mock_ssh):
        data = client.get("/api/settings").get_json()
        assert "cache_fresh_sec" in data
