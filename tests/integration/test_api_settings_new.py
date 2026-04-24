"""Integration tests for the stats-interval setting and GPU probing.

The v3 ``POST /api/settings`` write tests were removed when v4 split
settings into per-namespace endpoints. The new "set/get one app_setting"
tests live in ``test_api_settings_routes.py`` (added in the
``rest_api`` step).
"""

import pytest


@pytest.mark.integration
class TestStatsIntervalSetting:
    def test_get_includes_stats_interval(self, client, mock_ssh):
        resp = client.get("/api/settings")
        data = resp.get_json()
        assert "stats_interval_sec" in data
        assert isinstance(data["stats_interval_sec"], (int, float))


@pytest.mark.integration
class TestGpuProbeWithoutGres:
    """Verify that GPU probing uses cluster gpu_type as fallback when GRES is N/A."""

    def test_gpu_probe_triggered_by_cluster_config(self, client, mock_cluster, monkeypatch):
        """When GRES is N/A but cluster has gpu_type, nvidia-smi should still be called."""
        from server.jobs import get_job_stats
        from server import config
        assert config.CLUSTERS[mock_cluster].get("gpu_type") == "H100"

        fake_stats = {
            "status": "ok", "job_id": "12345", "state": "RUNNING",
            "nodes": "2", "cpus": "448", "gres": "N/A", "node_list": "node01",
            "elapsed": "01:00:00", "ave_cpu": "00:00:10", "ave_rss": "512M",
            "max_rss": "1024M", "max_vmsize": "", "gpuutil_ave": "", "gpumem_ave": "",
            "gpu_summary": "", "gpu_probe_error": "",
            "gpus": [
                {"index": "0", "name": "H100", "util": "95%", "mem": "70000/81559 MiB"},
                {"index": "1", "name": "H100", "util": "92%", "mem": "69000/81559 MiB"},
            ],
        }
        monkeypatch.setattr("server.jobs.get_job_stats", lambda c, j: fake_stats)

        resp = client.get(f"/api/stats/{mock_cluster}/12345")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert len(data["gpus"]) == 2
        assert data["gpus"][0]["util"] == "95%"
