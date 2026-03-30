"""Integration tests for new settings fields added during the redesign."""

import json
import pytest


@pytest.mark.integration
class TestStatsIntervalSetting:
    def test_get_includes_stats_interval(self, client, mock_ssh):
        resp = client.get("/api/settings")
        data = resp.get_json()
        assert "stats_interval_sec" in data
        assert isinstance(data["stats_interval_sec"], (int, float))

    def test_post_updates_stats_interval(self, client, mock_ssh, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        initial = client.get("/api/settings").get_json()
        (tmp_path / "config.json").write_text(json.dumps(initial))

        resp = client.post("/api/settings",
                           data=json.dumps({"stats_interval_sec": 600}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["settings"]["stats_interval_sec"] == 600

    def test_stats_interval_persists_after_reload(self, client, mock_ssh, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        initial = client.get("/api/settings").get_json()
        (tmp_path / "config.json").write_text(json.dumps(initial))

        client.post("/api/settings",
                    data=json.dumps({"stats_interval_sec": 900}),
                    content_type="application/json")

        saved = json.loads((tmp_path / "config.json").read_text())
        assert saved.get("stats_interval_sec") == 900


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
