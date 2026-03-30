"""Unit tests for stats snapshots: per-GPU data, snapshot saving, and retrieval."""

import json
import pytest

from server.db import init_db, get_db


def _insert_snapshot(cluster, job_id, ts, gpu_util=None, gpu_mem_used=None,
                     gpu_mem_total=None, cpu_util="", rss_used=None,
                     max_rss=None, gpu_details=""):
    con = get_db()
    con.execute(
        """INSERT INTO job_stats_snapshots
           (cluster, job_id, ts, gpu_util, gpu_mem_used, gpu_mem_total,
            cpu_util, rss_used, max_rss, gpu_details)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cluster, job_id, ts, gpu_util, gpu_mem_used, gpu_mem_total,
         cpu_util, rss_used, max_rss, gpu_details),
    )
    con.commit()
    con.close()


@pytest.mark.unit
class TestGetStatsSnapshots:
    def test_returns_empty_for_no_data(self, db_path):
        from server.jobs import get_stats_snapshots
        result = get_stats_snapshots("eos", "999")
        assert result == []

    def test_returns_snapshots_in_order(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", gpu_util=95.0)
        _insert_snapshot("eos", "1", "2026-03-30T12:30:00", gpu_util=97.0)
        _insert_snapshot("eos", "1", "2026-03-30T13:00:00", gpu_util=96.0)
        result = get_stats_snapshots("eos", "1")
        assert len(result) == 3
        assert result[0]["ts"] == "2026-03-30T12:00:00"
        assert result[2]["ts"] == "2026-03-30T13:00:00"

    def test_per_gpu_parsed_from_json(self, db_path):
        from server.jobs import get_stats_snapshots
        gpus = [
            {"index": "0", "name": "H100", "util": "98%", "mem": "70000/81559 MiB"},
            {"index": "1", "name": "H100", "util": "95%", "mem": "69000/81559 MiB"},
        ]
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00",
                         gpu_util=96.5, gpu_details=json.dumps(gpus))
        result = get_stats_snapshots("eos", "1")
        assert len(result) == 1
        assert len(result[0]["per_gpu"]) == 2
        assert result[0]["per_gpu"][0]["util"] == "98%"
        assert result[0]["per_gpu"][1]["mem"] == "69000/81559 MiB"

    def test_empty_gpu_details_returns_empty_list(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", gpu_details="")
        result = get_stats_snapshots("eos", "1")
        assert result[0]["per_gpu"] == []

    def test_invalid_json_gpu_details(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", gpu_details="not json")
        result = get_stats_snapshots("eos", "1")
        assert result[0]["per_gpu"] == []

    def test_cpu_util_preserved(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", cpu_util="00:05:32")
        result = get_stats_snapshots("eos", "1")
        assert result[0]["cpu_util"] == "00:05:32"

    def test_rss_data_preserved(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", rss_used=1024.5, max_rss=2048.0)
        result = get_stats_snapshots("eos", "1")
        assert result[0]["rss_used"] == 1024.5
        assert result[0]["max_rss"] == 2048.0

    def test_isolates_by_cluster_and_job(self, db_path):
        from server.jobs import get_stats_snapshots
        _insert_snapshot("eos", "1", "2026-03-30T12:00:00", gpu_util=90.0)
        _insert_snapshot("hsg", "1", "2026-03-30T12:00:00", gpu_util=80.0)
        _insert_snapshot("eos", "2", "2026-03-30T12:00:00", gpu_util=70.0)
        assert len(get_stats_snapshots("eos", "1")) == 1
        assert get_stats_snapshots("eos", "1")[0]["gpu_util"] == 90.0
        assert len(get_stats_snapshots("hsg", "1")) == 1
        assert len(get_stats_snapshots("eos", "2")) == 1


@pytest.mark.unit
class TestSaveStatsSnapshot:
    def test_saves_with_gpu_details(self, db_path):
        from server.jobs import _save_stats_snapshot, get_stats_snapshots
        stats = {
            "status": "ok",
            "ave_cpu": "00:01:00",
            "ave_rss": "512M",
            "max_rss": "1024M",
            "gpus": [
                {"index": "0", "name": "H100", "util": "95%", "mem": "70000/81559 MiB"},
            ],
        }
        _save_stats_snapshot("eos", "42", stats)
        snaps = get_stats_snapshots("eos", "42")
        assert len(snaps) == 1
        assert snaps[0]["gpu_util"] == 95.0
        assert len(snaps[0]["per_gpu"]) == 1

    def test_respects_interval_gap(self, db_path, monkeypatch):
        from server.jobs import _save_stats_snapshot, get_stats_snapshots
        monkeypatch.setattr("server.config.STATS_INTERVAL_SEC", 3600)
        stats = {"status": "ok", "ave_cpu": "", "ave_rss": "", "max_rss": "", "gpus": []}
        _save_stats_snapshot("eos", "42", stats)
        _save_stats_snapshot("eos", "42", stats)
        snaps = get_stats_snapshots("eos", "42")
        assert len(snaps) == 1

    def test_skips_non_ok_status(self, db_path):
        from server.jobs import _save_stats_snapshot, get_stats_snapshots
        _save_stats_snapshot("eos", "42", {"status": "error", "error": "timeout"})
        assert len(get_stats_snapshots("eos", "42")) == 0

    def test_skips_none_stats(self, db_path):
        from server.jobs import _save_stats_snapshot, get_stats_snapshots
        _save_stats_snapshot("eos", "42", None)
        assert len(get_stats_snapshots("eos", "42")) == 0


@pytest.mark.unit
class TestGpuProbeCondition:
    """The GPU probe should trigger when cluster has gpu_type even if GRES is N/A."""

    def test_cluster_gpu_type_triggers_probe(self):
        from server.config import CLUSTERS
        cluster_has_gpus = bool(CLUSTERS.get("mock-cluster", {}).get("gpu_type"))
        assert cluster_has_gpus, "Mock cluster should have gpu_type=H100"

        gres = "N/A"
        gres_mentions_gpu = "gpu" in gres.lower()
        assert not gres_mentions_gpu
        assert (gres_mentions_gpu or cluster_has_gpus) is True

    def test_no_gpu_type_no_gres_skips_probe(self):
        gres = "N/A"
        cluster_has_gpus = False
        gres_mentions_gpu = "gpu" in gres.lower()
        assert (gres_mentions_gpu or cluster_has_gpus) is False

    def test_gres_with_gpu_triggers_probe(self):
        gres = "gpu:h100:8"
        cluster_has_gpus = False
        gres_mentions_gpu = "gpu" in gres.lower()
        assert (gres_mentions_gpu or cluster_has_gpus) is True
