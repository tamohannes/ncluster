"""Unit tests for fetch_team_jobs in server/jobs.py."""

import pytest

from server.jobs import _parse_gres_gpu_count


class TestParseGresGpuCount:
    @pytest.mark.unit
    def test_gpu_colon_count(self):
        assert _parse_gres_gpu_count("gpu:8") == 8

    @pytest.mark.unit
    def test_gpu_type_colon_count(self):
        assert _parse_gres_gpu_count("gpu:a100:4") == 4

    @pytest.mark.unit
    def test_gpu_type_colon_count_h100(self):
        assert _parse_gres_gpu_count("gpu:h100:8") == 8

    @pytest.mark.unit
    def test_empty_string(self):
        assert _parse_gres_gpu_count("") == 0

    @pytest.mark.unit
    def test_na_string(self):
        assert _parse_gres_gpu_count("N/A") == 0

    @pytest.mark.unit
    def test_none(self):
        assert _parse_gres_gpu_count(None) == 0

    @pytest.mark.unit
    def test_multi_resource(self):
        assert _parse_gres_gpu_count("gpu:4,shard:16") == 4

    @pytest.mark.unit
    def test_gpu_only_no_count(self):
        assert _parse_gres_gpu_count("gpu") == 1


class TestFetchTeamJobsParsing:
    """Test the squeue output parsing logic used by fetch_team_jobs."""

    @pytest.mark.unit
    def test_running_job_parsed(self, mock_ssh, mock_cluster, monkeypatch):
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", ["test_acct"])
        monkeypatch.setattr("server.config.TEAM_MEMBERS", [])
        mock_ssh.set(mock_cluster, "squeue",
                     ("alice|RUNNING|None|2|gpu:8|batch|test_acct|train-v1|4:00:00\n", ""))

        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)

        assert result is not None
        assert len(result["jobs"]) == 1
        j = result["jobs"][0]
        assert j["user"] == "alice"
        assert j["state"] == "RUNNING"
        assert j["gpus"] == 16  # 2 nodes × 8 GPUs per node
        assert j["is_gpu"] is True

    @pytest.mark.unit
    def test_dependent_job_detected(self, mock_ssh, mock_cluster, monkeypatch):
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", ["test_acct"])
        monkeypatch.setattr("server.config.TEAM_MEMBERS", [])
        mock_ssh.set(mock_cluster, "squeue",
                     ("bob|PENDING|Dependency|1||batch|test_acct|eval-v2|2:00:00\n", ""))

        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)

        assert result is not None
        j = result["jobs"][0]
        assert j["state"] == "DEPENDENT"
        assert j["gpus"] == 8  # fallback gpus_per_node

    @pytest.mark.unit
    def test_cpu_job_identified(self, mock_ssh, mock_cluster, monkeypatch):
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", ["test_acct"])
        monkeypatch.setattr("server.config.TEAM_MEMBERS", [])
        mock_ssh.set(mock_cluster, "squeue",
                     ("carol|RUNNING|None|1||cpu_long|test_acct|preprocess|8:00:00\n", ""))

        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)

        j = result["jobs"][0]
        assert j["is_gpu"] is False
        assert j["gpus"] == 0

    @pytest.mark.unit
    def test_summary_computed(self, mock_ssh, mock_cluster, monkeypatch):
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", ["acct"])
        monkeypatch.setattr("server.config.TEAM_MEMBERS", [])
        lines = (
            "alice|RUNNING|None|2||batch|acct|job1|4:00:00\n"
            "alice|PENDING|Resources|1||batch|acct|job2|4:00:00\n"
            "bob|PENDING|Dependency|4||batch|acct|job3|4:00:00\n"
        )
        mock_ssh.set(mock_cluster, "squeue", (lines, ""))

        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)
        s = result["summary"]

        assert s["total_running"] == 16
        assert s["total_pending"] == 8
        assert s["total_dependent"] == 32
        assert s["by_user"]["alice"]["running"] == 16
        assert s["by_user"]["bob"]["dependent"] == 32

    @pytest.mark.unit
    def test_multi_node_gpu_count(self, mock_ssh, mock_cluster, monkeypatch):
        """Multi-node jobs must report total GPUs = nodes × per-node GRES."""
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", ["acct"])
        monkeypatch.setattr("server.config.TEAM_MEMBERS", [])
        lines = (
            "alice|RUNNING|None|128|gres/gpu:4|batch|acct|train-big|4:00:00\n"
            "bob|PENDING|Priority|2|gres/gpu:4|batch|acct|eval-2n|4:00:00\n"
        )
        mock_ssh.set(mock_cluster, "squeue", (lines, ""))

        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)

        assert result["jobs"][0]["gpus"] == 512   # 128 × 4
        assert result["jobs"][1]["gpus"] == 8     # 2 × 4
        assert result["summary"]["total_running"] == 512
        assert result["summary"]["total_pending"] == 8

    @pytest.mark.unit
    def test_local_cluster_returns_none(self):
        from server.jobs import fetch_team_jobs
        assert fetch_team_jobs("local") is None

    @pytest.mark.unit
    def test_no_ppp_accounts_returns_empty(self, mock_ssh, mock_cluster, monkeypatch):
        monkeypatch.setattr("server.config.PPP_ACCOUNTS", [])
        from server.jobs import fetch_team_jobs, _team_jobs_cache
        _team_jobs_cache.clear()
        result = fetch_team_jobs(mock_cluster)
        assert result is None or result["jobs"] == []
