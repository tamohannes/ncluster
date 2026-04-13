"""Unit tests for get_history search parameter."""

from datetime import datetime, timedelta

import pytest

from server.db import init_db, upsert_job, get_history
from server.config import PROJECTS


@pytest.mark.unit
class TestHistorySearch:
    @pytest.fixture(autouse=True)
    def _setup(self, db_path):
        upsert_job("eos", {"jobid": "100", "name": "hle_eval-math", "state": "COMPLETED"})
        upsert_job("eos", {"jobid": "101", "name": "hle_eval-gpqa", "state": "FAILED"})
        upsert_job("hsg", {"jobid": "200", "name": "artsiv_train-v3", "state": "RUNNING"})
        upsert_job("hsg", {"jobid": "201", "name": "artsiv_eval-swe", "state": "COMPLETED"})

    def test_search_by_substring(self):
        results = get_history(search="eval-math")
        assert len(results) == 1
        assert results[0]["job_name"] == "hle_eval-math"

    def test_search_partial_match(self):
        results = get_history(search="eval")
        names = [r["job_name"] for r in results]
        assert "hle_eval-math" in names
        assert "hle_eval-gpqa" in names
        assert "artsiv_eval-swe" in names

    def test_search_no_match(self):
        results = get_history(search="zzzznothing")
        assert len(results) == 0

    def test_search_combined_with_cluster(self):
        results = get_history(cluster="eos", search="eval")
        assert all(r["cluster"] == "eos" for r in results)
        assert len(results) == 2

    def test_search_combined_with_limit(self):
        results = get_history(search="eval", limit=1)
        assert len(results) == 1

    def test_search_case_insensitive_in_sqlite(self):
        results = get_history(search="HLE")
        assert len(results) == 2

    def test_search_matches_job_id(self):
        results = get_history(search="101")
        assert len(results) == 1
        assert results[0]["job_id"] == "101"

    def test_no_search_returns_all(self):
        results = get_history()
        assert len(results) == 4


@pytest.mark.unit
class TestHistoryFilters:
    @pytest.fixture(autouse=True)
    def _setup(self, db_path, monkeypatch):
        monkeypatch.setitem(PROJECTS, "alpha", {"prefix": "alpha_"})
        monkeypatch.setitem(PROJECTS, "beta", {"prefix": "beta_"})
        now = datetime.now()
        upsert_job("eos", {
            "jobid": "300",
            "name": "alpha_mpsf_eval-math",
            "state": "COMPLETED",
            "partition": "p-h100",
            "account": "research_team_alpha",
            "ended_at": now.isoformat(),
        })
        upsert_job("eos", {
            "jobid": "301",
            "name": "alpha_text_eval-gpqa",
            "state": "FAILED",
            "partition": "p-h100",
            "account": "research_team_alpha",
            "ended_at": (now - timedelta(days=1)).isoformat(),
        })
        upsert_job("hsg", {
            "jobid": "302",
            "name": "beta_train_run",
            "state": "RUNNING",
            "partition": "p-a100",
            "account": "research_team_beta",
            "started": now.isoformat(),
        })
        upsert_job("hsg", {
            "jobid": "303",
            "name": "alpha_mpsf_eval-chem",
            "state": "COMPLETED",
            "partition": "p-a100",
            "account": "research_team_alpha",
            "ended_at": (now - timedelta(days=10)).isoformat(),
        })

    def test_filter_by_campaign(self):
        results = get_history(campaign="mpsf")
        assert {r["job_id"] for r in results} == {"300", "303"}

    def test_filter_by_state_csv(self):
        results = get_history(state="FAILED,RUNNING")
        assert {r["job_id"] for r in results} == {"301", "302"}

    def test_filter_by_partition_and_account(self):
        results = get_history(partition="p-h100", account="research_team_alpha")
        assert {r["job_id"] for r in results} == {"300", "301"}

    def test_filter_by_days(self):
        results = get_history(days=7)
        assert {r["job_id"] for r in results} == {"300", "301", "302"}
