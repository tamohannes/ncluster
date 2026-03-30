"""Unit tests for get_history search parameter."""

import pytest

from server.db import init_db, upsert_job, get_history


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

    def test_no_search_returns_all(self):
        results = get_history()
        assert len(results) == 4
