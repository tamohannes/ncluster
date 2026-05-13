"""Unit tests for campaign board runtime status aggregation."""

from unittest.mock import patch

import pytest

from server.logbook_board_runtime import compute_board_runtime, summarize_run_logbook_status


@pytest.mark.unit
class TestSummarizeRunLogbookStatus:
    def test_not_found(self):
        assert summarize_run_logbook_status(None) == "not_found"

    def test_no_jobs_sdk_status(self):
        assert summarize_run_logbook_status({"jobs": [], "sdk_status": "SUBMITTING"}) == "submitting"

    def test_no_jobs_empty(self):
        assert summarize_run_logbook_status({"jobs": []}) == "no_jobs"

    def test_running_wins(self):
        run = {
            "jobs": [
                {"state": "COMPLETED"},
                {"state": "RUNNING"},
            ]
        }
        assert summarize_run_logbook_status(run) == "running"

    def test_pending(self):
        run = {"jobs": [{"state": "PENDING"}]}
        assert summarize_run_logbook_status(run) == "pending"

    def test_failed(self):
        run = {"jobs": [{"state": "FAILED"}]}
        assert summarize_run_logbook_status(run) == "failed"

    def test_cancelled(self):
        run = {"jobs": [{"state": "CANCELLED"}]}
        assert summarize_run_logbook_status(run) == "cancelled"

    def test_timeout(self):
        run = {"jobs": [{"state": "TIMEOUT"}]}
        assert summarize_run_logbook_status(run) == "timeout"

    def test_completed(self):
        run = {"jobs": [{"state": "COMPLETED"}, {"state": "COMPLETED"}]}
        assert summarize_run_logbook_status(run) == "completed"

    def test_mixed(self):
        run = {"jobs": [{"state": "COMPLETED"}, {"state": "UNKNOWN"}]}
        assert summarize_run_logbook_status(run) == "mixed"


@pytest.mark.unit
class TestComputeBoardRuntime:
    def test_run_metric_grid_cell_value(self):
        import json

        board = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "c"}],
                    "rows": [{"id": "r"}],
                    "cells": {
                        "r:c": {"cluster": "eos", "run_hash": "deadbeef", "scalar": "acc"},
                    },
                }
            ],
        }
        blob = json.dumps(board)
        with (
            patch("server.logbook_board_runtime.get_run_by_hash") as grb,
            patch("server.logbook_board_runtime.get_run_with_jobs") as grj,
            patch("server.logbook_board_runtime.get_run_metrics") as grm,
        ):
            grb.return_value = {"root_job_id": 1, "run_uuid": "uuid-1"}
            grj.return_value = {"jobs": [{"state": "COMPLETED"}], "sdk_status": ""}
            grm.return_value = {"scalar_latest": {"acc": {"value_num": 0.5}}}
            out = compute_board_runtime(blob)
        assert out["cells"]["0|r|c"]["status"] == "completed"
        assert out["cells"]["0|r|c"]["value"] == "0.5"
        assert out["cells"]["0|r|c"]["cluster"] == "eos"
        assert out["cells"]["0|r|c"].get("malfunctioned") is False

    def test_run_metric_grid_inherits_column_scalar_at_runtime(self):
        import json

        board = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "c", "label": "C", "scalar": "acc"}],
                    "rows": [{"id": "r"}],
                    "cells": {
                        "r:c": {"cluster": "eos", "run_hash": "deadbeef"},
                    },
                }
            ],
        }
        blob = json.dumps(board)
        with (
            patch("server.logbook_board_runtime.get_run_by_hash") as grb,
            patch("server.logbook_board_runtime.get_run_with_jobs") as grj,
            patch("server.logbook_board_runtime.get_run_metrics") as grm,
        ):
            grb.return_value = {"root_job_id": 1, "run_uuid": "uuid-1", "malfunctioned": 0}
            grj.return_value = {"jobs": [{"state": "COMPLETED"}], "sdk_status": ""}
            grm.return_value = {"scalar_latest": {"acc": {"value_num": 0.25}}}
            out = compute_board_runtime(blob)
        cell = out["cells"]["0|r|c"]
        assert cell["value"] == "0.25"
        assert cell.get("scalar") == "acc"

    def test_run_metric_grid_malfunctioned_flag(self):
        import json

        board = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "c"}],
                    "rows": [{"id": "r"}],
                    "cells": {"r:c": {"cluster": "eos", "run_hash": "deadbeef"}},
                }
            ],
        }
        blob = json.dumps(board)
        with (
            patch("server.logbook_board_runtime.get_run_by_hash") as grb,
            patch("server.logbook_board_runtime.get_run_with_jobs") as grj,
        ):
            grb.return_value = {"root_job_id": 1, "run_uuid": "", "malfunctioned": 1}
            grj.return_value = {"jobs": [{"state": "COMPLETED"}], "sdk_status": ""}
            out = compute_board_runtime(blob)
        assert out["cells"]["0|r|c"]["malfunctioned"] is True
