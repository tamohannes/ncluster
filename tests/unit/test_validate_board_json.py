"""Unit tests for logbook board_json validation."""

import json

import pytest

from server.logbooks import validate_board_json


@pytest.mark.unit
class TestValidateBoardJson:
    def test_empty_defaults(self):
        s = validate_board_json(None)
        assert '"version":1' in s
        assert '"sections":[]' in s

    def test_valid_minimal_section(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "title": "T",
                    "columns": [{"id": "m", "label": "Model"}],
                    "rows": [{"cells": {"m": "x"}, "cluster": "eos", "run_hash": "a1b2c3d4"}],
                }
            ],
        }
        s = validate_board_json(raw)
        assert "a1b2c3d4" in s

    def test_run_hash_requires_cluster(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "columns": [{"id": "m"}],
                    "rows": [{"cells": {"m": "x"}, "run_hash": "abc", "cluster": ""}],
                }
            ],
        }
        with pytest.raises(ValueError, match="cluster"):
            validate_board_json(raw)

    def test_run_status_column_normalized(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "title": "T",
                    "columns": [
                        {"id": "st", "label": "Status", "type": "run_status"},
                        {"id": "m", "label": "Model"},
                    ],
                    "rows": [{"cells": {"m": "x"}, "cluster": "eos", "run_hash": "a1b2c3d4"}],
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["columns"][0]["type"] == "run_status"

    def test_two_run_status_same_section_rejected(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "columns": [
                        {"id": "a", "type": "run_status"},
                        {"id": "b", "type": "run_status"},
                    ],
                    "rows": [],
                }
            ],
        }
        with pytest.raises(ValueError, match="run_status"):
            validate_board_json(raw)

    def test_run_status_each_section_ok(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "title": "A",
                    "columns": [{"id": "s", "type": "run_status"}],
                    "rows": [{"cells": {}, "cluster": "eos", "run_hash": "aaaaaaaa"}],
                },
                {
                    "title": "B",
                    "columns": [{"id": "t", "type": "run_status"}],
                    "rows": [{"cells": {}, "cluster": "dfw", "run_hash": "bbbbbbbb"}],
                },
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["columns"][0]["type"] == "run_status"
        assert out["sections"][1]["columns"][0]["type"] == "run_status"

    def test_unknown_section_type(self):
        raw = {
            "version": 1,
            "sections": [{"type": "bogus", "title": "X", "columns": [{"id": "a"}], "rows": []}],
        }
        with pytest.raises(ValueError, match="unknown type"):
            validate_board_json(raw)

    def test_run_metric_grid_valid(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "Grid",
                    "columns": [{"id": "c1", "label": "Col1"}],
                    "rows": [{"id": "r1", "label": "Row1"}],
                    "cells": {
                        "r1:c1": {"cluster": "eos", "run_hash": "a1b2c3d4", "scalar": "accuracy"},
                    },
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["type"] == "run_metric_grid"
        assert out["sections"][0]["cells"]["r1:c1"]["cluster"] == "eos"

    def test_run_metric_grid_requires_all_cells(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "a"}, {"id": "b"}],
                    "rows": [{"id": "r1"}, {"id": "r2"}],
                    "cells": {
                        "r1:a": {"cluster": "eos", "run_hash": "aaaaaaaa"},
                    },
                }
            ],
        }
        with pytest.raises(ValueError, match="missing cells"):
            validate_board_json(raw)

    def test_run_metric_grid_column_scalar_merged_into_cells(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "c", "label": "Metric", "scalar": "accuracy"}],
                    "rows": [{"id": "r", "label": "R1"}],
                    "cells": {
                        "r:c": {"cluster": "eos", "run_hash": "a1b2c3d4"},
                    },
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["cells"]["r:c"].get("scalar") == "accuracy"

    def test_run_metric_grid_cell_scalar_overrides_column(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "title": "G",
                    "columns": [{"id": "c", "scalar": "loss"}],
                    "rows": [{"id": "r"}],
                    "cells": {
                        "r:c": {"cluster": "eos", "run_hash": "a1b2c3d4", "scalar": "accuracy"},
                    },
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["cells"]["r:c"]["scalar"] == "accuracy"

    def test_run_metric_grid_column_unknown_key_rejected(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "type": "run_metric_grid",
                    "columns": [{"id": "c", "typo": "x"}],
                    "rows": [{"id": "r"}],
                    "cells": {"r:c": {"cluster": "eos", "run_hash": "a1b2c3d4"}},
                }
            ],
        }
        with pytest.raises(ValueError, match="only allows id, label"):
            validate_board_json(raw)
