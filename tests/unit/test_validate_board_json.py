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
                    "rows": [{"cells": {"m": "x"}}],
                }
            ],
        }
        s = validate_board_json(raw)
        assert '"m":"x"' in s

    def test_legacy_row_run_fields_are_dropped(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "columns": [{"id": "m"}],
                    "rows": [{"cells": {"m": "x"}, "cluster": "eos", "run_hash": "a1b2c3d4"}],
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["rows"] == [{"cells": {"m": "x"}}]

    def test_run_status_column_rejected(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "title": "T",
                    "columns": [
                        {"id": "st", "label": "Status", "type": "run_status"},
                        {"id": "m", "label": "Model"},
                    ],
                    "rows": [{"cells": {"m": "x"}}],
                }
            ],
        }
        with pytest.raises(ValueError, match="static string"):
            validate_board_json(raw)

    def test_string_column_type_is_dropped(self):
        raw = {
            "version": 1,
            "sections": [
                {
                    "columns": [{"id": "a", "type": "string"}],
                    "rows": [],
                }
            ],
        }
        out = json.loads(validate_board_json(raw))
        assert out["sections"][0]["columns"] == [{"id": "a", "label": "a"}]

    def test_unknown_section_type(self):
        raw = {
            "version": 1,
            "sections": [{"type": "bogus", "title": "X", "columns": [{"id": "a"}], "rows": []}],
        }
        with pytest.raises(ValueError, match="static table"):
            validate_board_json(raw)

    def test_run_metric_grid_rejected(self):
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
        with pytest.raises(ValueError, match="static table"):
            validate_board_json(raw)
