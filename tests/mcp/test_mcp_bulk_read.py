"""MCP bulk_read_logbooks tool contract tests."""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import bulk_read_logbooks


@pytest.mark.mcp
class TestBulkReadLogbooks:
    def test_single_project(self):
        listed = [{"id": 1, "title": "A", "body_preview": "..."}, {"id": 2, "title": "B", "body_preview": "..."}]
        full_1 = {"id": 1, "project": "alpha", "title": "A", "body": "body a"}
        full_2 = {"id": 2, "project": "alpha", "title": "B", "body": "body b"}

        with patch.object(mcp_server, "_lb_list", return_value=listed), \
             patch.object(mcp_server, "_lb_get", side_effect=[full_1, full_2]):
            result = bulk_read_logbooks(project="alpha", limit_per_project=10)

        assert result["status"] == "ok"
        assert result["count"] == 2
        assert not result["truncated"]
        assert result["projects"] == ["alpha"]
        assert len(result["entries"]) == 2
        assert result["entries"][0]["body"] == "body a"

    def test_all_projects(self):
        listed = [{"id": 1, "title": "X", "body_preview": "..."}]
        full = {"id": 1, "title": "X", "body": "y"}

        with patch.object(mcp_server, "list_logbook_projects", return_value=["alpha", "beta"]), \
             patch.object(mcp_server, "_lb_list", return_value=listed), \
             patch.object(mcp_server, "_lb_get", return_value=full):
            result = bulk_read_logbooks()

        assert result["status"] == "ok"
        assert result["count"] == 2
        assert set(result["projects"]) == {"alpha", "beta"}

    def test_max_entries_truncation(self):
        listed = [{"id": i, "title": "T"} for i in range(1, 6)]
        full = {"id": 1, "title": "T", "body": "b"}

        with patch.object(mcp_server, "_lb_list", return_value=listed), \
             patch.object(mcp_server, "_lb_get", return_value=full):
            result = bulk_read_logbooks(project="alpha", max_entries=3)

        assert result["status"] == "ok"
        assert result["count"] == 3
        assert result["truncated"] is True

    def test_invalid_sort(self):
        result = bulk_read_logbooks(project="alpha", sort="invalid")
        assert result["status"] == "error"

    def test_invalid_entry_type(self):
        result = bulk_read_logbooks(project="alpha", entry_type="bad")
        assert result["status"] == "error"

    def test_no_projects(self):
        with patch.object(mcp_server, "list_logbook_projects", return_value=[]):
            result = bulk_read_logbooks()
        assert result["status"] == "ok"
        assert result["count"] == 0
        assert result["entries"] == []

    def test_entry_read_error_skipped(self):
        listed = [{"id": 1, "title": "X", "body_preview": "..."}]
        error_resp = {"status": "error", "error": "not found"}
        with patch.object(mcp_server, "_lb_list", return_value=listed), \
             patch.object(mcp_server, "_lb_get", return_value=error_resp):
            result = bulk_read_logbooks(project="alpha")

        assert result["status"] == "ok"
        assert result["count"] == 0
        assert result["entries"] == []
