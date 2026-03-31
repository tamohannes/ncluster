"""MCP bulk_read_logbooks tool contract tests."""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import bulk_read_logbooks


def _mock_get(response):
    return patch.object(mcp_server, "_api_get", return_value=response)


@pytest.mark.mcp
class TestBulkReadLogbooks:
    def test_single_project(self):
        listed = [{"id": 1}, {"id": 2}]
        full_1 = {"id": 1, "project": "alpha", "title": "A", "body": "body a"}
        full_2 = {"id": 2, "project": "alpha", "title": "B", "body": "body b"}

        call_count = {"n": 0}
        def mock_get(url):
            call_count["n"] += 1
            if "entries?" in url:
                return listed
            if "/entries/1" in url:
                return full_1
            if "/entries/2" in url:
                return full_2
            return {"status": "error", "error": "unexpected"}

        with patch.object(mcp_server, "_api_get", side_effect=mock_get):
            result = bulk_read_logbooks(project="alpha", limit_per_project=10)

        assert result["status"] == "ok"
        assert result["count"] == 2
        assert not result["truncated"]
        assert result["projects"] == ["alpha"]
        assert len(result["entries"]) == 2
        assert result["entries"][0]["body"] == "body a"

    def test_all_projects(self):
        projects = [{"project": "alpha"}, {"project": "beta"}]

        def mock_get(url):
            if "/api/projects" in url:
                return projects
            if "entries?" in url:
                return [{"id": 1}]
            if "/entries/1" in url:
                return {"id": 1, "title": "X", "body": "y"}
            return {"status": "error"}

        with patch.object(mcp_server, "_api_get", side_effect=mock_get):
            result = bulk_read_logbooks()

        assert result["status"] == "ok"
        assert result["count"] == 2
        assert set(result["projects"]) == {"alpha", "beta"}

    def test_max_entries_truncation(self):
        listed = [{"id": i} for i in range(1, 6)]

        def mock_get(url):
            if "entries?" in url:
                return listed
            return {"id": 1, "title": "T", "body": "b"}

        with patch.object(mcp_server, "_api_get", side_effect=mock_get):
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
        with _mock_get([]):
            result = bulk_read_logbooks()
        assert result["status"] == "ok"
        assert result["count"] == 0
        assert result["entries"] == []

    def test_entry_read_error_recorded(self):
        def mock_get(url):
            if "entries?" in url:
                return [{"id": 1}]
            if "/entries/1" in url:
                return {"status": "error", "error": "not found"}
            return {"status": "error"}

        with patch.object(mcp_server, "_api_get", side_effect=mock_get):
            result = bulk_read_logbooks(project="alpha")

        assert result["status"] == "ok"
        assert result["count"] == 0
        assert "alpha:1" in result["errors"]
