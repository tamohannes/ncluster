"""MCP bulk_read_logbooks tool contract tests — in-process Flask architecture."""

import pytest
from unittest.mock import patch

from mcp_server import bulk_read_logbooks


@pytest.mark.mcp
class TestBulkReadLogbooks:
    async def test_single_project(self):
        resp = {
            "status": "ok", "count": 2, "truncated": False,
            "projects": ["alpha"],
            "entries": [
                {"id": 1, "project": "alpha", "title": "A", "body": "body a"},
                {"id": 2, "project": "alpha", "title": "B", "body": "body b"},
            ],
            "errors": {},
        }
        with patch("mcp_server._api", return_value=resp):
            result = await bulk_read_logbooks(project="alpha", limit_per_project=10)
        assert result["status"] == "ok"
        assert result["count"] == 2
        assert not result["truncated"]
        assert result["projects"] == ["alpha"]

    async def test_all_projects(self):
        resp = {
            "status": "ok", "count": 2, "truncated": False,
            "projects": ["alpha", "beta"],
            "entries": [{"id": 1, "title": "X", "body": "y"}, {"id": 2, "title": "Z", "body": "w"}],
            "errors": {},
        }
        with patch("mcp_server._api", return_value=resp):
            result = await bulk_read_logbooks()
        assert result["status"] == "ok"
        assert result["count"] == 2

    async def test_max_entries_truncation(self):
        resp = {
            "status": "ok", "count": 3, "truncated": True,
            "projects": ["alpha"],
            "entries": [{"id": i, "title": "T", "body": "b"} for i in range(3)],
            "errors": {},
        }
        with patch("mcp_server._api", return_value=resp):
            result = await bulk_read_logbooks(project="alpha", max_entries=3)
        assert result["status"] == "ok"
        assert result["count"] == 3
        assert result["truncated"] is True

    async def test_invalid_sort(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "sort must be one of: edited_at, created_at, title"}):
            result = await bulk_read_logbooks(project="alpha", sort="invalid")
        assert result["status"] == "error"

    async def test_invalid_entry_type(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "entry_type must be 'note', 'plan', or omitted"}):
            result = await bulk_read_logbooks(project="alpha", entry_type="bad")
        assert result["status"] == "error"

    async def test_no_projects(self):
        resp = {"status": "ok", "count": 0, "truncated": False, "projects": [], "entries": [], "errors": {}}
        with patch("mcp_server._api", return_value=resp):
            result = await bulk_read_logbooks()
        assert result["status"] == "ok"
        assert result["count"] == 0
        assert result["entries"] == []
