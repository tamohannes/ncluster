"""MCP logbook tool contract tests (disabled — logbooks moved to DeepLake)."""

import pytest
from unittest.mock import patch

import mcp_server

try:
    from mcp_server import (
        list_logbooks, read_logbook, add_logbook_entry,
        update_logbook_entry, create_logbook, delete_logbook,
    )
except ImportError:
    pytest.skip("Logbook MCP tools disabled — moved to DeepLake", allow_module_level=True)


def _mock_get(response):
    return patch.object(mcp_server, "_api_get", return_value=response)


def _mock_post_json(response):
    return patch.object(mcp_server, "_api_post_json", return_value=response)


def _mock_put_json(response):
    return patch.object(mcp_server, "_api_put_json", return_value=response)


def _mock_delete(response):
    return patch.object(mcp_server, "_api_delete", return_value=response)


@pytest.mark.mcp
class TestListLogbooks:
    def test_returns_list(self):
        with _mock_get([{"name": "experiments", "entry_count": 3}]):
            result = list_logbooks("alpha")
        assert isinstance(result, list)
        assert result[0]["name"] == "experiments"

    def test_empty_project(self):
        with _mock_get([]):
            result = list_logbooks("empty")
        assert result == []


@pytest.mark.mcp
class TestReadLogbook:
    def test_returns_content(self):
        with _mock_get({"name": "notes", "content": "## Note\n\nhello", "entries": ["## Note\n\nhello"]}):
            result = read_logbook("proj", "notes")
        assert result["name"] == "notes"
        assert len(result["entries"]) == 1

    def test_missing_logbook(self):
        with _mock_get({"name": "x", "content": "", "entries": [], "error": "Logbook not found"}):
            result = read_logbook("proj", "x")
        assert "error" in result


@pytest.mark.mcp
class TestAddLogbookEntry:
    def test_success(self):
        with _mock_post_json({"status": "ok", "entry_count": 1}):
            result = add_logbook_entry("proj", "notes", "## New entry")
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestUpdateLogbookEntry:
    def test_success(self):
        with _mock_put_json({"status": "ok", "entry_count": 2}):
            result = update_logbook_entry("proj", "notes", 0, "updated")
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestCreateLogbook:
    def test_success(self):
        with _mock_post_json({"status": "ok", "name": "bugs"}):
            result = create_logbook("proj", "bugs")
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestDeleteLogbook:
    def test_success(self):
        with _mock_delete({"status": "ok"}):
            result = delete_logbook("proj", "trash")
        assert result["status"] == "ok"
