"""MCP logbook tool contract tests — HTTP proxy architecture.

Mocks mcp_server._api to verify each logbook tool sends the right HTTP call.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    list_logbook_entries, read_logbook_entry, create_logbook_entry,
    update_logbook_entry, delete_logbook_entry,
)


@pytest.mark.mcp
class TestListLogbookEntries:
    def test_returns_list(self):
        entries = [{"id": 1, "title": "Note", "body_preview": "...", "created_at": "2026-03-28", "edited_at": "2026-03-28"}]
        with patch("mcp_server._api", return_value=entries):
            result = list_logbook_entries("alpha")
        assert isinstance(result, list)
        assert result[0]["title"] == "Note"

    def test_with_search_query(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            list_logbook_entries("alpha", query="CUDA")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        # Now uses Werkzeug's test client (`query_string=`) instead of httpx
        # (`params=`) since MCP runs the Flask app in-process.
        assert kwargs["query_string"]["q"] == "CUDA"

    def test_with_entry_type(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            list_logbook_entries("alpha", entry_type="plan")
        _, kwargs = mock.call_args
        assert kwargs["query_string"]["type"] == "plan"

    def test_with_sort(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            list_logbook_entries("alpha", sort="created_at")
        _, kwargs = mock.call_args
        assert kwargs["query_string"]["sort"] == "created_at"


@pytest.mark.mcp
class TestReadLogbookEntry:
    def test_returns_entry(self):
        entry = {"id": 1, "project": "alpha", "title": "Note", "body": "full content",
                 "created_at": "2026-03-28", "edited_at": "2026-03-28"}
        with patch("mcp_server._api", return_value=entry):
            result = read_logbook_entry("alpha", 1)
        assert result["title"] == "Note"
        assert result["body"] == "full content"

    def test_passes_args(self):
        with patch("mcp_server._api", return_value={}) as mock:
            read_logbook_entry("alpha", 42)
        mock.assert_called_once_with("GET", "/api/logbook/alpha/entries/42")


@pytest.mark.mcp
class TestCreateLogbookEntry:
    def test_creates_entry(self):
        resp = {"status": "ok", "id": 1, "created_at": "2026-03-28T10:00:00"}
        with patch("mcp_server._api", return_value=resp):
            result = create_logbook_entry("alpha", "New note", "body text")
        assert result["status"] == "ok"
        assert result["id"] == 1

    def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok", "id": 1}) as mock:
            create_logbook_entry("alpha", "Title", "Body", entry_type="plan")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        assert kwargs["json"]["title"] == "Title"
        assert kwargs["json"]["body"] == "Body"
        assert kwargs["json"]["entry_type"] == "plan"


@pytest.mark.mcp
class TestUpdateLogbookEntry:
    def test_updates_entry(self):
        resp = {"status": "ok", "id": 1, "edited_at": "2026-03-28T11:00:00"}
        with patch("mcp_server._api", return_value=resp):
            result = update_logbook_entry("alpha", 1, title="Updated")
        assert result["status"] == "ok"

    def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            update_logbook_entry("alpha", 1, title="T", body="B")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        assert kwargs["json"]["title"] == "T"
        assert kwargs["json"]["body"] == "B"


@pytest.mark.mcp
class TestDeleteLogbookEntry:
    def test_deletes_entry(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = delete_logbook_entry("alpha", 1)
        assert result["status"] == "ok"

    def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            delete_logbook_entry("alpha", 42)
        mock.assert_called_once_with("DELETE", "/api/logbook/alpha/entries/42")
