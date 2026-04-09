"""MCP logbook tool contract tests — direct-import architecture.

Mocks server.logbooks functions (_lb_list, _lb_get, _lb_create, etc.)
at the mcp_server module level.
"""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import (
    list_logbook_entries, read_logbook_entry, create_logbook_entry,
    update_logbook_entry, delete_logbook_entry,
)


@pytest.mark.mcp
class TestListLogbookEntries:
    def test_returns_list(self):
        entries = [{"id": 1, "title": "Note", "body_preview": "...", "created_at": "2026-03-28", "edited_at": "2026-03-28"}]
        with patch.object(mcp_server, "_lb_list", return_value=entries):
            result = list_logbook_entries("alpha")
        assert isinstance(result, list)
        assert result[0]["title"] == "Note"

    def test_with_search_query(self):
        with patch.object(mcp_server, "_lb_list", return_value=[]) as mock:
            list_logbook_entries("alpha", query="CUDA")
        mock.assert_called_once_with("alpha", query="CUDA", sort="edited_at", limit=50, entry_type=None)

    def test_with_entry_type(self):
        with patch.object(mcp_server, "_lb_list", return_value=[]) as mock:
            list_logbook_entries("alpha", entry_type="plan")
        mock.assert_called_once_with("alpha", query=None, sort="edited_at", limit=50, entry_type="plan")

    def test_with_sort(self):
        with patch.object(mcp_server, "_lb_list", return_value=[]) as mock:
            list_logbook_entries("alpha", sort="created_at")
        mock.assert_called_once_with("alpha", query=None, sort="created_at", limit=50, entry_type=None)


@pytest.mark.mcp
class TestReadLogbookEntry:
    def test_returns_entry(self):
        entry = {"id": 1, "project": "alpha", "title": "Note", "body": "full content",
                 "created_at": "2026-03-28", "edited_at": "2026-03-28"}
        with patch.object(mcp_server, "_lb_get", return_value=entry):
            result = read_logbook_entry("alpha", 1)
        assert result["title"] == "Note"
        assert result["body"] == "full content"

    def test_passes_args(self):
        with patch.object(mcp_server, "_lb_get", return_value={}) as mock:
            read_logbook_entry("alpha", 42)
        mock.assert_called_once_with("alpha", 42)


@pytest.mark.mcp
class TestCreateLogbookEntry:
    def test_creates_entry(self):
        resp = {"status": "ok", "id": 1, "created_at": "2026-03-28T10:00:00"}
        with patch.object(mcp_server, "_lb_create", return_value=resp):
            result = create_logbook_entry("alpha", "New note", "body text")
        assert result["status"] == "ok"
        assert result["id"] == 1

    def test_passes_args(self):
        with patch.object(mcp_server, "_lb_create", return_value={"status": "ok", "id": 1}) as mock:
            create_logbook_entry("alpha", "Title", "Body", entry_type="plan")
        mock.assert_called_once_with("alpha", "Title", "Body", entry_type="plan")


@pytest.mark.mcp
class TestUpdateLogbookEntry:
    def test_updates_entry(self):
        resp = {"status": "ok", "id": 1, "edited_at": "2026-03-28T11:00:00"}
        with patch.object(mcp_server, "_lb_update", return_value=resp):
            result = update_logbook_entry("alpha", 1, title="Updated")
        assert result["status"] == "ok"

    def test_passes_args(self):
        with patch.object(mcp_server, "_lb_update", return_value={"status": "ok"}) as mock:
            update_logbook_entry("alpha", 1, title="T", body="B")
        mock.assert_called_once_with("alpha", 1, title="T", body="B")


@pytest.mark.mcp
class TestDeleteLogbookEntry:
    def test_deletes_entry(self):
        with patch.object(mcp_server, "_lb_delete", return_value={"status": "ok"}):
            result = delete_logbook_entry("alpha", 1)
        assert result["status"] == "ok"

    def test_passes_args(self):
        with patch.object(mcp_server, "_lb_delete", return_value={"status": "ok"}) as mock:
            delete_logbook_entry("alpha", 42)
        mock.assert_called_once_with("alpha", 42)
