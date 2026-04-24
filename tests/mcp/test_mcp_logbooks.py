"""MCP logbook tool contract tests — in-process Flask architecture.

Mocks ``mcp_server._api`` (the synchronous Flask test_client wrapper) to
verify each logbook tool sends the right HTTP call. ``_api_async`` runs
the lambda in a worker thread, so the patch on the underlying sync
``_api`` still intercepts every call.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    list_logbook_entries, read_logbook_entry, create_logbook_entry,
    update_logbook_entry, delete_logbook_entry,
)


@pytest.mark.mcp
class TestListLogbookEntries:
    async def test_returns_list(self):
        entries = [{"id": 1, "title": "Note", "body_preview": "...", "created_at": "2026-03-28", "edited_at": "2026-03-28"}]
        with patch("mcp_server._api", return_value=entries):
            result = await list_logbook_entries("alpha")
        assert isinstance(result, list)
        assert result[0]["title"] == "Note"

    async def test_with_search_query(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            await list_logbook_entries("alpha", query="CUDA")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        # Now uses Werkzeug's test client (`query_string=`) instead of httpx
        # (`params=`) since MCP runs the Flask app in-process.
        assert kwargs["query_string"]["q"] == "CUDA"

    async def test_with_entry_type(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            await list_logbook_entries("alpha", entry_type="plan")
        _, kwargs = mock.call_args
        assert kwargs["query_string"]["type"] == "plan"

    async def test_with_sort(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            await list_logbook_entries("alpha", sort="created_at")
        _, kwargs = mock.call_args
        assert kwargs["query_string"]["sort"] == "created_at"


@pytest.mark.mcp
class TestReadLogbookEntry:
    async def test_returns_entry(self):
        entry = {"id": 1, "project": "alpha", "title": "Note", "body": "full content",
                 "created_at": "2026-03-28", "edited_at": "2026-03-28"}
        with patch("mcp_server._api", return_value=entry):
            result = await read_logbook_entry("alpha", 1)
        assert result["title"] == "Note"
        assert result["body"] == "full content"

    async def test_passes_args(self):
        with patch("mcp_server._api", return_value={}) as mock:
            await read_logbook_entry("alpha", 42)
        mock.assert_called_once_with("GET", "/api/logbook/alpha/entries/42")


@pytest.mark.mcp
class TestCreateLogbookEntry:
    async def test_creates_entry(self):
        resp = {"status": "ok", "id": 1, "created_at": "2026-03-28T10:00:00"}
        with patch("mcp_server._api", return_value=resp):
            result = await create_logbook_entry("alpha", "New note", "body text")
        assert result["status"] == "ok"
        assert result["id"] == 1

    async def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok", "id": 1}) as mock:
            await create_logbook_entry("alpha", "Title", "Body", entry_type="plan")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        assert kwargs["json"]["title"] == "Title"
        assert kwargs["json"]["body"] == "Body"
        assert kwargs["json"]["entry_type"] == "plan"


@pytest.mark.mcp
class TestUpdateLogbookEntry:
    async def test_updates_entry(self):
        resp = {"status": "ok", "id": 1, "edited_at": "2026-03-28T11:00:00"}
        with patch("mcp_server._api", return_value=resp):
            result = await update_logbook_entry("alpha", 1, title="Updated")
        assert result["status"] == "ok"

    async def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_logbook_entry("alpha", 1, title="T", body="B")
        mock.assert_called_once()
        _, kwargs = mock.call_args
        assert kwargs["json"]["title"] == "T"
        assert kwargs["json"]["body"] == "B"

    async def test_passes_entry_type_and_pinned(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_logbook_entry("alpha", 1, entry_type="plan", pinned=True)
        _, kwargs = mock.call_args
        assert kwargs["json"]["entry_type"] == "plan"
        assert kwargs["json"]["pinned"] is True

    async def test_passes_new_project(self):
        with patch("mcp_server._api", return_value={"status": "ok", "project": "beta"}) as mock:
            result = await update_logbook_entry("alpha", 1, new_project="beta")
        _, kwargs = mock.call_args
        # Lookup uses the source project; the body carries the rename target.
        args, _ = mock.call_args
        assert args[1] == "/api/logbook/alpha/entries/1"
        assert kwargs["json"]["new_project"] == "beta"
        assert result["project"] == "beta"

    async def test_omits_unset_fields(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_logbook_entry("alpha", 1, title="only-title")
        _, kwargs = mock.call_args
        # Only the explicitly-set field is sent; nothing else leaks into the
        # request body so callers can mutate one attribute at a time.
        assert kwargs["json"] == {"title": "only-title"}


@pytest.mark.mcp
class TestDeleteLogbookEntry:
    async def test_deletes_entry(self):
        with patch("mcp_server._api", return_value={"status": "ok"}):
            result = await delete_logbook_entry("alpha", 1)
        assert result["status"] == "ok"

    async def test_passes_args(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await delete_logbook_entry("alpha", 42)
        mock.assert_called_once_with("DELETE", "/api/logbook/alpha/entries/42")
