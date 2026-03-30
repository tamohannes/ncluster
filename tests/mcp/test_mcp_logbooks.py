"""MCP logbook v2 tool contract tests."""

import pytest
from unittest.mock import patch

import mcp_server
from mcp_server import (
    list_logbook_entries, read_logbook_entry, create_logbook_entry,
    update_logbook_entry, delete_logbook_entry, search_logbook,
)


def _mock_get(response):
    return patch.object(mcp_server, "_api_get", return_value=response)


def _mock_post_json(response):
    return patch.object(mcp_server, "_api_post_json", return_value=response)


@pytest.mark.mcp
class TestListLogbookEntries:
    def test_returns_list(self):
        entries = [{"id": 1, "title": "Note", "body_preview": "...", "created_at": "2026-03-28", "edited_at": "2026-03-28"}]
        with _mock_get(entries):
            result = list_logbook_entries("alpha")
        assert isinstance(result, list)
        assert result[0]["title"] == "Note"

    def test_with_search_query(self):
        with _mock_get([]) as mock:
            list_logbook_entries("alpha", query="CUDA")
            url = mock.call_args[0][0]
            assert "q=CUDA" in url

    def test_wraps_error_in_list(self):
        with _mock_get({"status": "error", "error": "fail"}):
            result = list_logbook_entries("alpha")
        assert isinstance(result, list)


@pytest.mark.mcp
class TestReadLogbookEntry:
    def test_returns_entry(self):
        entry = {"id": 1, "project": "alpha", "title": "Note", "body": "full content", "created_at": "2026-03-28", "edited_at": "2026-03-28"}
        with _mock_get(entry):
            result = read_logbook_entry("alpha", 1)
        assert result["title"] == "Note"
        assert result["body"] == "full content"

    def test_calls_correct_url(self):
        with _mock_get({}) as mock:
            read_logbook_entry("alpha", 42)
            assert "/api/logbook/alpha/entries/42" in mock.call_args[0][0]


@pytest.mark.mcp
class TestCreateLogbookEntry:
    def test_creates_entry(self):
        resp = {"status": "ok", "id": 1, "created_at": "2026-03-28T10:00:00"}
        with _mock_post_json(resp):
            result = create_logbook_entry("alpha", "New note", "body text")
        assert result["status"] == "ok"
        assert result["id"] == 1

    def test_calls_correct_url(self):
        with _mock_post_json({"status": "ok", "id": 1}) as mock:
            create_logbook_entry("alpha", "Title", "Body")
            url = mock.call_args[0][0]
            assert "/api/logbook/alpha/entries" in url
            payload = mock.call_args[0][1]
            assert payload["title"] == "Title"
            assert payload["body"] == "Body"


@pytest.mark.mcp
class TestUpdateLogbookEntry:
    def test_updates_entry(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = lambda s, *a: None
            mock_urlopen.return_value.read.return_value = b'{"status": "ok", "id": 1, "edited_at": "2026-03-28T11:00:00"}'
            result = update_logbook_entry("alpha", 1, title="Updated")
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestDeleteLogbookEntry:
    def test_deletes_entry(self):
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.return_value.__enter__ = lambda s: s
            mock_urlopen.return_value.__exit__ = lambda s, *a: None
            mock_urlopen.return_value.read.return_value = b'{"status": "ok"}'
            result = delete_logbook_entry("alpha", 1)
        assert result["status"] == "ok"


@pytest.mark.mcp
class TestSearchLogbook:
    def test_search_returns_list(self):
        entries = [{"id": 1, "project": "alpha", "title": "Match", "body_preview": "..."}]
        with _mock_get(entries):
            result = search_logbook("accuracy")
        assert isinstance(result, list)
        assert len(result) == 1

    def test_search_with_filters(self):
        with _mock_get([]) as mock:
            search_logbook("test", project="alpha", date_from="2026-01-01", date_to="2026-12-31")
            url = mock.call_args[0][0]
            assert "project=alpha" in url
            assert "from=2026-01-01" in url
            assert "to=2026-12-31" in url
