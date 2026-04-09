"""MCP direct-import integration tests.

Verifies MCP tools work end-to-end with the real DB fixture and mocked SSH,
without any HTTP layer. Replaces the old MCP-to-Flask boundary tests.
"""

import pytest

from mcp_server import (
    health_check, list_jobs, get_history,
    cancel_job, jobs_summary,
    list_logbook_entries, create_logbook_entry,
    read_logbook_entry, delete_logbook_entry,
)


@pytest.mark.integration
@pytest.mark.mcp
class TestMcpDirectImport:
    def test_health_check(self, db_path):
        result = health_check()
        assert result["status"] == "ok"

    def test_list_jobs_returns_list(self, db_path, mock_ssh):
        mock_ssh.set("mock-cluster", "squeue", ("", ""))
        result = list_jobs()
        assert isinstance(result, list)

    def test_list_jobs_unknown_cluster_error(self, db_path):
        result = list_jobs(cluster="nonexistent")
        assert len(result) == 1
        assert "error" in result[0]

    def test_get_history_returns_list(self, db_path):
        result = get_history(limit=5)
        assert isinstance(result, list)

    def test_jobs_summary_string(self, db_path, mock_ssh):
        mock_ssh.set("mock-cluster", "squeue", ("", ""))
        result = jobs_summary()
        assert isinstance(result, str)
        assert "Total:" in result

    def test_cancel_local_bad_pid(self, db_path):
        result = cancel_job("local", "99999999")
        assert result["status"] == "error"


@pytest.mark.integration
@pytest.mark.mcp
class TestMcpLogbookIntegration:
    def test_create_and_read(self, db_path):
        created = create_logbook_entry("test-proj", "Integration note", "body text")
        assert created["status"] == "ok"
        entry_id = created["id"]

        full = read_logbook_entry("test-proj", entry_id)
        assert full["title"] == "Integration note"
        assert full["body"] == "body text"

    def test_list_entries(self, db_path):
        create_logbook_entry("test-proj", "Entry A", "a")
        create_logbook_entry("test-proj", "Entry B", "b")
        result = list_logbook_entries("test-proj")
        assert isinstance(result, list)
        assert len(result) == 2

    def test_delete_entry(self, db_path):
        created = create_logbook_entry("test-proj", "To delete", "x")
        result = delete_logbook_entry("test-proj", created["id"])
        assert result["status"] == "ok"
        remaining = list_logbook_entries("test-proj")
        assert len(remaining) == 0
