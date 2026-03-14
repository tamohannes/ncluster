"""Unit tests for server/logbooks.py file operations."""

import os
import pytest

from server.logbooks import (
    list_logbooks, read_logbook, add_entry, update_entry, delete_entry,
    create_logbook, delete_logbook, rename_logbook,
    _split_entries, _sanitize_name,
)


@pytest.fixture()
def logbook_dir(tmp_path, monkeypatch):
    d = str(tmp_path / "logbooks")
    monkeypatch.setattr("server.logbooks.LOGBOOKS_DIR", d)
    return d


@pytest.mark.unit
class TestSplitEntries:
    def test_single_entry(self):
        assert _split_entries("## Note\n\nHello") == ["## Note\n\nHello"]

    def test_multiple_entries(self):
        content = "entry1\n---\nentry2\n---\nentry3"
        assert len(_split_entries(content)) == 3

    def test_empty(self):
        assert _split_entries("") == []
        assert _split_entries("   ") == []


@pytest.mark.unit
class TestSanitizeName:
    def test_strips_extension(self):
        assert _sanitize_name("notes.md") == "notes"

    def test_no_extension(self):
        assert _sanitize_name("notes") == "notes"

    def test_path_traversal(self):
        assert _sanitize_name("../../etc/passwd") == "passwd"


@pytest.mark.unit
class TestCreateLogbook:
    def test_create_new(self, logbook_dir):
        result = create_logbook("proj", "experiments")
        assert result["status"] == "ok"
        assert os.path.isfile(os.path.join(logbook_dir, "proj", "experiments.md"))

    def test_create_existing(self, logbook_dir):
        create_logbook("proj", "experiments")
        result = create_logbook("proj", "experiments")
        assert result["status"] == "ok"
        assert "Already exists" in result.get("message", "")


@pytest.mark.unit
class TestListLogbooks:
    def test_empty_project(self, logbook_dir):
        assert list_logbooks("proj") == []

    def test_lists_files(self, logbook_dir):
        create_logbook("proj", "notes")
        create_logbook("proj", "bugs")
        result = list_logbooks("proj")
        names = [lb["name"] for lb in result]
        assert "notes" in names
        assert "bugs" in names

    def test_entry_count(self, logbook_dir):
        create_logbook("proj", "notes")
        add_entry("proj", "notes", "entry 1")
        add_entry("proj", "notes", "entry 2")
        result = list_logbooks("proj")
        lb = next(l for l in result if l["name"] == "notes")
        assert lb["entry_count"] == 2


@pytest.mark.unit
class TestAddEntry:
    def test_add_to_new_logbook(self, logbook_dir):
        result = add_entry("proj", "notes", "## First note")
        assert result["status"] == "ok"
        assert result["entry_count"] == 1

    def test_prepends(self, logbook_dir):
        add_entry("proj", "notes", "first")
        add_entry("proj", "notes", "second")
        data = read_logbook("proj", "notes")
        assert data["entries"][0] == "second"
        assert data["entries"][1] == "first"


@pytest.mark.unit
class TestReadLogbook:
    def test_read_existing(self, logbook_dir):
        add_entry("proj", "notes", "hello")
        data = read_logbook("proj", "notes")
        assert data["name"] == "notes"
        assert len(data["entries"]) == 1
        assert "hello" in data["content"]

    def test_read_missing(self, logbook_dir):
        data = read_logbook("proj", "nonexistent")
        assert "error" in data


@pytest.mark.unit
class TestUpdateEntry:
    def test_update_valid_index(self, logbook_dir):
        add_entry("proj", "notes", "old content")
        result = update_entry("proj", "notes", 0, "new content")
        assert result["status"] == "ok"
        data = read_logbook("proj", "notes")
        assert data["entries"][0] == "new content"

    def test_update_out_of_range(self, logbook_dir):
        add_entry("proj", "notes", "only entry")
        result = update_entry("proj", "notes", 5, "nope")
        assert result["status"] == "error"

    def test_update_missing_logbook(self, logbook_dir):
        result = update_entry("proj", "nonexistent", 0, "content")
        assert result["status"] == "error"


@pytest.mark.unit
class TestDeleteEntry:
    def test_delete_valid(self, logbook_dir):
        add_entry("proj", "notes", "first")
        add_entry("proj", "notes", "second")
        result = delete_entry("proj", "notes", 0)
        assert result["status"] == "ok"
        assert result["entry_count"] == 1
        data = read_logbook("proj", "notes")
        assert data["entries"] == ["first"]

    def test_delete_out_of_range(self, logbook_dir):
        add_entry("proj", "notes", "only")
        result = delete_entry("proj", "notes", 5)
        assert result["status"] == "error"

    def test_delete_missing_logbook(self, logbook_dir):
        result = delete_entry("proj", "nope", 0)
        assert result["status"] == "error"

    def test_delete_last_entry(self, logbook_dir):
        add_entry("proj", "notes", "only")
        result = delete_entry("proj", "notes", 0)
        assert result["status"] == "ok"
        assert result["entry_count"] == 0


@pytest.mark.unit
class TestRenameLogbook:
    def test_rename_success(self, logbook_dir):
        create_logbook("proj", "old-name")
        add_entry("proj", "old-name", "content")
        result = rename_logbook("proj", "old-name", "new-name")
        assert result["status"] == "ok"
        assert result["name"] == "new-name"
        assert list_logbooks("proj")[0]["name"] == "new-name"
        data = read_logbook("proj", "new-name")
        assert "content" in data["entries"][0]

    def test_rename_missing(self, logbook_dir):
        result = rename_logbook("proj", "nope", "new")
        assert result["status"] == "error"

    def test_rename_conflict(self, logbook_dir):
        create_logbook("proj", "a")
        create_logbook("proj", "b")
        result = rename_logbook("proj", "a", "b")
        assert result["status"] == "error"

    def test_rename_empty_name(self, logbook_dir):
        create_logbook("proj", "a")
        result = rename_logbook("proj", "a", "")
        assert result["status"] == "error"


@pytest.mark.unit
class TestDeleteLogbook:
    def test_delete_existing(self, logbook_dir):
        create_logbook("proj", "trash")
        result = delete_logbook("proj", "trash")
        assert result["status"] == "ok"
        assert list_logbooks("proj") == []

    def test_delete_missing(self, logbook_dir):
        result = delete_logbook("proj", "nonexistent")
        assert result["status"] == "error"
