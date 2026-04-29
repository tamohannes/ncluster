"""Unit tests for server/logbooks.py — SQLite+FTS5 logbook CRUD and BM25 search."""

import sqlite3
import pytest

from server.logbooks import (
    list_entries, get_entry, create_entry, update_entry,
    delete_entry, search_entries, list_campaigns,
    _extract_campaign_from_title,
)
from server.db import init_db


@pytest.fixture(autouse=True)
def _fresh_db(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr("server.db.DB_PATH", db_path)
    monkeypatch.setattr("server.logbooks.get_db", lambda: _connect(db_path))
    init_db()
    monkeypatch.setattr("server.db.DB_PATH", db_path)


def _connect(path):
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


class TestCreateEntry:
    @pytest.mark.unit
    def test_basic_create(self):
        result = create_entry("alpha", "First note", "some body")
        assert result["status"] == "ok"
        assert result["id"] >= 1
        assert "created_at" in result

    @pytest.mark.unit
    def test_create_empty_body(self):
        result = create_entry("alpha", "Title only")
        assert result["status"] == "ok"


class TestGetEntry:
    @pytest.mark.unit
    def test_get_existing(self):
        r = create_entry("alpha", "Test", "body text")
        entry = get_entry("alpha", r["id"])
        assert entry["title"] == "Test"
        assert entry["body"] == "body text"
        assert entry["project"] == "alpha"

    @pytest.mark.unit
    def test_get_missing(self):
        result = get_entry("alpha", 9999)
        assert result.get("status") == "error"

    @pytest.mark.unit
    def test_get_wrong_project(self):
        r = create_entry("alpha", "Test", "body")
        result = get_entry("beta", r["id"])
        assert result.get("status") == "error"


class TestListEntries:
    @pytest.mark.unit
    def test_list_empty(self):
        assert list_entries("alpha") == []

    @pytest.mark.unit
    def test_list_multiple(self):
        create_entry("alpha", "Note 1", "body1")
        create_entry("alpha", "Note 2", "body2")
        create_entry("beta", "Other", "body3")
        entries = list_entries("alpha")
        assert len(entries) == 2
        assert all("body_preview" in e for e in entries)
        assert all("body" not in e for e in entries)

    @pytest.mark.unit
    def test_list_with_search(self):
        create_entry("alpha", "CUDA optimization", "Using mixed precision training")
        create_entry("alpha", "Bug report", "Segfault in data loader")
        results = list_entries("alpha", query="CUDA")
        assert len(results) == 1
        assert "CUDA" in results[0]["title"]

    @pytest.mark.unit
    def test_list_search_by_prefixed_id(self):
        target = create_entry("alpha", "Target note", "body")
        create_entry("alpha", "Other note", f"mentions {target['id']}")

        results = list_entries("alpha", query=f"#{target['id']}")

        assert len(results) == 1
        assert results[0]["id"] == target["id"]

    @pytest.mark.unit
    def test_list_search_by_bare_id_keeps_text_matches(self):
        target = create_entry("alpha", "Target note", "body")
        other = create_entry("alpha", "Other note", f"mentions {target['id']}")

        results = list_entries("alpha", query=str(target["id"]))

        assert [r["id"] for r in results] == [target["id"], other["id"]]

    @pytest.mark.unit
    def test_list_limit_offset(self):
        for i in range(5):
            create_entry("alpha", f"Note {i}", f"body {i}")
        page1 = list_entries("alpha", limit=2, offset=0)
        page2 = list_entries("alpha", limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert page1[0]["id"] != page2[0]["id"]


class TestUpdateEntry:
    @pytest.mark.unit
    def test_update_title(self):
        r = create_entry("alpha", "Old", "body")
        result = update_entry("alpha", r["id"], title="New")
        assert result["status"] == "ok"
        entry = get_entry("alpha", r["id"])
        assert entry["title"] == "New"
        assert entry["body"] == "body"

    @pytest.mark.unit
    def test_update_body(self):
        r = create_entry("alpha", "Title", "old body")
        update_entry("alpha", r["id"], body="new body")
        entry = get_entry("alpha", r["id"])
        assert entry["body"] == "new body"
        assert entry["title"] == "Title"

    @pytest.mark.unit
    def test_update_bumps_edited_at(self):
        r = create_entry("alpha", "Title", "body")
        entry_before = get_entry("alpha", r["id"])
        import time; time.sleep(0.01)
        update_entry("alpha", r["id"], title="Updated")
        entry_after = get_entry("alpha", r["id"])
        assert entry_after["edited_at"] >= entry_before["edited_at"]

    @pytest.mark.unit
    def test_update_missing(self):
        result = update_entry("alpha", 9999, title="X")
        assert result["status"] == "error"


class TestDeleteEntry:
    @pytest.mark.unit
    def test_delete_existing(self):
        r = create_entry("alpha", "To delete", "body")
        result = delete_entry("alpha", r["id"])
        assert result["status"] == "ok"
        assert get_entry("alpha", r["id"]).get("status") == "error"

    @pytest.mark.unit
    def test_delete_missing(self):
        result = delete_entry("alpha", 9999)
        assert result["status"] == "error"


class TestSearchEntries:
    @pytest.mark.unit
    def test_bm25_search(self):
        create_entry("alpha", "Experiment results", "Accuracy reached 92% on GPQA")
        create_entry("alpha", "Debug notes", "Fixed memory leak in data loader")
        create_entry("beta", "Other project", "Accuracy on different benchmark")
        results = search_entries("accuracy")
        assert len(results) == 2

    @pytest.mark.unit
    def test_search_filter_project(self):
        create_entry("alpha", "Note", "accuracy results")
        create_entry("beta", "Note", "accuracy results")
        results = search_entries("accuracy", project="alpha")
        assert len(results) == 1
        assert results[0]["project"] == "alpha"

    @pytest.mark.unit
    def test_global_search_by_id(self):
        target = create_entry("alpha", "Target note", "body")
        create_entry("beta", "Other note", "body")

        results = search_entries(f"id:{target['id']}")

        assert len(results) == 1
        assert results[0]["id"] == target["id"]
        assert results[0]["project"] == "alpha"

    @pytest.mark.unit
    def test_global_search_by_id_respects_project_filter(self):
        target = create_entry("alpha", "Target note", "body")

        assert search_entries(f"#{target['id']}", project="beta") == []

    @pytest.mark.unit
    def test_search_empty_query(self):
        create_entry("alpha", "Note", "body")
        assert search_entries("") == []
        assert search_entries(None) == []

    @pytest.mark.unit
    def test_search_no_results(self):
        create_entry("alpha", "Note", "body")
        results = search_entries("xyznonexistent")
        assert results == []

    @pytest.mark.unit
    def test_fts_syncs_on_update(self):
        r = create_entry("alpha", "Original", "original body")
        update_entry("alpha", r["id"], title="Updated title", body="updated body")
        assert len(search_entries("Updated")) == 1
        assert len(search_entries("Original")) == 0

    @pytest.mark.unit
    def test_fts_syncs_on_delete(self):
        r = create_entry("alpha", "Deletable", "unique content")
        assert len(search_entries("Deletable")) == 1
        delete_entry("alpha", r["id"])
        assert len(search_entries("Deletable")) == 0


class TestCampaignExtraction:
    @pytest.mark.unit
    def test_bracket_prefix(self):
        camp, title = _extract_campaign_from_title("[mpsf] EXP-5 results")
        assert camp == "mpsf"
        assert title == "EXP-5 results"

    @pytest.mark.unit
    def test_bracket_no_space(self):
        camp, title = _extract_campaign_from_title("[eval]Diamond results")
        assert camp == "eval"
        assert title == "Diamond results"

    @pytest.mark.unit
    def test_no_prefix(self):
        camp, title = _extract_campaign_from_title("EXP-5: no prefix")
        assert camp == ""
        assert title == "EXP-5: no prefix"

    @pytest.mark.unit
    def test_empty_string(self):
        camp, title = _extract_campaign_from_title("")
        assert camp == ""
        assert title == ""

    @pytest.mark.unit
    def test_none_input(self):
        camp, title = _extract_campaign_from_title(None)
        assert camp == ""
        assert title == ""


class TestCampaignCrud:
    @pytest.mark.unit
    def test_create_with_explicit_campaign(self):
        r = create_entry("alpha", "Note", campaign="mpsf")
        assert r["campaign"] == "mpsf"
        entry = get_entry("alpha", r["id"])
        assert entry["campaign"] == "mpsf"

    @pytest.mark.unit
    def test_create_auto_extract_campaign(self):
        r = create_entry("alpha", "[eval] Diamond results")
        assert r["campaign"] == "eval"
        entry = get_entry("alpha", r["id"])
        assert entry["title"] == "Diamond results"
        assert entry["campaign"] == "eval"

    @pytest.mark.unit
    def test_create_no_campaign(self):
        r = create_entry("alpha", "Plain title")
        entry = get_entry("alpha", r["id"])
        assert entry["campaign"] == ""

    @pytest.mark.unit
    def test_update_campaign(self):
        r = create_entry("alpha", "Note")
        update_entry("alpha", r["id"], campaign="Train")
        entry = get_entry("alpha", r["id"])
        assert entry["campaign"] == "train"

    @pytest.mark.unit
    def test_list_filter_by_campaign(self):
        create_entry("alpha", "A", campaign="mpsf")
        create_entry("alpha", "B", campaign="text")
        create_entry("alpha", "C", campaign="mpsf")
        results = list_entries("alpha", campaign="mpsf")
        assert len(results) == 2
        assert all(e["campaign"] == "mpsf" for e in results)

    @pytest.mark.unit
    def test_list_campaigns(self):
        create_entry("alpha", "A", campaign="mpsf")
        create_entry("alpha", "B", campaign="mpsf")
        create_entry("alpha", "C", campaign="text")
        create_entry("alpha", "D")
        campaigns = list_campaigns("alpha")
        names = {c["name"] for c in campaigns}
        assert "mpsf" in names
        assert "text" in names
        assert "" not in names
        mpsf = next(c for c in campaigns if c["name"] == "mpsf")
        assert mpsf["count"] == 2
