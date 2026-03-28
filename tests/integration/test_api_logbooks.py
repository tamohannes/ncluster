"""Integration tests for logbook API endpoints (disabled — logbooks moved to DeepLake)."""

import json
import pytest

pytestmark = pytest.mark.skip(reason="Logbook routes disabled — moved to DeepLake")


@pytest.fixture(autouse=True)
def _logbook_dir(tmp_path, monkeypatch):
    monkeypatch.setattr("server.logbooks.LOGBOOKS_DIR", str(tmp_path / "logbooks"))


@pytest.mark.integration
class TestLogbookApi:
    def test_list_empty(self, client):
        resp = client.get("/api/logbooks/testproj")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_create_logbook(self, client):
        resp = client.post("/api/logbook/testproj",
                           data=json.dumps({"name": "experiments"}),
                           content_type="application/json")
        assert resp.get_json()["status"] == "ok"

    def test_create_no_name(self, client):
        resp = client.post("/api/logbook/testproj",
                           data=json.dumps({}),
                           content_type="application/json")
        assert resp.status_code == 400

    def test_add_and_read_entry(self, client):
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "notes"}),
                     content_type="application/json")
        resp = client.post("/api/logbook/testproj/notes",
                           data=json.dumps({"content": "## First note\n\nHello world"}),
                           content_type="application/json")
        assert resp.get_json()["status"] == "ok"

        resp = client.get("/api/logbook/testproj/notes")
        data = resp.get_json()
        assert len(data["entries"]) == 1
        assert "Hello world" in data["entries"][0]

    def test_prepend_order(self, client):
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "first"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "second"}),
                     content_type="application/json")
        resp = client.get("/api/logbook/testproj/notes")
        entries = resp.get_json()["entries"]
        assert entries[0] == "second"
        assert entries[1] == "first"

    def test_update_entry(self, client):
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "original"}),
                     content_type="application/json")
        resp = client.put("/api/logbook/testproj/notes/0",
                          data=json.dumps({"content": "updated"}),
                          content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        data = client.get("/api/logbook/testproj/notes").get_json()
        assert data["entries"][0] == "updated"

    def test_update_no_content(self, client):
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "x"}),
                     content_type="application/json")
        resp = client.put("/api/logbook/testproj/notes/0",
                          data=json.dumps({}),
                          content_type="application/json")
        assert resp.status_code == 400

    def test_delete_logbook(self, client):
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "trash"}),
                     content_type="application/json")
        resp = client.delete("/api/logbook/testproj/trash")
        assert resp.get_json()["status"] == "ok"
        listing = client.get("/api/logbooks/testproj").get_json()
        assert not any(lb["name"] == "trash" for lb in listing)

    def test_list_logbooks(self, client):
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "a"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "b"}),
                     content_type="application/json")
        listing = client.get("/api/logbooks/testproj").get_json()
        names = [lb["name"] for lb in listing]
        assert "a" in names
        assert "b" in names

    def test_add_creates_logbook_if_missing(self, client):
        resp = client.post("/api/logbook/testproj/auto-created",
                           data=json.dumps({"content": "note"}),
                           content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        data = client.get("/api/logbook/testproj/auto-created").get_json()
        assert len(data["entries"]) == 1

    def test_delete_entry(self, client):
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "keep"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj/notes",
                     data=json.dumps({"content": "remove"}),
                     content_type="application/json")
        resp = client.delete("/api/logbook/testproj/notes/0")
        assert resp.get_json()["status"] == "ok"
        data = client.get("/api/logbook/testproj/notes").get_json()
        assert len(data["entries"]) == 1
        assert data["entries"][0] == "keep"

    def test_rename_logbook(self, client):
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "old"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj/old",
                     data=json.dumps({"content": "data"}),
                     content_type="application/json")
        resp = client.post("/api/logbook/testproj/old/rename",
                           data=json.dumps({"new_name": "new"}),
                           content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        listing = client.get("/api/logbooks/testproj").get_json()
        names = [lb["name"] for lb in listing]
        assert "new" in names
        assert "old" not in names

    def test_rename_no_name(self, client):
        client.post("/api/logbook/testproj",
                     data=json.dumps({"name": "x"}),
                     content_type="application/json")
        resp = client.post("/api/logbook/testproj/x/rename",
                           data=json.dumps({}),
                           content_type="application/json")
        assert resp.status_code == 400
