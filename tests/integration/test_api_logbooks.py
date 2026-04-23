"""Integration tests for logbook v2 API endpoints."""

import json
import pytest


@pytest.mark.integration
class TestLogbookApi:
    def test_list_empty(self, client):
        resp = client.get("/api/logbook/testproj/entries")
        assert resp.status_code == 200
        assert resp.get_json() == []

    def test_create_entry(self, client):
        resp = client.post("/api/logbook/testproj/entries",
                           data=json.dumps({"title": "First note", "body": "Hello world"}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["id"] >= 1

    def test_create_no_title(self, client):
        resp = client.post("/api/logbook/testproj/entries",
                           data=json.dumps({"body": "no title"}),
                           content_type="application/json")
        assert resp.status_code == 400

    def test_create_and_read(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "Test", "body": "## Heading\n\nContent"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]

        resp = client.get(f"/api/logbook/testproj/entries/{entry_id}")
        data = resp.get_json()
        assert data["title"] == "Test"
        assert "Content" in data["body"]
        assert data["project"] == "testproj"

    def test_read_missing(self, client):
        resp = client.get("/api/logbook/testproj/entries/9999")
        assert resp.status_code == 404

    def test_list_entries(self, client):
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "A", "body": "body a"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "B", "body": "body b"}),
                     content_type="application/json")
        resp = client.get("/api/logbook/testproj/entries")
        entries = resp.get_json()
        assert len(entries) == 2
        assert all("body_preview" in e for e in entries)
        assert all("body" not in e for e in entries)

    def test_list_with_search(self, client):
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "CUDA results", "body": "GPU util 95%"}),
                     content_type="application/json")
        client.post("/api/logbook/testproj/entries",
                     data=json.dumps({"title": "Bug report", "body": "segfault"}),
                     content_type="application/json")
        resp = client.get("/api/logbook/testproj/entries?q=CUDA")
        entries = resp.get_json()
        assert len(entries) == 1
        assert "CUDA" in entries[0]["title"]

    def test_update_entry(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "Old", "body": "old body"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/testproj/entries/{entry_id}",
                          data=json.dumps({"title": "New", "body": "new body"}),
                          content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        data = client.get(f"/api/logbook/testproj/entries/{entry_id}").get_json()
        assert data["title"] == "New"
        assert data["body"] == "new body"

    def test_update_missing(self, client):
        resp = client.put("/api/logbook/testproj/entries/9999",
                          data=json.dumps({"title": "X"}),
                          content_type="application/json")
        assert resp.status_code == 404

    def test_update_entry_type(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "Plan", "body": "..."}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/testproj/entries/{entry_id}",
                          data=json.dumps({"entry_type": "plan"}),
                          content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        data = client.get(f"/api/logbook/testproj/entries/{entry_id}").get_json()
        assert data["entry_type"] == "plan"

    def test_update_pinned(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "Pin me", "body": "x"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/testproj/entries/{entry_id}",
                          data=json.dumps({"pinned": True}),
                          content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        assert client.get(f"/api/logbook/testproj/entries/{entry_id}").get_json()["pinned"] == 1
        client.put(f"/api/logbook/testproj/entries/{entry_id}",
                   data=json.dumps({"pinned": False}),
                   content_type="application/json")
        assert client.get(f"/api/logbook/testproj/entries/{entry_id}").get_json()["pinned"] == 0

    def test_move_to_new_project(self, client):
        r = client.post("/api/logbook/oldproj/entries",
                        data=json.dumps({"title": "Will move", "body": "x"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/oldproj/entries/{entry_id}",
                          data=json.dumps({"new_project": "newproj"}),
                          content_type="application/json")
        body = resp.get_json()
        assert body["status"] == "ok"
        assert body["project"] == "newproj"
        # Entry is gone from the source project but readable under the new one.
        assert client.get(f"/api/logbook/oldproj/entries/{entry_id}").status_code == 404
        moved = client.get(f"/api/logbook/newproj/entries/{entry_id}").get_json()
        assert moved["project"] == "newproj"
        assert moved["title"] == "Will move"

    def test_move_combined_with_other_fields(self, client):
        r = client.post("/api/logbook/oldproj/entries",
                        data=json.dumps({"title": "Old title", "body": "old"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/oldproj/entries/{entry_id}",
                          data=json.dumps({
                              "new_project": "newproj",
                              "title": "Renamed",
                              "entry_type": "plan",
                              "pinned": True,
                          }),
                          content_type="application/json")
        assert resp.get_json()["status"] == "ok"
        moved = client.get(f"/api/logbook/newproj/entries/{entry_id}").get_json()
        assert moved["title"] == "Renamed"
        assert moved["entry_type"] == "plan"
        assert moved["pinned"] == 1

    def test_move_empty_new_project_rejected(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "x", "body": "x"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.put(f"/api/logbook/testproj/entries/{entry_id}",
                          data=json.dumps({"new_project": "   "}),
                          content_type="application/json")
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["status"] == "error"
        assert "non-empty" in body["error"]
        # Original entry untouched.
        assert client.get(f"/api/logbook/testproj/entries/{entry_id}").status_code == 200

    def test_delete_entry(self, client):
        r = client.post("/api/logbook/testproj/entries",
                        data=json.dumps({"title": "Delete me", "body": "x"}),
                        content_type="application/json")
        entry_id = r.get_json()["id"]
        resp = client.delete(f"/api/logbook/testproj/entries/{entry_id}")
        assert resp.get_json()["status"] == "ok"
        resp2 = client.get(f"/api/logbook/testproj/entries/{entry_id}")
        assert resp2.status_code == 404

    def test_delete_missing(self, client):
        resp = client.delete("/api/logbook/testproj/entries/9999")
        assert resp.status_code == 404

    def test_cross_project_search(self, client):
        client.post("/api/logbook/alpha/entries",
                     data=json.dumps({"title": "Alpha note", "body": "accuracy results"}),
                     content_type="application/json")
        client.post("/api/logbook/beta/entries",
                     data=json.dumps({"title": "Beta note", "body": "accuracy on benchmark"}),
                     content_type="application/json")
        resp = client.get("/api/logbook/search?q=accuracy")
        results = resp.get_json()
        assert len(results) == 2

    def test_search_filter_project(self, client):
        client.post("/api/logbook/alpha/entries",
                     data=json.dumps({"title": "A", "body": "shared term"}),
                     content_type="application/json")
        client.post("/api/logbook/beta/entries",
                     data=json.dumps({"title": "B", "body": "shared term"}),
                     content_type="application/json")
        resp = client.get("/api/logbook/search?q=shared&project=alpha")
        results = resp.get_json()
        assert len(results) == 1
        assert results[0]["project"] == "alpha"

    def test_search_empty_query(self, client):
        resp = client.get("/api/logbook/search?q=")
        assert resp.get_json() == []
