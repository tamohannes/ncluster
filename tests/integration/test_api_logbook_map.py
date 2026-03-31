"""Integration tests for logbook map API and #id cross-reference links."""

import json
import pytest


@pytest.mark.integration
class TestLogbookLinks:
    """Verify that #id references in entry bodies create logbook_links."""

    def _create(self, client, project, title, body="", entry_type="note"):
        r = client.post(
            f"/api/logbook/{project}/entries",
            data=json.dumps({"title": title, "body": body, "entry_type": entry_type}),
            content_type="application/json",
        )
        assert r.status_code == 200
        return r.get_json()["id"]

    def test_create_entry_with_ref_creates_link(self, client):
        id_a = self._create(client, "proj", "Plan A", entry_type="plan")
        id_b = self._create(client, "proj", "Note B", body=f"Follow-up to #{id_a}")

        resp = client.get("/api/logbook/proj/map")
        data = resp.get_json()
        links = data["links"]
        assert any(
            l["source_id"] == id_b and l["target_id"] == id_a for l in links
        ), f"Expected link from {id_b} -> {id_a}, got {links}"

    def test_update_body_updates_links(self, client):
        id_a = self._create(client, "proj", "Entry A")
        id_b = self._create(client, "proj", "Entry B")
        id_c = self._create(client, "proj", "Entry C", body=f"Refs #{id_a}")

        resp = client.get("/api/logbook/proj/map")
        links_before = resp.get_json()["links"]
        assert any(l["source_id"] == id_c and l["target_id"] == id_a for l in links_before)

        client.put(
            f"/api/logbook/proj/entries/{id_c}",
            data=json.dumps({"body": f"Now refs #{id_b} instead"}),
            content_type="application/json",
        )

        resp = client.get("/api/logbook/proj/map")
        links_after = resp.get_json()["links"]
        assert not any(l["source_id"] == id_c and l["target_id"] == id_a for l in links_after)
        assert any(l["source_id"] == id_c and l["target_id"] == id_b for l in links_after)

    def test_self_reference_ignored(self, client):
        id_a = self._create(client, "proj", "Self ref", body="See #999999")
        client.put(
            f"/api/logbook/proj/entries/{id_a}",
            data=json.dumps({"body": f"Self ref #{id_a}"}),
            content_type="application/json",
        )
        resp = client.get("/api/logbook/proj/map")
        links = resp.get_json()["links"]
        assert not any(l["source_id"] == id_a and l["target_id"] == id_a for l in links)

    def test_no_refs_no_links(self, client):
        self._create(client, "proj", "No refs", body="Plain body text")
        resp = client.get("/api/logbook/proj/map")
        assert resp.get_json()["links"] == []

    def test_delete_entry_removes_links(self, client):
        id_a = self._create(client, "proj", "Parent")
        id_b = self._create(client, "proj", "Child", body=f"Refs #{id_a}")
        client.delete(f"/api/logbook/proj/entries/{id_b}")
        resp = client.get("/api/logbook/proj/map")
        links = resp.get_json()["links"]
        assert not any(l["source_id"] == id_b for l in links)


@pytest.mark.integration
class TestLogbookMapApi:
    """Verify /api/logbook/<project>/map returns correct structure."""

    def _create(self, client, project, title, body="", entry_type="note"):
        r = client.post(
            f"/api/logbook/{project}/entries",
            data=json.dumps({"title": title, "body": body, "entry_type": entry_type}),
            content_type="application/json",
        )
        return r.get_json()["id"]

    def test_map_empty_project(self, client):
        resp = client.get("/api/logbook/empty/map")
        data = resp.get_json()
        assert data["nodes"] == []
        assert data["links"] == []

    def test_map_returns_all_nodes(self, client):
        self._create(client, "proj", "A")
        self._create(client, "proj", "B")
        self._create(client, "proj", "C")
        resp = client.get("/api/logbook/proj/map")
        data = resp.get_json()
        assert len(data["nodes"]) == 3

    def test_map_node_fields(self, client):
        self._create(client, "proj", "Test Node", entry_type="plan")
        resp = client.get("/api/logbook/proj/map")
        node = resp.get_json()["nodes"][0]
        assert "id" in node
        assert node["title"] == "Test Node"
        assert node["entry_type"] == "plan"
        assert "created_at" in node
        assert "edited_at" in node

    def test_map_cross_project_isolation(self, client):
        id_a = self._create(client, "alpha", "Alpha entry")
        self._create(client, "beta", "Beta entry", body=f"Ref #{id_a}")
        resp_a = client.get("/api/logbook/alpha/map")
        resp_b = client.get("/api/logbook/beta/map")
        assert len(resp_a.get_json()["nodes"]) == 1
        assert len(resp_b.get_json()["nodes"]) == 1


@pytest.mark.integration
class TestStorageQuotaStatus:
    """Verify storage quota endpoint returns 200 even for unsupported clusters."""

    def test_quota_unsupported_cluster_returns_200(self, client, mock_cluster):
        resp = client.get(f"/api/storage_quota/{mock_cluster}")
        assert resp.status_code == 200

    def test_quota_unknown_cluster_returns_404(self, client):
        resp = client.get("/api/storage_quota/nonexistent-cluster-xyz")
        assert resp.status_code == 404


@pytest.mark.integration
class TestCreateUpdateWithoutEmbeddings:
    """Verify create/update still work after embedding removal."""

    def test_create_entry_no_embedding_error(self, client):
        resp = client.post(
            "/api/logbook/proj/entries",
            data=json.dumps({"title": "After embed removal", "body": "Test body"}),
            content_type="application/json",
        )
        data = resp.get_json()
        assert data["status"] == "ok"
        assert "id" in data

    def test_update_entry_no_embedding_error(self, client):
        r = client.post(
            "/api/logbook/proj/entries",
            data=json.dumps({"title": "Original", "body": "old"}),
            content_type="application/json",
        )
        entry_id = r.get_json()["id"]
        resp = client.put(
            f"/api/logbook/proj/entries/{entry_id}",
            data=json.dumps({"title": "Updated", "body": "new body with #1 ref"}),
            content_type="application/json",
        )
        assert resp.get_json()["status"] == "ok"
