"""Integration tests for logbook_links (#id cross-references) and related APIs."""

import json

import pytest

from server.db import get_db


def _project_links(project: str) -> list[dict]:
    con = get_db()
    rows = con.execute(
        """SELECT l.source_id, l.target_id FROM logbook_links l
           JOIN logbook_entries e ON l.source_id = e.id
           WHERE e.project = ?""",
        (project,),
    ).fetchall()
    con.close()
    return [{"source_id": r["source_id"], "target_id": r["target_id"]} for r in rows]


@pytest.mark.integration
class TestLogbookLinks:
    """Verify that #id references in entry bodies create logbook_links rows."""

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

        links = _project_links("proj")
        assert any(
            l["source_id"] == id_b and l["target_id"] == id_a for l in links
        ), f"Expected link from {id_b} -> {id_a}, got {links}"

    def test_update_body_updates_links(self, client):
        id_a = self._create(client, "proj", "Entry A")
        id_b = self._create(client, "proj", "Entry B")
        id_c = self._create(client, "proj", "Entry C", body=f"Refs #{id_a}")

        links_before = _project_links("proj")
        assert any(l["source_id"] == id_c and l["target_id"] == id_a for l in links_before)

        client.put(
            f"/api/logbook/proj/entries/{id_c}",
            data=json.dumps({"body": f"Now refs #{id_b} instead"}),
            content_type="application/json",
        )

        links_after = _project_links("proj")
        assert not any(l["source_id"] == id_c and l["target_id"] == id_a for l in links_after)
        assert any(l["source_id"] == id_c and l["target_id"] == id_b for l in links_after)

    def test_self_reference_ignored(self, client):
        id_a = self._create(client, "proj", "Self ref", body="See #999999")
        client.put(
            f"/api/logbook/proj/entries/{id_a}",
            data=json.dumps({"body": f"Self ref #{id_a}"}),
            content_type="application/json",
        )
        links = _project_links("proj")
        assert not any(l["source_id"] == id_a and l["target_id"] == id_a for l in links)

    def test_no_refs_no_links(self, client):
        self._create(client, "proj", "No refs", body="Plain body text")
        assert _project_links("proj") == []

    def test_delete_entry_removes_links(self, client):
        id_a = self._create(client, "proj", "Parent")
        id_b = self._create(client, "proj", "Child", body=f"Refs #{id_a}")
        client.delete(f"/api/logbook/proj/entries/{id_b}")
        links = _project_links("proj")
        assert not any(l["source_id"] == id_b for l in links)


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
