"""Integration tests for the mind_map logbook entry type (per-campaign DAG)."""

import json
import pytest


def _create_mind_map(client, project, campaign, **extra):
    """Helper: POST a fresh mind_map and return the created entry dict."""
    payload = {"campaign": campaign}
    payload.update(extra)
    resp = client.post(
        f"/api/logbook/{project}/mind_map",
        data=json.dumps(payload),
        content_type="application/json",
    )
    return resp


def _get_mind_map(client, project, campaign):
    resp = client.get(f"/api/logbook/{project}/mind_map?campaign={campaign}")
    return resp


@pytest.mark.integration
class TestMindMapCrud:
    def test_get_missing_returns_404(self, client):
        resp = _get_mind_map(client, "mmproj", "nope")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "not_found"
        assert data["project"] == "mmproj"
        assert data["campaign"] == "nope"

    def test_get_without_campaign_query_is_400(self, client):
        resp = client.get("/api/logbook/mmproj/mind_map")
        assert resp.status_code == 400

    def test_create_minimal_default_graph(self, client):
        resp = _create_mind_map(client, "mmproj", "v1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["campaign"] == "v1"
        assert data["id"] >= 1
        got = _get_mind_map(client, "mmproj", "v1").get_json()
        assert got["entry_type"] == "mind_map"
        assert got["campaign"] == "v1"
        gj = json.loads(got["graph_json"])
        assert gj == {"version": 1, "nodes": [], "edges": []}

    def test_create_with_inline_graph(self, client):
        graph = {
            "version": 1,
            "nodes": [
                {"id": "a", "title": "Task A", "status": "active",
                 "summary": "first node", "description": "details"},
                {"id": "b", "title": "Task B", "status": "planned"},
            ],
            "edges": [
                {"id": "e1", "from": "a", "to": "b", "kind": "success",
                 "label": "after A passes"},
            ],
        }
        resp = _create_mind_map(
            client, "mmproj", "alpha",
            title="Alpha mind map", body="prose",
            graph_json=graph, campaign_goal="Land MCPv2 integration end-to-end.",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "alpha").get_json()
        assert got["title"] == "Alpha mind map"
        assert got["campaign_goal"] == "Land MCPv2 integration end-to-end."
        gj = json.loads(got["graph_json"])
        assert {n["id"] for n in gj["nodes"]} == {"a", "b"}
        # Optional fields are preserved verbatim.
        a = next(n for n in gj["nodes"] if n["id"] == "a")
        assert a["summary"] == "first node"
        assert a["description"] == "details"
        # Edge kind survives the round-trip.
        assert gj["edges"][0]["kind"] == "success"

    def test_create_without_campaign_is_400(self, client):
        resp = client.post(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({"title": "no campaign"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_singleton_enforced(self, client):
        first = _create_mind_map(client, "mmproj", "solo")
        assert first.status_code == 200
        first_id = first.get_json()["id"]
        dup = _create_mind_map(client, "mmproj", "solo")
        assert dup.status_code == 409
        body = dup.get_json()
        assert body["existing_id"] == first_id

    def test_list_mind_maps(self, client):
        _create_mind_map(client, "mmproj", "c1", title="One")
        _create_mind_map(client, "mmproj", "c2", title="Two")
        rows = client.get("/api/logbook/mmproj/mind_maps").get_json()
        assert isinstance(rows, list)
        camps = {r["campaign"] for r in rows}
        assert {"c1", "c2"}.issubset(camps)
        # Each row carries the canonical fields.
        first = rows[0]
        assert "entry_id" in first and "title" in first and "edited_at" in first


@pytest.mark.integration
class TestMindMapValidator:
    def test_unknown_node_status_rejected(self, client):
        bad = {"version": 1, "nodes": [{"id": "a", "title": "T", "status": "weird"}], "edges": []}
        resp = _create_mind_map(client, "mmproj", "v", graph_json=bad)
        assert resp.status_code == 400
        assert "status" in resp.get_json()["error"]

    def test_missing_title_rejected(self, client):
        bad = {"version": 1, "nodes": [{"id": "a", "status": "active"}], "edges": []}
        resp = _create_mind_map(client, "mmproj", "v2", graph_json=bad)
        assert resp.status_code == 400
        assert "title" in resp.get_json()["error"]

    def test_duplicate_node_id_rejected(self, client):
        bad = {
            "version": 1,
            "nodes": [
                {"id": "a", "title": "First", "status": "active"},
                {"id": "a", "title": "Second", "status": "planned"},
            ],
            "edges": [],
        }
        resp = _create_mind_map(client, "mmproj", "v3", graph_json=bad)
        assert resp.status_code == 400
        assert "duplicate" in resp.get_json()["error"]

    def test_edge_to_unknown_node_rejected(self, client):
        bad = {
            "version": 1,
            "nodes": [{"id": "a", "title": "Only", "status": "active"}],
            "edges": [{"id": "e1", "from": "a", "to": "ghost", "kind": "default"}],
        }
        resp = _create_mind_map(client, "mmproj", "v4", graph_json=bad)
        assert resp.status_code == 400
        assert "to" in resp.get_json()["error"] or "node id" in resp.get_json()["error"]

    def test_self_loop_rejected(self, client):
        bad = {
            "version": 1,
            "nodes": [{"id": "a", "title": "A", "status": "active"}],
            "edges": [{"id": "e1", "from": "a", "to": "a", "kind": "default"}],
        }
        resp = _create_mind_map(client, "mmproj", "v5", graph_json=bad)
        assert resp.status_code == 400
        assert "itself" in resp.get_json()["error"]

    def test_unknown_edge_kind_rejected(self, client):
        bad = {
            "version": 1,
            "nodes": [
                {"id": "a", "title": "A", "status": "active"},
                {"id": "b", "title": "B", "status": "planned"},
            ],
            "edges": [{"id": "e1", "from": "a", "to": "b", "kind": "ascending"}],
        }
        resp = _create_mind_map(client, "mmproj", "v6", graph_json=bad)
        assert resp.status_code == 400

    def test_blocker_and_verification_kinds_accepted(self, client):
        graph = {
            "version": 1,
            "nodes": [
                {"id": "feat", "title": "feature", "status": "active"},
                {"id": "dep", "title": "blocking dep", "status": "blocked"},
                {"id": "ci", "title": "passing CI", "status": "done"},
            ],
            "edges": [
                {"id": "e-block", "from": "dep", "to": "feat", "kind": "blocker"},
                {"id": "e-verify", "from": "ci", "to": "feat", "kind": "verification"},
            ],
        }
        resp = _create_mind_map(client, "mmproj", "kinds", graph_json=graph)
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "kinds").get_json()
        gj = json.loads(got["graph_json"])
        kinds = {e["id"]: e["kind"] for e in gj["edges"]}
        assert kinds == {"e-block": "blocker", "e-verify": "verification"}


@pytest.mark.integration
class TestMindMapUpdate:
    def test_put_replaces_graph(self, client):
        _create_mind_map(client, "mmproj", "putc")
        new_graph = {
            "version": 1,
            "nodes": [{"id": "x", "title": "Whole graph", "status": "active"}],
            "edges": [],
        }
        resp = client.put(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({"campaign": "putc", "graph_json": new_graph}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "putc").get_json()
        gj = json.loads(got["graph_json"])
        assert [n["id"] for n in gj["nodes"]] == ["x"]

    def test_put_missing_campaign_is_400(self, client):
        resp = client.put(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_put_unknown_campaign_is_404(self, client):
        resp = client.put(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({"campaign": "ghostcamp"}),
            content_type="application/json",
        )
        assert resp.status_code == 404


@pytest.mark.integration
class TestMindMapPatch:
    def _seed(self, client, campaign):
        graph = {
            "version": 1,
            "nodes": [
                {"id": "a", "title": "A", "status": "active"},
                {"id": "b", "title": "B", "status": "planned"},
            ],
            "edges": [
                {"id": "e1", "from": "a", "to": "b", "kind": "default"},
            ],
        }
        _create_mind_map(client, "mmproj", campaign, graph_json=graph)

    def test_set_status_op(self, client):
        self._seed(client, "patch1")
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "patch1",
                "ops": [{"op": "set_status", "id": "a", "status": "done"}],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "patch1").get_json()
        gj = json.loads(got["graph_json"])
        a = next(n for n in gj["nodes"] if n["id"] == "a")
        assert a["status"] == "done"

    def test_add_node_and_edge_ops(self, client):
        self._seed(client, "patch2")
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "patch2",
                "ops": [
                    {"op": "add_node", "node": {
                        "id": "c", "title": "Branch C", "status": "active",
                        "summary": "new exploration",
                    }},
                    {"op": "add_edge", "edge": {
                        "id": "e2", "from": "b", "to": "c", "kind": "branch",
                        "label": "hypothesis",
                    }},
                ],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "patch2").get_json()
        gj = json.loads(got["graph_json"])
        ids = [n["id"] for n in gj["nodes"]]
        assert "c" in ids
        assert any(e["id"] == "e2" and e["kind"] == "branch" for e in gj["edges"])

    def test_remove_node_cascades_edges(self, client):
        self._seed(client, "patch3")
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "patch3",
                "ops": [{"op": "remove_node", "id": "a"}],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "patch3").get_json()
        gj = json.loads(got["graph_json"])
        assert [n["id"] for n in gj["nodes"]] == ["b"]
        assert gj["edges"] == []  # cascading remove

    def test_update_node_patch(self, client):
        self._seed(client, "patch4")
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "patch4",
                "ops": [{
                    "op": "update_node", "id": "a",
                    "patch": {"title": "A renamed", "summary": "added"},
                }],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "patch4").get_json()
        gj = json.loads(got["graph_json"])
        a = next(n for n in gj["nodes"] if n["id"] == "a")
        assert a["title"] == "A renamed"
        assert a["summary"] == "added"

    def test_patch_unknown_node_atomic(self, client):
        self._seed(client, "patch5")
        before = _get_mind_map(client, "mmproj", "patch5").get_json()["graph_json"]
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "patch5",
                "ops": [
                    {"op": "set_status", "id": "a", "status": "done"},
                    {"op": "set_status", "id": "ghost", "status": "done"},
                ],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 400
        # Atomic: the first valid op must NOT have persisted.
        after = _get_mind_map(client, "mmproj", "patch5").get_json()["graph_json"]
        assert before == after

    def test_patch_empty_ops_is_400(self, client):
        self._seed(client, "patch6")
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({"campaign": "patch6", "ops": []}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_patch_unknown_campaign_is_404(self, client):
        resp = client.patch(
            "/api/logbook/mmproj/mind_map",
            data=json.dumps({
                "campaign": "doesnt-exist",
                "ops": [{"op": "set_status", "id": "a", "status": "done"}],
            }),
            content_type="application/json",
        )
        assert resp.status_code == 404


@pytest.mark.integration
class TestMindMapEditorIntegration:
    """Mind_map + the generic entries endpoint and type filter."""

    def test_list_filter_mind_map_type(self, client):
        client.post(
            "/api/logbook/mmproj/entries",
            data=json.dumps({"title": "regular", "body": "n"}),
            content_type="application/json",
        )
        _create_mind_map(client, "mmproj", "filt", title="Mind map filt")
        resp = client.get("/api/logbook/mmproj/entries?type=mind_map")
        entries = resp.get_json()
        assert len(entries) == 1
        assert entries[0]["entry_type"] == "mind_map"

    def test_create_via_generic_entries_endpoint(self, client):
        resp = client.post(
            "/api/logbook/mmproj/entries",
            data=json.dumps({
                "title": "Made via /entries",
                "entry_type": "mind_map",
                "campaign": "generic",
                "graph_json": {"version": 1, "nodes": [], "edges": []},
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "generic").get_json()
        assert got["entry_type"] == "mind_map"

    def test_graph_json_rejected_for_note(self, client):
        # First create a note.
        r = client.post(
            "/api/logbook/mmproj/entries",
            data=json.dumps({"title": "Just a note", "body": "n"}),
            content_type="application/json",
        )
        nid = r.get_json()["id"]
        bad = client.put(
            f"/api/logbook/mmproj/entries/{nid}",
            data=json.dumps({"graph_json": {"version": 1, "nodes": [], "edges": []}}),
            content_type="application/json",
        )
        assert bad.status_code == 400


@pytest.mark.integration
class TestConvertCampaignBoardToMindMap:
    def test_convert_seeds_from_board(self, client):
        # Create a legacy board first.
        client.post(
            "/api/logbook/mmproj/entries",
            data=json.dumps({
                "title": "Legacy board",
                "body": "## Setup\n\nLegacy notes",
                "entry_type": "campaign_board",
                "campaign": "legacy",
                "campaign_goal": "Legacy goal",
            }),
            content_type="application/json",
        )
        resp = client.post(
            "/api/logbook/mmproj/mind_map/from_campaign_board",
            data=json.dumps({"campaign": "legacy"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        got = _get_mind_map(client, "mmproj", "legacy").get_json()
        assert got["entry_type"] == "mind_map"
        assert "Legacy notes" in got["body"]
        assert got["campaign_goal"] == "Legacy goal"
        # The original board still exists.
        board = client.get(
            "/api/logbook/mmproj/campaign_board?campaign=legacy"
        ).get_json()
        assert board["entry_type"] == "campaign_board"

    def test_convert_existing_returns_409(self, client):
        client.post(
            "/api/logbook/mmproj/entries",
            data=json.dumps({
                "title": "B",
                "entry_type": "campaign_board",
                "campaign": "twice",
            }),
            content_type="application/json",
        )
        first = client.post(
            "/api/logbook/mmproj/mind_map/from_campaign_board",
            data=json.dumps({"campaign": "twice"}),
            content_type="application/json",
        )
        assert first.status_code == 200
        again = client.post(
            "/api/logbook/mmproj/mind_map/from_campaign_board",
            data=json.dumps({"campaign": "twice"}),
            content_type="application/json",
        )
        assert again.status_code == 409
        assert again.get_json()["status"] == "exists"

    def test_convert_without_board_returns_404(self, client):
        resp = client.post(
            "/api/logbook/mmproj/mind_map/from_campaign_board",
            data=json.dumps({"campaign": "no-board"}),
            content_type="application/json",
        )
        assert resp.status_code == 404
