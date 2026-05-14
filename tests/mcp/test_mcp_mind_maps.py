"""MCP mind_map tool contract tests — in-process Flask architecture.

Mocks ``mcp_server._api`` (the synchronous Flask test_client wrapper) to
verify each mind_map MCP tool sends the right HTTP call. ``_api_async``
runs the lambda in a worker thread, so the patch on the underlying sync
``_api`` still intercepts every call.

Mirrors the campaign_board MCP tests at
``tests/mcp/test_mcp_logbooks.py::TestCampaignBoardTools``.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    get_mind_map, list_mind_maps, create_mind_map,
    update_mind_map, patch_mind_map, convert_campaign_board_to_mind_map,
    create_logbook_entry, update_logbook_entry,
)


_EMPTY_GRAPH = {"version": 1, "nodes": [], "edges": []}
_SIMPLE_GRAPH = {
    "version": 1,
    "nodes": [
        {"id": "a", "title": "A", "status": "active"},
        {"id": "b", "title": "B", "status": "planned"},
    ],
    "edges": [{"id": "e1", "from": "a", "to": "b", "kind": "default"}],
}


@pytest.mark.mcp
class TestGetMindMap:
    async def test_dispatches_get_with_campaign(self):
        payload = {"id": 9, "entry_type": "mind_map", "campaign": "mcpv2"}
        with patch("mcp_server._api", return_value=payload) as mock:
            result = await get_mind_map("mcp-tools", "mcpv2")
        assert result["id"] == 9
        args, kwargs = mock.call_args
        assert args[0] == "GET"
        assert args[1] == "/api/logbook/mcp-tools/mind_map"
        assert kwargs["query_string"]["campaign"] == "mcpv2"

    async def test_lowercases_and_strips_campaign(self):
        with patch("mcp_server._api", return_value={}) as mock:
            await get_mind_map("hle", "  MPSF  ")
        _, kwargs = mock.call_args
        assert kwargs["query_string"]["campaign"] == "mpsf"


@pytest.mark.mcp
class TestListMindMaps:
    async def test_returns_list(self):
        rows = [
            {"campaign": "mpsf", "entry_id": 9, "title": "Mind map", "edited_at": "x"},
        ]
        with patch("mcp_server._api", return_value=rows) as mock:
            result = await list_mind_maps("hle")
        assert result == rows
        mock.assert_called_once_with("GET", "/api/logbook/hle/mind_maps")

    async def test_non_list_response_falls_back_to_empty(self):
        # Defensive: the MCP wrapper should hand back a list even if the
        # API returns something unexpected (matches the campaign_board tool).
        with patch("mcp_server._api", return_value=None):
            result = await list_mind_maps("hle")
        assert result == []


@pytest.mark.mcp
class TestCreateMindMap:
    async def test_passes_full_payload(self):
        resp = {"status": "ok", "id": 7}
        with patch("mcp_server._api", return_value=resp) as mock:
            result = await create_mind_map(
                "hle", "mpsf",
                title="T", body="B", graph_json=_SIMPLE_GRAPH,
                campaign_goal="Beat baseline.",
            )
        assert result["status"] == "ok"
        args, kwargs = mock.call_args
        assert args[0] == "POST"
        assert args[1] == "/api/logbook/hle/mind_map"
        body = kwargs["json"]
        assert body["campaign"] == "mpsf"
        assert body["title"] == "T"
        assert body["body"] == "B"
        assert body["graph_json"] == _SIMPLE_GRAPH
        assert body["campaign_goal"] == "Beat baseline."

    async def test_omits_unset_optional_fields(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await create_mind_map("hle", "mpsf")
        _, kwargs = mock.call_args
        # Matches create_campaign_board shape: title/graph_json/campaign_goal
        # are omitted when not provided; body has a default of "" and is
        # always forwarded so the route can normalize it consistently.
        assert kwargs["json"] == {"campaign": "mpsf", "body": ""}
        assert "title" not in kwargs["json"]
        assert "graph_json" not in kwargs["json"]
        assert "campaign_goal" not in kwargs["json"]


@pytest.mark.mcp
class TestUpdateMindMap:
    async def test_dispatches_put(self):
        resp = {"status": "ok", "id": 7}
        with patch("mcp_server._api", return_value=resp) as mock:
            result = await update_mind_map(
                "hle", "mpsf", body="new", graph_json=_SIMPLE_GRAPH,
                campaign_goal="updated goal",
            )
        assert result["status"] == "ok"
        args, kwargs = mock.call_args
        assert args[0] == "PUT"
        assert args[1] == "/api/logbook/hle/mind_map"
        body = kwargs["json"]
        assert body["campaign"] == "mpsf"
        assert body["body"] == "new"
        assert body["graph_json"] == _SIMPLE_GRAPH
        assert body["campaign_goal"] == "updated goal"

    async def test_omits_unset_fields(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_mind_map("hle", "mpsf", title="only-title")
        _, kwargs = mock.call_args
        assert kwargs["json"] == {"campaign": "mpsf", "title": "only-title"}


@pytest.mark.mcp
class TestPatchMindMap:
    async def test_dispatches_patch_with_ops(self):
        ops = [
            {"op": "set_status", "id": "a", "status": "done"},
            {"op": "add_node", "node": {"id": "c", "title": "C", "status": "active"}},
        ]
        resp = {"status": "ok", "id": 7, "edited_at": "x"}
        with patch("mcp_server._api", return_value=resp) as mock:
            result = await patch_mind_map("hle", "mpsf", ops=ops)
        assert result["status"] == "ok"
        args, kwargs = mock.call_args
        assert args[0] == "PATCH"
        assert args[1] == "/api/logbook/hle/mind_map"
        assert kwargs["json"]["campaign"] == "mpsf"
        assert kwargs["json"]["ops"] == ops

    async def test_lowercases_campaign(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await patch_mind_map(
                "hle", "  MPSF  ",
                ops=[{"op": "set_status", "id": "a", "status": "done"}],
            )
        _, kwargs = mock.call_args
        assert kwargs["json"]["campaign"] == "mpsf"


@pytest.mark.mcp
class TestConvertCampaignBoardToMindMap:
    async def test_dispatches_post(self):
        resp = {"status": "ok", "id": 12}
        with patch("mcp_server._api", return_value=resp) as mock:
            result = await convert_campaign_board_to_mind_map("hle", "mpsf")
        assert result["status"] == "ok"
        args, kwargs = mock.call_args
        assert args[0] == "POST"
        assert args[1] == "/api/logbook/hle/mind_map/from_campaign_board"
        assert kwargs["json"]["campaign"] == "mpsf"

    async def test_lowercases_campaign(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await convert_campaign_board_to_mind_map("hle", "MPSF")
        _, kwargs = mock.call_args
        assert kwargs["json"]["campaign"] == "mpsf"


@pytest.mark.mcp
class TestGenericEntriesToolsForwardGraphJson:
    """Confirm the generic create/update tools still flow graph_json through."""

    async def test_create_logbook_entry_forwards_graph_json(self):
        with patch("mcp_server._api", return_value={"status": "ok", "id": 1}) as mock:
            await create_logbook_entry(
                "alpha", "T", body="B",
                entry_type="mind_map", campaign="alpha",
                graph_json=_EMPTY_GRAPH,
            )
        _, kwargs = mock.call_args
        assert kwargs["json"]["entry_type"] == "mind_map"
        assert kwargs["json"]["campaign"] == "alpha"
        assert kwargs["json"]["graph_json"] == _EMPTY_GRAPH

    async def test_update_logbook_entry_forwards_graph_json(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_logbook_entry("alpha", 1, graph_json=_SIMPLE_GRAPH)
        _, kwargs = mock.call_args
        assert kwargs["json"] == {"graph_json": _SIMPLE_GRAPH}
