"""MCP project tool contract tests.

Mocks ``mcp_server._api`` (the synchronous Flask test_client wrapper) to
verify each tool sends the right HTTP call to the in-process Flask test
client. ``_api_async`` runs the lambda in a worker thread, so the patch
on the underlying sync ``_api`` still intercepts every call. Mirrors the
logbook test pattern.
"""

import pytest
from unittest.mock import patch

from mcp_server import (
    create_project,
    delete_project,
    list_projects,
    update_project,
)


@pytest.mark.mcp
class TestListProjects:
    async def test_returns_list(self):
        rows = [
            {"name": "alpha", "color": "#abcdef", "emoji": "🧪", "prefixes": [{"prefix": "alpha_"}]},
        ]
        with patch("mcp_server._api", return_value=rows):
            result = await list_projects()
        assert isinstance(result, list)
        assert result[0]["name"] == "alpha"

    async def test_passes_path(self):
        with patch("mcp_server._api", return_value=[]) as mock:
            await list_projects()
        mock.assert_called_once_with("GET", "/api/projects/all")

    async def test_wraps_error_dict(self):
        with patch("mcp_server._api", return_value={"status": "error", "error": "boom"}):
            result = await list_projects()
        assert isinstance(result, list)
        assert result[0]["status"] == "error"


@pytest.mark.mcp
class TestCreateProject:
    async def test_basic_create(self):
        resp = {"status": "ok", "project": {"name": "alpha"}}
        with patch("mcp_server._api", return_value=resp) as mock:
            result = await create_project("alpha", prefixes=["alpha_"])
        assert result["status"] == "ok"
        mock.assert_called_once()
        args, kwargs = mock.call_args
        assert args == ("POST", "/api/projects")
        body = kwargs["json"]
        assert body["name"] == "alpha"
        assert body["prefixes"] == ["alpha_"]
        assert body["campaign_delimiter"] == "_"
        assert body["description"] == ""

    async def test_passes_color_emoji(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await create_project("alpha", prefixes=["a_"], color="#abcdef", emoji="🧪")
        body = mock.call_args.kwargs["json"]
        assert body["color"] == "#abcdef"
        assert body["emoji"] == "🧪"

    async def test_passes_default_campaign(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await create_project("alpha", prefixes=["a_"], default_campaign="forced")
        body = mock.call_args.kwargs["json"]
        assert body["default_campaign"] == "forced"

    async def test_omits_optional_fields_when_none(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await create_project("alpha", prefixes=["a_"])
        body = mock.call_args.kwargs["json"]
        assert "color" not in body
        assert "emoji" not in body
        assert "default_campaign" not in body

    async def test_no_prefixes_defaults_to_empty_list(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await create_project("manual")
        body = mock.call_args.kwargs["json"]
        assert body["prefixes"] == []


@pytest.mark.mcp
class TestUpdateProject:
    async def test_path_includes_name(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_project("alpha", color="#000000")
        args, _ = mock.call_args
        assert args == ("PUT", "/api/projects/alpha")

    async def test_only_provided_fields_sent(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_project("alpha", color="#000000", description="updated")
        body = mock.call_args.kwargs["json"]
        assert body == {"color": "#000000", "description": "updated"}

    async def test_empty_patch_when_all_none(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_project("alpha")
        body = mock.call_args.kwargs["json"]
        assert body == {}

    async def test_passes_prefixes(self):
        with patch("mcp_server._api", return_value={"status": "ok"}) as mock:
            await update_project(
                "alpha",
                prefixes=[{"prefix": "x_", "default_campaign": "forced"}],
            )
        body = mock.call_args.kwargs["json"]
        assert body["prefixes"] == [{"prefix": "x_", "default_campaign": "forced"}]


@pytest.mark.mcp
class TestDeleteProject:
    async def test_path_and_method(self):
        with patch("mcp_server._api", return_value={"status": "ok", "deleted": "alpha"}) as mock:
            result = await delete_project("alpha")
        assert result["status"] == "ok"
        mock.assert_called_once_with("DELETE", "/api/projects/alpha")
