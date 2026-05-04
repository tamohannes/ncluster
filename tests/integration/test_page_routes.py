"""Smoke tests for direct browser routes that serve the app shell."""

import pytest


@pytest.mark.integration
class TestPageRoutes:
    def test_direct_app_routes_serve_index_shell(self, client):
        for path in [
            "/live",
            "/runs",
            "/metrics",
            "/compute",
            "/project/hle",
            "/logbook",
            "/logbook/hle",
            "/logbook/hle/entry/123",
            "/run/eos/deadbeef",
            "/explorer/eos/123/%2Ftmp%2Fout.log",
        ]:
            resp = client.get(path)
            assert resp.status_code == 200, path
            assert b'id="topbar-tabs"' in resp.data
