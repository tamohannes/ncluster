"""Integration tests for /api/settings routes."""

import json
import pytest


@pytest.mark.integration
class TestApiSettingsGet:
    def test_returns_config(self, client, mock_ssh):
        resp = client.get("/api/settings")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "port" in data
        assert "ssh_timeout" in data
        assert "clusters" in data

    def test_contains_cache_fresh(self, client, mock_ssh):
        data = client.get("/api/settings").get_json()
        assert "cache_fresh_sec" in data


@pytest.mark.integration
class TestApiSettingsPost:
    def test_partial_patch(self, client, mock_ssh, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        # write initial config so reload_config can write to it
        import json as _json
        initial = client.get("/api/settings").get_json()
        (tmp_path / "config.json").write_text(_json.dumps(initial))

        resp = client.post("/api/settings",
                           data=json.dumps({"ssh_timeout": 15}),
                           content_type="application/json")
        data = resp.get_json()
        assert data["status"] == "ok"
        assert data["settings"]["ssh_timeout"] == 15

    def test_invalid_body_400(self, client, mock_ssh):
        resp = client.post("/api/settings",
                           data="not json",
                           content_type="application/json")
        assert resp.status_code == 400

    def test_empty_body_400(self, client, mock_ssh):
        resp = client.post("/api/settings",
                           data=json.dumps(None),
                           content_type="application/json")
        assert resp.status_code == 400


@pytest.mark.integration
class TestApiSettingsTriggersWdsSnapshot:
    """Settings changes that affect WDS inputs (team_gpu_allocations,
    ppps, team) must kick off a fresh WDS snapshot in the background so
    the next read of ``wds_history`` reflects the new value without
    waiting up to 15 minutes for the next periodic tick."""

    def _setup(self, client, tmp_path, monkeypatch):
        monkeypatch.setattr("server.config.CONFIG_PATH", str(tmp_path / "config.json"))
        initial = client.get("/api/settings").get_json()
        (tmp_path / "config.json").write_text(json.dumps(initial))

    def _patch_snapshot(self, monkeypatch):
        import threading
        called = threading.Event()

        def _stub():
            called.set()
            return 0

        monkeypatch.setattr("server.wds.compute_wds_snapshot", _stub)
        return called

    def test_team_gpu_allocations_change_triggers_snapshot(
        self, client, mock_ssh, tmp_path, monkeypatch,
    ):
        self._setup(client, tmp_path, monkeypatch)
        called = self._patch_snapshot(monkeypatch)

        resp = client.post(
            "/api/settings",
            data=json.dumps({"team_gpu_allocations": {"dfw": 512}}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert called.wait(timeout=2.0), (
            "POST /api/settings with team_gpu_allocations should trigger "
            "compute_wds_snapshot in a background thread."
        )

    def test_ppps_change_triggers_snapshot(
        self, client, mock_ssh, tmp_path, monkeypatch,
    ):
        self._setup(client, tmp_path, monkeypatch)
        called = self._patch_snapshot(monkeypatch)

        resp = client.post(
            "/api/settings",
            data=json.dumps({"ppps": {"foo": "12345"}}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        assert called.wait(timeout=2.0)

    def test_unrelated_change_does_not_trigger_snapshot(
        self, client, mock_ssh, tmp_path, monkeypatch,
    ):
        self._setup(client, tmp_path, monkeypatch)
        called = self._patch_snapshot(monkeypatch)

        resp = client.post(
            "/api/settings",
            data=json.dumps({"ssh_timeout": 12}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        # Allow plenty of time — the snapshot must NOT fire for a setting
        # that doesn't influence the WDS calculation.
        assert not called.wait(timeout=0.5), (
            "ssh_timeout doesn't influence WDS — snapshot should not run."
        )
