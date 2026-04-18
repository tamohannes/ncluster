"""Tests for the in-process MCP wiring.

`mcp_init()` is the lean boot path the MCP stdio process uses to bring up the
shared SQLite DB and the cheap housekeeping threads — but explicitly NOT the
single-writer threads (backups, mounts, WDS snapshots, the progress scraper)
nor the cluster poller. Those stay gunicorn-only so the two processes don't
race on external state.

The follower poller is responsible for opportunistically starting the poller
in the MCP process when gunicorn is unreachable; that behaviour lives in
`tests/unit/test_mcp_follower.py`.
"""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest


def _record_loop_calls(monkeypatch):
    """Replace each long-running loop function with a no-op recorder.

    Globally monkey-patching ``threading.Thread.start`` would break Python's
    own thread bookkeeping (and pytest's). Patching the loop targets directly
    is enough: when a thread is spawned with a no-op target it starts, runs
    the no-op, and exits cleanly — no deadlock risk.
    """
    calls = []

    def _make_recorder(label):
        def _rec(*_a, **_kw):
            calls.append(label)
        return _rec

    targets = [
        ("server.ssh", "ssh_pool_gc_loop"),
        ("server.config", "cache_gc_loop"),
        ("server.backup", "backup_loop"),
        ("server.mounts", "mount_health_loop"),
        ("server.wds", "wds_snapshot_loop"),
        ("server.poller", "start_poller"),
        ("server.progress_scraper", "start_progress_scraper"),
    ]
    for module_path, attr in targets:
        import importlib
        mod = importlib.import_module(module_path)
        monkeypatch.setattr(mod, attr, _make_recorder(attr))

    # Also intercept the watchdog notify loop in the app module itself.
    import app as app_mod
    monkeypatch.setattr(app_mod, "_watchdog_notify_loop", _make_recorder("_watchdog_notify_loop"))
    return calls


# ─── mcp_init: starts only the allowed threads ─────────────────────────────


def test_mcp_init_starts_only_lean_threads(db_path, monkeypatch):
    """MCP must NOT start backup, mount-health, WDS snapshot, progress scraper,
    poller, or watchdog threads — those would collide with gunicorn."""
    calls = _record_loop_calls(monkeypatch)

    import app
    app.mcp_init()

    # Wait briefly for daemon threads to actually run their (no-op) bodies.
    import time
    time.sleep(0.05)

    assert "ssh_pool_gc_loop" in calls, calls
    assert "cache_gc_loop" in calls, calls

    for forbidden in (
        "backup_loop", "mount_health_loop", "wds_snapshot_loop",
        "start_progress_scraper", "_watchdog_notify_loop", "start_poller",
    ):
        assert forbidden not in calls, (
            f"mcp_init must NOT start {forbidden} (gunicorn-only); got {calls}"
        )


def test_run_init_starts_full_thread_set(db_path, monkeypatch):
    """The gunicorn boot path must still start every long-running daemon."""
    calls = _record_loop_calls(monkeypatch)

    import app
    app._run_init()

    import time
    time.sleep(0.05)

    for required in (
        "ssh_pool_gc_loop", "cache_gc_loop",
        "backup_loop", "mount_health_loop", "wds_snapshot_loop",
        "_watchdog_notify_loop", "start_poller", "start_progress_scraper",
    ):
        assert required in calls, (
            f"gunicorn _run_init must start {required}; got {calls}"
        )


# ─── Flask test client: /api/health is reachable in-process ────────────────


def test_inproc_health_endpoint_returns_ok(client):
    """The Flask test client (the same primitive mcp_server.py uses) must
    return the standard /api/health JSON envelope."""
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert "board_version" in body
    assert "active_requests" in body


# ─── poller lifecycle: stop_poller is cooperative and idempotent ───────────


def test_stop_poller_is_idempotent_when_not_running():
    """Calling stop_poller before any start should be a quick no-op."""
    from server import poller as p

    # Reset module state so a previous test can't leak a thread reference.
    with p._poller_lock:
        p._poller_thread = None
    assert p.stop_poller(timeout=0.1) is True
    assert p.poller_running() is False


def test_start_then_stop_poller_terminates_thread(monkeypatch):
    """A real start_poller / stop_poller cycle must spawn one thread and
    cleanly shut it down within the timeout."""
    from server import poller as p

    # Don't actually poll any cluster — patch run() to block on the stop event
    # so we can verify the cooperative stop signal makes it through.
    def _fake_run(self):
        self._stop.wait()

    monkeypatch.setattr(p.Poller, "run", _fake_run)

    # Start fresh
    with p._poller_lock:
        p._poller_thread = None
    assert not p.poller_running()

    p.start_poller()
    assert p.poller_running()

    # A second start_poller() must NOT spawn a duplicate thread.
    first_thread = p._poller_thread
    p.start_poller()
    assert p._poller_thread is first_thread, "start_poller should be idempotent"

    assert p.stop_poller(timeout=2.0) is True
    assert not p.poller_running()


# ─── upload_logbook_image: test-client multipart shape ─────────────────────


def test_upload_logbook_image_works_through_test_client(client, db_path, tmp_path):
    """upload_logbook_image used to use httpx files=...; ensure the new
    Werkzeug `(BytesIO, filename)` tuple under `data={"file": ...}` reaches
    the route correctly via app.test_client()."""
    from io import BytesIO

    # The smallest valid PNG: 1x1 transparent pixel.
    png_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc\xf8\x0f"
        b"\x00\x00\x01\x01\x00\x05\xfe\x02\xfe\xa3\xf2\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    resp = client.post(
        "/api/logbook/test-proj/images",
        data={"file": (BytesIO(png_bytes), "tiny.png")},
        content_type="multipart/form-data",
    )
    assert resp.status_code in (200, 201), resp.get_data(as_text=True)
    body = resp.get_json()
    assert body.get("status") == "ok"
    # Server may dedupe the filename (tiny_1.png) if it already exists; just
    # confirm it produced a logbook image URL for our project.
    assert body.get("url", "").startswith("/api/logbook/test-proj/images/")
    assert body.get("url", "").endswith(".png")
