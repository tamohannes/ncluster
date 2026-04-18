"""Tests for the follower poller in mcp_server.

The follower watches gunicorn's `/api/health` and starts the local cluster
poller after `_FOLLOWER_FAIL_THRESHOLD` consecutive failed probes. As soon
as the leader answers again, the follower stops the local poller. This file
exercises that decision logic via the extracted `_follower_step` helper so
no real timers, threads, or network calls are needed.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def mcp_module():
    """Import mcp_server. Side-effect-free at import time — `_start_follower`
    only fires from the `__main__` block, so test imports don't spin up a
    real probe thread."""
    import mcp_server as mod
    return mod


def _wire_step(mcp_module, monkeypatch, *, healthy, running):
    """Stub the leader probe + poller state for a single _follower_step call.

    Returns a `calls` dict counting how often start_poller / stop_poller
    were invoked during the call.
    """
    calls = {"start": 0, "stop": 0}

    monkeypatch.setattr(mcp_module, "_probe_leader", lambda: healthy)
    monkeypatch.setattr(mcp_module, "poller_running", lambda: running)

    def _start():
        calls["start"] += 1

    def _stop(timeout=5.0):
        calls["stop"] += 1
        return True

    monkeypatch.setattr(mcp_module, "start_poller", _start)
    monkeypatch.setattr(mcp_module, "stop_poller", _stop)
    return calls


def test_healthy_probe_resets_failure_counter(mcp_module, monkeypatch):
    calls = _wire_step(mcp_module, monkeypatch, healthy=True, running=False)

    next_count = mcp_module._follower_step(consecutive_failures=2)

    assert next_count == 0
    assert calls == {"start": 0, "stop": 0}


def test_healthy_probe_stops_local_poller_if_running(mcp_module, monkeypatch):
    calls = _wire_step(mcp_module, monkeypatch, healthy=True, running=True)

    next_count = mcp_module._follower_step(consecutive_failures=5)

    assert next_count == 0
    assert calls == {"start": 0, "stop": 1}, "follower must hand polling back"


def test_failed_probe_below_threshold_only_increments(mcp_module, monkeypatch):
    threshold = mcp_module._FOLLOWER_FAIL_THRESHOLD
    calls = _wire_step(mcp_module, monkeypatch, healthy=False, running=False)

    next_count = mcp_module._follower_step(consecutive_failures=threshold - 2)

    assert next_count == threshold - 1
    assert calls == {"start": 0, "stop": 0}, (
        "must not take over polling until the threshold is reached"
    )


def test_failed_probe_at_threshold_starts_local_poller(mcp_module, monkeypatch):
    threshold = mcp_module._FOLLOWER_FAIL_THRESHOLD
    calls = _wire_step(mcp_module, monkeypatch, healthy=False, running=False)

    next_count = mcp_module._follower_step(consecutive_failures=threshold - 1)

    assert next_count == threshold
    assert calls == {"start": 1, "stop": 0}, "must take over polling on the Nth failure"


def test_failed_probe_does_not_double_start_when_already_running(mcp_module, monkeypatch):
    threshold = mcp_module._FOLLOWER_FAIL_THRESHOLD
    calls = _wire_step(mcp_module, monkeypatch, healthy=False, running=True)

    next_count = mcp_module._follower_step(consecutive_failures=threshold + 5)

    assert next_count == threshold + 6
    assert calls == {"start": 0, "stop": 0}, (
        "must not call start_poller again while the local poller is already running"
    )


def test_full_recovery_cycle(mcp_module, monkeypatch):
    """End-to-end: leader goes down -> threshold reached -> takeover ->
    leader recovers -> follower stops local poller -> counter resets."""
    threshold = mcp_module._FOLLOWER_FAIL_THRESHOLD
    state = {"healthy": False, "running": False, "calls": {"start": 0, "stop": 0}}

    monkeypatch.setattr(mcp_module, "_probe_leader", lambda: state["healthy"])
    monkeypatch.setattr(mcp_module, "poller_running", lambda: state["running"])

    def _start():
        state["running"] = True
        state["calls"]["start"] += 1

    def _stop(timeout=5.0):
        state["running"] = False
        state["calls"]["stop"] += 1
        return True

    monkeypatch.setattr(mcp_module, "start_poller", _start)
    monkeypatch.setattr(mcp_module, "stop_poller", _stop)

    consec = 0
    # Leader is down: probe N times — only the Nth flips the switch.
    for i in range(threshold):
        consec = mcp_module._follower_step(consec)
    assert state["running"] is True
    assert state["calls"] == {"start": 1, "stop": 0}
    assert consec == threshold

    # Leader stays down a few more probes — must NOT spawn duplicates.
    for _ in range(3):
        consec = mcp_module._follower_step(consec)
    assert state["calls"] == {"start": 1, "stop": 0}
    assert consec == threshold + 3

    # Leader recovers — follower stops local poller and resets counter.
    state["healthy"] = True
    consec = mcp_module._follower_step(consec)
    assert consec == 0
    assert state["running"] is False
    assert state["calls"] == {"start": 1, "stop": 1}

    # Subsequent healthy probes are no-ops.
    for _ in range(3):
        consec = mcp_module._follower_step(consec)
    assert state["calls"] == {"start": 1, "stop": 1}


# ─── _probe_leader: contract with urllib ───────────────────────────────────


def test_probe_leader_returns_true_on_2xx(mcp_module, monkeypatch):
    """A 200 OK from /api/health = healthy."""

    class _FakeResp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda req, timeout=None: _FakeResp(),
    )
    assert mcp_module._probe_leader() is True


def test_probe_leader_returns_false_on_connection_error(mcp_module, monkeypatch):
    """When gunicorn is not listening, urllib raises ConnectionRefusedError."""
    def _boom(req, timeout=None):
        raise ConnectionRefusedError("no listener on :7272")

    monkeypatch.setattr("urllib.request.urlopen", _boom)
    assert mcp_module._probe_leader() is False


def test_probe_leader_returns_false_on_timeout(mcp_module, monkeypatch):
    """A wedged worker that doesn't answer = unhealthy."""
    import socket

    def _timeout(req, timeout=None):
        raise socket.timeout("probe timed out")

    monkeypatch.setattr("urllib.request.urlopen", _timeout)
    assert mcp_module._probe_leader() is False
