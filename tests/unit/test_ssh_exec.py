"""Unit tests for the simplified OpenSSH transport layer."""

import subprocess

import pytest


@pytest.fixture(autouse=True)
def _reset_circuit_breaker():
    from server.ssh import _cb_failures, _cb_lock

    with _cb_lock:
        _cb_failures.clear()
    yield
    with _cb_lock:
        _cb_failures.clear()


@pytest.mark.unit
def test_ssh_run_invokes_openssh_subprocess(mock_cluster, monkeypatch):
    from server.config import CLUSTERS
    from server.ssh import ssh_run_with_timeout

    captured = {}

    def fake_run(argv, capture_output, text, timeout, check, input=None):
        captured["argv"] = argv
        captured["timeout"] = timeout
        captured["input"] = input
        return subprocess.CompletedProcess(argv, 0, "hello\n", "")

    monkeypatch.setattr("server.ssh.subprocess.run", fake_run)

    out, err = ssh_run_with_timeout(mock_cluster, "echo hello", timeout_sec=7)

    assert out == "hello"
    assert err == ""
    assert captured["argv"][0] == "ssh"
    assert f"{CLUSTERS[mock_cluster]['user']}@{CLUSTERS[mock_cluster]['host']}" in captured["argv"]
    # Single-line commands are embedded in the argv; multiline go via stdin.
    assert captured["argv"][-1].startswith("bash -lc ") or captured["argv"][-1] == "bash -l"
    assert captured["timeout"] == 7


@pytest.mark.unit
def test_ssh_timeout_trips_circuit_breaker(mock_cluster, monkeypatch):
    from server.ssh import _cb_is_open, ssh_run_with_timeout

    def fake_run(*args, input=None, **kwargs):
        raise subprocess.TimeoutExpired("ssh", 5)

    monkeypatch.setattr("server.ssh.subprocess.run", fake_run)

    with pytest.raises(TimeoutError):
        ssh_run_with_timeout(mock_cluster, "squeue -u $USER", timeout_sec=5)

    assert _cb_is_open(mock_cluster)
