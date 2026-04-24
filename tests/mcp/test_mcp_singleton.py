"""Tests for the MCP singleton lock with new-wins eviction.

We've observed up to 3 ``mcp_server.py`` children alive concurrently
when Cursor's MCP client respawns without reaping the previous one.
Each orphan held DB connections, a follower poller thread, and an
SSH semaphore budget. The singleton lock makes a new MCP process
the rightful owner: when it starts and finds the lock held, it asks
the prior holder to exit (SIGTERM, then SIGKILL after a grace window)
before taking over.
"""

import os
import subprocess
import sys
import time

import pytest

from mcp_server import _acquire_singleton_lock


@pytest.mark.mcp
class TestSingletonLock:
    def test_first_holder_acquires_immediately(self, tmp_path):
        lock_path = str(tmp_path / "mcp.lock")

        fd = _acquire_singleton_lock(lock_path=lock_path, grace_sec=2.0)
        assert fd is not None
        assert os.path.isfile(lock_path)
        with open(lock_path) as f:
            recorded_pid = int(f.read().strip())
        assert recorded_pid == os.getpid()
        os.close(fd)

    def test_second_call_evicts_orphan_holder(self, tmp_path):
        lock_path = str(tmp_path / "mcp.lock")

        # Simulate a prior MCP process: a Python child that takes the
        # lock then sleeps. This child responds to SIGTERM by exiting
        # (Python's default SIGTERM behaviour kills the interpreter).
        holder_script = (
            "import fcntl, os, sys, time\n"
            f"p = {lock_path!r}\n"
            "fd = os.open(p, os.O_CREAT|os.O_RDWR, 0o600)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\n"
            "os.ftruncate(fd, 0)\n"
            "os.write(fd, str(os.getpid()).encode())\n"
            "sys.stdout.write('locked\\n'); sys.stdout.flush()\n"
            "time.sleep(120)\n"
        )
        holder = subprocess.Popen(
            [sys.executable, "-c", holder_script],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            # Wait for the holder to take the lock (it prints 'locked'
            # right after flock).
            line = holder.stdout.readline()
            assert line.strip() == "locked", f"holder did not start: {line!r}"

            # Now we try to acquire from this test process — should
            # SIGTERM the holder and take over within the grace window.
            t0 = time.monotonic()
            fd = _acquire_singleton_lock(lock_path=lock_path, grace_sec=3.0)
            elapsed = time.monotonic() - t0

            assert fd is not None
            assert elapsed < 3.0, f"eviction took {elapsed:.2f}s"

            # The holder should now be dead (default SIGTERM handler
            # exits the interpreter).
            holder.wait(timeout=5)
            assert holder.returncode != 0  # killed by signal

            # The lock file now records OUR pid.
            with open(lock_path) as f:
                assert int(f.read().strip()) == os.getpid()
            os.close(fd)
        finally:
            if holder.poll() is None:
                holder.kill()
                holder.wait(timeout=5)

    def test_returns_none_when_holder_refuses_to_die(self, tmp_path, monkeypatch):
        """If the holder PID exists but the kill() returns ESRCH (no such
        process — e.g. PID was reused) and the lock file is somehow
        intact, we still try to acquire and stamp our own PID. This test
        pins that we don't crash on the unusual paths.
        """
        lock_path = str(tmp_path / "mcp.lock")

        # Pretend the prior holder was PID 999999 (almost certainly does
        # not exist on this system). Write that PID + take an exclusive
        # lock with a fd we control so the acquire path sees the lock as
        # held.
        with open(lock_path, "w") as f:
            f.write("999999\n")

        import fcntl as _fcntl
        held_fd = os.open(lock_path, os.O_RDWR)
        _fcntl.flock(held_fd, _fcntl.LOCK_EX)

        try:
            # Holder is "fake" — kill(999999) raises ProcessLookupError,
            # the loop falls through, the grace window expires, we try
            # the SIGKILL path which also raises ProcessLookupError, and
            # then we try a final flock which still fails. End result:
            # _acquire returns None (we couldn't take over) — that's the
            # intended fail-safe.
            result = _acquire_singleton_lock(lock_path=lock_path, grace_sec=0.5)
            assert result is None
        finally:
            _fcntl.flock(held_fd, _fcntl.LOCK_UN)
            os.close(held_fd)
