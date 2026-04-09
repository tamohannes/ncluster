"""Live MCP destructive tests: cancel_job on throwaway job.

Run with: pytest tests/live/test_mcp_live_cancel_cleanup.py -m "live and destructive" -v
"""

import subprocess
import os
import signal

import pytest

from mcp_server import cancel_job
from tests.live.helpers.slurm_fixture import (
    LIVE_CLUSTER,
    submit_throwaway_job, cancel_throwaway_job, wait_for_job_state,
)

pytestmark = [pytest.mark.live, pytest.mark.destructive, pytest.mark.mcp]


def _skip_if_no_cluster():
    if not LIVE_CLUSTER:
        pytest.skip("No live cluster configured")


class TestMcpLiveDestructive:
    def test_cancel_job_via_mcp(self):
        _skip_if_no_cluster()
        job_id = submit_throwaway_job(LIVE_CLUSTER, duration_sec=120)
        try:
            state = wait_for_job_state(LIVE_CLUSTER, job_id, {"RUNNING", "PENDING"}, timeout=60)
            assert state in {"RUNNING", "PENDING"}

            result = cancel_job(LIVE_CLUSTER, job_id)
            assert result.get("status") == "ok"

            final = wait_for_job_state(LIVE_CLUSTER, job_id, {"CANCELLED", "GONE"}, timeout=30)
            assert final in {"CANCELLED", "GONE"}
        finally:
            cancel_throwaway_job(LIVE_CLUSTER, job_id)

    def test_cancel_local_pid_smoke(self):
        proc = subprocess.Popen(["sleep", "300"])
        try:
            result = cancel_job("local", str(proc.pid))
            assert result.get("status") == "ok"
            proc.wait(timeout=5)
        finally:
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
