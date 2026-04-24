"""MCP server for clausius — in-process Flask, async-safe tool dispatch.

The MCP server runs the same Flask app as gunicorn but inside its own
process. Tool calls go through Werkzeug's WSGI loop via `app.test_client()`,
so there is no HTTP dependency on the gunicorn UI service: when gunicorn
crashes or restarts, the MCP server keeps serving every tool against the
same SQLite database.

Concurrency model
-----------------
FastMCP runs sync tool handlers directly on its asyncio event loop (see
``mcp.server.fastmcp.utilities.func_metadata.call_fn_with_arg_validation``).
Several of our routes (live cluster polling, ``where_to_submit``,
``run_script``) trigger SSH work that can block for many seconds when a
cluster flaps. If a sync handler ran on the event loop, that block would
prevent FastMCP from reading Cursor's stdio heartbeats, and Cursor would
close the transport — observed once already, ~2 s after an SSH circuit
breaker opened.

We therefore do two things:

  * Every tool is ``async def`` and routes through ``_api_async`` /
    ``_api_text_async``, which off-load the synchronous ``test_client``
    call to a worker thread via ``anyio.to_thread.run_sync``. The event
    loop stays responsive while the SSH work runs in the background.
  * Every off-threaded call is wrapped in ``asyncio.wait_for`` with a
    wall-clock timeout. On timeout we return a structured error so the
    agent sees the failure clearly instead of an opaque "Connection
    closed". The underlying request keeps running until it finishes —
    Python can't cancel a thread — but the per-cluster SSH semaphore
    inside ``server.ssh`` (max 2 in-flight per cluster, 8 globally)
    bounds how badly this can pile up.

Both processes share:

  - SQLite (WAL mode, multi-process safe)
  - The `server.ssh` module (each process has its own bounded semaphores)
  - The on-disk config and mounts

Single-writer responsibilities (backups, mount remounts, WDS snapshots,
the progress scraper) stay in gunicorn — see `_run_init` in app.py.

Follower poller
---------------
A small daemon thread probes `http://localhost:7272/api/health` every
``_FOLLOWER_INTERVAL_SEC``. After ``_FOLLOWER_FAIL_THRESHOLD`` consecutive
failures we assume gunicorn is down and start the cluster poller in this
process so MCP keeps reporting fresh data. The first successful probe
hands polling back to gunicorn.
"""

import asyncio
import builtins
import fcntl
import functools
import logging
import os
import signal
import sys
import threading
import time
from io import BytesIO
from typing import Optional

import anyio.to_thread
from mcp.server.fastmcp import FastMCP

# Heavy imports (Flask app + every route + ssh + db) are deferred to the
# first tool call via ``_ensure_initialized``. Cursor's MCP client gives
# the child a few seconds after spawn before it considers the connection
# stuck; doing the full init inline at module import added enough latency
# that 'MCP took too long to start, marking failed' was a real risk.
# Server.poller is light (no Flask), so we keep its imports eager — we
# need ``poller_running`` for ``health_check`` and ``mcp_self_check``.
from server.poller import poller_running, start_poller, stop_poller

mcp = FastMCP("clausius")

_init_lock = threading.Lock()
_initialized = False
_app = None
_client = None


def _ensure_initialized() -> None:
    """Lazily import ``app`` and create the Flask test client.

    Called from inside ``_api`` / ``_api_text`` (i.e. on a worker thread
    via anyio.to_thread.run_sync) so the heavy import doesn't block
    module load — and therefore doesn't block Cursor's MCP startup
    handshake. Idempotent and thread-safe.
    """
    global _app, _client, _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        from app import app as _imported_app, mcp_init as _imported_mcp_init
        _imported_mcp_init()
        _app = _imported_app
        _client = _app.test_client()
        _initialized = True

log = logging.getLogger("server.mcp")


# ── Per-PID log tagging ──────────────────────────────────────────────────────
#
# When more than one MCP process is alive (parent agent restart, palette
# reload), they all write to the shared ``data/clausius.log`` and the
# only way to tell their lines apart was to correlate timestamps with
# ``ps``. Tag every ``server.mcp`` log line with ``[mcp:pid=NNNNN]`` so
# the source process is obvious in the file.

class _PidTagFilter(logging.Filter):
    """Inject ``pid=N`` into every record on the server.mcp logger."""
    _PID = os.getpid()

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = f"[mcp:pid={self._PID}] {record.msg}"
        return True


log.addFilter(_PidTagFilter())


# ── Tool-handler exception isolation ─────────────────────────────────────────
#
# A bare exception from a tool handler is a worst-case failure mode for the
# MCP transport: FastMCP turns the exception into a JSON-RPC error response,
# but if the exception happens after we've started writing or the message
# framing is mid-flight, the stdio stream can desync and Cursor closes the
# connection. We therefore wrap every ``@mcp.tool()``-decorated function in
# an ``_isolate_tool`` decorator that catches any Exception and converts it
# to a structured error envelope. The wrapping is hooked via a tiny shim
# around ``mcp.tool()`` so individual tool definitions stay untouched.

# Per-tool counters: {tool_name: {"calls": int, "errors": int,
# "samples_ms": list[float] (bounded)}}. Surfaced via ``mcp_self_check``
# so an agent can spot a single misbehaving tool without grepping logs.
_TOOL_STATS_LOCK = threading.Lock()
_TOOL_STATS: dict = {}
_TOOL_STATS_SAMPLE_CAP = 100


def _record_tool_call(tool_name: str, elapsed_ms: float, errored: bool) -> None:
    with _TOOL_STATS_LOCK:
        rec = _TOOL_STATS.setdefault(tool_name, {
            "calls": 0, "errors": 0, "samples_ms": [],
        })
        rec["calls"] += 1
        if errored:
            rec["errors"] += 1
        samples = rec["samples_ms"]
        samples.append(elapsed_ms)
        # Bound the sample buffer so a long-lived process doesn't grow
        # without bound. Reservoir-style drop the oldest.
        if len(samples) > _TOOL_STATS_SAMPLE_CAP:
            del samples[0:len(samples) - _TOOL_STATS_SAMPLE_CAP]


def _isolate_tool(fn):
    """Wrap an async tool fn so any Exception becomes a structured error,
    and record per-tool call/error counters + latency samples."""
    name = getattr(fn, "__name__", "?")

    @functools.wraps(fn)
    async def _wrapper(*args, **kwargs):
        t0 = time.monotonic()
        errored = False
        try:
            return await fn(*args, **kwargs)
        except Exception as exc:
            errored = True
            log.exception("tool %s raised: %s", name, exc)
            return {
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}",
            }
        finally:
            _record_tool_call(name, (time.monotonic() - t0) * 1000.0, errored)
    return _wrapper


_original_mcp_tool = mcp.tool


def _isolated_mcp_tool(*decorator_args, **decorator_kwargs):
    """Drop-in for ``mcp.tool()`` that auto-isolates the decorated fn."""
    def _outer(fn):
        return _original_mcp_tool(*decorator_args, **decorator_kwargs)(_isolate_tool(fn))
    return _outer


# Replace mcp.tool BEFORE any tool definitions execute.
mcp.tool = _isolated_mcp_tool


# ── Stdout discipline ────────────────────────────────────────────────────────
#
# The MCP stdio transport owns sys.stdout for JSON-RPC framing. ANY
# stray ``print()`` call from a tool handler (or library it imports)
# can corrupt the framing and close the stream. Redirect builtins.print
# so accidental writes go to stderr instead. We only install this in
# the __main__ path so test imports of ``mcp_server`` don't change
# global print behaviour out from under pytest.

_original_print = builtins.print


def _safe_print(*args, **kwargs):
    """Drop-in for ``print()`` that forces output to stderr."""
    kwargs["file"] = sys.stderr
    _original_print(*args, **kwargs)


def _install_stdout_safety() -> None:
    """Replace ``builtins.print`` with a stderr-safe variant."""
    builtins.print = _safe_print


def _restore_stdout() -> None:
    """Undo :func:`_install_stdout_safety` — used in tests."""
    builtins.print = _original_print


# ── Timeout / off-thread policy ──────────────────────────────────────────────
#
# Default per-call wall-clock budget. Long enough for legitimate multi-cluster
# work (``where_to_submit`` SSHes to up to 8 clusters; observed 12 s on a
# normal day), short enough that we can return a structured error before any
# upstream MCP transport health check times out.
_DEFAULT_TIMEOUT_SEC = float(os.environ.get("CLAUSIUS_MCP_TOOL_TIMEOUT_SEC", "25"))

# Logbook image uploads buffer the full multipart payload before returning,
# so they need more headroom than the default tool budget.
_UPLOAD_TIMEOUT_SEC = float(os.environ.get("CLAUSIUS_MCP_UPLOAD_TIMEOUT_SEC", "60"))

# anyio's default thread limiter (40 tokens) is left untouched — the real
# cap on inflight cluster work is the per-cluster SSH semaphore inside
# ``server.ssh`` (max 2 per cluster, 8 globally). Non-SSH calls are fast
# and won't pile up. Setting ``current_default_thread_limiter`` would have
# to happen inside the running event loop, which we don't have here at
# import time.


# ── Idle-timeout watchdog ────────────────────────────────────────────────────
#
# Even with the singleton lock, an MCP process can end up idle indefinitely
# (Cursor crashes without sending a clean shutdown, the parent goes to
# sleep, the user closes the IDE without exiting). Without a watchdog
# the process holds DB connections, threads, and the singleton lock
# forever. The watchdog tracks the last successful tool-call timestamp
# and ``os._exit(0)``s the process if it's been idle longer than
# ``CLAUSIUS_MCP_IDLE_SHUTDOWN_SEC`` (default 30 minutes).

_IDLE_SHUTDOWN_SEC = float(
    os.environ.get("CLAUSIUS_MCP_IDLE_SHUTDOWN_SEC", "1800")
)
_activity_lock = threading.Lock()
_last_activity_ts: float = time.monotonic()


def _record_activity() -> None:
    """Mark NOW as the last successful MCP activity. Called from every
    ``_api_async`` / ``_api_text_async`` entry so the idle watchdog
    measures real usage, not just liveness probes."""
    global _last_activity_ts
    with _activity_lock:
        _last_activity_ts = time.monotonic()


def _idle_shutdown_step(idle_threshold_sec: float) -> bool:
    """One pass of the idle-watchdog decision. Returns True iff the
    process has been idle longer than ``idle_threshold_sec`` and should
    exit. Pulled out of the loop so the decision is unit-testable
    without timers or threads."""
    with _activity_lock:
        idle = time.monotonic() - _last_activity_ts
    return idle > idle_threshold_sec


def _idle_watchdog_loop() -> None:
    """Daemon loop that periodically checks the idle threshold and
    exits the process via ``os._exit(0)`` if exceeded."""
    interval = min(60.0, max(1.0, _IDLE_SHUTDOWN_SEC / 4))
    while True:
        time.sleep(interval)
        try:
            if _idle_shutdown_step(_IDLE_SHUTDOWN_SEC):
                log.warning(
                    "idle: no MCP activity for >%.0fs; exiting cleanly",
                    _IDLE_SHUTDOWN_SEC,
                )
                os._exit(0)
        except Exception:
            log.exception("idle watchdog tick failed")


def _start_idle_watchdog() -> threading.Thread:
    t = threading.Thread(target=_idle_watchdog_loop, daemon=True,
                         name="mcp-idle-watchdog")
    t.start()
    return t


# ── In-process API helpers ───────────────────────────────────────────────────

def _api(method, path, **kwargs):
    """Invoke a Flask route through the test client and return parsed JSON.

    Synchronous: must only be called from a worker thread (see
    ``_api_async`` for the event-loop-safe wrapper). Errors are surfaced as
    ``{"status": "error", "error": "..."}`` so callers don't need exception
    handling.
    """
    _ensure_initialized()
    try:
        resp = _client.open(path=path, method=method, **kwargs)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}
    if resp.status_code >= 500:
        # Mirror the legacy proxy's behaviour: still try to surface the
        # JSON error body if the route produced one.
        if resp.is_json:
            return resp.get_json()
        return {"status": "error", "error": f"HTTP {resp.status_code}: {resp.get_data(as_text=True)[:200]}"}
    if resp.is_json:
        return resp.get_json()
    return resp.get_data(as_text=True)


def _api_text(method, path, **kwargs):
    """Like ``_api`` but returns the raw response body as text."""
    _ensure_initialized()
    try:
        resp = _client.open(path=path, method=method, **kwargs)
    except Exception as exc:
        return f"Error: {exc}"
    return resp.get_data(as_text=True)


async def _api_async(method, path, *, timeout: Optional[float] = None, **kwargs):
    """Off-thread ``_api`` with a wall-clock ``timeout``.

    Lets the FastMCP event loop keep reading stdin while the underlying
    Flask route blocks on SSH or disk I/O. On timeout we return a
    structured error rather than letting the call hang the transport.
    """
    if timeout is None:
        timeout = _DEFAULT_TIMEOUT_SEC
    _record_activity()
    try:
        return await asyncio.wait_for(
            anyio.to_thread.run_sync(lambda: _api(method, path, **kwargs)),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        log.warning("MCP API call timed out after %.1fs: %s %s", timeout, method, path)
        return {
            "status": "error",
            "error": f"in-process API call timed out after {timeout:.0f}s; cluster may be slow or unreachable",
        }
    except Exception as exc:
        log.exception("MCP API call failed unexpectedly: %s %s", method, path)
        return {"status": "error", "error": str(exc)}


async def _api_text_async(method, path, *, timeout: Optional[float] = None, **kwargs):
    """Off-thread ``_api_text`` with a wall-clock ``timeout``."""
    if timeout is None:
        timeout = _DEFAULT_TIMEOUT_SEC
    _record_activity()
    try:
        return await asyncio.wait_for(
            anyio.to_thread.run_sync(lambda: _api_text(method, path, **kwargs)),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        log.warning("MCP API text call timed out after %.1fs: %s %s", timeout, method, path)
        return f"Error: in-process API call timed out after {timeout:.0f}s"
    except Exception as exc:
        log.exception("MCP API text call failed unexpectedly: %s %s", method, path)
        return f"Error: {exc}"


# ── Follower poller ──────────────────────────────────────────────────────────

_FOLLOWER_URL = os.environ.get("CLAUSIUS_LEADER_URL", "http://localhost:7272") + "/api/health"
_FOLLOWER_INTERVAL_SEC = float(os.environ.get("CLAUSIUS_FOLLOWER_INTERVAL_SEC", "10"))
_FOLLOWER_FAIL_THRESHOLD = int(os.environ.get("CLAUSIUS_FOLLOWER_FAIL_THRESHOLD", "3"))
_FOLLOWER_PROBE_TIMEOUT_SEC = float(os.environ.get("CLAUSIUS_FOLLOWER_PROBE_TIMEOUT_SEC", "2"))


def _probe_leader():
    """Return True iff the gunicorn leader's /api/health responds with 2xx."""
    import urllib.request

    try:
        req = urllib.request.Request(_FOLLOWER_URL, method="GET")
        with urllib.request.urlopen(req, timeout=_FOLLOWER_PROBE_TIMEOUT_SEC) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def _follower_step(consecutive_failures):
    """Apply one follower decision based on a single liveness probe.

    Returns the updated `consecutive_failures` counter. Pulled out of the
    loop so the leadership transition can be unit-tested without timers
    or threads.
    """
    healthy = _probe_leader()
    if healthy:
        if poller_running():
            log.info("follower: leader healthy, stopping local poller")
            stop_poller()
        return 0

    consecutive_failures += 1
    if (
        consecutive_failures >= _FOLLOWER_FAIL_THRESHOLD
        and not poller_running()
    ):
        log.warning(
            "follower: leader unreachable for %d probes, taking over polling",
            consecutive_failures,
        )
        start_poller()
    return consecutive_failures


def _follower_loop():
    """Watch gunicorn liveness and start/stop our poller accordingly.

    We only take over polling once the leader has been silent for
    ``_FOLLOWER_FAIL_THRESHOLD`` consecutive probes (~30 s by default) so
    a transient gunicorn restart doesn't trigger a costly handover. As soon
    as the leader answers we step back — duplicate polling during the
    transition is harmless because every poll write is idempotent.
    """
    consecutive_failures = 0
    while True:
        try:
            consecutive_failures = _follower_step(consecutive_failures)
        except Exception:
            log.exception("follower tick failed")
        time.sleep(_FOLLOWER_INTERVAL_SEC)


def _start_follower():
    """Spawn the follower-poller daemon.

    Started from the ``__main__`` block below so that importing this module
    (e.g. from tests) is side-effect-free — no probe thread, no real socket.
    """
    t = threading.Thread(target=_follower_loop, daemon=True, name="mcp-follower")
    t.start()
    return t


# ── helpers ───────────────────────────────────────────────────────────────────

_JOB_FIELDS = [
    "jobid", "name", "state", "reason", "elapsed", "timelimit",
    "nodes", "gres", "partition", "submitted", "account",
    "started_local", "ended_local",
    "progress", "depends_on", "dependents", "dep_details",
    "project", "project_color", "project_emoji", "campaign",
    "_pinned", "exit_code", "crash_detected", "est_start",
]


def _slim_job(cluster: str, job: dict) -> dict:
    out = {"cluster": cluster}
    for k in _JOB_FIELDS:
        v = job.get(k)
        if v is not None and v != "" and v != []:
            out[k] = v
    return out


# ── tools ─────────────────────────────────────────────────────────────────────

_PROCESS_START_TS = time.monotonic()


@mcp.tool()
async def health_check() -> dict:
    """Quick health check. Returns ok if the MCP server is running."""
    svc = await _api_async("GET", "/api/health")
    if isinstance(svc, dict) and svc.get("status") == "ok":
        return {
            "status": "ok",
            "service": "in-process",
            "board_version": svc.get("board_version"),
            "follower_active": poller_running(),
        }
    return {"status": "ok", "service": "degraded", "note": "in-process API responded with an error"}


@mcp.tool()
async def mcp_self_check() -> dict:
    """Diagnostic snapshot of THIS MCP process.

    Lets agents distinguish 'MCP slow' from 'MCP dead' from 'cluster
    slow' before reaching for the gunicorn log. Returns:

      - ``pid``: this MCP process's PID
      - ``uptime_sec``: seconds since this process started
      - ``last_activity_sec_ago``: seconds since the most recent tool
        call (relevant to the idle-shutdown watchdog)
      - ``follower_active``: True iff this process is currently running
        the cluster poller (gunicorn is down)
      - ``leader_url`` / ``leader_reachable``: gunicorn liveness probe
      - ``tool_stats``: per-tool {calls, errors, p50_ms, p99_ms} for
        the latest sample window (capped at 100 samples per tool)
    """

    def _percentile(values, p):
        if not values:
            return None
        s = sorted(values)
        k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
        return round(s[k], 2)

    with _TOOL_STATS_LOCK:
        snapshot = {}
        for name, rec in _TOOL_STATS.items():
            samples = list(rec["samples_ms"])
            snapshot[name] = {
                "calls": rec["calls"],
                "errors": rec["errors"],
                "p50_ms": _percentile(samples, 50),
                "p99_ms": _percentile(samples, 99),
                "samples_in_window": len(samples),
            }

    with _activity_lock:
        idle_sec = time.monotonic() - _last_activity_ts

    return {
        "pid": os.getpid(),
        "uptime_sec": round(time.monotonic() - _PROCESS_START_TS, 1),
        "last_activity_sec_ago": round(idle_sec, 1),
        "follower_active": poller_running(),
        "leader_url": _FOLLOWER_URL,
        "leader_reachable": _probe_leader(),
        "tool_stats": snapshot,
    }


@mcp.tool()
async def list_jobs(cluster: Optional[str] = None, project: Optional[str] = None) -> list[dict]:
    """List active jobs across all clusters, or filtered by cluster/project.

    Returns compact job records with state, progress, dependencies, and est_start.
    Includes both live squeue jobs and board-pinned terminal jobs.
    """
    if cluster:
        data = await _api_async("GET", f"/api/jobs/{cluster}")
        if data.get("status") == "error":
            return [{"error": data.get("error", "Unknown error")}]
        jobs = [_slim_job(cluster, j) for j in data.get("jobs", [])]
    else:
        snapshot = await _api_async("GET", "/api/jobs")
        jobs = []
        if isinstance(snapshot, dict):
            for cname, cdata in snapshot.items():
                if not isinstance(cdata, dict):
                    continue
                for j in cdata.get("jobs", []):
                    jobs.append(_slim_job(cname, j))

    if project:
        jobs = [j for j in jobs if j.get("project") == project]
    return jobs


@mcp.tool()
async def list_log_files(cluster: str, job_id: str) -> dict:
    """Discover available log and result files for a job.

    Returns lists of direct log files and explorable directories.
    """
    return await _api_async("GET", f"/api/log_files/{cluster}/{job_id}", query_string={"force": "1"})


@mcp.tool()
async def get_job_log(
    cluster: str,
    job_id: str,
    path: Optional[str] = None,
    lines: int = 150,
) -> str:
    """Read the tail of a log file for a job.

    If path is omitted, the best file is auto-selected. Returns raw log text.
    """
    params = {"lines": str(lines)}
    if path:
        params["path"] = path
    data = await _api_async("GET", f"/api/log/{cluster}/{job_id}", query_string=params)
    if isinstance(data, dict):
        if data.get("status") == "error":
            return f"Error: {data.get('error', 'Unknown error')}"
        return data.get("content", "(empty)")
    return str(data)


@mcp.tool()
async def get_job_stats(cluster: str, job_id: str) -> dict:
    """Get resource stats for a running job (CPU, memory, GPU utilisation)."""
    return await _api_async("GET", f"/api/stats/{cluster}/{job_id}")


@mcp.tool()
async def get_run_info(cluster: str, root_job_id: str) -> dict:
    """Get detailed run info: batch script, scontrol, env vars, conda state, and associated jobs."""
    return await _api_async("GET", f"/api/run_info/{cluster}/{root_job_id}")


@mcp.tool()
async def get_history(
    cluster: Optional[str] = None,
    project: Optional[str] = None,
    campaign: Optional[str] = None,
    state: Optional[str] = None,
    partition: Optional[str] = None,
    account: Optional[str] = None,
    search: Optional[str] = None,
    days: Optional[int] = None,
    limit: int = 50,
) -> list[dict]:
    """Get past job history, filterable by cluster, project, campaign, state, partition, account, search, and recent days.

    String filters accept a single value. ``state`` and ``campaign`` also accept comma-separated values.
    """
    params = {"limit": str(limit)}
    if cluster:
        params["cluster"] = cluster
    if project:
        params["project"] = project
    if campaign:
        params["campaign"] = campaign
    if state:
        params["state"] = state
    if partition:
        params["partition"] = partition
    if account:
        params["account"] = account
    if search:
        params["q"] = search
    if days is not None:
        params["days"] = str(days)
    data = await _api_async("GET", "/api/history", query_string=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
async def cancel_job(cluster: str, job_id: str) -> dict:
    """Cancel a running or pending job. Destructive — only when user explicitly asks."""
    return await _api_async("POST", f"/api/cancel/{cluster}/{job_id}")


@mcp.tool()
async def cancel_jobs(cluster: str, job_ids: list[str]) -> dict:
    """Cancel multiple jobs on a cluster. Destructive — only when user explicitly asks."""
    return await _api_async("POST", f"/api/cancel_jobs/{cluster}", json={"job_ids": job_ids})


@mcp.tool()
async def run_script(
    cluster: str,
    script: str,
    interpreter: str = "python3",
    timeout: int = 120,
) -> dict:
    """Run a script on a cluster via SSH and return its output.

    Args:
        cluster: Target cluster name.
        script: Full source code.
        interpreter: "python3" (default), "bash", or "sh".
        timeout: Max seconds (1-300, default 120).
    """
    # Give the wrapper enough wall-clock slack on top of the user-requested
    # script timeout so a script that genuinely runs for ``timeout`` seconds
    # doesn't trip our outer ``asyncio.wait_for`` first.
    wrapper_timeout = max(_DEFAULT_TIMEOUT_SEC, float(timeout) + 15.0)
    return await _api_async(
        "POST", f"/api/run_script/{cluster}",
        timeout=wrapper_timeout,
        json={
            "script": script,
            "interpreter": interpreter,
            "timeout": timeout,
        },
    )


# ── cluster info ──────────────────────────────────────────────────────────────

@mcp.tool()
async def get_partitions(cluster: Optional[str] = None) -> dict:
    """Get Slurm partition details: state, time limits, priority, nodes, GPUs, queue depth.

    Returns per-partition data including idle_nodes, pending_jobs, gpus_per_node,
    priority_tier, preempt_mode, and access restrictions.
    """
    if cluster:
        return await _api_async("GET", f"/api/partitions/{cluster}")
    return await _api_async("GET", "/api/partitions")


@mcp.tool()
async def where_to_submit(
    nodes: int = 1,
    gpus_per_node: int = 8,
    gpu_type: str = "",
) -> dict:
    """Rank clusters by WDS score (0-100) for job submission.

    Combines PPP allocations, fairshare, team usage, queue pressure, and
    cluster occupancy. Higher WDS = better. >=75 good, 50-74 moderate, <50 unlikely.

    Args:
        nodes: GPU nodes needed (default 1).
        gpus_per_node: GPUs per node (default 8).
        gpu_type: Prefer clusters with this GPU (e.g. "H100", "B200").
    """
    return await _api_async("POST", "/api/where_to_submit", json={
        "nodes": nodes,
        "gpus_per_node": gpus_per_node,
        "gpu_type": gpu_type,
    })


# ── mount & board tools ──────────────────────────────────────────────────────

@mcp.tool()
async def get_mounts() -> dict:
    """Get SSHFS mount status for all clusters."""
    return await _api_async("GET", "/api/mounts")


@mcp.tool()
async def mount_cluster(cluster: str, action: str = "mount") -> dict:
    """Mount or unmount a cluster's remote filesystem via SSHFS."""
    if action not in ("mount", "unmount"):
        return {"status": "error", "error": "action must be 'mount' or 'unmount'"}
    return await _api_async("POST", f"/api/mount/{action}/{cluster}")


@mcp.tool()
async def clear_failed(cluster: str) -> dict:
    """Dismiss all failed/cancelled/timeout job pins from a cluster's board."""
    return await _api_async("POST", f"/api/clear_failed/{cluster}")


@mcp.tool()
async def clear_completed(cluster: str) -> dict:
    """Dismiss all completed job pins from a cluster's board."""
    return await _api_async("POST", f"/api/clear_completed/{cluster}")


# ── project tools ─────────────────────────────────────────────────────────────

@mcp.tool()
async def list_projects() -> list[dict]:
    """List every registered project with its color, emoji, prefixes, and metadata.

    Each entry contains: name, color, emoji, prefixes (list of {prefix,
    default_campaign?}), campaign_delimiter, description, created_at, updated_at.
    """
    data = await _api_async("GET", "/api/projects/all")
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
async def create_project(
    name: str,
    prefixes: Optional[list] = None,
    color: Optional[str] = None,
    emoji: Optional[str] = None,
    default_campaign: Optional[str] = None,
    campaign_delimiter: str = "_",
    description: str = "",
) -> dict:
    """Create a new project so jobs whose names start with one of its prefixes
    are auto-assigned to it.

    Args:
      name: lowercase project key (letters, digits, hyphens). Becomes the
        sidebar label and the value stored in ``job_history.project``.
      prefixes: list of prefix specs. Each item is either a string like
        ``"artsiv_"`` or a dict like ``{"prefix": "hle_chem", "default_campaign": "chem"}``.
        Pass an empty list / omit to create a "manual" project that doesn't
        auto-route any jobs.
      color: hex color (e.g. ``"#9effbb"``). Auto-picked from the palette if omitted.
      emoji: single emoji char. Auto-picked if omitted.
      default_campaign: shortcut applied to a single-prefix project.
      campaign_delimiter: char used to split the run-details remainder when
        deriving the campaign (default ``"_"``).
      description: free-form note about the project.
    """
    payload = {
        "name": name,
        "prefixes": prefixes if prefixes is not None else [],
        "campaign_delimiter": campaign_delimiter or "_",
        "description": description or "",
    }
    if color:
        payload["color"] = color
    if emoji:
        payload["emoji"] = emoji
    if default_campaign:
        payload["default_campaign"] = default_campaign
    return await _api_async("POST", "/api/projects", json=payload)


@mcp.tool()
async def update_project(
    name: str,
    color: Optional[str] = None,
    emoji: Optional[str] = None,
    prefixes: Optional[list] = None,
    default_campaign: Optional[str] = None,
    campaign_delimiter: Optional[str] = None,
    description: Optional[str] = None,
) -> dict:
    """Update a registered project's metadata. Pass only the fields you want
    to change; ``None`` is treated as "leave unchanged".

    Note: replacing ``prefixes`` does not retroactively re-extract the
    ``project`` field on existing job rows. Run a manual SQL update or call
    a future re-extract helper if you need that.
    """
    payload = {}
    if color is not None:
        payload["color"] = color
    if emoji is not None:
        payload["emoji"] = emoji
    if prefixes is not None:
        payload["prefixes"] = prefixes
    if default_campaign is not None:
        payload["default_campaign"] = default_campaign
    if campaign_delimiter is not None:
        payload["campaign_delimiter"] = campaign_delimiter
    if description is not None:
        payload["description"] = description
    return await _api_async("PUT", f"/api/projects/{name}", json=payload)


@mcp.tool()
async def delete_project(name: str) -> dict:
    """Delete a registered project. Destructive — does not touch job history,
    but jobs that referenced this project name will stop appearing in the
    project sidebar (their stored ``project`` string is left as-is)."""
    return await _api_async("DELETE", f"/api/projects/{name}")


# ── v4 config management tools ────────────────────────────────────────────────

# Clusters -------------------------------------------------------------------

@mcp.tool()
async def list_cluster_configs() -> list[dict]:
    """List every registered cluster with SSH, GPU, mount, and allocation details.

    Returns full cluster records (not the live-job data — use ``list_jobs`` for that).
    """
    data = await _api_async("GET", "/api/clusters")
    return data if isinstance(data, list) else []


@mcp.tool()
async def get_cluster_config(name: str) -> dict:
    """Read the full configuration record for one cluster."""
    return await _api_async("GET", f"/api/clusters/{name}")


@mcp.tool()
async def add_cluster_config(
    name: str,
    host: str,
    gpu_type: str = "",
    gpus_per_node: int = 0,
    gpu_mem_gb: int = 0,
    port: int = 22,
    ssh_user: str = "",
    ssh_key: str = "",
    account: str = "",
    aihub_name: str = "",
    data_host: str = "",
    mount_paths: Optional[list[str]] = None,
    mount_aliases: Optional[dict] = None,
    team_gpu_alloc: str = "",
    enabled: bool = True,
) -> dict:
    """Register a new cluster. ``name`` and ``host`` are required; everything
    else has sensible defaults. SSH user/key fall back to the bootstrap values.
    """
    payload = {
        "name": name, "host": host, "gpu_type": gpu_type,
        "gpus_per_node": gpus_per_node, "gpu_mem_gb": gpu_mem_gb,
        "port": port, "ssh_user": ssh_user, "ssh_key": ssh_key,
        "account": account, "aihub_name": aihub_name,
        "data_host": data_host, "team_gpu_alloc": team_gpu_alloc,
        "enabled": enabled,
    }
    if mount_paths is not None:
        payload["mount_paths"] = mount_paths
    if mount_aliases is not None:
        payload["mount_aliases"] = mount_aliases
    return await _api_async("POST", "/api/clusters", json=payload)


@mcp.tool()
async def update_cluster_config(name: str, **fields) -> dict:
    """Update one or more fields on an existing cluster.

    Pass only the fields you want to change (e.g. ``gpu_type="B200"``).
    Accepted fields: host, data_host, port, ssh_user, ssh_key, account,
    gpu_type, gpu_mem_gb, gpus_per_node, aihub_name, mount_paths,
    mount_aliases, team_gpu_alloc, enabled, position.
    """
    return await _api_async("PUT", f"/api/clusters/{name}", json=fields)


@mcp.tool()
async def remove_cluster_config(name: str) -> dict:
    """Remove a registered cluster. Destructive — does not delete historical
    jobs; they stay queryable in History. Only when explicitly asked."""
    return await _api_async("DELETE", f"/api/clusters/{name}")


# Team members ---------------------------------------------------------------

@mcp.tool()
async def list_team_members() -> list[dict]:
    """List every team member (username, display_name, email)."""
    data = await _api_async("GET", "/api/team/members")
    return data if isinstance(data, list) else []


@mcp.tool()
async def add_team_member(
    username: str,
    display_name: str = "",
    email: str = "",
) -> dict:
    """Add a team member by username."""
    return await _api_async("POST", "/api/team/members", json={
        "username": username, "display_name": display_name, "email": email,
    })


@mcp.tool()
async def remove_team_member(username: str) -> dict:
    """Remove a team member. Does not delete their historical job data."""
    return await _api_async("DELETE", f"/api/team/members/{username}")


# PPP accounts ---------------------------------------------------------------

@mcp.tool()
async def list_ppp_accounts() -> list[dict]:
    """List every PPP (Performance Project) account with name and id."""
    data = await _api_async("GET", "/api/team/ppps")
    return data if isinstance(data, list) else []


@mcp.tool()
async def add_ppp_account(
    name: str,
    ppp_id: str = "",
    description: str = "",
) -> dict:
    """Add a PPP account. ``ppp_id`` is the numeric project id used by AI Hub."""
    return await _api_async("POST", "/api/team/ppps", json={
        "name": name, "ppp_id": ppp_id, "description": description,
    })


@mcp.tool()
async def update_ppp_account(
    name: str,
    ppp_id: Optional[str] = None,
    description: Optional[str] = None,
) -> dict:
    """Update PPP account fields. Pass only the fields to change."""
    payload = {}
    if ppp_id is not None:
        payload["ppp_id"] = ppp_id
    if description is not None:
        payload["description"] = description
    return await _api_async("PUT", f"/api/team/ppps/{name}", json=payload)


@mcp.tool()
async def remove_ppp_account(name: str) -> dict:
    """Remove a PPP account. Only when explicitly asked."""
    return await _api_async("DELETE", f"/api/team/ppps/{name}")


# Path bases -----------------------------------------------------------------

@mcp.tool()
async def list_path_bases(kind: Optional[str] = None) -> list[dict]:
    """List registered path entries.

    ``kind`` is one of: ``log_search``, ``nemo_run``, ``mount_lustre_prefix``.
    Omit to list all kinds.
    """
    if kind:
        data = await _api_async("GET", f"/api/paths/{kind}")
    else:
        results = []
        for k in ("log_search", "nemo_run", "mount_lustre_prefix"):
            d = await _api_async("GET", f"/api/paths/{k}")
            if isinstance(d, list):
                results.extend(d)
        return results
    return data if isinstance(data, list) else []


@mcp.tool()
async def add_path_base(kind: str, path: str) -> dict:
    """Add a path entry. ``kind`` is ``log_search``, ``nemo_run``, or
    ``mount_lustre_prefix``. ``path`` may contain ``$USER``."""
    return await _api_async("POST", f"/api/paths/{kind}", json={"path": path})


@mcp.tool()
async def remove_path_base(kind: str, path: str) -> dict:
    """Remove a path entry by kind and exact path."""
    return await _api_async("DELETE", f"/api/paths/{kind}", json={"path": path})


# Process filters ------------------------------------------------------------

@mcp.tool()
async def list_process_filters(mode: Optional[str] = None) -> list[dict]:
    """List local-process filter patterns.

    ``mode`` is ``include`` or ``exclude``. Omit to list both.
    """
    if mode:
        data = await _api_async("GET", f"/api/process_filters/{mode}")
    else:
        results = []
        for m in ("include", "exclude"):
            d = await _api_async("GET", f"/api/process_filters/{m}")
            if isinstance(d, list):
                results.extend(d)
        return results
    return data if isinstance(data, list) else []


@mcp.tool()
async def add_process_filter(mode: str, pattern: str) -> dict:
    """Add an include/exclude pattern for local process scanning."""
    return await _api_async("POST", f"/api/process_filters/{mode}", json={"pattern": pattern})


@mcp.tool()
async def remove_process_filter(mode: str, pattern: str) -> dict:
    """Remove a process filter pattern."""
    return await _api_async("DELETE", f"/api/process_filters/{mode}", json={"pattern": pattern})


# App settings ---------------------------------------------------------------

@mcp.tool()
async def get_app_setting(key: str) -> dict:
    """Read one app setting. Returns value, default, description, and source."""
    return await _api_async("GET", f"/api/settings/{key}")


@mcp.tool()
async def set_app_setting(key: str, value) -> dict:
    """Set one app setting. Value is type-checked against the registered schema.

    Well-known keys: ssh_timeout, cache_fresh_sec, stats_interval_sec,
    backup_interval_hours, backup_max_keep, team_name,
    aihub_opensearch_url, dashboard_url, aihub_cache_ttl_sec,
    wds_snapshot_interval_sec, sdk_ingest_token.
    """
    return await _api_async("PUT", f"/api/settings/{key}", json={"value": value})


@mcp.tool()
async def list_app_settings() -> dict:
    """List every app setting with its current value, default, and source."""
    data = await _api_async("GET", "/api/settings")
    if isinstance(data, dict):
        return data
    return {}


# ── logbook tools ─────────────────────────────────────────────────────────────

@mcp.tool()
async def list_logbook_entries(
    project: str,
    query: Optional[str] = None,
    sort: str = "edited_at",
    limit: int = 50,
    entry_type: Optional[str] = None,
) -> list[dict]:
    """List logbook entries for a project, optionally filtered by BM25 search.

    Returns: id, project, title, body_preview, entry_type, created_at, edited_at.
    Sort: "edited_at" (default), "created_at", "title".
    """
    params = {"sort": sort, "limit": str(limit)}
    if query:
        params["q"] = query
    if entry_type:
        params["type"] = entry_type
    data = await _api_async("GET", f"/api/logbook/{project}/entries", query_string=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and data.get("status") == "error":
        return [data]
    return []


@mcp.tool()
async def read_logbook_entry(project: str, entry_id: int) -> dict:
    """Read a single logbook entry with full markdown body."""
    return await _api_async("GET", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
async def bulk_read_logbooks(
    project: Optional[str] = None,
    entry_type: Optional[str] = None,
    sort: str = "created_at",
    limit_per_project: int = 200,
    max_entries: int = 1000,
) -> dict:
    """Bulk-read full logbook entries for one or all projects in a single call.

    Returns full entries with markdown bodies. Use for comprehensive context gathering.
    """
    body = {"sort": sort, "limit_per_project": limit_per_project, "max_entries": max_entries}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return await _api_async("POST", "/api/logbook/bulk_read", json=body)


@mcp.tool()
async def find_logbook_entries(
    pattern: str,
    project: Optional[str] = None,
    field: str = "title",
    regex: bool = False,
    entry_type: Optional[str] = None,
    full_body: bool = True,
    limit: int = 50,
) -> dict:
    """Find logbook entries by substring or regex match on title or body.

    Args:
        pattern: Search string (substring by default, regex if regex=True).
        field: "title" (default), "body", or "both".
        regex: Treat pattern as Python regex.
        full_body: Return full body (default True) or preview only.
    """
    body = {"pattern": pattern, "field": field, "regex": regex, "full_body": full_body, "limit": limit}
    if project:
        body["project"] = project
    if entry_type:
        body["entry_type"] = entry_type
    return await _api_async("POST", "/api/logbook/find", json=body)


@mcp.tool()
async def create_logbook_entry(project: str, title: str, body: str = "", entry_type: str = "note") -> dict:
    """Create a new logbook entry. Supports markdown, #N cross-refs, @run-name refs, images.

    See the project-logbook workspace rule for full formatting guidelines.
    entry_type: "note" (results/findings) or "plan" (plans/designs).
    """
    return await _api_async("POST", f"/api/logbook/{project}/entries", json={
        "title": title,
        "body": body,
        "entry_type": entry_type,
    })


@mcp.tool()
async def update_logbook_entry(
    project: str,
    entry_id: int,
    title: Optional[str] = None,
    body: Optional[str] = None,
    entry_type: Optional[str] = None,
    pinned: Optional[bool] = None,
    new_project: Optional[str] = None,
) -> dict:
    """Update any subset of a logbook entry's mutable attributes. Bumps edited_at.

    Args:
        project: The entry's current project (used to look it up).
        entry_id: Globally unique entry id.
        title: New title string.
        body: New markdown body. Re-parses #N references and rebuilds the link table.
        entry_type: "note" or "plan". Invalid values are silently ignored.
        pinned: True to pin, False to unpin. Pinned entries sort to the top
            of list_logbook_entries.
        new_project: Move the entry to a different project. Entry IDs are
            globally unique so cross-project #N references keep working.
            Pass the bare project name (e.g. "hle"), not a URL.

    All fields except project/entry_id are optional; pass only the ones you
    want to change. Returns {"status": "ok", "id", "edited_at", "project"?}.
    """
    payload = {}
    if title is not None:
        payload["title"] = title
    if body is not None:
        payload["body"] = body
    if entry_type is not None:
        payload["entry_type"] = entry_type
    if pinned is not None:
        payload["pinned"] = pinned
    if new_project is not None:
        payload["new_project"] = new_project
    return await _api_async("PUT", f"/api/logbook/{project}/entries/{entry_id}", json=payload)


@mcp.tool()
async def delete_logbook_entry(project: str, entry_id: int) -> dict:
    """Delete a logbook entry. Destructive."""
    return await _api_async("DELETE", f"/api/logbook/{project}/entries/{entry_id}")


@mcp.tool()
async def upload_logbook_image(project: str, image_path: str) -> dict:
    """Upload a local image/HTML file to a project's logbook image store.

    Supported: .png, .jpg, .jpeg, .gif, .webp, .svg, .html, .htm
    See project-logbook workspace rule for embedding and HTML figure requirements.
    """
    if not os.path.isfile(image_path):
        return {"status": "error", "error": f"File not found: {image_path}"}
    _record_activity()
    filename = os.path.basename(image_path)
    with open(image_path, "rb") as f:
        data = f.read()

    def _do_upload():
        _ensure_initialized()
        try:
            # Werkzeug's test client accepts a `(BytesIO, filename)` tuple
            # under the multipart field name; this matches what httpx's
            # `files=` did against the live HTTP endpoint.
            resp = _client.post(
                f"/api/logbook/{project}/images",
                data={"file": (BytesIO(data), filename)},
                content_type="multipart/form-data",
            )
            if resp.is_json:
                return resp.get_json()
            return {"status": "error", "error": f"HTTP {resp.status_code}: {resp.get_data(as_text=True)[:200]}"}
        except Exception as exc:
            return {"status": "error", "error": str(exc)}

    try:
        return await asyncio.wait_for(
            anyio.to_thread.run_sync(_do_upload),
            timeout=_UPLOAD_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        log.warning("logbook image upload timed out after %.1fs: %s", _UPLOAD_TIMEOUT_SEC, image_path)
        return {"status": "error", "error": f"image upload timed out after {_UPLOAD_TIMEOUT_SEC:.0f}s"}
    except Exception as exc:
        log.exception("logbook image upload failed unexpectedly: %s", image_path)
        return {"status": "error", "error": str(exc)}


# ── resources ────────────────────────────────────────────────────────────────

@mcp.resource("jobs://summary")
async def jobs_summary() -> str:
    """Quick overview of all clusters: running/pending/failed counts."""
    data = await _api_async("GET", "/api/jobs_summary")
    if isinstance(data, dict) and data.get("status") == "ok":
        return data.get("summary", "")
    return f"Error: {data.get('error', 'Unknown error')}" if isinstance(data, dict) else str(data)


# ── singleton lock ───────────────────────────────────────────────────────────
#
# Cursor's MCP client occasionally spawns a fresh ``mcp_server.py`` child
# without reaping the previous one (tab reload, sleep/wake, MCP restart
# from the command palette, parent agent crash). In production we have
# observed up to 3 mcp_server processes alive simultaneously, each with
# its own DB connections, follower poller thread, and SSH semaphore
# budget. This is wasteful AND the orphan processes make it harder to
# debug "Not connected" errors because nobody can tell which one Cursor
# is actually talking to.
#
# A POSIX file lock on a per-user lock file gives us "only one MCP
# server alive at a time" with new-wins semantics: when a new process
# starts and finds the lock held, it reads the holder's PID, sends
# SIGTERM, waits up to ``_SINGLETON_EVICT_GRACE_SEC`` for graceful exit,
# then SIGKILL if needed. The new process is the one Cursor is currently
# talking to, so it should win.

_SINGLETON_LOCK_PATH = os.path.expanduser(
    os.environ.get("CLAUSIUS_MCP_LOCK_PATH", "~/.clausius/mcp.lock")
)
_SINGLETON_EVICT_GRACE_SEC = float(
    os.environ.get("CLAUSIUS_MCP_EVICT_GRACE_SEC", "5")
)

# Module-level handle so the lock fd stays open for the process lifetime.
_singleton_lock_fd: Optional[int] = None


def _acquire_singleton_lock(
    lock_path: str = _SINGLETON_LOCK_PATH,
    grace_sec: float = _SINGLETON_EVICT_GRACE_SEC,
) -> Optional[int]:
    """Acquire the MCP singleton lock, evicting any prior holder.

    Returns the open file descriptor (caller must keep it alive for the
    process lifetime so the lock stays held), or ``None`` if we couldn't
    get the lock.
    """
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        # Another process holds the lock. Read the holder's PID, ask it
        # to exit, then reclaim.
        try:
            with open(lock_path) as f:
                holder_pid = int(f.read().strip() or "0")
        except (ValueError, OSError):
            holder_pid = 0

        if holder_pid > 0 and holder_pid != os.getpid():
            log.warning("singleton: evicting prior MCP holder pid=%s", holder_pid)
            try:
                os.kill(holder_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except OSError as exc:
                log.warning("singleton: kill(SIGTERM, %s) failed: %s", holder_pid, exc)

        deadline = time.monotonic() + grace_sec
        while time.monotonic() < deadline:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                time.sleep(0.1)
        else:
            # Grace expired — escalate to SIGKILL and try once more.
            if holder_pid > 0 and holder_pid != os.getpid():
                log.warning(
                    "singleton: holder pid=%s did not exit in %.1fs, SIGKILL",
                    holder_pid, grace_sec,
                )
                try:
                    os.kill(holder_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError as exc:
                    log.warning("singleton: kill(SIGKILL, %s) failed: %s", holder_pid, exc)
                time.sleep(0.5)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError) as exc:
                log.error("singleton: could not acquire lock after escalation: %s", exc)
                os.close(fd)
                return None

    # Got the lock. Stamp our PID so the next would-be holder knows who
    # to evict.
    try:
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
    except OSError as exc:
        log.warning("singleton: writing PID to lock file failed: %s", exc)
    return fd


# ── main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _install_stdout_safety()
    _singleton_lock_fd = _acquire_singleton_lock()
    if _singleton_lock_fd is None:
        log.error("singleton: could not acquire MCP lock; exiting cleanly")
        os._exit(1)
    _start_follower()
    _start_idle_watchdog()
    mcp.run()
