"""Bounded SSH command execution via OpenSSH subprocesses.

This replaces the old pooled Paramiko transport stack with a much simpler
model: each command is one short-lived ``ssh`` subprocess with a hard timeout.
We keep the circuit breaker, concurrency caps, and request-counter watchdog
because those still protect the web app under cluster failures.
"""

import atexit
import logging
import os
import signal
import subprocess
import threading
import time

from .config import CLUSTERS, SSH_TIMEOUT

log = logging.getLogger(__name__)

_thread_ctx = threading.local()

# Global concurrency limit: keep SSH work bounded so cached/non-SSH routes stay
# responsive even when clusters flap.
_ssh_semaphore = threading.Semaphore(8)

# Per-cluster concurrency cap so one dead cluster cannot monopolize SSH work.
_MAX_PER_CLUSTER = 2
_per_cluster_sem_lock = threading.Lock()
_per_cluster_sems = {}  # cluster -> Semaphore


def _get_cluster_sem(cluster_name):
    with _per_cluster_sem_lock:
        sem = _per_cluster_sems.get(cluster_name)
        if sem is None:
            sem = threading.Semaphore(_MAX_PER_CLUSTER)
            _per_cluster_sems[cluster_name] = sem
        return sem


# -- Circuit breaker ---------------------------------------------------------

_CB_COOLDOWN_SEC = 60
_cb_lock = threading.Lock()
_cb_failures = {}  # cluster -> {"ts": monotonic, "count": int}


def _cb_record_failure(cluster):
    with _cb_lock:
        rec = _cb_failures.get(cluster)
        now = time.monotonic()
        if rec:
            rec["ts"] = now
            rec["count"] = min(rec["count"] + 1, 10)
            cooldown = min(_CB_COOLDOWN_SEC * rec["count"], 300)
            log.warning("circuit breaker: %s failure #%d, cooldown %ds",
                        cluster, rec["count"], cooldown)
        else:
            _cb_failures[cluster] = {"ts": now, "count": 1}
            log.warning("circuit breaker OPEN: %s (first failure, cooldown %ds)",
                        cluster, _CB_COOLDOWN_SEC)


def _cb_record_success(cluster):
    with _cb_lock:
        was_open = cluster in _cb_failures
        _cb_failures.pop(cluster, None)
    if was_open:
        log.warning("circuit breaker CLOSED: %s recovered", cluster)


def _cb_is_open(cluster):
    """True if the circuit breaker is open (cluster should be skipped)."""
    with _cb_lock:
        rec = _cb_failures.get(cluster)
        if not rec:
            return False
        elapsed = time.monotonic() - rec["ts"]
        cooldown = min(_CB_COOLDOWN_SEC * rec["count"], 300)
        if elapsed >= cooldown:
            return False
        return True


def get_circuit_breaker_status():
    """Return current CB state for diagnostics / the settings page."""
    with _cb_lock:
        now = time.monotonic()
        return {
            cluster: {
                "failures": rec["count"],
                "cooldown_remaining": max(
                    0,
                    round(min(_CB_COOLDOWN_SEC * rec["count"], 300) - (now - rec["ts"]))
                ),
            }
            for cluster, rec in _cb_failures.items()
        }


# -- Helpers ----------------------------------------------------------------

def _shell_quote(s):
    """Quote a string for safe use inside ``bash -lc``."""
    return "'" + str(s).replace("'", "'\"'\"'") + "'"


def _ssh_connect_timeout(timeout_sec):
    return max(1, min(int(timeout_sec), int(SSH_TIMEOUT or timeout_sec or 1)))


def _ssh_argv(cluster_name, timeout_sec, host_override=None):
    cfg = CLUSTERS[cluster_name]
    host = host_override or cfg["host"]
    if not host:
        raise RuntimeError(f"Cluster {cluster_name} has no SSH host configured")

    argv = [
        'ssh',
        '-T',
        '-o', 'BatchMode=yes',
        '-o', f'ConnectTimeout={_ssh_connect_timeout(timeout_sec)}',
        '-o', 'ConnectionAttempts=1',
        '-o', 'ServerAliveInterval=15',
        '-o', 'ServerAliveCountMax=1',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'LogLevel=ERROR',
    ]

    port = cfg.get('port')
    if port:
        argv += ['-p', str(port)]

    key = cfg.get('key')
    if key:
        argv += ['-i', str(key)]

    argv.append(f"{cfg['user']}@{host}")
    return argv


def _build_remote_command(command):
    return f"bash -lc {_shell_quote(command)}"


def _run_ssh_subprocess(cluster_name, command, timeout_sec, *, host_override=None, record_breaker=True):
    if record_breaker and _cb_is_open(cluster_name):
        raise RuntimeError(
            f"SSH to {cluster_name}: circuit breaker open (cluster recently unreachable)"
        )

    cluster_sem = _get_cluster_sem(cluster_name)
    if not cluster_sem.acquire(timeout=min(timeout_sec, 4)):
        raise RuntimeError(
            f"SSH to {cluster_name}: per-cluster concurrency limit reached"
        )
    try:
        acquired = _ssh_semaphore.acquire(timeout=min(timeout_sec, 8))
        if not acquired:
            raise RuntimeError(
                f"SSH to {cluster_name}: too many concurrent operations (semaphore timeout)"
            )
        try:
            argv = _ssh_argv(cluster_name, timeout_sec, host_override=host_override)
            argv.append(_build_remote_command(command))
            try:
                result = subprocess.run(
                    argv,
                    capture_output=True,
                    text=True,
                    timeout=timeout_sec,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                if record_breaker:
                    _cb_record_failure(cluster_name)
                raise TimeoutError(
                    f"SSH to {cluster_name} timed out after {timeout_sec}s"
                ) from exc
            except FileNotFoundError as exc:
                if record_breaker:
                    _cb_record_failure(cluster_name)
                raise RuntimeError('OpenSSH client not available') from exc

            out = (result.stdout or '').strip()
            err = (result.stderr or '').strip()

            if result.returncode == 255:
                if record_breaker:
                    _cb_record_failure(cluster_name)
                raise RuntimeError(err or out or f"SSH to {cluster_name} failed")

            if record_breaker:
                _cb_record_success(cluster_name)
            return out, err
        finally:
            _ssh_semaphore.release()
    finally:
        cluster_sem.release()


# -- Public API --------------------------------------------------------------

_CANCEL_MARKER = '__CLAUSIUS_CANCEL__'


def _build_cancel_script(job_ids):
    joined = ' '.join(job_ids)
    return f"""for jid in {joined}; do
  if out=$(scancel "$jid" 2>&1); then
    printf '{_CANCEL_MARKER}:OK:%s\\n' "$jid"
  else
    rc=$?
    out=$(printf '%s' "$out" | tr '\\n' ' ' | sed 's/[[:space:]]\\+/ /g; s/^ //; s/ $//')
    printf '{_CANCEL_MARKER}:ERR:%s:%s:%s\\n' "$jid" "$rc" "$out"
  fi
done"""


def ssh_run(cluster_name, command):
    return _run_ssh_subprocess(cluster_name, command, SSH_TIMEOUT)


def ssh_run_with_timeout(cluster_name, command, timeout_sec=20):
    return _run_ssh_subprocess(cluster_name, command, timeout_sec)


def cancel_jobs_with_report(cluster_name, job_ids, timeout_sec=20, chunk_size=25):
    """Cancel jobs via `scancel` and report success/failure per job ID."""
    sanitized = [str(jid).strip() for jid in job_ids if str(jid).strip()]
    if not sanitized:
        return {'cancelled_ids': [], 'errors': []}

    step = max(1, int(chunk_size))
    cancelled_ids = []
    errors = []

    import time
    start_time = time.monotonic()
    overall_timeout = timeout_sec + 5

    for i in range(0, len(sanitized), step):
        if time.monotonic() - start_time > overall_timeout:
            msg = f"Overall timeout exceeded ({overall_timeout}s)"
            for jid in sanitized[i:]:
                errors.append({'job_id': jid, 'error': msg, 'exit_code': None})
            break

        chunk = sanitized[i:i + step]
        try:
            out, err = ssh_run_with_timeout(
                cluster_name,
                _build_cancel_script(chunk),
                timeout_sec=timeout_sec,
            )
        except Exception as exc:
            msg = str(exc)
            for jid in sanitized[i:]:
                errors.append({'job_id': jid, 'error': msg, 'exit_code': None})
            break

        seen = set()
        for raw_line in out.splitlines():
            line = raw_line.strip()
            if not line.startswith(f'{_CANCEL_MARKER}:'):
                continue
            parts = line.split(':', 4)
            if len(parts) < 3:
                continue
            status = parts[1].strip()
            jid = parts[2].strip()
            if not jid:
                continue
            seen.add(jid)
            if status == 'OK':
                cancelled_ids.append(jid)
                continue
            exit_code = None
            if len(parts) > 3:
                try:
                    exit_code = int(parts[3])
                except (TypeError, ValueError):
                    exit_code = None
            message = parts[4].strip() if len(parts) > 4 else ''
            errors.append({
                'job_id': jid,
                'error': message or err.strip() or 'scancel failed',
                'exit_code': exit_code,
            })

        fallback = err.strip() or 'No cancel status returned'
        for jid in chunk:
            if jid in seen:
                continue
            errors.append({'job_id': jid, 'error': fallback, 'exit_code': None})

    return {'cancelled_ids': cancelled_ids, 'errors': errors}


def ssh_run_data(cluster_name, command):
    return _ssh_exec_data(cluster_name, command, SSH_TIMEOUT)


def ssh_run_data_with_timeout(cluster_name, command, timeout_sec=20):
    return _ssh_exec_data(cluster_name, command, timeout_sec)


def enable_standalone_ssh():
    """Compatibility no-op for old background-worker call sites."""
    _thread_ctx.standalone = True


def _ssh_exec_data(cluster_name, command, timeout_sec):
    """Execute on the data-copier node, falling back to the login node."""
    cfg = CLUSTERS.get(cluster_name, {})
    data_host = cfg.get('data_host', '')
    if not data_host:
        return _run_ssh_subprocess(cluster_name, command, timeout_sec)

    try:
        return _run_ssh_subprocess(
            cluster_name,
            command,
            timeout_sec,
            host_override=data_host,
            record_breaker=False,
        )
    except Exception:
        log.warning(
            'ssh_data: DC node %s unreachable for %s, falling back to login',
            data_host, cluster_name,
        )
        return _run_ssh_subprocess(cluster_name, command, timeout_sec)


def close_cluster_client(cluster_name):
    """Compatibility no-op: no persistent SSH clients are kept anymore."""
    return None


def close_all_clients():
    """Compatibility no-op for the old pooled-client implementation."""
    return None


atexit.register(close_all_clients)


# -- Watchdog loop -----------------------------------------------------------
#
# Two jobs:
#   1. Observability: when many request threads are in flight, log a snapshot
#      (method/path/age) of the oldest stuck requests so the next wedge is
#      diagnosable from journalctl alone.
#   2. Self-recovery: if the active count stays at the load-shedding ceiling
#      for two consecutive ticks (~30s), assume something is permanently
#      wedged inside a request thread and SIGTERM ourselves so gunicorn's
#      arbiter respawns the worker.  This converts an indefinite outage into
#      a ~15 s blip (graceful_timeout=15 in gunicorn.conf.py).

_WATCHDOG_TICK_SEC = 15
_WATCHDOG_LOG_THRESHOLD = 16
_WATCHDOG_RESET_THRESHOLD = 10  # streak resets when active drops well below cap

_watchdog_high_streak = 0
_watchdog_restart_pending = False


def _format_active_snapshot(snapshot):
    parts = []
    for item in snapshot:
        method = item.get("method") or "?"
        path = item.get("path") or "?"
        age_s = (item.get("age_ms") or 0) / 1000.0
        parts.append(f"{method} {path} age={age_s:.1f}s")
    return " | ".join(parts) if parts else "(empty)"


def _watchdog_log_active():
    """Log stuck-request snapshot and self-restart on persistent wedge.

    The set-based _active_threads in routes.py self-heals (dead threads are
    pruned on read), but it cannot help when threads are alive-but-blocked
    (e.g. stuck inside a hung FUSE syscall).  For that case the snapshot
    tells us *what* was stuck and the SIGTERM gets us moving again.
    """
    global _watchdog_high_streak, _watchdog_restart_pending

    if _watchdog_restart_pending:
        return

    try:
        from . import routes

        count = routes._active_request_count()
        max_active = getattr(routes, "_MAX_ACTIVE", 20)

        if count >= max_active:
            _watchdog_high_streak += 1
        elif count <= _WATCHDOG_RESET_THRESHOLD:
            _watchdog_high_streak = 0

        if count >= _WATCHDOG_LOG_THRESHOLD:
            snapshot = routes._active_request_snapshot(limit=8)
            log.warning(
                "watchdog: %d active requests (streak=%d) — oldest: %s",
                count,
                _watchdog_high_streak,
                _format_active_snapshot(snapshot),
            )

        if _watchdog_high_streak >= 2:
            log.error(
                "watchdog: wedge confirmed (%d active for %d ticks); "
                "SIGTERM self for arbiter respawn",
                count,
                _watchdog_high_streak,
            )
            _watchdog_restart_pending = True
            try:
                os.kill(os.getpid(), signal.SIGTERM)
            except Exception:
                log.exception("watchdog: SIGTERM failed")
    except Exception:
        log.exception("watchdog tick failed")


def ssh_pool_gc_loop():
    """Background watchdog for request load observability and self-recovery."""
    while True:
        _watchdog_log_active()
        time.sleep(_WATCHDOG_TICK_SEC)
