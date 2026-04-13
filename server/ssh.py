"""SSH connection pool and command execution.

Three-lane pool:

* **primary** — request threads (Slurm queries, job listing).
* **background** — worker threads (metadata fetching, progress).
* **data** — file-I/O operations routed to the cluster's data-copier
  (DC) node when configured, falling back to the login node otherwise.

Each lane has one pooled paramiko client per cluster and its own
per-cluster lock, so lanes never block each other.

Background threads call ``enable_standalone_ssh()`` once at startup;
all subsequent ``ssh_run`` / ``ssh_run_with_timeout`` calls are
automatically routed to the background lane.

Concurrency safety:
- Per-cluster locks protect only pool dict access (fast, never I/O).
- SSH connection creation happens OUTSIDE the lock to prevent
  thread starvation when multiple threads reconnect simultaneously.
- A global semaphore caps concurrent SSH operations so request
  threads always remain available for cached/non-SSH responses.
- All paramiko I/O (channel open, reads, writes) is wrapped in a
  daemon thread with Thread.join(timeout) to enforce hard wall-clock
  deadlines — paramiko's own timeout parameters are unreliable when
  the transport's internal thread is blocked.
"""

import atexit
import logging
import socket
import threading
import time

import paramiko

from .config import (
    CLUSTERS, SSH_TIMEOUT, SSH_IDLE_TTL_SEC,
    _ssh_pool_lock, _ssh_pool, _ssh_cluster_locks,
)

log = logging.getLogger(__name__)

_thread_ctx = threading.local()

# Background lane — mirrors the primary pool structure.
_bg_pool = {}           # cluster -> {"client": SSHClient, "last_used": float}
_bg_locks = {}          # cluster -> Lock

# Data lane — connects to data_host (DC node) when configured.
_data_pool = {}         # cluster -> {"client": SSHClient, "last_used": float}
_data_locks = {}        # cluster -> Lock

# Global concurrency limit: at most 8 of 32 gthread workers can be
# in SSH I/O simultaneously, leaving 24 free for cached responses,
# health checks, and non-SSH routes.  Previously 12 — lowered after
# repeated watchdog kills when clusters flap simultaneously.
_ssh_semaphore = threading.Semaphore(8)

# Channel-open timeout is capped independently of command timeout.
# If the server can't open a channel in this window, the transport is broken.
_CHAN_OPEN_TIMEOUT = 5

# Per-cluster concurrency cap: prevents one flaky cluster from consuming
# all semaphore slots.  Each cluster can have at most this many concurrent
# SSH operations across all lanes.
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

# ── Circuit breaker ──────────────────────────────────────────────────────────
# When a cluster fails SSH, back off for _CB_COOLDOWN_SEC before retrying.
# This prevents a dead cluster from consuming all worker threads on timeouts.

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


# ── Thread-based hard timeout wrapper ────────────────────────────────────────

def _run_with_deadline(fn, timeout_sec):
    """Run *fn* in a daemon thread, returning its result or raising on timeout.

    Paramiko's built-in timeout parameters are unreliable: they depend on
    internal Event.wait() calls that can miss the deadline when the
    transport thread is blocked on socket I/O.  This wrapper enforces a
    true wall-clock deadline via Thread.join(timeout).
    """
    result = [None]
    exc = [None]

    def _worker():
        try:
            result[0] = fn()
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    if t.is_alive():
        raise socket.timeout(
            f"SSH operation timed out after {timeout_sec}s (thread still running)"
        )
    if exc[0]:
        raise exc[0]
    return result[0]


# ── Client creation ──────────────────────────────────────────────────────────

_DNS_TIMEOUT_SEC = 3


def _resolve_host(hostname, port, timeout=_DNS_TIMEOUT_SEC):
    """Resolve hostname with a bounded timeout.

    getaddrinfo() uses the system resolver whose timeout is controlled by
    /etc/resolv.conf and can be 30s+.  We run it in a worker thread so a
    DNS outage never blocks the SSH pool longer than *timeout* seconds.
    """
    result = [None]
    exc = [None]

    def _do():
        try:
            infos = socket.getaddrinfo(hostname, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
            if infos:
                result[0] = infos[0][4][0]
        except Exception as e:
            exc[0] = e

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        raise socket.timeout(f"DNS resolution timed out for {hostname} after {timeout}s")
    if exc[0]:
        raise exc[0]
    if not result[0]:
        raise socket.gaierror(f"Could not resolve {hostname}")
    return result[0]


def _ssh_client(cluster_name, host_override=None):
    cfg = CLUSTERS[cluster_name]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    host = host_override or cfg["host"]
    try:
        ip = _resolve_host(host, cfg["port"])
        client.connect(
            ip, port=cfg["port"], username=cfg["user"],
            key_filename=cfg["key"],
            timeout=SSH_TIMEOUT, banner_timeout=SSH_TIMEOUT, auth_timeout=SSH_TIMEOUT,
        )
    except Exception:
        client.close()
        raise
    try:
        client.get_transport().set_keepalive(30)
    except Exception:
        pass
    return client


# ── Per-cluster locks ────────────────────────────────────────────────────────

def _get_cluster_lock(cluster_name):
    with _ssh_pool_lock:
        if cluster_name not in _ssh_cluster_locks:
            _ssh_cluster_locks[cluster_name] = threading.Lock()
        return _ssh_cluster_locks[cluster_name]


def _get_bg_lock(cluster_name):
    with _ssh_pool_lock:
        if cluster_name not in _bg_locks:
            _bg_locks[cluster_name] = threading.Lock()
        return _bg_locks[cluster_name]


def _get_data_lock(cluster_name):
    with _ssh_pool_lock:
        if cluster_name not in _data_locks:
            _data_locks[cluster_name] = threading.Lock()
        return _data_locks[cluster_name]


# ── Pooled client helpers ────────────────────────────────────────────────────

def _get_pooled_client(pool, lock, cluster_name, force_new=False, host_override=None):
    """Return a pooled client from *pool*.

    The per-cluster *lock* is held ONLY during pool dict access (microseconds).
    SSH connection creation happens outside the lock so a slow reconnect
    never blocks other threads from using their already-open transports.
    """
    now = time.monotonic()
    need_new = force_new
    old_client = None

    with lock:
        if not force_new:
            rec = pool.get(cluster_name)
            if rec:
                client = rec["client"]
                try:
                    tr = client.get_transport()
                    if tr and tr.is_active():
                        rec["last_used"] = now
                        return client
                except Exception:
                    pass
                pool.pop(cluster_name, None)
                old_client = client
                need_new = True
        else:
            old_rec = pool.pop(cluster_name, None)
            if old_rec:
                old_client = old_rec["client"]
            need_new = True

    if old_client:
        try:
            old_client.close()
        except Exception:
            pass

    if not need_new:
        return None  # should not happen

    # SSH connection creation happens WITHOUT holding the lock.
    client = _ssh_client(cluster_name, host_override=host_override)

    with lock:
        # Another thread may have won the race — check before inserting.
        existing = pool.get(cluster_name)
        if existing:
            try:
                tr = existing["client"].get_transport()
                if tr and tr.is_active():
                    # Discard ours, use theirs.
                    try:
                        client.close()
                    except Exception:
                        pass
                    existing["last_used"] = now
                    return existing["client"]
            except Exception:
                pool.pop(cluster_name, None)
                try:
                    existing["client"].close()
                except Exception:
                    pass
        pool[cluster_name] = {"client": client, "last_used": now}
    return client


def _close_pool_client(pool, lock, cluster_name):
    with lock:
        rec = pool.pop(cluster_name, None)
    if rec:
        try:
            rec["client"].close()
        except Exception:
            pass


def close_cluster_client(cluster_name):
    _close_pool_client(_ssh_pool, _get_cluster_lock(cluster_name), cluster_name)


def close_all_clients():
    """Close every pooled connection (all lanes).  Called at interpreter exit."""
    pools_and_lock_fns = [
        (_ssh_pool, _get_cluster_lock),
        (_bg_pool, _get_bg_lock),
        (_data_pool, _get_data_lock),
    ]
    for pool, lock_fn in pools_and_lock_fns:
        with _ssh_pool_lock:
            clusters = list(pool.keys())
        for c in clusters:
            _close_pool_client(pool, lock_fn(c), c)
    log.info("SSH pool: closed all connections")


atexit.register(close_all_clients)


# ── GC loop ──────────────────────────────────────────────────────────────────

_SSH_MAX_AGE_SEC = 600  # Force-close connections older than 10 minutes

def ssh_pool_gc_loop():
    while True:
        now = time.monotonic()
        pools_and_lock_fns = [
            (_ssh_pool, _get_cluster_lock),
            (_bg_pool, _get_bg_lock),
            (_data_pool, _get_data_lock),
        ]
        for pool, lock_fn in pools_and_lock_fns:
            stale = []
            to_probe = []  # (cluster, transport) — probe outside the lock
            with _ssh_pool_lock:
                for cluster, rec in list(pool.items()):
                    if _cb_is_open(cluster):
                        stale.append(cluster)
                        continue
                    age = now - rec.get("last_used", 0)
                    if age > SSH_IDLE_TTL_SEC:
                        stale.append(cluster)
                        continue
                    created = rec.get("created", rec.get("last_used", 0))
                    if now - created > _SSH_MAX_AGE_SEC:
                        stale.append(cluster)
                        continue
                    try:
                        tr = rec["client"].get_transport()
                        if not tr or not tr.is_active():
                            stale.append(cluster)
                        else:
                            to_probe.append((cluster, tr))
                    except Exception:
                        stale.append(cluster)
            for cluster, tr in to_probe:
                if not _transport_is_healthy(tr):
                    stale.append(cluster)
            for cluster in stale:
                _close_pool_client(pool, lock_fn(cluster), cluster)

        _watchdog_reset_active_requests()
        time.sleep(15)


_wd_prev_count = 0
_wd_stable_cycles = 0
_WD_DRIFT_THRESHOLD = 16


def _watchdog_reset_active_requests():
    """Correct the active request counter if it has drifted.

    Gunicorn's gthread worker can silently kill threads that exceed the
    45s timeout.  If that happens mid-request, the teardown hook never
    runs and _active_requests stays inflated permanently.

    Two triggers:
      1) Immediate reset when counter exceeds _MAX_ACTIVE.
      2) Trend-based reset when counter stays above _WD_DRIFT_THRESHOLD
         for 2 consecutive cycles (~30s) — catches slow drift before
         full saturation.
    """
    global _wd_prev_count, _wd_stable_cycles
    try:
        from . import routes
        with routes._active_lock:
            current = routes._active_requests

        if current > routes._MAX_ACTIVE:
            with routes._active_lock:
                log.warning(
                    "watchdog: _active_requests=%d exceeds max=%d, resetting to 0",
                    routes._active_requests, routes._MAX_ACTIVE,
                )
                routes._active_requests = 0
            _wd_prev_count = 0
            _wd_stable_cycles = 0
            return

        if current >= _WD_DRIFT_THRESHOLD:
            if current >= _wd_prev_count and _wd_prev_count >= _WD_DRIFT_THRESHOLD:
                _wd_stable_cycles += 1
            else:
                _wd_stable_cycles = 1
            if _wd_stable_cycles >= 2:
                with routes._active_lock:
                    log.warning(
                        "watchdog: _active_requests=%d stuck above %d for %d cycles, resetting to 0",
                        routes._active_requests, _WD_DRIFT_THRESHOLD, _wd_stable_cycles,
                    )
                    routes._active_requests = 0
                _wd_stable_cycles = 0
        else:
            _wd_stable_cycles = 0

        _wd_prev_count = current
    except Exception:
        pass


# ── Public API ───────────────────────────────────────────────────────────────

def _shell_quote(s):
    """Quote a string for use as a single argument to bash -lc."""
    return "'" + s.replace("'", "'\"'\"'") + "'"


def ssh_run(cluster_name, command):
    return _ssh_exec(cluster_name, command, SSH_TIMEOUT)


def ssh_run_with_timeout(cluster_name, command, timeout_sec=20):
    return _ssh_exec(cluster_name, command, timeout_sec)


def ssh_run_data(cluster_name, command):
    """Run a command on the cluster's data-copier node if configured.

    Falls back to the login node (via ssh_run) when no data_host is set
    or when the DC node is unreachable.
    """
    return _ssh_exec_data(cluster_name, command, SSH_TIMEOUT)


def ssh_run_data_with_timeout(cluster_name, command, timeout_sec=20):
    """Like ssh_run_data but with a custom timeout."""
    return _ssh_exec_data(cluster_name, command, timeout_sec)


def enable_standalone_ssh():
    """Mark the current thread to use the background SSH lane.

    Call at the start of any background/worker thread so all subsequent
    ssh_run / ssh_run_with_timeout calls use the background pool instead
    of the primary pool, avoiding lock contention with request threads.
    """
    _thread_ctx.standalone = True


# ── Core execution ───────────────────────────────────────────────────────────

def _transport_is_healthy(tr):
    """Quick probe: open and immediately close a channel.

    Returns False if the transport is "active" at the paramiko level but
    the SSH server refuses new channels (the state that causes hangs).
    Uses _run_with_deadline so a broken transport can't block the caller.
    """
    if not tr or not tr.is_active():
        return False
    try:
        def _probe():
            chan = tr.open_session(timeout=_CHAN_OPEN_TIMEOUT)
            chan.close()
        _run_with_deadline(_probe, _CHAN_OPEN_TIMEOUT)
        return True
    except Exception:
        return False


def _get_transport(pool, lock, cluster_name, force_new=False, host_override=None):
    """Return an active paramiko Transport for *cluster_name*.

    The per-cluster lock is held only during pool dict access, never I/O.
    """
    client = _get_pooled_client(pool, lock, cluster_name,
                                force_new=force_new, host_override=host_override)
    tr = client.get_transport()
    if not tr or not tr.is_active():
        raise paramiko.SSHException("transport inactive after connect")
    return tr


def _exec_on_transport(transport, command, timeout_sec):
    """Open a channel on *transport*, run *command*, return (stdout, stderr).

    The entire operation (channel open + exec + read) runs inside
    _run_with_deadline so a broken transport can never block the calling
    thread past the wall-clock timeout.
    """
    def _do():
        chan_timeout = min(_CHAN_OPEN_TIMEOUT, timeout_sec)
        chan = transport.open_session(timeout=chan_timeout)
        try:
            chan.settimeout(timeout_sec)
            chan.exec_command("bash")
            chan.sendall((command + "\nexit\n").encode())
            chan.shutdown_write()

            stdout = chan.makefile("rb", -1)
            stderr = chan.makefile_stderr("rb", -1)
            out = stdout.read().decode(errors="replace").strip()
            err = stderr.read().decode(errors="replace").strip()
        finally:
            try:
                chan.close()
            except Exception:
                pass
        return out, err

    return _run_with_deadline(_do, timeout_sec)


def _ssh_exec_data(cluster_name, command, timeout_sec):
    """Execute on the data-copier node, falling back to login on failure."""
    if _cb_is_open(cluster_name):
        raise paramiko.SSHException(
            f"SSH to {cluster_name}: circuit breaker open (cluster recently unreachable)"
        )

    cfg = CLUSTERS.get(cluster_name, {})
    data_host = cfg.get("data_host", "")
    if not data_host:
        return _ssh_exec(cluster_name, command, timeout_sec)

    pool = _data_pool
    lock = _get_data_lock(cluster_name)

    for attempt in (1, 2):
        try:
            tr = _get_transport(pool, lock, cluster_name,
                                force_new=(attempt == 2), host_override=data_host)
            out, err = _exec_on_transport(tr, command, timeout_sec)
            with lock:
                rec = pool.get(cluster_name)
                if rec:
                    rec["last_used"] = time.monotonic()
            return out, err
        except Exception:
            _close_pool_client(pool, lock, cluster_name)
            if attempt == 2:
                log.warning(
                    "ssh_data: DC node %s unreachable for %s, falling back to login",
                    data_host, cluster_name,
                )
                return _ssh_exec(cluster_name, command, timeout_sec)


def _ssh_exec(cluster_name, command, timeout_sec):
    if _cb_is_open(cluster_name):
        raise paramiko.SSHException(
            f"SSH to {cluster_name}: circuit breaker open (cluster recently unreachable)"
        )

    if getattr(_thread_ctx, "standalone", False):
        pool = _bg_pool
        lock = _get_bg_lock(cluster_name)
    else:
        pool = _ssh_pool
        lock = _get_cluster_lock(cluster_name)

    cluster_sem = _get_cluster_sem(cluster_name)
    if not cluster_sem.acquire(timeout=min(timeout_sec, 4)):
        raise paramiko.SSHException(
            f"SSH to {cluster_name}: per-cluster concurrency limit reached"
        )
    try:
        acquired = _ssh_semaphore.acquire(timeout=min(timeout_sec, 8))
        if not acquired:
            raise paramiko.SSHException(
                f"SSH to {cluster_name}: too many concurrent operations (semaphore timeout)"
            )
        try:
            for attempt in (1, 2):
                try:
                    tr = _get_transport(pool, lock, cluster_name, force_new=(attempt == 2))
                    out, err = _exec_on_transport(tr, command, timeout_sec)
                    with lock:
                        rec = pool.get(cluster_name)
                        if rec:
                            rec["last_used"] = time.monotonic()
                    _cb_record_success(cluster_name)
                    return out, err
                except Exception:
                    _close_pool_client(pool, lock, cluster_name)
                    if attempt == 2:
                        _cb_record_failure(cluster_name)
                        raise
        finally:
            _ssh_semaphore.release()
    finally:
        cluster_sem.release()
