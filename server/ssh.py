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

    The per-cluster *lock* is held during pool access and new-connection
    creation (bounded by DNS timeout + SSH_TIMEOUT) but NOT during
    command I/O — that happens in ``_exec_on_transport`` which only needs
    the transport reference, not the lock.
    """
    now = time.monotonic()

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
                try:
                    client.close()
                except Exception:
                    pass
        else:
            old = pool.pop(cluster_name, None)
            if old:
                try:
                    old["client"].close()
                except Exception:
                    pass

        client = _ssh_client(cluster_name, host_override=host_override)
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
            with _ssh_pool_lock:
                for cluster, rec in list(pool.items()):
                    if now - rec.get("last_used", 0) > SSH_IDLE_TTL_SEC:
                        stale.append(cluster)
                    else:
                        try:
                            tr = rec["client"].get_transport()
                            if not tr or not tr.is_active():
                                stale.append(cluster)
                        except Exception:
                            stale.append(cluster)
            for cluster in stale:
                _close_pool_client(pool, lock_fn(cluster), cluster)
        time.sleep(60)


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

def _get_transport(pool, lock, cluster_name, force_new=False, host_override=None):
    """Return an active paramiko Transport for *cluster_name*.

    The per-cluster lock is held during pool lookup and (when needed)
    connection creation, but NOT during command I/O.  Callers open their
    own channel on the returned transport, which supports multiplexing.
    """
    client = _get_pooled_client(pool, lock, cluster_name,
                                force_new=force_new, host_override=host_override)
    tr = client.get_transport()
    if not tr or not tr.is_active():
        raise paramiko.SSHException("transport inactive after connect")
    return tr


def _exec_on_transport(transport, command, timeout_sec):
    """Open a channel on *transport*, run *command*, return (stdout, stderr).

    Paramiko transports multiplex channels — this is safe to call from
    multiple threads concurrently on the same transport.
    """
    chan = transport.open_session(timeout=timeout_sec)
    try:
        chan.settimeout(timeout_sec)
        chan.exec_command("bash")
        chan.sendall((command + "\nexit\n").encode())
        chan.shutdown_write()

        stdout = chan.makefile("rb", -1)
        stderr = chan.makefile_stderr("rb", -1)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
    finally:
        try:
            chan.close()
        except Exception:
            pass
    return out, err


def _ssh_exec_data(cluster_name, command, timeout_sec):
    """Execute on the data-copier node, falling back to login on failure."""
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
    if getattr(_thread_ctx, "standalone", False):
        pool = _bg_pool
        lock = _get_bg_lock(cluster_name)
    else:
        pool = _ssh_pool
        lock = _get_cluster_lock(cluster_name)

    for attempt in (1, 2):
        try:
            tr = _get_transport(pool, lock, cluster_name, force_new=(attempt == 2))
            out, err = _exec_on_transport(tr, command, timeout_sec)
            with lock:
                rec = pool.get(cluster_name)
                if rec:
                    rec["last_used"] = time.monotonic()
            return out, err
        except Exception:
            _close_pool_client(pool, lock, cluster_name)
            if attempt == 2:
                raise
