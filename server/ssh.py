"""SSH connection pool and command execution.

Two-lane pool: a *primary* lane for request threads and a *background*
lane for worker threads.  Each lane has one pooled paramiko client per
cluster and its own per-cluster lock, so background work never blocks
the main polling path and vice-versa.  Total connections per cluster: 2.

Background threads call ``enable_standalone_ssh()`` once at startup;
all subsequent ``ssh_run`` / ``ssh_run_with_timeout`` calls are
automatically routed to the background lane.
"""

import atexit
import logging
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


# ── Client creation ──────────────────────────────────────────────────────────

def _ssh_client(cluster_name):
    cfg = CLUSTERS[cluster_name]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            cfg["host"], port=cfg["port"], username=cfg["user"],
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


# ── Pooled client helpers ────────────────────────────────────────────────────

def _get_pooled_client(pool, cluster_name, force_new=False):
    """Return a pooled client from *pool*.  Caller MUST hold the matching lock."""
    now = time.monotonic()
    old_to_close = None
    with _ssh_pool_lock:
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
                old_to_close = pool.pop(cluster_name, None)
        else:
            old_to_close = pool.pop(cluster_name, None)

    if old_to_close:
        try:
            old_to_close["client"].close()
        except Exception:
            pass

    client = _ssh_client(cluster_name)
    with _ssh_pool_lock:
        pool[cluster_name] = {"client": client, "last_used": now}
    return client


def _close_pool_client(pool, cluster_name):
    with _ssh_pool_lock:
        rec = pool.pop(cluster_name, None)
    if rec:
        try:
            rec["client"].close()
        except Exception:
            pass


def close_cluster_client(cluster_name):
    _close_pool_client(_ssh_pool, cluster_name)


def close_all_clients():
    """Close every pooled connection (both lanes).  Called at interpreter exit."""
    for pool in (_ssh_pool, _bg_pool):
        with _ssh_pool_lock:
            clusters = list(pool.keys())
        for c in clusters:
            _close_pool_client(pool, c)
    log.info("SSH pool: closed all connections")


atexit.register(close_all_clients)


# ── GC loop ──────────────────────────────────────────────────────────────────

def ssh_pool_gc_loop():
    while True:
        now = time.monotonic()
        for pool in (_ssh_pool, _bg_pool):
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
                _close_pool_client(pool, cluster)
        time.sleep(60)


# ── Public API ───────────────────────────────────────────────────────────────

def _shell_quote(s):
    """Quote a string for use as a single argument to bash -lc."""
    return "'" + s.replace("'", "'\"'\"'") + "'"


def ssh_run(cluster_name, command):
    return _ssh_exec(cluster_name, command, SSH_TIMEOUT)


def ssh_run_with_timeout(cluster_name, command, timeout_sec=20):
    return _ssh_exec(cluster_name, command, timeout_sec)


def enable_standalone_ssh():
    """Mark the current thread to use the background SSH lane.

    Call at the start of any background/worker thread so all subsequent
    ssh_run / ssh_run_with_timeout calls use the background pool instead
    of the primary pool, avoiding lock contention with request threads.
    """
    _thread_ctx.standalone = True


# ── Core execution ───────────────────────────────────────────────────────────

def _ssh_exec(cluster_name, command, timeout_sec):
    if getattr(_thread_ctx, "standalone", False):
        pool = _bg_pool
        lock = _get_bg_lock(cluster_name)
    else:
        pool = _ssh_pool
        lock = _get_cluster_lock(cluster_name)

    for attempt in (1, 2):
        with lock:
            client = _get_pooled_client(pool, cluster_name, force_new=(attempt == 2))
            try:
                stdin_ch, stdout, stderr = client.exec_command(
                    "bash", timeout=timeout_sec,
                )
                try:
                    stdin_ch.write(command + "\nexit\n")
                    stdin_ch.flush()
                    stdin_ch.channel.shutdown_write()
                    out = stdout.read().decode().strip()
                    err = stderr.read().decode().strip()
                finally:
                    try:
                        stdout.channel.close()
                    except Exception:
                        pass
                with _ssh_pool_lock:
                    rec = pool.get(cluster_name)
                    if rec:
                        rec["last_used"] = time.monotonic()
                return out, err
            except Exception:
                _close_pool_client(pool, cluster_name)
                if attempt == 2:
                    raise
