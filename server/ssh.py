"""SSH connection pool and command execution.

One pooled paramiko client per cluster.  The per-cluster lock is held for the
*entire* exec_command lifecycle so two threads never share a transport
concurrently (which caused orphaned sshd processes on the remote host).
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


def _get_cluster_lock(cluster_name):
    with _ssh_pool_lock:
        if cluster_name not in _ssh_cluster_locks:
            _ssh_cluster_locks[cluster_name] = threading.Lock()
        return _ssh_cluster_locks[cluster_name]


def _get_pooled_client(cluster_name, force_new=False):
    """Return a pooled client.  Caller MUST hold the per-cluster lock."""
    now = time.monotonic()
    old_to_close = None
    with _ssh_pool_lock:
        if not force_new:
            rec = _ssh_pool.get(cluster_name)
            if rec:
                client = rec["client"]
                try:
                    tr = client.get_transport()
                    if tr and tr.is_active():
                        rec["last_used"] = now
                        return client
                except Exception:
                    pass
                old_to_close = _ssh_pool.pop(cluster_name, None)
        else:
            old_to_close = _ssh_pool.pop(cluster_name, None)

    if old_to_close:
        try:
            old_to_close["client"].close()
        except Exception:
            pass

    client = _ssh_client(cluster_name)
    with _ssh_pool_lock:
        _ssh_pool[cluster_name] = {"client": client, "last_used": now}
    return client


def close_cluster_client(cluster_name):
    with _ssh_pool_lock:
        rec = _ssh_pool.pop(cluster_name, None)
    if rec:
        try:
            rec["client"].close()
        except Exception:
            pass


def close_all_clients():
    """Close every pooled connection.  Called at interpreter exit."""
    with _ssh_pool_lock:
        clusters = list(_ssh_pool.keys())
    for c in clusters:
        close_cluster_client(c)
    log.info("SSH pool: closed all connections")


atexit.register(close_all_clients)


def ssh_pool_gc_loop():
    while True:
        now = time.monotonic()
        stale = []
        with _ssh_pool_lock:
            for cluster, rec in list(_ssh_pool.items()):
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
            close_cluster_client(cluster)
        time.sleep(60)


def _shell_quote(s):
    """Quote a string for use as a single argument to bash -lc."""
    return "'" + s.replace("'", "'\"'\"'") + "'"


def ssh_run(cluster_name, command):
    return _ssh_exec(cluster_name, command, SSH_TIMEOUT)


def ssh_run_with_timeout(cluster_name, command, timeout_sec=20):
    return _ssh_exec(cluster_name, command, timeout_sec)


def _ssh_exec(cluster_name, command, timeout_sec):
    lock = _get_cluster_lock(cluster_name)
    for attempt in (1, 2):
        with lock:
            client = _get_pooled_client(cluster_name, force_new=(attempt == 2))
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
                    rec = _ssh_pool.get(cluster_name)
                    if rec:
                        rec["last_used"] = time.monotonic()
                return out, err
            except Exception:
                close_cluster_client(cluster_name)
                if attempt == 2:
                    raise
