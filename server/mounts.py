"""Mount resolution, status, and path helpers."""

import logging
import os
import re
import subprocess
import threading
import time

from .config import (
    CLUSTERS, MOUNT_MAP, MOUNT_REMOTE_MAP, MOUNT_ALIASES, MOUNT_SCRIPT_PATH,
    _cache_set, _dir_list_cache,
)

log = logging.getLogger(__name__)

_mount_ok = {}
_mount_ok_ts = {}
_MOUNT_OK_TTL = 30
_mount_ok_locks = {}
_mount_ok_locks_lock = threading.Lock()
_mount_refresh_inflight = set()
_mount_refresh_lock = threading.Lock()

def _get_mount_ok_lock(cluster_name):
    with _mount_ok_locks_lock:
        if cluster_name not in _mount_ok_locks:
            _mount_ok_locks[cluster_name] = threading.Lock()
        return _mount_ok_locks[cluster_name]


def _refresh_mount_ok_async(cluster_name, roots):
    """Run _test_mount_alive in a background thread; never blocks the caller."""
    with _mount_refresh_lock:
        if cluster_name in _mount_refresh_inflight:
            return
        _mount_refresh_inflight.add(cluster_name)

    def _worker():
        try:
            ok = _test_mount_alive(roots[0])
            _mount_ok[cluster_name] = ok
            _mount_ok_ts[cluster_name] = time.monotonic()
            if not ok:
                log.warning("Mount for %s is stale (background check)", cluster_name)
        except Exception as e:
            log.debug("Mount refresh for %s failed: %s", cluster_name, e)
        finally:
            with _mount_refresh_lock:
                _mount_refresh_inflight.discard(cluster_name)

    threading.Thread(target=_worker, daemon=True, name=f"mount-refresh-{cluster_name}").start()


def _is_cluster_mount_ok(cluster_name):
    """Strictly non-blocking check: is this cluster's mount known-healthy?

    Never blocks the request thread. Returns the last known cached value
    immediately. If the cache is stale or absent, schedules a background
    refresh for next time. On the very first call (no cache yet) we use an
    optimistic estimate from /proc/mounts only — the actual `ls` test
    happens in the background.
    """
    cached = _mount_ok.get(cluster_name)
    ts = _mount_ok_ts.get(cluster_name, 0)
    age = time.monotonic() - ts

    roots = MOUNT_MAP.get(cluster_name, [])
    if not roots:
        return False

    mps = _proc_mount_points()
    any_mounted = any(_resolve(r) in mps for r in roots)

    if not any_mounted:
        _mount_ok[cluster_name] = False
        _mount_ok_ts[cluster_name] = time.monotonic()
        return False

    if cached is not None and age < _MOUNT_OK_TTL:
        return cached

    if cached is None or age >= _MOUNT_OK_TTL:
        _refresh_mount_ok_async(cluster_name, roots)

    if cached is None:
        return True
    return cached


def _proc_mount_points():
    """Read /proc/mounts and return a set of mounted paths (no filesystem stat).
    Uses realpath to resolve symlinks so paths match regardless of symlink vs target."""
    try:
        with open("/proc/mounts", "r") as f:
            return {line.split()[1] for line in f if len(line.split()) >= 2}
    except Exception:
        return set()


_resolved_cache = {}
def _resolve(path):
    """Resolve a path using realpath so symlinks match /proc/mounts entries.
    Cached to avoid hanging on stale FUSE mounts during live requests."""
    if path not in _resolved_cache:
        # We use a subprocess to resolve realpath safely without hanging the main thread
        try:
            import subprocess
            proc = subprocess.run(
                ["readlink", "-f", os.path.expanduser(path)],
                capture_output=True, text=True, timeout=1
            )
            if proc.returncode == 0 and proc.stdout.strip():
                _resolved_cache[path] = proc.stdout.strip()
            else:
                _resolved_cache[path] = os.path.realpath(os.path.expanduser(path))
        except (subprocess.TimeoutExpired, Exception):
            _resolved_cache[path] = os.path.abspath(os.path.expanduser(path))
    return _resolved_cache[path]


def _is_mounted(path):
    """Check if path is a mount point using /proc/mounts (never blocks on stale FUSE)."""
    return _resolve(path) in _proc_mount_points()


def _resolve_symlink_candidates(mount_root, remote_path, out, seen):
    parts = remote_path.strip("/").split("/")
    cur = mount_root
    for i, part in enumerate(parts):
        candidate = os.path.join(cur, part)
        if os.path.islink(candidate):
            target = os.readlink(candidate)
            if target.startswith("/"):
                rel_target = target.split("/lustre/", 1)[1] if target.startswith("/lustre/") else target.lstrip("/")
                rest = "/".join(parts[i + 1:])
                resolved = os.path.normpath(os.path.join(mount_root, rel_target, rest))
                if resolved not in seen:
                    seen.add(resolved)
                    out.append(resolved)
                return
        if not os.path.exists(candidate):
            return
        cur = candidate


def _local_candidates_for_remote_path(cluster_name, remote_path):
    roots = MOUNT_MAP.get(cluster_name, [])
    remote_bases = MOUNT_REMOTE_MAP.get(cluster_name, [])
    if not roots or not remote_path:
        return []
    rp = str(remote_path).strip()
    if not rp.startswith("/"):
        return []
    out = []
    seen = set()

    # With MOUNT_REMOTE_MAP: strip the remote base to get the relative path
    for i, root in enumerate(roots):
        if i < len(remote_bases) and remote_bases[i]:
            base = remote_bases[i].rstrip("/")
            if rp.startswith(base + "/"):
                rel = rp[len(base):].lstrip("/")
                cand = os.path.normpath(os.path.join(root, rel))
                if cand not in seen:
                    seen.add(cand)
                    out.append(cand)
            elif rp == base:
                if root not in seen:
                    seen.add(root)
                    out.append(root)

    # Check mount_aliases (symlink paths that resolve to the same mount)
    if not out:
        for alias, idx in MOUNT_ALIASES.get(cluster_name, []):
            alias = alias.rstrip("/")
            if idx < len(roots) and rp.startswith(alias + "/"):
                rel = rp[len(alias):].lstrip("/")
                cand = os.path.normpath(os.path.join(roots[idx], rel))
                if cand not in seen:
                    seen.add(cand)
                    out.append(cand)
            elif idx < len(roots) and rp == alias:
                if roots[idx] not in seen:
                    seen.add(roots[idx])
                    out.append(roots[idx])

    # Fallback: old-style whole-mount (no MOUNT_REMOTE_MAP entry)
    if not out:
        suffixes = [rp.lstrip("/")]
        if rp.startswith("/lustre/"):
            suffixes.append(rp.split("/lustre/", 1)[1])
        for root in roots:
            for suf in suffixes:
                cand = os.path.normpath(os.path.join(root, suf))
                if cand not in seen:
                    seen.add(cand)
                    out.append(cand)
    return out


def resolve_mounted_path(cluster_name, remote_path, want_dir=False):
    if not remote_path:
        return ""
    checker = os.path.isdir if want_dir else os.path.isfile
    if remote_path.startswith("/home/") and checker(remote_path):
        return remote_path
    if cluster_name != "local" and not _is_cluster_mount_ok(cluster_name):
        return ""
    for cand in _local_candidates_for_remote_path(cluster_name, remote_path):
        if checker(cand):
            return cand
    return ""


def resolve_file_path(cluster, remote_path):
    """Return (local_path_or_None, source)."""
    if cluster == "local":
        return (remote_path if os.path.isfile(remote_path) else None), "local"
    mounted = resolve_mounted_path(cluster, remote_path, want_dir=False)
    if mounted:
        return mounted, "mount"
    return None, "ssh"


def list_local_dir(path):
    entries = []
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        entries.append({
            "name": name,
            "path": full,
            "is_dir": os.path.isdir(full),
            "size": os.path.getsize(full) if os.path.isfile(full) else None,
        })
    return entries


def prefetch_nested_dir_cache_local(cluster, request_path, local_base_path, entries, limit=8):
    try:
        warmed = 0
        for e in entries:
            if warmed >= limit:
                break
            if not e.get("is_dir"):
                continue
            name = e.get("name", "")
            if not name:
                continue
            child_req_path = request_path.rstrip("/") + "/" + name
            child_local_path = os.path.join(local_base_path, name)
            if not os.path.isdir(child_local_path):
                continue
            child_entries = list_local_dir(child_local_path)
            if cluster != "local":
                for ce in child_entries:
                    ce["path"] = child_req_path.rstrip("/") + "/" + ce["name"]
            payload = {
                "status": "ok",
                "path": child_req_path,
                "entries": child_entries,
                "source": "local" if cluster == "local" else "mount",
                "resolved_path": child_local_path,
            }
            _cache_set(_dir_list_cache, (cluster, child_req_path), payload)
            warmed += 1
    except Exception:
        pass


def cluster_mount_status(cluster_name):
    roots = MOUNT_MAP.get(cluster_name, [])
    active = mounted_roots(cluster_name)
    return {
        "cluster": cluster_name,
        "mounted": bool(active),
        "root": active[0] if active else (_resolve(roots[0]) if roots else ""),
        "roots": [_resolve(r) for r in roots],
        "active_roots": active,
    }


def all_mount_status():
    return {name: cluster_mount_status(name) for name in CLUSTERS if name != "local"}


def mounted_root(cluster_name):
    """Return the first mounted root for a cluster, or empty string."""
    mps = _proc_mount_points()
    for r in MOUNT_MAP.get(cluster_name, []):
        p = _resolve(r)
        if p in mps:
            return p
    return ""


def mounted_roots(cluster_name):
    """Return all currently mounted roots for a cluster."""
    if cluster_name != "local" and not _is_cluster_mount_ok(cluster_name):
        return []
    mps = _proc_mount_points()
    out = []
    for r in MOUNT_MAP.get(cluster_name, []):
        p = _resolve(r)
        if p in mps:
            out.append(p)
    return out


def remote_path_from_mounted(cluster_name, local_path):
    """Convert a local mount path back to its remote equivalent.

    With indexed mounts, each root maps to a specific remote path
    from MOUNT_REMOTE_MAP.
    """
    roots = MOUNT_MAP.get(cluster_name, [])
    remote_paths = MOUNT_REMOTE_MAP.get(cluster_name, [])
    lp = os.path.abspath(local_path)

    mps = _proc_mount_points()
    for i, root in enumerate(roots):
        rp = _resolve(root)
        if rp not in mps:
            continue
        try:
            rel = os.path.relpath(lp, rp)
        except Exception:
            continue
        if rel.startswith(".."):
            continue
        if i < len(remote_paths) and remote_paths[i]:
            base = remote_paths[i].rstrip("/")
            if rel == ".":
                return base
            return base + "/" + rel
        if rel == ".":
            return "/"
        return "/" + rel.lstrip("/")
    return ""


def find_job_logs_on_mount(cluster_name, job_id):
    """Check local mounts for log files matching job_id.

    Uses a targeted approach instead of walking the whole nemo-run tree
    (which is too slow over sshfs). Scans top-level nemo-run subdirs,
    and for each checks at most 3 levels deep for sbatch scripts.
    """
    if cluster_name != "local" and not _is_cluster_mount_ok(cluster_name):
        return None

    roots = mounted_roots(cluster_name)
    if not roots:
        return None

    job_str = str(job_id)
    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    output_re = re.compile(r'#SBATCH\s+--output=(\S+)')

    for root in roots:
        nemo_run = os.path.join(root, "nemo-run")
        if not os.path.isdir(nemo_run):
            continue

        try:
            run_dirs = os.listdir(nemo_run)
        except Exception:
            continue

        for run_name in run_dirs:
            result = _scan_run_dir_for_job(
                cluster_name, root,
                os.path.join(nemo_run, run_name),
                job_str, output_re, allowed_suffixes,
            )
            if result:
                return result

    return None


def _scan_run_dir_for_job(cluster_name, mount_root, run_dir, job_str,
                          output_re, allowed_suffixes, max_depth=3):
    """Scan a single nemo-run/<name>/ dir for sbatch scripts matching job_str.

    Uses targeted listdir at each level instead of os.walk to avoid
    slow sshfs round-trips for irrelevant subtrees.
    """
    return _scan_dir_recursive(
        cluster_name, mount_root, run_dir, job_str,
        output_re, allowed_suffixes, 0, max_depth,
    )


def _scan_dir_recursive(cluster_name, mount_root, path, job_str,
                         output_re, allowed_suffixes, depth, max_depth):
    if depth > max_depth:
        return None
    try:
        entries = os.listdir(path)
    except Exception:
        return None

    sbatch_files = [e for e in entries if e.endswith("sbatch.sh")]
    subdirs = []

    for sb_name in sbatch_files:
        result = _check_sbatch_for_job(
            cluster_name, mount_root,
            os.path.join(path, sb_name),
            job_str, output_re, allowed_suffixes,
        )
        if result:
            return result

    if depth < max_depth:
        for e in entries:
            full = os.path.join(path, e)
            if e.startswith("."):
                continue
            try:
                if os.path.isdir(full):
                    subdirs.append(full)
            except Exception:
                continue

        for sub in subdirs:
            result = _scan_dir_recursive(
                cluster_name, mount_root, sub, job_str,
                output_re, allowed_suffixes, depth + 1, max_depth,
            )
            if result:
                return result

    return None


def _check_sbatch_for_job(cluster_name, mount_root, sbatch_path, job_str,
                           output_re, allowed_suffixes):
    """Read a sbatch script, extract --output path, check for job files."""
    try:
        with open(sbatch_path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = output_re.search(line)
                if not m:
                    continue
                out_pattern = m.group(1).replace("%j", job_str)
                log_dir = os.path.dirname(out_pattern)
                local_log_dir = _resolve_log_dir_on_mount(
                    cluster_name, mount_root, log_dir,
                )
                if not local_log_dir or not os.path.isdir(local_log_dir):
                    return None

                matched_files = []
                try:
                    for lf in os.listdir(local_log_dir):
                        if job_str not in lf:
                            continue
                        if not lf.lower().endswith(allowed_suffixes):
                            continue
                        full = os.path.join(local_log_dir, lf)
                        rp = remote_path_from_mounted(cluster_name, full)
                        if rp:
                            matched_files.append(rp)
                except Exception:
                    return None

                if not matched_files:
                    return None

                from .logs import label_and_sort_files
                files = label_and_sort_files(list(dict.fromkeys(matched_files)))
                rp_dir = remote_path_from_mounted(cluster_name, local_log_dir)
                dirs = []
                if rp_dir:
                    dirs = [{"label": "output", "path": os.path.dirname(rp_dir)}]
                return {"files": files, "dirs": dirs}
    except Exception:
        pass
    return None


def _resolve_log_dir_on_mount(cluster_name, mount_root, remote_log_dir):
    """Try to find remote_log_dir under mount_root.

    The log dir from sbatch is an absolute remote path. We need to find
    it under one of our mount roots.
    """
    roots = MOUNT_MAP.get(cluster_name, [])
    remote_paths = MOUNT_REMOTE_MAP.get(cluster_name, [])

    mps = _proc_mount_points()
    for i, root in enumerate(roots):
        rp = _resolve(root)
        if rp not in mps:
            continue
        if i < len(remote_paths) and remote_paths[i]:
            remote_base = remote_paths[i].rstrip("/")
            if remote_log_dir.startswith(remote_base + "/"):
                rel = remote_log_dir[len(remote_base):].lstrip("/")
                local = os.path.join(rp, rel)
                if os.path.isdir(local):
                    return local
            elif remote_log_dir.startswith(remote_base):
                return rp

    return ""


def run_mount_script(action, cluster="all"):
    if action not in {"mount", "unmount"}:
        return False, "Invalid action."
    if cluster != "all" and (cluster not in CLUSTERS or cluster == "local"):
        return False, "Unknown cluster."
    script = os.path.abspath(MOUNT_SCRIPT_PATH)
    if not os.path.isfile(script):
        return False, f"Mount script not found: {script}"
    cmd = [script, action]
    if cluster and cluster != "all":
        cmd.append(cluster)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if proc.returncode != 0:
            msg = "\n".join(x for x in [out, err] if x).strip() or f"{action} failed"
            return False, msg
        return True, out or f"{action} completed"
    except Exception as e:
        return False, str(e)


# ── Mount health check ──────────────────────────────────────────────────────

MOUNT_HEALTH_INTERVAL = 120  # check every 2 minutes


def _test_mount_alive(mount_path, timeout_sec=4):
    """Test if a FUSE mount is responsive by listing it in a subprocess with timeout."""
    try:
        proc = subprocess.run(
            ["ls", mount_path],
            capture_output=True, timeout=timeout_sec,
        )
        return proc.returncode == 0
    except (subprocess.TimeoutExpired, Exception):
        return False


def _remount_cluster(cluster_name):
    """Force-unmount stale FUSE and remount a cluster."""
    mps = _proc_mount_points()
    for mp in mps:
        if f"/mounts/{cluster_name}/" in mp:
            try:
                subprocess.run(["fusermount", "-uz", mp],
                               capture_output=True, timeout=10)
            except Exception:
                pass
    time.sleep(1)
    ok, msg = run_mount_script("mount", cluster_name)
    return ok, msg


def mount_health_check():
    """Check all mounted clusters and remount any stale ones."""
    mps = _proc_mount_points()
    checked = 0
    remounted = 0
    for cluster_name in list(CLUSTERS.keys()):
        if cluster_name == "local":
            continue

        active = [mp for mp in mps if f"/mounts/{cluster_name}/" in mp]
        if not active:
            continue

        checked += 1
        test_path = active[0]
        if _test_mount_alive(test_path):
            continue

        _mount_ok[cluster_name] = False
        _mount_ok_ts[cluster_name] = time.monotonic()
        log.warning("Stale mount detected for %s (%s), remounting…", cluster_name, test_path)
        ok, msg = _remount_cluster(cluster_name)
        if ok:
            log.info("Remounted %s: %s", cluster_name, msg)
            _mount_ok[cluster_name] = True
            _mount_ok_ts[cluster_name] = time.monotonic()
            remounted += 1
        else:
            log.warning("Remount failed for %s: %s", cluster_name, msg)

    return checked, remounted


def mount_health_loop():
    """Background loop: periodically check mount health and auto-remount."""
    time.sleep(60)
    while True:
        try:
            checked, remounted = mount_health_check()
            if remounted:
                log.info("Mount health: checked %d, remounted %d", checked, remounted)
        except Exception as e:
            log.warning("Mount health check error: %s", e)
        time.sleep(MOUNT_HEALTH_INTERVAL)
