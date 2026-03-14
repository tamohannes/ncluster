"""Mount resolution, status, and path helpers."""

import os
import subprocess

from .config import (
    CLUSTERS, MOUNT_MAP, MOUNT_SCRIPT_PATH, MOUNT_LUSTRE_PREFIXES,
    _cache_set, _dir_list_cache,
)


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
    if not roots or not remote_path:
        return []
    rp = str(remote_path).strip()
    if not rp.startswith("/"):
        return []
    out = []
    seen = set()
    suffixes = [rp.lstrip("/")]
    if rp.startswith("/lustre/"):
        suffixes.append(rp.split("/lustre/", 1)[1])
    for root in roots:
        for suf in suffixes:
            cand = os.path.normpath(os.path.join(root, suf))
            if cand not in seen:
                seen.add(cand)
                out.append(cand)
        rel = rp.split("/lustre/", 1)[1] if rp.startswith("/lustre/") else rp.lstrip("/")
        _resolve_symlink_candidates(root, "/" + rel, out, seen)
    return out


def resolve_mounted_path(cluster_name, remote_path, want_dir=False):
    if not remote_path:
        return ""
    checker = os.path.isdir if want_dir else os.path.isfile
    if remote_path.startswith("/home/") and checker(remote_path):
        return remote_path
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
    mounted_root = ""
    for r in roots:
        p = os.path.abspath(os.path.expanduser(r))
        if os.path.ismount(p):
            mounted_root = p
            break
    return {
        "cluster": cluster_name,
        "mounted": bool(mounted_root),
        "root": mounted_root or (os.path.abspath(os.path.expanduser(roots[0])) if roots else ""),
        "roots": [os.path.abspath(os.path.expanduser(r)) for r in roots],
    }


def all_mount_status():
    return {name: cluster_mount_status(name) for name in CLUSTERS if name != "local"}


def mounted_root(cluster_name):
    for r in MOUNT_MAP.get(cluster_name, []):
        p = os.path.abspath(os.path.expanduser(r))
        if os.path.ismount(p):
            return p
    return ""


def remote_path_from_mounted(cluster_name, local_path):
    root = mounted_root(cluster_name)
    if not root:
        return ""
    lp = os.path.abspath(local_path)
    try:
        rel = os.path.relpath(lp, root)
    except Exception:
        return ""
    if rel == ".":
        return "/"
    return "/" + rel.lstrip("/")


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
