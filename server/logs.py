"""Log discovery, tail reading, and JSONL file operations."""

import json
import os
import re
import select
import shlex
import subprocess
import time
from glob import glob

from .config import (
    APP_ROOT, CLUSTERS, LOG_SEARCH_BASES,
    _dir_label,
    _cache_get, _cache_set,
    _log_index_cache, _log_content_cache, LOG_INDEX_TTL_SEC,
)
from .ssh import ssh_run, ssh_run_with_timeout
from .mounts import (
    resolve_mounted_path, resolve_file_path,
    mounted_root, remote_path_from_mounted,
)

from .crash_detect import detect_crash, detect_soft_failure  # noqa: F401 — re-exported for consumers

_PROGRESS_RE = re.compile(r'(\d{1,3})%(?:\||$|\s)', re.MULTILINE)


def extract_progress(content):
    if not content:
        return None
    matches = _PROGRESS_RE.findall(content[-4096:])
    if matches:
        pct = int(matches[-1])
        if 0 <= pct <= 100:
            return pct
    return None


def tail_local_file(path, lines):
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return "(empty file)"
            chunk = min(size, lines * 4096)
            f.seek(-chunk, 2)
            data = f.read()
            text = data.decode("utf-8", errors="replace")
            tail = text.split("\n")
            if len(tail) > lines:
                tail = tail[-lines:]
            return "\n".join(tail)
    except Exception as e:
        return f"Could not read local mounted file: {e}"


def _extract_arg_value(tokens, key):
    for idx, token in enumerate(tokens):
        if token == key and idx + 1 < len(tokens):
            return tokens[idx + 1]
        prefix = f"{key}="
        if token.startswith(prefix):
            return token[len(prefix):]
    return ""


def _safe_proc_readlink(path):
    try:
        return os.readlink(path)
    except Exception:
        return ""


def _is_regular_local_file_target(target):
    if not target:
        return False
    if target.startswith(("pipe:", "socket:", "anon_inode:")):
        return False
    if target.startswith("/dev/"):
        return False
    return os.path.isfile(target)


def _local_child_pids(pid):
    try:
        with open(f"/proc/{pid}/task/{pid}/children", "r", encoding="utf-8", errors="replace") as fh:
            raw = fh.read().strip()
        if not raw:
            return []
        return [p for p in raw.split() if p.isdigit()]
    except Exception:
        return []


def _collect_file_logs_from_pid(pid, seen_files):
    out = []
    proc_dir = f"/proc/{pid}"
    for fd, label in (("1", "stdout"), ("2", "stderr")):
        target = _safe_proc_readlink(f"{proc_dir}/fd/{fd}")
        if _is_regular_local_file_target(target) and target not in seen_files:
            seen_files.add(target)
            out.append({"label": label, "path": target})
    return out


def _read_local_procfd_snapshot(pid, fd_num, lines=200):
    path = f"/proc/{pid}/fd/{fd_num}"
    if not os.path.exists(path):
        return f"Process stream not found: {path}"
    fdesc = None
    chunks = []
    total = 0
    max_bytes = 256 * 1024
    try:
        fdesc = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
        deadline = time.monotonic() + 0.35
        while time.monotonic() < deadline and total < max_bytes:
            r, _, _ = select.select([fdesc], [], [], 0.05)
            if not r:
                break
            try:
                data = os.read(fdesc, 8192)
            except BlockingIOError:
                break
            if not data:
                break
            chunks.append(data)
            total += len(data)
    except Exception as e:
        return f"Could not read process stream: {e}"
    finally:
        if fdesc is not None:
            try:
                os.close(fdesc)
            except Exception:
                pass
    text = b"".join(chunks).decode("utf-8", errors="replace")
    if not text:
        return "(no buffered output captured from live process stream)"
    return "\n".join(text.splitlines()[-max(1, int(lines)):])


def _collect_recent_local_files(root, max_files=40):
    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    out = []
    if not root or not os.path.isdir(root):
        return out
    for cur, _, files in os.walk(root):
        rel = os.path.relpath(cur, root)
        depth = 0 if rel == "." else rel.count(os.sep) + 1
        if depth > 2:
            continue
        for name in files:
            lower = name.lower()
            if not lower.endswith(allowed_suffixes):
                continue
            full = os.path.join(cur, name)
            try:
                mtime = os.path.getmtime(full)
            except Exception:
                mtime = 0.0
            out.append((mtime, full))
    out.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in out[:max_files]]


def label_log(name):
    n = name.lower()
    if "main" in n and "srun" in n:    return "main output"
    if "server" in n and "srun" in n:  return "server output"
    if "sandbox" in n and "srun" in n: return "sandbox output"
    if "sbatch" in n:                  return "sbatch log"
    if n.endswith(".out"):             return "stdout"
    if n.endswith(".err"):             return "stderr"
    return name


def label_and_sort_files(paths):
    ORDER = {"main output": 0, "server output": 1, "sandbox output": 2, "sbatch log": 3, "sbatch stderr": 4}
    files = [{"label": label_log(os.path.basename(p)), "path": p} for p in paths]
    files.sort(key=lambda f: ORDER.get(f["label"], 10))
    return files


def local_job_log_files(job_id):
    if not str(job_id).isdigit():
        return {"files": [], "dirs": [], "error": "Local job id must be a PID."}
    pid = str(job_id)
    proc_dir = f"/proc/{pid}"
    if not os.path.isdir(proc_dir):
        return {"files": [], "dirs": [], "error": f"Local process {pid} is not running."}

    cmdline_bytes = b""
    try:
        with open(f"{proc_dir}/cmdline", "rb") as fh:
            cmdline_bytes = fh.read()
    except Exception:
        pass
    tokens = []
    if cmdline_bytes:
        tokens = [t.decode("utf-8", errors="replace") for t in cmdline_bytes.split(b"\x00") if t]
    else:
        ps = subprocess.run(["ps", "-p", pid, "-o", "args="], capture_output=True, text=True, timeout=3)
        raw = (ps.stdout or "").strip()
        if raw:
            try:
                tokens = shlex.split(raw)
            except Exception:
                tokens = raw.split()

    cwd = _safe_proc_readlink(f"{proc_dir}/cwd")
    discovered_dirs = []
    output_dir = _extract_arg_value(tokens, "--output_dir") or _extract_arg_value(tokens, "++output_dir")
    output_file = _extract_arg_value(tokens, "--output_file") or _extract_arg_value(tokens, "++output_file")

    if output_dir:
        if not output_dir.startswith("/") and cwd:
            output_dir = os.path.normpath(os.path.join(cwd, output_dir))
        discovered_dirs.append(output_dir)
    if output_file:
        if not output_file.startswith("/") and cwd:
            output_file = os.path.normpath(os.path.join(cwd, output_file))
        discovered_dirs.append(os.path.dirname(output_file))

    expanded_dirs = []
    for d in discovered_dirs:
        expanded_dirs.extend([d, os.path.join(d, "eval-logs"), os.path.join(d, "eval-results"), os.path.join(d, "tmp-eval-results")])

    seen_dirs = set()
    dirs = []
    for d in expanded_dirs:
        if not d:
            continue
        nd = os.path.normpath(d)
        if nd in seen_dirs:
            continue
        seen_dirs.add(nd)
        if os.path.isdir(nd):
            dirs.append({"label": _dir_label(nd), "path": nd})

    files = []
    seen_files = set()
    files.extend(_collect_file_logs_from_pid(pid, seen_files))
    if not files:
        for cpid in _local_child_pids(pid):
            files.extend(_collect_file_logs_from_pid(cpid, seen_files))

    for d in [x["path"] for x in dirs]:
        for p in _collect_recent_local_files(d):
            if p in seen_files:
                continue
            seen_files.add(p)
            files.append({"label": label_log(os.path.basename(p)), "path": p})

    if not files:
        for fd, lbl in (("1", "stdout"), ("2", "stderr")):
            target = _safe_proc_readlink(f"{proc_dir}/fd/{fd}")
            if target.startswith("pipe:"):
                files.append({"label": f"{lbl} stream", "path": f"procfd://{pid}/{fd}"})

    err = ""
    if not files and not dirs:
        err = "No local log files auto-discovered."
    return {"files": files, "dirs": dirs, "error": err}


def discover_job_logs_from_mount(cluster_name, job_id):
    """Deprecated — log discovery now uses SSH (scontrol/sacct).
    Mounts are only used for reading files, not discovering them."""
    return None


def _derive_result_dirs(files, cluster_name=None):
    """Find the output directory (parent of log dir) and return it as a
    browsable directory. The UI's tree browser handles subdirectory
    expansion, so we just need the root output path."""
    if not files:
        return []
    log_dir = os.path.dirname(files[0]["path"])
    output_dir = os.path.dirname(log_dir)
    if not output_dir or output_dir == log_dir:
        return []
    return [{"label": "output", "path": output_dir}]


def fetch_log_tail(cluster_name, log_path, lines=150):
    try:
        if cluster_name == "local":
            if str(log_path).startswith("procfd://"):
                try:
                    tail = str(log_path)[len("procfd://"):]
                    pid, fd = tail.split("/", 1)
                    return _read_local_procfd_snapshot(pid, fd, lines=lines)
                except Exception:
                    return "Invalid local process stream path."
            result = subprocess.run(["tail", f"-n{lines}", log_path], capture_output=True, text=True, timeout=5)
            return result.stdout or result.stderr or "(empty file)"
        mt = resolve_mounted_path(cluster_name, log_path, want_dir=False)
        if mt:
            return tail_local_file(mt, lines)
        cmd = f"[ -f '{log_path}' ] && tail -n {lines} '{log_path}' || echo '__NOT_FOUND__'"
        out, _ = ssh_run(cluster_name, cmd)
        if "__NOT_FOUND__" in out:
            return f"File not found on cluster:\n{log_path}"
        return out or "(empty file)"
    except Exception as e:
        return f"Could not read log: {e}"


def _db_log_path(cluster_name, job_id):
    """Look up the stored log_path for a job from the history DB."""
    try:
        import sqlite3
        from .config import DB_PATH
        con = sqlite3.connect(DB_PATH)
        row = con.execute(
            "SELECT log_path FROM job_history WHERE cluster=? AND job_id=?",
            (cluster_name, str(job_id)),
        ).fetchone()
        con.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return ""


def _try_local_discovery(cluster_name, job_id, db_path):
    """Try to discover log files from the local mount without SSH.

    If the log directory from the DB path is accessible via mount, list
    files locally. Returns None if mount is unavailable or no files found.
    """
    if not db_path:
        return None
    logdir = os.path.dirname(db_path)
    if not logdir:
        return None
    mounted_dir = resolve_mounted_path(cluster_name, logdir, want_dir=True)
    if not mounted_dir or not os.path.isdir(mounted_dir):
        return None

    ORDER = {"main output": 0, "server output": 1, "sandbox output": 2, "sbatch log": 3, "sbatch stderr": 4}
    allowed = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    sid = str(job_id)
    files = []
    jobid_files = []
    try:
        for name in sorted(os.listdir(mounted_dir)):
            if sid not in name:
                continue
            fpath = os.path.join(logdir, name)
            local = os.path.join(mounted_dir, name)
            if not os.path.isfile(local):
                continue
            if not name.lower().endswith(allowed):
                continue
            entry = {"label": label_log(name), "path": fpath}
            files.append(entry)
            jobid_files.append(entry)
    except OSError:
        return None

    if not files:
        return None
    files.sort(key=lambda f: ORDER.get(f["label"], 10))
    dirs = _derive_result_dirs(jobid_files, cluster_name)
    return {"files": files, "dirs": dirs}


def get_job_log_files(cluster_name, job_id):
    if cluster_name == "local":
        return local_job_log_files(job_id)

    db_path = _db_log_path(cluster_name, job_id)

    local = _try_local_discovery(cluster_name, job_id, db_path)
    if local:
        return local
    db_logdir_clause = ""
    if db_path:
        db_logdir = os.path.dirname(db_path).replace("'", "'\\''")
        db_logdir_clause = f"""
if [ -z "$LOGDIR" ]; then
  [ -d '{db_logdir}' ] && LOGDIR='{db_logdir}'
fi
"""

    script = f"""#!/bin/sh
JOB={job_id}
emit() {{ echo "FILE:$1:$2"; }}
LOGDIR=""
STDOUT=""
STDERR=""

SCTL=$(scontrol show job "$JOB" 2>/dev/null)
if [ -n "$SCTL" ]; then
  OWNER=$(echo "$SCTL" | tr ' ' '\\n' | grep '^UserId=' | head -1 | cut -d= -f2- | cut -d'(' -f1)
  if [ -n "$OWNER" ] && [ "$OWNER" != "$USER" ]; then
    SCTL=""
  fi
fi
if [ -n "$SCTL" ]; then
  STDOUT=$(echo "$SCTL" | tr ' ' '\\n' | grep '^StdOut=' | cut -d= -f2- | sed "s/%j/$JOB/g")
  STDERR=$(echo "$SCTL" | tr ' ' '\\n' | grep '^StdErr=' | cut -d= -f2- | sed "s/%j/$JOB/g")
  [ -n "$STDOUT" ] && LOGDIR=$(dirname "$STDOUT")
fi

if [ -z "$LOGDIR" ]; then
  STDOUT=$(sacct -u $USER -j "$JOB" --format=StdOut --noheader -P 2>/dev/null | head -1 | tr -d ' ')
  [ -n "$STDOUT" ] && LOGDIR=$(dirname "$(echo "$STDOUT" | sed "s/%j/$JOB/g")")
fi
{db_logdir_clause}
if [ -n "$LOGDIR" ] && [ -d "$LOGDIR" ]; then
  ls -1 "$LOGDIR" 2>/dev/null | grep "$JOB" | sort | while read NAME; do
    F="$LOGDIR/$NAME"
    [ -f "$F" ] && emit "$NAME" "$F"
  done
fi

[ -n "$STDOUT" ] && [ -f "$STDOUT" ] && emit "$(basename "$STDOUT")" "$STDOUT"
[ -n "$STDERR" ] && [ "$STDERR" != "$STDOUT" ] && [ -f "$STDERR" ] && emit "$(basename "$STDERR")" "$STDERR"
"""
    try:
        out, _ = ssh_run_with_timeout(cluster_name, script, timeout_sec=25)
    except Exception as e:
        return {"files": [], "dirs": [], "error": f"SSH error: {e}"}

    seen = set()
    files = []
    jobid_files = []
    ORDER = {"main output": 0, "server output": 1, "sandbox output": 2, "sbatch log": 3, "sbatch stderr": 4}
    allowed_suffixes = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")

    extra_dirs = []
    for line in out.splitlines():
        if line.startswith("DIR:"):
            dparts = line[4:].split(":", 1)
            if len(dparts) == 2:
                extra_dirs.append({"label": dparts[0].strip(), "path": dparts[1].strip()})
            continue
        if not line.startswith("FILE:"):
            continue
        parts = line[5:].split(":", 1)
        if len(parts) != 2:
            continue
        raw_label, path = parts[0].strip(), parts[1].strip()
        if not path or path in seen:
            continue
        if not path.lower().endswith(allowed_suffixes):
            continue
        seen.add(path)
        entry = {"label": label_log(raw_label), "path": path}
        files.append(entry)
        if str(job_id) in os.path.basename(path):
            jobid_files.append(entry)

    files.sort(key=lambda f: ORDER.get(f["label"], 10))
    dirs = _derive_result_dirs(jobid_files, cluster_name) + extra_dirs

    if not files and not dirs:
        fallback = _search_log_bases(cluster_name, job_id)
        if fallback:
            files = fallback.get("files", [])
            dirs = fallback.get("dirs", [])

    return {"files": files, "dirs": dirs}


def _search_log_bases(cluster_name, job_id):
    """Fallback: search log_search_bases for a directory matching the job's run name."""
    if cluster_name == "local":
        return None
    search_script = f"""#!/bin/sh
JNAME=$(sacct -j {job_id} --format=JobName%-200 --noheader -P 2>/dev/null | head -1 | tr -d ' ')
[ -z "$JNAME" ] && exit 0
RUNNAME=$(echo "$JNAME" | sed 's/^[^_]*_//')
[ -z "$RUNNAME" ] && exit 0
for BASE in {" ".join(f'"{b}"' for b in LOG_SEARCH_BASES)}; do
  [ -d "$BASE" ] || continue
  FOUND=$(find "$BASE" -maxdepth 3 -type d -name "$RUNNAME" 2>/dev/null | head -1)
  if [ -n "$FOUND" ] && [ -d "$FOUND" ]; then
    for F in "$FOUND"/*.log "$FOUND"/*.out "$FOUND"/*.err "$FOUND"/*.json "$FOUND"/*.jsonl; do
      [ -f "$F" ] && echo "FILE:$(basename "$F"):$F"
    done
    for SUB in eval-logs eval-results output; do
      [ -d "$FOUND/$SUB" ] && echo "DIR:$SUB:$FOUND/$SUB"
    done
    # Check one level of subdirs for log files
    for SD in "$FOUND"/*/; do
      [ -d "$SD" ] || continue
      SDNAME=$(basename "$SD")
      for F in "$SD"/*.log "$SD"/*.out "$SD"/*.jsonl; do
        [ -f "$F" ] && echo "FILE:$SDNAME/$(basename "$F"):$F"
      done
    done
    break
  fi
done
"""
    try:
        out, _ = ssh_run_with_timeout(cluster_name, search_script, timeout_sec=20)
    except Exception:
        return None
    if not out.strip():
        return None

    files = []
    dirs = []
    seen = set()
    allowed = (".log", ".out", ".err", ".txt", ".json", ".jsonl", ".jsonl-async", ".md")
    for line in out.splitlines():
        if line.startswith("DIR:"):
            parts = line[4:].split(":", 1)
            if len(parts) == 2:
                dirs.append({"label": parts[0].strip(), "path": parts[1].strip()})
        elif line.startswith("FILE:"):
            parts = line[5:].split(":", 1)
            if len(parts) == 2:
                raw_label, path = parts[0].strip(), parts[1].strip()
                if path and path not in seen and path.lower().endswith(allowed):
                    seen.add(path)
                    files.append({"label": label_log(raw_label), "path": path})
    return {"files": files, "dirs": dirs} if (files or dirs) else None


def get_job_log_files_cached(cluster_name, job_id, force=False):
    key = (cluster_name, str(job_id))
    if not force:
        cached = _cache_get(_log_index_cache, key, LOG_INDEX_TTL_SEC)
        if cached is not None:
            return cached
    value = get_job_log_files(cluster_name, str(job_id))
    if not value.get("error"):
        _cache_set(_log_index_cache, key, value)
    return value


# ─── JSONL readers ───────────────────────────────────────────────────────────

def read_jsonl_index(filepath, preview_chars=150, limit=100, mode="last"):
    try:
        if mode == "first" and limit == 0:
            # Count-only mode: just return total line count, no records.
            try:
                wc = subprocess.run(["wc", "-l", filepath],
                                    capture_output=True, text=True, timeout=15)
                total = int(wc.stdout.strip().split()[0]) if wc.stdout.strip() else 0
            except Exception:
                total = 0
            return {"status": "ok", "total": total, "count": 0,
                    "mode": mode, "limit": 0, "records": []}

        if mode == "first" and limit > 0:
            # Fast path: use head to read only first N lines.
            # Skip json.loads validation (just check starts with '{').
            # Skip wc -l (total returned as -1 = unknown).
            records = []
            try:
                result = subprocess.run(
                    ["head", f"-n{limit}", filepath],
                    capture_output=True, text=True, timeout=10,
                )
                raw_lines = result.stdout.split("\n")
            except Exception:
                raw_lines = []
                with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
                    for line in fh:
                        raw_lines.append(line)
                        if len(raw_lines) >= limit:
                            break
            for i, line in enumerate(raw_lines):
                stripped = line.strip()
                if not stripped:
                    continue
                records.append({
                    "line": i,
                    "preview": stripped[:preview_chars],
                    "valid": stripped.startswith("{"),
                    "size": len(stripped),
                })
            return {"status": "ok", "total": -1, "count": len(records),
                    "mode": mode, "limit": limit, "records": records}

        # For "last" or "all": must scan the full file.
        all_records = []
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                stripped = line.strip()
                if not stripped:
                    continue
                all_records.append({
                    "line": i,
                    "preview": stripped[:preview_chars],
                    "valid": stripped.startswith("{"),
                    "size": len(stripped),
                })

        total = len(all_records)
        if mode == "all" or limit <= 0:
            records = all_records
        else:
            records = all_records[-limit:]
        return {"status": "ok", "total": total, "count": len(records),
                "mode": mode, "limit": limit, "records": records}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def read_jsonl_record(filepath, line_num):
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                if i == line_num:
                    return {"status": "ok", "line": line_num, "content": line.strip()}
        return {"status": "error", "error": f"Line {line_num} not found"}
    except Exception as e:
        return {"status": "error", "error": str(e)}
