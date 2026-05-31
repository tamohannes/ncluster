#!/usr/bin/env bash
set -euo pipefail

# Mount/unmount per-user cluster directories via sshfs.
# Each cluster has mount_paths[] in config.json — each path gets its own
# sshfs mount at ~/.clausius/mounts/<cluster>/<index>/.
#
#
# Usage:
#   ./scripts/sshfs_logs.sh mount
#   ./scripts/sshfs_logs.sh unmount
#   ./scripts/sshfs_logs.sh status
#   ./scripts/sshfs_logs.sh mount <cluster>
#   ./scripts/sshfs_logs.sh unmount <cluster>
#   ./scripts/sshfs_logs.sh status <cluster>

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../conf/config.json"

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: ${CONFIG_FILE}"
  echo "Copy config.example.json to config.json and fill in your cluster details."
  exit 1
fi

ACTION="${1:-status}"
BASE="${HOME}/.clausius/mounts"
USER_NAME="${CLAUSIUS_SSH_USER:-${USER:-}}"
if [[ -z "$USER_NAME" ]]; then
  USER_NAME="$(whoami)"
fi
TARGET_CLUSTER="${2:-all}"
FAILED=0
KEY_PATH="${CLAUSIUS_SSH_KEY:-${HOME}/.ssh/id_ed25519}"

_cluster_field() {
  python3 -c "
import json, sys
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
c = cfg.get('clusters', {}).get('$1', {})
print(c.get('$2', '${3:-}'))
"
}

_cluster_names() {
  python3 -c "
import json
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
for name in cfg.get('clusters', {}):
    print(name)
"
}

_cluster_mount_paths() {
  python3 -c "
import json, os
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
c = cfg.get('clusters', {}).get('$1', {})
paths = c.get('mount_paths', [])
user = os.environ.get('CLAUSIUS_SSH_USER') or os.environ.get('USER') or 'user'
for p in paths:
    print(p.replace('\$USER', user))
"
}

mount_cluster() {
  local c="$1"
  local host; host="$(_cluster_field "$c" host)"
  local port; port="$(_cluster_field "$c" port 22)"
  local ssh_cmd
  ssh_cmd="ssh -F ${HOME}/.ssh/config -o BatchMode=yes -o IdentitiesOnly=yes -o PreferredAuthentications=publickey -o StrictHostKeyChecking=accept-new -p ${port}"
  # Shell ssh (not the sftp ssh_command) for the pre-mount path probe below.
  local probe_ssh="ssh -F ${HOME}/.ssh/config -o BatchMode=yes -o ConnectTimeout=15 -o IdentitiesOnly=yes -o PreferredAuthentications=publickey -o StrictHostKeyChecking=accept-new -o IdentityFile=${KEY_PATH} -p ${port}"

  local idx=0
  local rc=0
  while IFS= read -r remote_path; do
    [[ -z "$remote_path" ]] && continue
    local target="${BASE}/${c}/${idx}"

    # Skip only if the mount is actually alive. A prior sshfs that was SIGKILLed
    # (e.g. on `systemctl restart`, which kills the service cgroup) leaves a dead
    # "Transport endpoint is not connected" FUSE stub here; sshfs then refuses to
    # mount over it. Detect that and force-clean before (re)mounting. `ls -d` is
    # an O(1) getattr, so this stays fast even for huge home directories.
    if timeout 5 mountpoint -q "$target" 2>/dev/null && timeout 6 ls -d "$target" >/dev/null 2>&1; then
      echo "[${c}/${idx}] already mounted at ${target}"
      idx=$((idx + 1))
      continue
    fi
    fusermount -uz "$target" 2>/dev/null || umount -l "$target" 2>/dev/null || true
    mkdir -p "$target" 2>/dev/null || true

    # Resolve symlinks remotely: sshfs cannot use a symlink as its mount root
    # ("Not a directory"), and resolving also survives portfolio reorganizations
    # where the canonical user path is a symlink to the real location.
    local resolved
    resolved="$(${probe_ssh} "${USER_NAME}@${host}" "readlink -f -- '${remote_path}'" 2>/dev/null | head -n1)"
    if [[ -n "$resolved" && "$resolved" != "$remote_path" ]]; then
      echo "[${c}/${idx}] resolved ${remote_path} -> ${resolved}"
      remote_path="$resolved"
    fi

    echo "[${c}/${idx}] mounting ${host}:${remote_path} -> ${target}"
    local err_file; err_file="$(mktemp)"
    if sshfs "${USER_NAME}@${host}:${remote_path}" "$target" \
      -o ssh_command="${ssh_cmd}" \
      -o IdentityFile="${KEY_PATH}" \
      -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
      -o cache=yes,kernel_cache,auto_cache \
      -o attr_timeout=60,entry_timeout=60,negative_timeout=15 2>"$err_file"; then
      echo "[${c}/${idx}] ok"
    else
      local reason; reason="$(tail -n1 "$err_file" 2>/dev/null)"
      echo "[${c}/${idx}] mount failed (${remote_path})${reason:+: ${reason}}"
      rmdir "$target" 2>/dev/null || true
      rc=1
    fi
    rm -f "$err_file"
    idx=$((idx + 1))
  done < <(_cluster_mount_paths "$c")

  if [[ "$idx" -eq 0 ]]; then
    echo "[${c}] no mount_paths configured"
  fi
  return "$rc"
}

unmount_cluster() {
  local c="$1"
  local cluster_base="${BASE}/${c}"

  # Unmount old-style single mount (migration from remote_root)
  # Use -uz (lazy) to handle stale/unreachable FUSE mounts after reboot.
  if timeout 2 mountpoint -q "$cluster_base" 2>/dev/null; then
    echo "[${c}] unmounting old-style mount at ${cluster_base}"
    fusermount -uz "$cluster_base" 2>/dev/null || umount -l "$cluster_base" 2>/dev/null || true
  fi

  # Unmount indexed submounts
  if [[ -d "$cluster_base" ]]; then
    for sub in "$cluster_base"/*/; do
      [[ -d "$sub" ]] || continue
      if timeout 2 mountpoint -q "$sub" 2>/dev/null; then
        echo "[${c}] unmounting ${sub}"
        fusermount -uz "$sub" 2>/dev/null || umount -l "$sub" 2>/dev/null || true
      elif grep -qs " ${sub%/} fuse.sshfs" /proc/mounts 2>/dev/null; then
        echo "[${c}] force-unmounting stale ${sub}"
        fusermount -uz "$sub" 2>/dev/null || umount -l "$sub" 2>/dev/null || true
      fi
      rmdir "$sub" 2>/dev/null || true
    done
  fi
}

status_cluster() {
  local c="$1"
  local cluster_base="${BASE}/${c}"

  # Check old-style single mount
  if mountpoint -q "$cluster_base" 2>/dev/null; then
    echo "[${c}] old-style mount at ${cluster_base} (should migrate)"
    return
  fi

  local found=0
  if [[ -d "$cluster_base" ]]; then
    for sub in "$cluster_base"/*/; do
      [[ -d "$sub" ]] || continue
      if mountpoint -q "$sub"; then
        echo "[${c}] mounted: ${sub}"
        found=1
      fi
    done
  fi

  if [[ "$found" -eq 0 ]]; then
    echo "[${c}] not mounted"
  fi
}

cluster_exists() {
  python3 -c "
import json, sys
with open('${CONFIG_FILE}') as f:
    cfg = json.load(f)
sys.exit(0 if '$1' in cfg.get('clusters', {}) else 1)
"
}

if ! command -v sshfs >/dev/null 2>&1 && [[ "$ACTION" == "mount" ]]; then
  echo "sshfs is not installed. Install it first (e.g. sudo apt install sshfs)."
  exit 1
fi

mkdir -p "$BASE"

case "$ACTION" in
  mount)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      if ! mount_cluster "$TARGET_CLUSTER"; then
        echo "[${TARGET_CLUSTER}] mount failed"
        FAILED=1
      fi
    else
      while IFS= read -r c; do
        if ! mount_cluster "$c"; then
          echo "[${c}] mount failed"
          FAILED=1
        fi
      done < <(_cluster_names)
    fi
    ;;
  unmount)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      if ! unmount_cluster "$TARGET_CLUSTER"; then
        echo "[${TARGET_CLUSTER}] unmount failed"
        FAILED=1
      fi
    else
      while IFS= read -r c; do
        if ! unmount_cluster "$c"; then
          echo "[${c}] unmount failed"
          FAILED=1
        fi
      done < <(_cluster_names)
    fi
    ;;
  status)
    if [[ "$TARGET_CLUSTER" != "all" ]]; then
      cluster_exists "$TARGET_CLUSTER" || { echo "Unknown cluster: ${TARGET_CLUSTER}"; exit 2; }
      status_cluster "$TARGET_CLUSTER"
    else
      while IFS= read -r c; do status_cluster "$c"; done < <(_cluster_names)
    fi
    ;;
  *)
    echo "Unknown action: ${ACTION}"
    echo "Usage: $0 {mount|unmount|status} [cluster|all]"
    exit 2
    ;;
esac

if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
