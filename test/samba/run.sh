#!/usr/bin/env bash
#
# Run the SMB (Samba) integration test against a SeaweedFS FUSE mount.
#
# Pipeline:
#   1. start a self-contained "weed mini" (master + volume + filer in one)
#   2. mount the filesystem with "weed mount"
#   3. export a subdirectory of the mount over SMB with smbd
#   4. drive the share with smbclient (test/samba/smb_tests.sh)
#
# Everything runs as the current user on unprivileged ports, so no sudo is
# required. State lives under a temp work dir and is removed on exit.
#
# Requirements: weed in $PATH, fusermount3, and Samba's smbd / smbclient /
# smbpasswd (Debian/Ubuntu: apt-get install samba smbclient).
#
# Usage:
#   test/samba/run.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEED_BIN="${WEED_BIN:-weed}"
WORK_DIR="${WORK_DIR:-$(mktemp -d /tmp/seaweedfs-samba.XXXXXX)}"
MOUNT_DIR="${MOUNT_DIR:-${WORK_DIR}/mnt}"
MOUNT2_DIR="${MOUNT2_DIR:-${WORK_DIR}/mnt2}"
DATA_DIR="${DATA_DIR:-${WORK_DIR}/data}"
LOG_DIR="${LOG_DIR:-${WORK_DIR}/logs}"
STATE_DIR="${WORK_DIR}/samba"
SHARE_DIR="${MOUNT_DIR}/share"
SHARE_DIR2="${MOUNT2_DIR}/share"

FILER_PORT="${FILER_PORT:-28888}"
FILER_ADDR="127.0.0.1:${FILER_PORT}"
SMB_PORT="${SMB_PORT:-4450}"
SMB_SHARE="seaweedfs"
SMB_USER="${SMB_USER:-$(id -un)}"
SMB_PASS="${SMB_PASS:-seaweedfs}"

SMBD_BIN="$(command -v smbd || echo /usr/sbin/smbd)"
SMBPASSWD_BIN="$(command -v smbpasswd || echo /usr/bin/smbpasswd)"

CI_LOG_DIR="/tmp/seaweedfs-samba-logs"

mini_pid=""
mount_pid=""
mount2_pid=""
smbd_pid=""

unmount_dir() {
  local dir="$1"
  if mountpoint -q "${dir}" 2>/dev/null; then
    fusermount3 -u "${dir}" 2>/dev/null ||
      fusermount -u "${dir}" 2>/dev/null || true
  fi
}

cleanup() {
  set +e
  if [[ -n "${smbd_pid}" ]] && kill -0 "${smbd_pid}" 2>/dev/null; then
    kill -TERM "${smbd_pid}" 2>/dev/null || true
    wait "${smbd_pid}" 2>/dev/null || true
  fi
  for p in "${mount_pid}" "${mount2_pid}"; do
    if [[ -n "${p}" ]] && kill -0 "${p}" 2>/dev/null; then
      kill -TERM "${p}" 2>/dev/null || true
      wait "${p}" 2>/dev/null || true
    fi
  done
  unmount_dir "${MOUNT_DIR}"
  unmount_dir "${MOUNT2_DIR}"
  if [[ -n "${mini_pid}" ]] && kill -0 "${mini_pid}" 2>/dev/null; then
    kill -TERM "${mini_pid}" 2>/dev/null || true
    wait "${mini_pid}" 2>/dev/null || true
  fi
  # Copy logs to a fixed path for CI artifact upload.
  mkdir -p "${CI_LOG_DIR}"
  cp "${LOG_DIR}"/*.log "${LOG_DIR}"/*.out "${STATE_DIR}/smbd.log" "${CI_LOG_DIR}/" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

mkdir -p "${MOUNT_DIR}" "${MOUNT2_DIR}" "${DATA_DIR}" "${LOG_DIR}" \
  "${STATE_DIR}/private" "${STATE_DIR}/state" "${STATE_DIR}/cache" \
  "${STATE_DIR}/lock" "${STATE_DIR}/pid" "${STATE_DIR}/ncalrpc"

# --- 1. weed mini -----------------------------------------------------------
echo "==> Starting weed mini on ${FILER_ADDR}"
"${WEED_BIN}" mini \
  -dir="${DATA_DIR}" \
  -ip=127.0.0.1 \
  -filer.port="${FILER_PORT}" \
  -s3=false \
  -webdav=false \
  -admin.ui=false \
  >"${LOG_DIR}/mini.log" 2>&1 &
mini_pid=$!

for i in $(seq 1 60); do
  if (echo >"/dev/tcp/127.0.0.1/${FILER_PORT}") 2>/dev/null; then
    break
  fi
  if ! kill -0 "${mini_pid}" 2>/dev/null; then
    echo "weed mini exited early; log tail:" >&2
    tail -n 100 "${LOG_DIR}/mini.log" >&2 || true
    exit 1
  fi
  sleep 0.5
done
if ! (echo >"/dev/tcp/127.0.0.1/${FILER_PORT}") 2>/dev/null; then
  echo "weed mini filer did not become reachable within 30s; log tail:" >&2
  tail -n 100 "${LOG_DIR}/mini.log" >&2 || true
  exit 1
fi

# --- 2. weed mount (two mounts, both with -dlm) -----------------------------
# mount_with_dlm <mountpoint> <logfile> <pid-var-name>
mount_with_dlm() {
  local dir="$1" log="$2" pidvar="$3" pid
  echo "==> Mounting SeaweedFS at ${dir} with -dlm"
  "${WEED_BIN}" mount \
    -filer="${FILER_ADDR}" \
    -dir="${dir}" \
    -filer.path=/ \
    -dirAutoCreate \
    -dlm \
    >"${log}" 2>&1 &
  pid=$!
  printf -v "${pidvar}" '%s' "${pid}"
  for _ in $(seq 1 60); do
    if mountpoint -q "${dir}"; then
      return 0
    fi
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "weed mount (${dir}) exited early; log tail:" >&2
      tail -n 100 "${log}" >&2 || true
      exit 1
    fi
    sleep 0.5
  done
  echo "FUSE mount ${dir} did not come up within 30s" >&2
  tail -n 100 "${log}" >&2 || true
  exit 1
}

mount_with_dlm "${MOUNT_DIR}" "${LOG_DIR}/mount.log" mount_pid
mount_with_dlm "${MOUNT2_DIR}" "${LOG_DIR}/mount2.log" mount2_pid

mkdir -p "${SHARE_DIR}"

# --- 3. smbd ----------------------------------------------------------------
echo "==> Generating smb.conf and starting smbd on port ${SMB_PORT}"
SMB_CONF="${STATE_DIR}/smb.conf"
sed -e "s#@SHARE_PATH@#${SHARE_DIR}#g" \
  -e "s#@STATE_DIR@#${STATE_DIR}#g" \
  -e "s#@SMB_PORT@#${SMB_PORT}#g" \
  -e "s#@FORCE_USER@#${SMB_USER}#g" \
  "${SCRIPT_DIR}/smb.conf.template" >"${SMB_CONF}"

printf '%s\n%s\n' "${SMB_PASS}" "${SMB_PASS}" |
  "${SMBPASSWD_BIN}" -c "${SMB_CONF}" -a -s "${SMB_USER}"

"${SMBD_BIN}" -F --no-process-group -s "${SMB_CONF}" >"${LOG_DIR}/smbd.out" 2>&1 &
smbd_pid=$!

for i in $(seq 1 60); do
  if (echo >"/dev/tcp/127.0.0.1/${SMB_PORT}") 2>/dev/null; then
    break
  fi
  if ! kill -0 "${smbd_pid}" 2>/dev/null; then
    echo "smbd exited early; log tail:" >&2
    tail -n 100 "${LOG_DIR}/smbd.out" "${STATE_DIR}/smbd.log" 2>/dev/null >&2 || true
    exit 1
  fi
  sleep 0.5
done
if ! (echo >"/dev/tcp/127.0.0.1/${SMB_PORT}") 2>/dev/null; then
  echo "smbd did not become reachable within 30s; log tail:" >&2
  tail -n 100 "${LOG_DIR}/smbd.out" "${STATE_DIR}/smbd.log" 2>/dev/null >&2 || true
  exit 1
fi

# --- 4. run the test batteries ---------------------------------------------
rc=0

echo "==> Running SMB functional test battery"
SMB_HOST=127.0.0.1 \
  SMB_SHARE="${SMB_SHARE}" \
  SMB_PORT="${SMB_PORT}" \
  SMB_USER="${SMB_USER}" \
  SMB_PASS="${SMB_PASS}" \
  SHARE_FS_PATH="${SHARE_DIR}" \
  "${SCRIPT_DIR}/smb_tests.sh" || rc=1

echo "==> Running SMB locking / concurrency test battery"
SMB_HOST=127.0.0.1 \
  SMB_SHARE="${SMB_SHARE}" \
  SMB_PORT="${SMB_PORT}" \
  SMB_USER="${SMB_USER}" \
  SMB_PASS="${SMB_PASS}" \
  MOUNT_SHARE="${SHARE_DIR}" \
  MOUNT2_SHARE="${SHARE_DIR2}" \
  "${SCRIPT_DIR}/lock_tests.sh" || rc=1

exit "${rc}"
