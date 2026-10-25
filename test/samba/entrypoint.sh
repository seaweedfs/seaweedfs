#!/usr/bin/env bash
#
# Entrypoint for the samba test container.
#
# Mounts SeaweedFS over FUSE twice, both with distributed locking (-dlm) so the
# locking tests can exercise cross-mount write coordination:
#   - MOUNT_DIR  (/mnt/seaweedfs)  is exported over SMB by smbd
#   - MOUNT2_DIR (/mnt/seaweedfs2) is a second, independent mount of the same
#     filer used to contend with the SMB writer
#
# Both mounts see the same filer path, so .../share is the same data on each.
# smbd runs in the foreground (as root, which owns the mounts), so the share
# uses "force user = root".
set -euo pipefail

FILER="${FILER:-filer:8888}"
MOUNT_DIR="${MOUNT_DIR:-/mnt/seaweedfs}"
MOUNT2_DIR="${MOUNT2_DIR:-/mnt/seaweedfs2}"
SHARE_DIR="${MOUNT_DIR}/share"
STATE_DIR="${STATE_DIR:-/var/lib/samba-test}"
SMB_PORT="${SMB_PORT:-445}"
SMB_USER="${SMB_USER:-smbtest}"
SMB_PASS="${SMB_PASS:-smbtest}"

mkdir -p "${MOUNT_DIR}" "${MOUNT2_DIR}" \
  "${STATE_DIR}/private" "${STATE_DIR}/state" "${STATE_DIR}/cache" \
  "${STATE_DIR}/lock" "${STATE_DIR}/pid" "${STATE_DIR}/ncalrpc"

# mount_seaweedfs <mountpoint> <logfile> — mount with -dlm and wait for it.
mount_seaweedfs() {
  local dir="$1" log="$2"
  echo "==> Mounting SeaweedFS (${FILER}) at ${dir} with -dlm"
  weed -v=1 mount \
    -filer="${FILER}" \
    -dir="${dir}" \
    -filer.path=/ \
    -dirAutoCreate \
    -allowOthers \
    -dlm \
    >"${log}" 2>&1 &
  local pid=$!
  for _ in $(seq 1 120); do
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
  echo "FUSE mount ${dir} did not come up" >&2
  tail -n 100 "${log}" >&2 || true
  exit 1
}

mount_seaweedfs "${MOUNT_DIR}" /var/log/weed-mount.log
mount_seaweedfs "${MOUNT2_DIR}" /var/log/weed-mount2.log

mkdir -p "${SHARE_DIR}"
chmod 0777 "${SHARE_DIR}"

# --- configure and start smbd ----------------------------------------------
echo "==> Configuring Samba share on port ${SMB_PORT}"
sed -e "s#@SHARE_PATH@#${SHARE_DIR}#g" \
  -e "s#@STATE_DIR@#${STATE_DIR}#g" \
  -e "s#@SMB_PORT@#${SMB_PORT}#g" \
  -e "s#@FORCE_USER@#root#g" \
  /smb.conf.template >/etc/samba/smb.conf

id -u "${SMB_USER}" >/dev/null 2>&1 || useradd -M -s /usr/sbin/nologin "${SMB_USER}"
printf '%s\n%s\n' "${SMB_PASS}" "${SMB_PASS}" | smbpasswd -a -s "${SMB_USER}"

echo "==> Starting smbd"
exec smbd -F --no-process-group -s /etc/samba/smb.conf
