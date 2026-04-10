#!/usr/bin/env bash
#
# Run the pjdfstest POSIX compliance suite against a SeaweedFS FUSE mount.
#
# This script:
#   1. Starts a self-contained "weed mini" server (master+volume+filer in one)
#   2. Mounts the filesystem with "weed mount"
#   3. Builds pjdfstest from upstream and runs it under prove(1)
#
# Requirements: weed in $PATH, fusermount3, perl with TAP::Harness::Archive,
# autoconf, make, sudo (pjdfstest exercises chown/chmod which need root).
#
# Usage:
#   test/pjdfstest/run.sh                # runs full suite
#   PJDFSTEST_TESTS=tests/chmod ./run.sh # runs a subset

set -euo pipefail

WEED_BIN="${WEED_BIN:-weed}"
WORK_DIR="${WORK_DIR:-/tmp/seaweedfs-pjdfstest}"
MOUNT_DIR="${MOUNT_DIR:-${WORK_DIR}/mnt}"
DATA_DIR="${DATA_DIR:-${WORK_DIR}/data}"
LOG_DIR="${LOG_DIR:-${WORK_DIR}/logs}"
FILER_PORT="${FILER_PORT:-28888}"
FILER_ADDR="127.0.0.1:${FILER_PORT}"

# Pin to a known-good upstream commit so CI is reproducible. Override via env
# if you want to test against a fork (e.g. juicefs uses sanwan/pjdfstest).
PJDFSTEST_REPO="${PJDFSTEST_REPO:-https://github.com/pjd/pjdfstest.git}"
PJDFSTEST_REF="${PJDFSTEST_REF:-master}"
PJDFSTEST_TESTS="${PJDFSTEST_TESTS:-tests/}"

mini_pid=""
mount_pid=""

cleanup() {
  set +e
  if [[ -n "${mount_pid}" ]] && kill -0 "${mount_pid}" 2>/dev/null; then
    kill -TERM "${mount_pid}" 2>/dev/null || true
    wait "${mount_pid}" 2>/dev/null || true
  fi
  if mountpoint -q "${MOUNT_DIR}" 2>/dev/null; then
    fusermount3 -u "${MOUNT_DIR}" 2>/dev/null || \
      fusermount -u "${MOUNT_DIR}" 2>/dev/null || \
      sudo umount "${MOUNT_DIR}" 2>/dev/null || true
  fi
  if [[ -n "${mini_pid}" ]] && kill -0 "${mini_pid}" 2>/dev/null; then
    kill -TERM "${mini_pid}" 2>/dev/null || true
    wait "${mini_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

mkdir -p "${MOUNT_DIR}" "${DATA_DIR}" "${LOG_DIR}"

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

# Wait for filer to accept connections.
for i in $(seq 1 60); do
  if (echo > "/dev/tcp/127.0.0.1/${FILER_PORT}") 2>/dev/null; then
    break
  fi
  if ! kill -0 "${mini_pid}" 2>/dev/null; then
    echo "weed mini exited early; log tail:" >&2
    tail -n 100 "${LOG_DIR}/mini.log" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! (echo > "/dev/tcp/127.0.0.1/${FILER_PORT}") 2>/dev/null; then
  echo "weed mini filer did not become reachable within 30s; log tail:" >&2
  tail -n 100 "${LOG_DIR}/mini.log" >&2 || true
  exit 1
fi

echo "==> Mounting SeaweedFS at ${MOUNT_DIR}"
# allowOthers is required so that pjdfstest's setuid/setgid sub-tests (run via
# sudo) can access files created as the invoking user.
sudo "${WEED_BIN}" mount \
  -filer="${FILER_ADDR}" \
  -dir="${MOUNT_DIR}" \
  -filer.path=/ \
  -dirAutoCreate \
  -allowOthers=true \
  >"${LOG_DIR}/mount.log" 2>&1 &
mount_pid=$!

for i in $(seq 1 60); do
  if mountpoint -q "${MOUNT_DIR}"; then
    break
  fi
  if ! kill -0 "${mount_pid}" 2>/dev/null; then
    echo "weed mount exited early; log tail:" >&2
    tail -n 100 "${LOG_DIR}/mount.log" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! mountpoint -q "${MOUNT_DIR}"; then
  echo "FUSE mount did not come up within 30s" >&2
  tail -n 100 "${LOG_DIR}/mount.log" >&2 || true
  exit 1
fi

echo "==> Cloning and building pjdfstest"
PJDFSTEST_DIR="${WORK_DIR}/pjdfstest"
if [[ ! -d "${PJDFSTEST_DIR}/.git" ]]; then
  git clone "${PJDFSTEST_REPO}" "${PJDFSTEST_DIR}"
fi
git -C "${PJDFSTEST_DIR}" remote set-url origin "${PJDFSTEST_REPO}"
git -C "${PJDFSTEST_DIR}" fetch --depth 1 origin "${PJDFSTEST_REF}"
git -C "${PJDFSTEST_DIR}" checkout --detach FETCH_HEAD
(
  cd "${PJDFSTEST_DIR}"
  autoreconf -ifs
  ./configure
  make pjdfstest
)

# pjdfstest must run inside the filesystem under test.
TEST_ROOT="${MOUNT_DIR}/pjdfstest-root"
sudo mkdir -p "${TEST_ROOT}"
sudo cp -a "${PJDFSTEST_DIR}/." "${TEST_ROOT}/"

echo "==> Running pjdfstest (${PJDFSTEST_TESTS})"
cd "${TEST_ROOT}"
sudo prove -rv "${PJDFSTEST_TESTS}"
