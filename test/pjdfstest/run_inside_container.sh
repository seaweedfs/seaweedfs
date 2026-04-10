#!/usr/bin/env bash
#
# Runs pjdfstest inside the mount container against /mnt/seaweedfs.
# Invoked via: docker compose exec mount /run.sh
set -euo pipefail

MOUNT_DIR="/mnt/seaweedfs"
PJDFSTEST_TESTS="${PJDFSTEST_TESTS:-tests/}"

# Copy pjdfstest into the mounted filesystem so tests exercise the FS under test.
TEST_ROOT="${MOUNT_DIR}/pjdfstest-root"
mkdir -p "${TEST_ROOT}"
cp -r /opt/pjdfstest/. "${TEST_ROOT}/"

echo "==> Running pjdfstest (${PJDFSTEST_TESTS})"
cd "${TEST_ROOT}"
prove -rv "${PJDFSTEST_TESTS}"
