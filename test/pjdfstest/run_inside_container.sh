#!/usr/bin/env bash
#
# Runs pjdfstest inside the mount container against /mnt/seaweedfs.
# Invoked via: docker compose exec mount /run.sh
#
# Uses known_failures.txt to skip tests with known issues. Any failure
# in a test NOT in the skip list causes a non-zero exit (regression).
set -euo pipefail

MOUNT_DIR="/mnt/seaweedfs"
PJDFSTEST_TESTS="${PJDFSTEST_TESTS:-tests/}"
KNOWN_FAILURES="/known_failures.txt"

# Copy pjdfstest into the mounted filesystem so tests exercise the FS under test.
TEST_ROOT="${MOUNT_DIR}/pjdfstest-root"
mkdir -p "${TEST_ROOT}"
cp -r /opt/pjdfstest/. "${TEST_ROOT}/"

cd "${TEST_ROOT}"

# Build the list of tests to run, excluding known failures.
if [[ -f "${KNOWN_FAILURES}" ]] && [[ "${PJDFSTEST_TESTS}" == "tests/" ]]; then
  mapfile -t skip < <(grep -v '^#' "${KNOWN_FAILURES}" | grep -v '^$')
  all_tests=()
  while IFS= read -r -d '' t; do
    all_tests+=("$t")
  done < <(find tests/ -name '*.t' -print0 | sort -z)

  run_tests=()
  for t in "${all_tests[@]}"; do
    is_skipped=false
    for s in "${skip[@]}"; do
      if [[ "$t" == "$s" ]]; then
        is_skipped=true
        break
      fi
    done
    if ! $is_skipped; then
      run_tests+=("$t")
    fi
  done

  echo "==> Running pjdfstest (${#run_tests[@]} tests, ${#skip[@]} skipped)"
  prove -rv "${run_tests[@]}"
else
  echo "==> Running pjdfstest (${PJDFSTEST_TESTS})"
  prove -rv "${PJDFSTEST_TESTS}"
fi
