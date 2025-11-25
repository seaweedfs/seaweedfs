#!/bin/bash
set -e

echo "=========================================="
echo "Testing All Three Debug Modes"
echo "=========================================="
echo ""

cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs/test/java/spark

# Mode 1: SEAWEED_ONLY (default)
echo "=== MODE 1: SEAWEED_ONLY ==="
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true \
  spark-tests bash -c 'cd /workspace && mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1' \
  | grep -E "Tests run|BUILD SUCCESS|BUILD FAILURE|EOFException" | tail -5
echo ""

# Mode 2: LOCAL_ONLY
echo "=== MODE 2: LOCAL_ONLY ==="
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true \
  -e SEAWEEDFS_DEBUG_MODE=LOCAL_ONLY \
  -e SEAWEEDFS_DEBUG_DIR=/workspace/target/debug-local \
  spark-tests bash -c 'mkdir -p /workspace/target/debug-local && cd /workspace && mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1' \
  | grep -E "Tests run|BUILD SUCCESS|BUILD FAILURE|EOFException|length is too low" | tail -5
echo ""

# Mode 3: DUAL_COMPARE
echo "=== MODE 3: DUAL_COMPARE ==="
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true \
  -e SEAWEEDFS_DEBUG_MODE=DUAL_COMPARE \
  -e SEAWEEDFS_DEBUG_DIR=/workspace/target/debug-dual \
  spark-tests bash -c 'mkdir -p /workspace/target/debug-dual && cd /workspace && mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1' \
  | grep -E "Tests run|BUILD SUCCESS|BUILD FAILURE|EOFException" | tail -5
echo ""

echo "=========================================="
echo "Test Summary"
echo "=========================================="
