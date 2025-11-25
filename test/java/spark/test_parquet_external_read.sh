#!/bin/bash
set -e

echo "=== Testing if Parquet file can be read by external tools ==="

# Use our working ParquetMemoryComparisonTest to write a file
echo "1. Writing Parquet file with ParquetWriter (known to work)..."
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true spark-tests bash -c '
cd /workspace
mvn test -Dtest=ParquetMemoryComparisonTest#testCompareMemoryVsSeaweedFSParquet -q 2>&1 | tail -10
' > /tmp/write_test.log 2>&1

# The test writes to: /test-spark/comparison-test.parquet
echo "2. Downloading file from SeaweedFS..."
curl -s "http://localhost:8888/test-spark/comparison-test.parquet" -o /tmp/test.parquet

if [ ! -f /tmp/test.parquet ] || [ ! -s /tmp/test.parquet ]; then
    echo "ERROR: Failed to download file!"
    echo "Checking if file exists..."
    curl -s "http://localhost:8888/test-spark/?pretty=y"
    exit 1
fi

FILE_SIZE=$(stat -f%z /tmp/test.parquet 2>/dev/null || stat --format=%s /tmp/test.parquet 2>/dev/null)
echo "Downloaded $FILE_SIZE bytes"

# Install parquet-tools if needed
pip3 install -q parquet-tools 2>&1 | grep -v "Requirement already satisfied" || true

echo ""
echo "=== File Header (first 100 bytes) ==="
hexdump -C /tmp/test.parquet | head -10

echo ""
echo "=== File Footer (last 100 bytes) ==="
tail -c 100 /tmp/test.parquet | hexdump -C

echo ""
echo "=== Parquet Metadata ==="
parquet-tools inspect /tmp/test.parquet 2>&1 || echo "FAILED to inspect"

echo ""
echo "=== Try to read data ==="
parquet-tools show /tmp/test.parquet 2>&1 | head -20 || echo "FAILED to read data"

echo ""
echo "=== Conclusion ==="
if parquet-tools show /tmp/test.parquet > /dev/null 2>&1; then
    echo "✅ SUCCESS: File written to SeaweedFS can be read by parquet-tools!"
    echo "This proves the file format is valid."
else
    echo "❌ FAILED: File cannot be read by parquet-tools"
    echo "The file may be corrupted."
fi

