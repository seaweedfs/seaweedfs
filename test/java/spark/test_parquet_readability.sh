#!/bin/bash
set -e

echo "=== Testing if Parquet file written by Spark can be read by parquet-tools ==="

# Run the test to write a Parquet file
echo "1. Writing Parquet file with Spark..."
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true spark-tests bash -c '
cd /workspace
mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery -q 2>&1 | tail -5
' > /tmp/write_test.log 2>&1 || true

# Find the Parquet file that was written
echo "2. Finding Parquet file..."
PARQUET_FILE=$(docker compose run --rm spark-tests bash -c '
curl -s "http://seaweedfs-filer:8888/test-spark/employees/?pretty=y" | grep -oP "\"name\":\s*\"\K[^\"]+\.parquet" | head -1
' 2>&1 | grep -v "Creating" | grep "\.parquet" | head -1)

if [ -z "$PARQUET_FILE" ]; then
    echo "ERROR: No Parquet file found!"
    exit 1
fi

echo "Found file: $PARQUET_FILE"

# Download the file
echo "3. Downloading file from SeaweedFS..."
curl -s "http://localhost:8888/test-spark/employees/$PARQUET_FILE" -o /tmp/test.parquet

if [ ! -f /tmp/test.parquet ] || [ ! -s /tmp/test.parquet ]; then
    echo "ERROR: Failed to download file!"
    exit 1
fi

FILE_SIZE=$(stat -f%z /tmp/test.parquet 2>/dev/null || stat --format=%s /tmp/test.parquet 2>/dev/null)
echo "Downloaded $FILE_SIZE bytes"

# Try to read with parquet-tools
echo "4. Reading with parquet-tools..."
pip3 install -q parquet-tools 2>&1 | grep -v "Requirement already satisfied" || true

echo ""
echo "=== Parquet Metadata ==="
parquet-tools inspect /tmp/test.parquet 2>&1 || echo "FAILED to inspect"

echo ""
echo "=== Try to read data ==="
parquet-tools show /tmp/test.parquet 2>&1 || echo "FAILED to read data"

echo ""
echo "=== Conclusion ==="
if parquet-tools show /tmp/test.parquet > /dev/null 2>&1; then
    echo "✅ SUCCESS: File can be read by parquet-tools!"
    echo "The file itself is VALID Parquet format."
    echo "The issue is specific to how Spark reads it back."
else
    echo "❌ FAILED: File cannot be read by parquet-tools"
    echo "The file is CORRUPTED or has invalid Parquet format."
fi

