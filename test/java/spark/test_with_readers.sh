#!/bin/bash
set -e

echo "=== Testing Parquet file with multiple readers ==="
echo ""

# Start services
docker compose up -d 2>&1 | grep -v "Running"
sleep 2

# Run test and capture chunk ID
echo "1. Writing Parquet file and capturing chunk ID..."
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true spark-tests bash -c '
cd /workspace
mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1
' 2>&1 | tee /tmp/test_output.log | tail -20 &
TEST_PID=$!

# Wait for the file to be written
echo "2. Waiting for file write..."
sleep 10

# Extract chunk ID from logs
CHUNK_ID=$(grep "PARQUET FILE WRITTEN TO EMPLOYEES" /tmp/test_output.log | grep -oP 'CHUNKS: \[\K[^\]]+' | head -1)

if [ -z "$CHUNK_ID" ]; then
    echo "Waiting more..."
    sleep 5
    CHUNK_ID=$(grep "PARQUET FILE WRITTEN TO EMPLOYEES" /tmp/test_output.log | grep -oP 'CHUNKS: \[\K[^\]]+' | head -1)
fi

if [ -z "$CHUNK_ID" ]; then
    echo "ERROR: Could not find chunk ID in logs"
    echo "Log excerpt:"
    grep -E "PARQUET|CHUNKS|employees" /tmp/test_output.log | tail -20
    kill $TEST_PID 2>/dev/null || true
    exit 1
fi

echo "Found chunk ID: $CHUNK_ID"

# Download directly from volume server
echo "3. Downloading from volume server..."
curl -s "http://localhost:8080/$CHUNK_ID" -o /tmp/test.parquet

if [ ! -f /tmp/test.parquet ] || [ ! -s /tmp/test.parquet ]; then
    echo "ERROR: Download failed!"
    exit 1
fi

FILE_SIZE=$(stat -f%z /tmp/test.parquet 2>/dev/null || stat --format=%s /tmp/test.parquet 2>/dev/null)
echo "Downloaded: $FILE_SIZE bytes"
echo ""

# Kill test process
kill $TEST_PID 2>/dev/null || true
wait $TEST_PID 2>/dev/null || true

# Test with readers
echo "=== Testing with Multiple Parquet Readers ==="
echo ""

# Check magic bytes
echo "1. Magic Bytes:"
FIRST=$(head -c 4 /tmp/test.parquet | xxd -p)
LAST=$(tail -c 4 /tmp/test.parquet | xxd -p)
echo "   First 4 bytes: $FIRST"
echo "   Last 4 bytes: $LAST"
if [ "$FIRST" = "50415231" ] && [ "$LAST" = "50415231" ]; then
    echo "   ✅ Valid PAR1 magic"
else
    echo "   ❌ Invalid magic!"
fi
echo ""

# Python pyarrow
echo "2. Python pyarrow:"
python3 -c "
import pyarrow.parquet as pq
try:
    table = pq.read_table('/tmp/test.parquet')
    print(f'   ✅ Read {table.num_rows} rows, {table.num_columns} columns')
    print(f'   Data: {table.to_pandas().to_dict(\"records\")}')
except Exception as e:
    print(f'   ❌ FAILED: {e}')
" 2>&1
echo ""

# Pandas
echo "3. Pandas:"
python3 -c "
import pandas as pd
try:
    df = pd.read_parquet('/tmp/test.parquet')
    print(f'   ✅ Read {len(df)} rows')
    print(f'   Data:\n{df}')
except Exception as e:
    print(f'   ❌ FAILED: {e}')
" 2>&1
echo ""

# DuckDB
echo "4. DuckDB:"
python3 -c "
import duckdb
try:
    conn = duckdb.connect(':memory:')
    result = conn.execute('SELECT * FROM \"/tmp/test.parquet\"').fetchall()
    print(f'   ✅ Read {len(result)} rows')
    print(f'   Data: {result}')
except Exception as e:
    print(f'   ❌ FAILED: {e}')
" 2>&1
echo ""

echo "=== Summary ==="
echo "File: $FILE_SIZE bytes"
echo "If readers succeeded: File is VALID ✅"
echo "If readers failed: Footer metadata is corrupted ❌"

