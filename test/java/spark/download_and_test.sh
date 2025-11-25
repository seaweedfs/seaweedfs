#!/bin/bash
set -e

echo "=== Downloading Parquet file and testing with multiple readers ==="
echo ""

# Start services if not running
docker compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer 2>&1 | grep -v "Running"
sleep 3

# Write a file using Spark
echo "1. Writing Parquet file with Spark..."
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true spark-tests bash -c '
cd /workspace
# Run the test that writes a file
mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1 | tail -20
' > /tmp/spark_write.log 2>&1 &
WRITE_PID=$!

# Wait a bit for file to be written
sleep 8

# Find and download the file from the temporary directory
echo "2. Finding Parquet file in temporary directory..."
TEMP_FILE=$(docker compose exec -T seaweedfs-filer sh -c '
find /data -name "*.parquet" -type f 2>/dev/null | grep -v "_SUCCESS" | head -1
' 2>&1 | tr -d '\r')

if [ -z "$TEMP_FILE" ]; then
    echo "Waiting for file to be written..."
    sleep 5
    TEMP_FILE=$(docker compose exec -T seaweedfs-filer sh -c '
    find /data -name "*.parquet" -type f 2>/dev/null | grep -v "_SUCCESS" | head -1
    ' 2>&1 | tr -d '\r')
fi

if [ -z "$TEMP_FILE" ]; then
    echo "ERROR: No Parquet file found!"
    echo "Checking what files exist..."
    docker compose exec -T seaweedfs-filer sh -c 'find /data -type f 2>/dev/null | head -20'
    wait $WRITE_PID
    exit 1
fi

echo "Found: $TEMP_FILE"

# Copy file from container
echo "3. Copying file from container..."
docker compose cp seaweedfs-filer:$TEMP_FILE /tmp/spark_written.parquet 2>&1 | grep -v "Successfully"

# Also try to get it via HTTP
echo "4. Also downloading via HTTP API..."
# Get the file path relative to /data
REL_PATH=$(echo $TEMP_FILE | sed 's|/data||')
curl -s "http://localhost:8888${REL_PATH}" -o /tmp/spark_written_http.parquet 2>&1

# Use whichever file is larger/valid
if [ -f /tmp/spark_written.parquet ] && [ -s /tmp/spark_written.parquet ]; then
    cp /tmp/spark_written.parquet /tmp/test.parquet
    echo "Using file copied from container"
elif [ -f /tmp/spark_written_http.parquet ] && [ -s /tmp/spark_written_http.parquet ]; then
    cp /tmp/spark_written_http.parquet /tmp/test.parquet
    echo "Using file downloaded via HTTP"
else
    echo "ERROR: Failed to get file!"
    exit 1
fi

FILE_SIZE=$(stat -f%z /tmp/test.parquet 2>/dev/null || stat --format=%s /tmp/test.parquet 2>/dev/null)
echo "Got file: $FILE_SIZE bytes"
echo ""

# Kill the write process
kill $WRITE_PID 2>/dev/null || true
wait $WRITE_PID 2>/dev/null || true

# Now test with various readers
echo "=== Testing with Multiple Parquet Readers ==="
echo ""

# 1. Check magic bytes
echo "1. Magic Bytes Check:"
echo -n "   First 4 bytes: "
head -c 4 /tmp/test.parquet | xxd -p
echo -n "   Last 4 bytes: "
tail -c 4 /tmp/test.parquet | xxd -p

FIRST=$(head -c 4 /tmp/test.parquet | xxd -p)
LAST=$(tail -c 4 /tmp/test.parquet | xxd -p)
if [ "$FIRST" = "50415231" ] && [ "$LAST" = "50415231" ]; then
    echo "   ✅ Valid PAR1 magic bytes"
else
    echo "   ❌ Invalid magic bytes!"
fi
echo ""

# 2. Python pyarrow
echo "2. Testing with Python pyarrow:"
python3 << 'PYEOF'
try:
    import pyarrow.parquet as pq
    table = pq.read_table('/tmp/test.parquet')
    print(f"   ✅ SUCCESS: Read {table.num_rows} rows, {table.num_columns} columns")
    print(f"   Schema: {table.schema}")
    print(f"   First row: {table.to_pandas().iloc[0].to_dict()}")
except Exception as e:
    print(f"   ❌ FAILED: {e}")
PYEOF
echo ""

# 3. DuckDB
echo "3. Testing with DuckDB:"
python3 << 'PYEOF'
try:
    import duckdb
    conn = duckdb.connect(':memory:')
    result = conn.execute("SELECT * FROM '/tmp/test.parquet'").fetchall()
    print(f"   ✅ SUCCESS: Read {len(result)} rows")
    print(f"   Data: {result}")
except Exception as e:
    print(f"   ❌ FAILED: {e}")
PYEOF
echo ""

# 4. Pandas
echo "4. Testing with Pandas:"
python3 << 'PYEOF'
try:
    import pandas as pd
    df = pd.read_parquet('/tmp/test.parquet')
    print(f"   ✅ SUCCESS: Read {len(df)} rows, {len(df.columns)} columns")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Data:\n{df}")
except Exception as e:
    print(f"   ❌ FAILED: {e}")
PYEOF
echo ""

# 5. Java ParquetReader (using our test container)
echo "5. Testing with Java ParquetReader:"
docker compose run --rm spark-tests bash -c '
cat > /tmp/ReadParquet.java << "JAVAEOF"
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.example.data.Group;

public class ReadParquet {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path("/tmp/test.parquet");
        
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf).build()) {
            Group group;
            int count = 0;
            while ((group = reader.read()) != null && count < 5) {
                System.out.println("   Row " + count + ": " + group);
                count++;
            }
            System.out.println("   ✅ SUCCESS: Read " + count + " rows");
        } catch (Exception e) {
            System.out.println("   ❌ FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
JAVAEOF

# Copy the file into container
cat > /tmp/test.parquet
' < /tmp/test.parquet 2>&1 | head -1

echo ""
echo "=== Summary ==="
echo "File size: $FILE_SIZE bytes"
echo "If all readers succeeded, the file is VALID."
echo "If readers failed, the footer metadata is corrupted."

