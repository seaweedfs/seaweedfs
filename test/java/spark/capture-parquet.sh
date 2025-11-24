#!/bin/bash
# Run Spark test and capture the Parquet file before cleanup

echo "Starting SeaweedFS services..."
docker compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer
sleep 10

echo "Running Spark test in background..."
docker compose run --rm -e SEAWEEDFS_TEST_ENABLED=true spark-tests bash -c "mvn test -Dtest=SparkSQLTest#testCreateTableAndQuery 2>&1" > /tmp/spark-test-capture.log &
TEST_PID=$!

echo "Monitoring for Parquet file creation..."
while kill -0 $TEST_PID 2>/dev/null; do
    # Check if employees directory exists
    FILES=$(curl -s http://localhost:8888/test-spark/employees/ 2>/dev/null | grep -o 'part-[^"]*\.parquet' || echo "")
    if [ -n "$FILES" ]; then
        echo "Found Parquet file(s)!"
        for FILE in $FILES; do
            echo "Downloading: $FILE"
            curl -s "http://localhost:8888/test-spark/employees/$FILE" > "/tmp/$FILE"
            FILE_SIZE=$(stat -f%z "/tmp/$FILE" 2>/dev/null || stat --format=%s "/tmp/$FILE" 2>/dev/null)
            echo "Downloaded $FILE: $FILE_SIZE bytes"
            
            if [ -f "/tmp/$FILE" ] && [ $FILE_SIZE -gt 0 ]; then
                echo "SUCCESS: Captured $FILE"
                echo "Installing parquet-tools..."
                pip3 install -q parquet-tools 2>/dev/null || echo "parquet-tools might already be installed"
                
                echo ""
                echo "=== Parquet File Metadata ==="
                python3 -m parquet_tools meta "/tmp/$FILE" || echo "parquet-tools failed"
                
                echo ""
                echo "=== File Header (first 100 bytes) ==="
                hexdump -C "/tmp/$FILE" | head -10
                
                echo ""
                echo "=== File Footer (last 100 bytes) ==="
                tail -c 100 "/tmp/$FILE" | hexdump -C
                
                kill $TEST_PID 2>/dev/null
                exit 0
            fi
        done
    fi
    sleep 0.5
done

echo "Test completed, checking logs..."
tail -50 /tmp/spark-test-capture.log
