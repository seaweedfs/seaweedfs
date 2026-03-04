#!/bin/sh
set -e

echo "=== Performance Test: On-Demand File Access ==="

# Generate test files of various sizes on central
for SIZE_MB in 1 10 50 100 400; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  echo "Uploading ${SIZE_MB}MB file to central..."
  dd if=/dev/urandom bs=1M count=$SIZE_MB 2>/dev/null | \
    curl -s -X PUT -T - "$CENTRAL_S3/testbucket/$FILENAME"
done

echo ""
echo "=== Cold Read (first access, triggers on-demand cache) ==="
for SIZE_MB in 1 10 50 100 400; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  START=$(date +%s%N)
  curl -s -o /dev/null "$REPLICA_S3/testbucket/$FILENAME"
  END=$(date +%s%N)
  DURATION_MS=$(( (END - START) / 1000000 ))
  SPEED_MBPS=$(( SIZE_MB * 1000 / (DURATION_MS + 1) ))
  echo "  ${SIZE_MB}MB cold read: ${DURATION_MS}ms (${SPEED_MBPS} MB/s)"
done

echo ""
echo "=== Warm Read (cached, served from local volumes) ==="
for SIZE_MB in 1 10 50 100 400; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  START=$(date +%s%N)
  curl -s -o /dev/null "$REPLICA_S3/testbucket/$FILENAME"
  END=$(date +%s%N)
  DURATION_MS=$(( (END - START) / 1000000 ))
  SPEED_MBPS=$(( SIZE_MB * 1000 / (DURATION_MS + 1) ))
  echo "  ${SIZE_MB}MB warm read: ${DURATION_MS}ms (${SPEED_MBPS} MB/s)"
done

echo ""
echo "=== LIST Performance ==="
# Upload 100 small files
echo "Uploading 100 files for LIST test..."
for i in $(seq 1 100); do
  echo "content_$i" | curl -s -X PUT -T - "$CENTRAL_S3/testbucket/listtest/file_$(printf '%03d' $i).txt" &
  [ $((i % 20)) -eq 0 ] && wait
done
wait
sleep 2

START=$(date +%s%N)
curl -s -o /dev/null "$REPLICA_S3/testbucket?list-type=2&prefix=listtest/"
END=$(date +%s%N)
DURATION_MS=$(( (END - START) / 1000000 ))
echo "  LIST 100 files (hybrid): ${DURATION_MS}ms"

echo ""
echo "=== Performance test complete ==="
