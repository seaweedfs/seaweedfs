#!/bin/sh
# Performance benchmarks for on-demand remote storage
# Uses filer HTTP API.

CENTRAL="http://$CENTRAL_FILER"
REPLICA="http://$REPLICA_FILER"

echo "=== Performance Test: On-Demand File Access ==="

# Generate test files of various sizes on central
for SIZE_MB in 1 10 50; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  echo "Uploading ${SIZE_MB}MB file to central..."
  dd if=/dev/urandom of="/tmp/$FILENAME" bs=1M count=$SIZE_MB 2>/dev/null
  curl -s -F "file=@/tmp/$FILENAME" "$CENTRAL/buckets/testbucket/" > /dev/null 2>&1
done

echo ""
echo "=== Cold Read (first access, triggers on-demand cache) ==="
for SIZE_MB in 1 10 50; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  START=$(date +%s%N)
  curl -s -o /dev/null "$REPLICA/buckets/testbucket/$FILENAME"
  END=$(date +%s%N)
  DURATION_MS=$(( (END - START) / 1000000 ))
  if [ "$DURATION_MS" -gt 0 ]; then
    SPEED_MBPS=$(( SIZE_MB * 1000 / DURATION_MS ))
  else
    SPEED_MBPS="N/A"
  fi
  echo "  ${SIZE_MB}MB cold read: ${DURATION_MS}ms (${SPEED_MBPS} MB/s)"
done

echo ""
echo "=== Warm Read (cached, served from local volumes) ==="
for SIZE_MB in 1 10 50; do
  FILENAME="perftest_${SIZE_MB}mb.bin"
  START=$(date +%s%N)
  curl -s -o /dev/null "$REPLICA/buckets/testbucket/$FILENAME"
  END=$(date +%s%N)
  DURATION_MS=$(( (END - START) / 1000000 ))
  if [ "$DURATION_MS" -gt 0 ]; then
    SPEED_MBPS=$(( SIZE_MB * 1000 / DURATION_MS ))
  else
    SPEED_MBPS="N/A"
  fi
  echo "  ${SIZE_MB}MB warm read: ${DURATION_MS}ms (${SPEED_MBPS} MB/s)"
done

echo ""
echo "=== LIST Performance ==="
# Upload 50 small files
echo "Uploading 50 files for LIST test..."
for i in $(seq 1 50); do
  echo "content_$i" > "/tmp/listfile_$(printf '%03d' $i).txt"
  curl -s -F "file=@/tmp/listfile_$(printf '%03d' $i).txt" "$CENTRAL/buckets/testbucket/listtest/" > /dev/null 2>&1 &
  [ $((i % 10)) -eq 0 ] && wait
done
wait
sleep 2

START=$(date +%s%N)
curl -s -o /dev/null "$REPLICA/buckets/testbucket/listtest/?pretty=y"
END=$(date +%s%N)
DURATION_MS=$(( (END - START) / 1000000 ))
echo "  LIST 50 files (hybrid): ${DURATION_MS}ms"

# Cleanup
rm -f /tmp/perftest_*.bin /tmp/listfile_*.txt

echo ""
echo "=== Performance test complete ==="
