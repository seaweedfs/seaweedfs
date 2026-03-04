#!/bin/sh
set -e

CENTRAL="http://$CENTRAL_FILER"
REPLICA="http://$REPLICA_FILER"

echo "=== Waiting for clusters to be ready ==="

wait_for_http() {
  local url="$1"
  local max_attempts=90
  for i in $(seq 1 $max_attempts); do
    HTTP_CODE=$(curl -so /dev/null -w "%{http_code}" --connect-timeout 2 "$url" 2>/dev/null) || HTTP_CODE="000"
    if [ "$HTTP_CODE" != "000" ]; then
      echo "$url is ready (HTTP $HTTP_CODE)"
      return 0
    fi
    if [ "$i" = "$max_attempts" ]; then
      echo "TIMEOUT waiting for $url"
      return 1
    fi
    echo "Waiting for $url... ($i/$max_attempts)"
    sleep 2
  done
}

wait_for_http "$CENTRAL"
wait_for_http "$REPLICA"

# Give filers time to fully initialize
sleep 5

echo "=== Setting up remote mount on replica ==="
# Create bucket directory on central via filer
curl -s -X POST "$CENTRAL/buckets/testbucket/" > /dev/null 2>&1 || true
sleep 2

# Verify central filer is working
echo "Verifying central filer..."
echo "hello" | curl -s -X POST -T - "$CENTRAL/buckets/testbucket/healthcheck.txt"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CENTRAL/buckets/testbucket/healthcheck.txt")
echo "Central filer healthcheck: HTTP $STATUS"

# Configure remote storage on replica (pointing at central's S3 endpoint)
echo "Configuring remote storage..."
echo 'remote.configure -name=central -type=s3 -s3.access_key=testkey -s3.secret_key=testsecret -s3.endpoint=http://central-s3:8333 -s3.region=us-east-1 -s3.force_path_style' | \
  weed shell -master="$REPLICA_MASTER" -filer="$REPLICA_FILER"

sleep 2

# Mount the bucket
echo "Mounting remote bucket..."
echo 'remote.mount -dir=/buckets/testbucket -remote=central/testbucket' | \
  weed shell -master="$REPLICA_MASTER" -filer="$REPLICA_FILER"

sleep 5

echo "=== Running functional tests ==="
FUNC_EXIT=0
/tests/test_ondemand.sh || FUNC_EXIT=$?

echo ""
echo "=== Running performance tests ==="
PERF_EXIT=0
/tests/test_performance.sh || PERF_EXIT=$?

echo ""
if [ "$FUNC_EXIT" -eq 0 ] && [ "$PERF_EXIT" -eq 0 ]; then
  echo "=== ALL TESTS PASSED ==="
  exit 0
else
  echo "=== SOME TESTS FAILED ==="
  exit 1
fi
