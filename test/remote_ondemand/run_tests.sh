#!/bin/sh
set -e

echo "=== Waiting for clusters to be ready ==="
# Wait for both S3 gateways to respond
for endpoint in "$CENTRAL_S3" "$REPLICA_S3"; do
  for i in $(seq 1 30); do
    if curl -sf "$endpoint" >/dev/null 2>&1; then break; fi
    echo "Waiting for $endpoint... ($i/30)"
    sleep 2
  done
done

echo "=== Setting up remote mount on replica ==="
weed shell -master=$REPLICA_MASTER -filer=$REPLICA_FILER <<CMDS
remote.configure -name=central -type=s3 \
  -s3.endpoint=$CENTRAL_S3 \
  -s3.access_key=admin -s3.secret_key=admin
remote.mount -dir=/buckets/testbucket -remote=central/testbucket
CMDS

echo "=== Running functional tests ==="
/tests/test_ondemand.sh

echo "=== Running performance tests ==="
/tests/test_performance.sh

echo "=== ALL TESTS PASSED ==="
