#!/usr/bin/env bash
# Spin a mini cluster plus the vectors gateway and engine, run the boto3
# conformance subset, tear everything down.
set -euo pipefail
cd "$(dirname "$0")"

DATA=mini-data
rm -rf "$DATA"
PIDS=()
cleanup() {
  kill "${PIDS[@]}" 2>/dev/null || true
  rm -rf "$DATA"
}
trap cleanup EXIT

./weed mini -dir "$DATA" \
  -s3=false -webdav=false -admin.ui=false \
  -s3.port.lance 9101 &
PIDS+=($!)

./weed s3 \
  -filer 127.0.0.1:8888 \
  -ip 127.0.0.1 \
  -port 8334 \
  -port.lance 9102 \
  -port.vectors 9201 \
  -vectors.grpc 127.0.0.1:9301 &
PIDS+=($!)

./weed-vector-server \
  --namespace http://127.0.0.1:9101/lance \
  --listen 127.0.0.1:9301 &
PIDS+=($!)

for _ in $(seq 1 60); do
  curl -sf -X POST http://127.0.0.1:9101/v1/namespace/list >/dev/null 2>&1 && break
  sleep 1
done
sleep 3

python3 -m pytest -xvs s3vectors_test.py
