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
  -s3=false -webdav=false -admin.ui=false &
PIDS+=($!)

./weed s3 \
  -filer 127.0.0.1:8888 \
  -config s3.json \
  -ip 127.0.0.1 \
  -ip.bind 127.0.0.1 \
  -port 8334 \
  -port.lance 9102 &
PIDS+=($!)

./weed-vector \
  --namespace http://127.0.0.1:9102 \
  --masters 127.0.0.1:9333 \
  --storage-option aws_access_key_id=test \
  --storage-option aws_secret_access_key=test \
  --listen 127.0.0.1:9301 &
PIDS+=($!)

# Any HTTP response (even an auth rejection) means the lance catalog is up.
for _ in $(seq 1 60); do
  code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9102/v1/namespace/root/list || true)
  [ "$code" != "000" ] && break
  sleep 1
done
[ "${code:-000}" != "000" ] || { echo "lance catalog on :9102 never came up" >&2; exit 1; }

for _ in $(seq 1 60); do
  (exec 3<>/dev/tcp/127.0.0.1/9301) 2>/dev/null && break
  sleep 1
done
(exec 3<>/dev/tcp/127.0.0.1/9301) 2>/dev/null || { echo "vector server on :9301 never came up" >&2; exit 1; }

python3 -m pytest -xvs s3vectors_test.py
