#!/usr/bin/env bash
#
# Runs the Snowflake s3compat API test suite
# (https://github.com/snowflakedb/snowflake-s3compat-api-test-suite) against a
# locally-built SeaweedFS server.
#
# Required on PATH: weed (or WEED_BIN), aws, mvn, java, git.
#
# Env overrides:
#   WEED_BIN            path to the weed binary               (default: weed)
#   WORK_DIR            scratch dir for data + suite clone    (default: mktemp -d,
#                       removed on success, kept on failure)
#   MASTER_PORT         master http port                      (default: 9333)
#   VOLUME_PORT         volume http port                      (default: 8080)
#   FILER_PORT          filer http port                       (default: 8888)
#   S3_PORT             s3 endpoint port                      (default: 8333)
#   METRICS_PORT        metrics http port                     (default: 9324)
#   SKIP_SERVER_START   if set, do not start weed; prepare and run the suite
#                       against ENDPOINT_URL
#   SUITE_REPO          git url of the test suite             (default: upstream)
#   SUITE_REV           suite commit to check out             (default: pinned SHA)
#
# The suite env vars (BUCKET_NAME_1, PREFIX_FOR_PAGE_LISTING,
# PAGE_LISTING_TOTAL_SIZE, NOT_ACCESSIBLE_BUCKET) default to the same values
# prepare.sh uses.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

WEED_BIN="${WEED_BIN:-weed}"
MASTER_PORT="${MASTER_PORT:-9333}"
VOLUME_PORT="${VOLUME_PORT:-8080}"
FILER_PORT="${FILER_PORT:-8888}"
S3_PORT="${S3_PORT:-8333}"
METRICS_PORT="${METRICS_PORT:-9324}"
ENDPOINT_URL="${ENDPOINT_URL:-http://127.0.0.1:$S3_PORT}"
SUITE_REPO="${SUITE_REPO:-https://github.com/snowflakedb/snowflake-s3compat-api-test-suite.git}"
# Pinned upstream revision verified against SeaweedFS; bump deliberately.
SUITE_REV="${SUITE_REV:-8ae535b35fff0d8a72e21bba4e51281ac991cab9}"
WORK_DIR_CREATED=""
if [ -z "${WORK_DIR:-}" ]; then
  WORK_DIR="$(mktemp -d)"
  WORK_DIR_CREATED=1
fi

export BUCKET_NAME_1="${BUCKET_NAME_1:-sf-snowflake-test}"
export NOT_ACCESSIBLE_BUCKET="${NOT_ACCESSIBLE_BUCKET:-sf-denied-bucket}"
export PREFIX_FOR_PAGE_LISTING="${PREFIX_FOR_PAGE_LISTING:-test-suite/page-listing/}"
export PAGE_LISTING_TOTAL_SIZE="${PAGE_LISTING_TOTAL_SIZE:-1100}"
export ENDPOINT_URL

# The suite reads credentials from these variables.
export S3COMPAT_ACCESS_KEY="${S3COMPAT_ACCESS_KEY:-snowflake_compat_access}"
export S3COMPAT_SECRET_KEY="${S3COMPAT_SECRET_KEY:-snowflake_compat_secret}"
export AWS_ACCESS_KEY_ID="$S3COMPAT_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$S3COMPAT_SECRET_KEY"

WEED_PID=""
cleanup() {
  status=$?
  if [ -n "$WEED_PID" ]; then
    kill "$WEED_PID" 2>/dev/null || true
    sleep 2
    kill -9 "$WEED_PID" 2>/dev/null || true
  fi
  if [ -n "$WORK_DIR_CREATED" ]; then
    if [ "$status" -eq 0 ]; then
      rm -rf "$WORK_DIR"
    else
      echo "Work dir kept for debugging: $WORK_DIR" >&2
    fi
  fi
}
trap cleanup EXIT

wait_for_url() {
  local url="$1" name="$2"
  for i in $(seq 1 30); do
    if curl -s "$url" > /dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    echo "Waiting for $name... ($i/30)"
    sleep 2
  done
  echo "ERROR: $name did not become ready" >&2
  return 1
}

if [ -z "${SKIP_SERVER_START:-}" ]; then
  WEED_DATA_DIR="$WORK_DIR/data"
  mkdir -p "$WEED_DATA_DIR"

  echo "Starting SeaweedFS (data dir: $WEED_DATA_DIR)"
  "$WEED_BIN" server -filer -filer.maxMB=64 -s3 -ip 127.0.0.1 -ip.bind 127.0.0.1 \
    -dir="$WEED_DATA_DIR" \
    -master.raftHashicorp -master.electionTimeout 1s -master.volumeSizeLimitMB=5000 \
    -volume.max=4 -volume.preStopSeconds=1 \
    -master.peers=none \
    -master.port="$MASTER_PORT" -volume.port="$VOLUME_PORT" -filer.port="$FILER_PORT" -s3.port="$S3_PORT" \
    -metricsPort="$METRICS_PORT" \
    -s3.allowDeleteBucketNotEmpty=true \
    -s3.autoCreateBucket=false \
    -s3.port.iceberg=0 -s3.port.lance=0 \
    -s3.config="$SCRIPT_DIR/s3.json" \
    > "$WORK_DIR/weed.log" 2>&1 &
  WEED_PID=$!

  wait_for_url "http://127.0.0.1:$MASTER_PORT/cluster/status" "Master server"
  wait_for_url "http://127.0.0.1:$VOLUME_PORT/status" "Volume server"
  wait_for_url "http://127.0.0.1:$FILER_PORT/" "Filer"
  wait_for_url "$ENDPOINT_URL/" "S3 API"
  echo "All SeaweedFS components are ready"
fi

"$SCRIPT_DIR/prepare.sh"

SUITE_DIR="$WORK_DIR/snowflake-s3compat-api-test-suite"
if [ ! -d "$SUITE_DIR" ]; then
  git init -q "$SUITE_DIR"
  git -C "$SUITE_DIR" remote add origin "$SUITE_REPO"
  git -C "$SUITE_DIR" fetch -q --depth 1 origin "$SUITE_REV"
  git -C "$SUITE_DIR" checkout -q FETCH_HEAD
fi

# The suite forces virtual-hosted style bucket addressing, which needs wildcard
# DNS (<bucket>.<endpoint>) that does not exist for a local endpoint. Switch it
# to path-style access, which SeaweedFS supports.
STORAGE_CLIENT="$SUITE_DIR/s3compatapi/src/main/java/com/snowflake/s3compatapitestsuite/compatapi/S3CompatStorageClient.java"
sed -i.bak 's/setPathStyleAccess(false)/setPathStyleAccess(true)/' "$STORAGE_CLIENT"
rm -f "$STORAGE_CLIENT.bak"
grep -q 'setPathStyleAccess(true)' "$STORAGE_CLIENT"

cd "$SUITE_DIR/s3compatapi"

END_POINT="$ENDPOINT_URL" \
REGION_1=us-east-1 \
REGION_2=us-west-2 \
mvn -B test -Dtest=S3CompatApiTest
