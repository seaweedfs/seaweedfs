#!/usr/bin/env bash
# Starts a single-node SeaweedFS cluster for one cell of the digest matrix and
# waits until the master can assign a file id. The process ids go to
# $WORK_DIR/pids, so `xargs kill < "$WORK_DIR/pids"` stops the cell.
#
#   DIGEST_MODE    md5 (GODEBUG=fips140=off), sha256 (fips140=on with the
#                  filer's -digest=sha256) or fips140only (GODEBUG=fips140=only)
#   VOLUME_SERVER  go (weed server's own volume server) or rust (weed-volume)
#   WEED, WEED_VOLUME, WORK_DIR, S3_CONFIG  binaries, scratch dir, identities
#
# security.toml enables JWT signing for the volume servers, so every chunk
# write and read in the cell goes through a volume server's JWT check.
set -euo pipefail

: "${DIGEST_MODE:?md5, sha256 or fips140only}"
: "${VOLUME_SERVER:?go or rust}"
: "${S3_CONFIG:?path to the S3 identities file}"
WEED=${WEED:-weed}
WEED_VOLUME=${WEED_VOLUME:-weed-volume}
WORK_DIR=${WORK_DIR:-/tmp/seaweedfs-digest-matrix}

case "$DIGEST_MODE" in
  md5)         export GODEBUG=fips140=off; digest_flag=() ;;
  sha256)      export GODEBUG=fips140=on;  digest_flag=(-filer.digest=sha256) ;;
  fips140only) export GODEBUG=fips140=only; digest_flag=() ;;
  *) echo "unknown DIGEST_MODE $DIGEST_MODE" >&2; exit 2 ;;
esac
case "$VOLUME_SERVER" in
  go)   volume_flags=(-volume.port=8080 -volume.max=100 -volume.preStopSeconds=1) ;;
  rust) volume_flags=(-volume=false) ;;
  *) echo "unknown VOLUME_SERVER $VOLUME_SERVER" >&2; exit 2 ;;
esac

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/conf" "$WORK_DIR/data" "$WORK_DIR/volume"
cat > "$WORK_DIR/conf/security.toml" <<EOF
[jwt.signing]
key = "$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"

[jwt.signing.read]
key = "$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')"
EOF

cd "$WORK_DIR/conf"
# weed reads a flag's value from the environment variable named after it
# (s3.config from S3_CONFIG), so this script's inputs stay out of its env.
unset_inputs=(-u DIGEST_MODE -u VOLUME_SERVER -u WEED -u WEED_VOLUME -u WORK_DIR -u S3_CONFIG)
env "${unset_inputs[@]}" "$WEED" -v 1 server -filer -filer.maxMB=64 -s3 -ip=127.0.0.1 -ip.bind=0.0.0.0 \
  -dir="$WORK_DIR/data" -master.raftHashicorp -master.electionTimeout 1s \
  -master.volumeSizeLimitMB=100 -master.port=9333 -filer.port=8888 -s3.port=8000 \
  -s3.allowDeleteBucketNotEmpty=true -s3.config="$S3_CONFIG" -master.peers=none \
  "${volume_flags[@]}" "${digest_flag[@]}" > "$WORK_DIR/weed.log" 2>&1 &
echo "$!" > "$WORK_DIR/pids"

if [ "$VOLUME_SERVER" = rust ]; then
  env "${unset_inputs[@]}" "$WEED_VOLUME" --port 8080 --port.grpc 18080 --ip 127.0.0.1 --ip.bind 0.0.0.0 \
    --dir "$WORK_DIR/volume" --max 100 --master 127.0.0.1:9333 \
    --securityFile "$WORK_DIR/conf/security.toml" --preStopSeconds 1 \
    > "$WORK_DIR/volume.log" 2>&1 &
  echo "$!" >> "$WORK_DIR/pids"
fi

for _ in $(seq 1 60); do
  if curl -sf "http://127.0.0.1:9333/dir/assign" | grep -q '"fid"' \
     && curl -sf -o /dev/null "http://127.0.0.1:8888/" \
     && curl -s -o /dev/null "http://127.0.0.1:8000/"; then
    echo "cluster ready: DIGEST_MODE=$DIGEST_MODE VOLUME_SERVER=$VOLUME_SERVER"
    exit 0
  fi
  sleep 2
done
echo "cluster did not become ready" >&2
tail -50 "$WORK_DIR/weed.log" >&2 || true
[ -f "$WORK_DIR/volume.log" ] && tail -50 "$WORK_DIR/volume.log" >&2
exit 1
