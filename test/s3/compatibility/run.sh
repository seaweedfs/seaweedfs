#!/usr/bin/env bash

CONTAINER_NAME=${CONTAINER_NAME:-s3test-instance}
CONF_FILE=${CONF_FILE:-s3tests.conf}
WEED_BIN=${WEED_BIN:-../../../weed/weed}
TEST_RAW_OUTPUT_FILE=${TEST_RAW_OUTPUT_FILE:-compat.raw.txt}
TEST_PROCESSED_OUTPUT_FILE=${TEST_PROCESSED_OUTPUT_FILE:-compat.summary.txt}

# Set up debugging for this bash script if DEBUG is set
if [ -n "${DEBUG}" ]; then
    echo -e "DEBUG set [${DEBUG}], enabling debugging output...";
    set -ex
fi

# Reset from possible previous test run
killall -9 weed || echo "already stopped"
rm -Rf tmp
mkdir tmp
docker stop $CONTAINER_NAME || echo "already stopped"

# Ensure ulimit is set to reasonable value
ulimit -n 10000

# Start weed w/ filer + s3 in the background
$WEED_BIN server \
          -filer \
          -s3 \
          -volume.max 0 \
          -master.volumeSizeLimitMB 5 \
          -dir "$(pwd)/tmp" \
          1>&2>weed.log &

# Wait for master to start up
echo -e "\n[info] waiting for master @ 9333...";
until curl --output /dev/null --silent --head --fail http://127.0.0.1:9333; do
    printf '.';
    sleep 5;
done
sleep 3;

# Wait for s3 to start up
echo -e "\n[info] waiting for S3 @ 8333...";
until curl --output /dev/null --silent --fail http://127.0.0.1:8333; do
    printf '.';
    sleep 5;
done
sleep 3;

# Determine whether docker net
DOCKER_NET_HOST_ARGS=""
if [ -n "${DOCKER_NET_HOST}" ]; then
    DOCKER_NET_HOST_ARGS="--net=host"
    echo -e "\n[info] setting docker to het nost"
fi

echo -e "\n[warn] You may have to run with UNFILTERED=y to disable output filtering, if you get the broken pipe error";
echo -e "\n[info] running tests with unfiltered output...";
docker run \
       --name $CONTAINER_NAME \
       --rm \
       ${DOCKER_NET_HOST_ARGS} \
       -e S3TEST_CONF=$CONF_FILE \
       -v "$(pwd)"/$CONF_FILE:/s3-tests/s3tests.conf \
       -it \
       s3tests \
       ./virtualenv/bin/nosetests \
       s3tests_boto3/functional/test_s3.py \
       -v \
       -a 'resource=object,!bucket-policy,!versioning,!encryption' \
    | tee ${TEST_RAW_OUTPUT_FILE}

# If the summary logs are present, process them
if [ -f "${TEST_RAW_OUTPUT_FILE}" ]; then
    cat ${TEST_RAW_OUTPUT_FILE} | sed -n -e '/botocore.hooks/!p;//q' | tee ${TEST_PROCESSED_OUTPUT_FILE}
    echo -e "\n[info] ✅ Successfully wrote processed output @ [${TEST_PROCESSED_OUTPUT_FILE}]";
    if [ -z "${TEST_KEEP_RAW_OUTPUT}" ]; then
        echo -e "\n[info] removing test raw output file @ [${TEST_RAW_OUTPUT_FILE}] (to disable this, set TEST_KEEP_RAW_OUTPUT=y)...";
        rm -rf ${TEST_RAW_OUTPUT_FILE};
    fi
else
    echo -e "\n[warn] failed to find raw output @ [${TEST_RAW_OUTPUT_FILE}]";
fi

echo -e "\n[info] stopping [${CONTAINER_NAME}] container...";
docker stop $CONTAINER_NAME || echo "[info] already stopped";

echo -e "\n[info] stopping seaweedfs processes (all, via kill -9)...";
killall -9 weed;
