#!/usr/bin/env bash

set -ex

killall -9 weed || echo "already stopped"
rm -Rf tmp
mkdir tmp
docker stop s3test-instance || echo "already stopped"

ulimit -n 10000
../../../weed/weed server -filer -s3 -volume.max 0   -master.volumeSizeLimitMB 5 -dir "$(pwd)/tmp" 1>&2>weed.log &

until $(curl --output /dev/null --silent --head --fail http://127.0.0.1:9333); do
    printf '.'
    sleep 5
done
sleep 3

rm -Rf logs-full.txt logs-summary.txt
# docker run --name s3test-instance --rm -e S3TEST_CONF=s3tests.conf -v `pwd`/s3tests.conf:/s3-tests/s3tests.conf -it s3tests    ./virtualenv/bin/nosetests s3tests_boto3/functional/test_s3.py:test_get_obj_tagging -v  -a 'resource=object,!bucket-policy,!versioning,!encryption'
docker run --name s3test-instance --rm -e S3TEST_CONF=s3tests.conf -v `pwd`/s3tests.conf:/s3-tests/s3tests.conf -it s3tests    ./virtualenv/bin/nosetests s3tests_boto3/functional/test_s3.py -v  -a 'resource=object,!bucket-policy,!versioning,!encryption' | sed -n -e '/botocore.hooks/!p;//q' | tee logs-summary.txt

docker stop s3test-instance || echo "already stopped"
killall -9 weed
