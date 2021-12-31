# Running S3 Compatibility tests against SeaweedFS

This is using [the tests from CephFS](https://github.com/ceph/s3-tests).

## Prerequisites

- have Docker installed
- this has been executed on Mac. On Linux, the hostname in `s3tests.conf`  needs to be adjusted.

## Running tests

- `./prepare.sh` to build the docker image
- `./run.sh` to execute all tests
