# the tests only support python 3.6, not newer
#FROM ubuntu:latest
FROM nexus.stripchat.tech/python:slim-buster

# Installed required system deps
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y git-core tzdata tox

# Clone Ceph S3 tests
RUN git clone https://github.com/ceph/s3-tests.git

WORKDIR s3-tests

# Pin to a certain commit on ceph/s3-tests
# https://github.com/ceph/s3-tests/commit/9a6a1e9f197fc9fb031b809d1e057635c2ff8d4e

# Install python deps
RUN tox --notest
