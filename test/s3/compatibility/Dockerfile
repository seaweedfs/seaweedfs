# the tests only support python 3.6, not newer
#FROM ubuntu:latest
FROM python:3.6.15-slim-bullseye

# Installed required system deps
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y -qq git-core sudo tzdata ca-certificates libxml2-dev libxslt1-dev zlib1g-dev python3-lxml gcc python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python deps
RUN pip install virtualenv

# Clone Ceph S3 tests
COPY fix_s3_tests_bucket_conflicts.py /tmp/fix_s3_tests_bucket_conflicts.py
ENV S3_TESTS_PATH=s3-tests
RUN git clone https://github.com/ceph/s3-tests.git \
    && python3 /tmp/fix_s3_tests_bucket_conflicts.py

WORKDIR s3-tests

RUN pip install -r requirements.txt \
    && pip install tox \
    && pip install -e .
