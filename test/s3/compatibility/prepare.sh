#!/usr/bin/env bash

set -ex

docker build  --progress=plain  -t s3tests .
