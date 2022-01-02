# the tests only support python 3.6, not newer
FROM ubuntu:latest

RUN apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y git-core sudo tzdata
RUN git clone https://github.com/ceph/s3-tests.git
WORKDIR s3-tests

# we pin a certain commit
RUN git checkout 9a6a1e9f197fc9fb031b809d1e057635c2ff8d4e

RUN ./bootstrap
