FROM chrislusf/seaweedfs:e2e

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    --no-install-recommends \
    --no-install-suggests \
    autoconf \
    automake \
    build-essential \
    git \
    libtap-harness-archive-perl \
    perl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG PJDFSTEST_REPO=https://github.com/pjd/pjdfstest.git
ARG PJDFSTEST_REF=master

RUN git clone --depth 1 --branch "${PJDFSTEST_REF}" "${PJDFSTEST_REPO}" /opt/pjdfstest && \
    cd /opt/pjdfstest && \
    autoreconf -ifs && \
    ./configure && \
    make pjdfstest

COPY run_inside_container.sh /run.sh
RUN chmod +x /run.sh
