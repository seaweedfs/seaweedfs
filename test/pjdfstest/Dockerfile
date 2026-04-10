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
ARG PJDFSTEST_REF=03eb25706d8dbf3611c3f820b45b7a5e09a36c06

RUN git clone "${PJDFSTEST_REPO}" /opt/pjdfstest && \
    cd /opt/pjdfstest && \
    git checkout "${PJDFSTEST_REF}" && \
    autoreconf -ifs && \
    ./configure && \
    make pjdfstest

COPY run_inside_container.sh /run.sh
COPY known_failures.txt /known_failures.txt
RUN chmod +x /run.sh
