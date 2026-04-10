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

ARG PJDFSTEST_REPO=https://github.com/sanwan/pjdfstest.git
ARG PJDFSTEST_REF=d25636a227606f8960e5179741d8f4ad7030ef41

RUN git clone "${PJDFSTEST_REPO}" /opt/pjdfstest && \
    cd /opt/pjdfstest && \
    git checkout "${PJDFSTEST_REF}" && \
    autoreconf -ifs && \
    ./configure && \
    make pjdfstest

COPY run_inside_container.sh /run.sh
COPY known_failures.txt /known_failures.txt
RUN chmod +x /run.sh
