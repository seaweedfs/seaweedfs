FROM chrislusf/seaweedfs:e2e

RUN apt-install \
    autoconf \
    automake \
    build-essential \
    git \
    libtap-harness-archive-perl \
    perl

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
