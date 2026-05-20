FROM chrislusf/seaweedfs:e2e

RUN apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 update && \
    DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 install -y \
    --no-install-recommends \
    --no-install-suggests \
    samba \
    smbclient \
    python3-minimal \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY smb.conf.template /smb.conf.template
COPY smb_tests.sh /smb_tests.sh
COPY lock_tests.sh /lock_tests.sh
COPY entrypoint.sh /entrypoint.sh
COPY run_inside_container.sh /run_inside_container.sh
RUN chmod +x /smb_tests.sh /lock_tests.sh /entrypoint.sh /run_inside_container.sh

ENTRYPOINT ["/entrypoint.sh"]
