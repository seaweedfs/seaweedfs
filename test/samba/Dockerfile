FROM chrislusf/seaweedfs:e2e

RUN apt-install \
    samba \
    smbclient \
    python3-minimal

COPY smb.conf.template /smb.conf.template
COPY smb_tests.sh /smb_tests.sh
COPY lock_tests.sh /lock_tests.sh
COPY entrypoint.sh /entrypoint.sh
COPY run_inside_container.sh /run_inside_container.sh
RUN chmod +x /smb_tests.sh /lock_tests.sh /entrypoint.sh /run_inside_container.sh

ENTRYPOINT ["/entrypoint.sh"]
