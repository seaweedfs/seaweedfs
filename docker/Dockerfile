FROM progrium/busybox

RUN opkg-install curl
RUN echo tlsv1 >> ~/.curlrc

RUN curl -s -L https://github.com/chrislusf/seaweedfs/releases/latest | egrep -o '/chrislusf/seaweedfs/releases/download/.*/linux_amd64.tar.gz' | wget --base=http://github.com/ -i - && \
  tar xzvf linux_amd64.tar.gz && \
  mv weed /usr/bin/ && \
  rm -r linux_amd64.tar.gz

EXPOSE 8080
EXPOSE 9333

VOLUME /data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
