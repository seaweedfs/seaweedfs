FROM frolvlad/alpine-glibc

# Tried to use curl only (curl -o /tmp/linux_amd64.tar.gz ...), however it turned out that the following tar command failed with "gzip: stdin: not in gzip format"
RUN apk add --no-cache --virtual build-dependencies --update wget curl ca-certificates && \
    wget -P /tmp https://github.com/$(curl -s -L https://github.com/chrislusf/seaweedfs/releases/latest | egrep -o 'chrislusf/seaweedfs/releases/download/.*/linux_amd64.tar.gz') && \
    tar -C /usr/bin/ -xzvf /tmp/linux_amd64.tar.gz && \
    apk del build-dependencies && \
    rm -rf /tmp/*

# volume server gprc port
EXPOSE 18080
# volume server http port
EXPOSE 8080
# filer server gprc port
EXPOSE 18888
# filer server http port
EXPOSE 8888
# master server shared gprc port
EXPOSE 19333
# master server shared http port
EXPOSE 9333
# s3 server http port
EXPOSE 8333

VOLUME /data

COPY filer.toml /etc/seaweedfs/filer.toml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
