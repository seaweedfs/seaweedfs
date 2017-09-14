FROM frolvlad/alpine-glibc:alpine-3.5

# Tried to use curl only (curl -o /tmp/linux_amd64.tar.gz ...), however it turned out that the following tar command failed with "gzip: stdin: not in gzip format"
RUN apk add --no-cache --virtual build-dependencies --update wget curl ca-certificates && \
    wget -P /tmp https://github.com/$(curl -s -L https://github.com/chrislusf/seaweedfs/releases/latest | egrep -o 'chrislusf/seaweedfs/releases/download/.*/linux_amd64.tar.gz') && \
    tar -C /usr/bin/ -xzvf /tmp/linux_amd64.tar.gz && \
    apk del build-dependencies && \
    rm -rf /tmp/*

EXPOSE 8080
EXPOSE 9333

VOLUME /data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
