FROM frolvlad/alpine-glibc:alpine-3.4

RUN apk add --no-cache --virtual=build-dependencies --update curl wget ca-certificates && \
    wget -P /tmp https://github.com/$(curl -s -L https://github.com/chrislusf/seaweedfs/releases/latest | egrep -o '/chrislusf/seaweedfs/releases/download/.*/linux_amd64.tar.gz') && \
    tar -C /usr/bin/ -xzvf /tmp/linux_amd64.tar.gz && \
    apk del curl wget ca-certificates build-dependencies && \
    rm -rf /tmp/*

EXPOSE 8080
EXPOSE 9333

VOLUME /data

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
