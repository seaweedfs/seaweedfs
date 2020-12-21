FROM alpine

RUN \
    ARCH=$(if [ $(uname -m) == "x86_64" ] && [ $(getconf LONG_BIT) == "64" ]; then echo "amd64"; \
         elif [ $(uname -m) == "x86_64" ] && [ $(getconf LONG_BIT) == "32" ]; then echo "386"; \
         elif [ $(uname -m) == "aarch64" ]; then echo "arm64"; \
         elif [ $(uname -m) == "armv7l" ]; then echo "arm"; \
         elif [ $(uname -m) == "armv6l" ]; then echo "arm"; fi;) && \
    echo "Building for $ARCH" 1>&2 && \
    SUPERCRONIC_SHA1SUM=$(echo $ARCH | sed 's/386/e0126b0102b9f388ecd55714358e3ad60d0cebdb/g' | sed 's/amd64/5ddf8ea26b56d4a7ff6faecdd8966610d5cb9d85/g' | sed 's/arm64/e2714c43e7781bf1579c85aa61259245f56dbba1/g' | sed 's/arm/47481c3341bc3a1ae91a728e0cc63c8e6d3791ad/g') && \
    SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.1.9/supercronic-linux-$ARCH && \
    SUPERCRONIC=supercronic-linux-$ARCH && \
    # Install SeaweedFS and Supercronic ( for cron job mode )
    apk add --no-cache --virtual build-dependencies --update wget curl ca-certificates && \
    apk add fuse && \
    wget -P /tmp https://github.com/$(curl -s -L https://github.com/chrislusf/seaweedfs/releases/latest | egrep -o "chrislusf/seaweedfs/releases/download/.*/linux_$ARCH.tar.gz") && \
    tar -C /usr/bin/ -xzvf /tmp/linux_$ARCH.tar.gz && \
    curl -fsSLO "$SUPERCRONIC_URL" && \
    echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - && \
    chmod +x "$SUPERCRONIC" && \
    mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" && \
    ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic && \
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

RUN mkdir -p /data/filerldb2

VOLUME /data

COPY filer.toml /etc/seaweedfs/filer.toml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
