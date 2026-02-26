FROM alpine:3.20
RUN apk add --no-cache curl && \
    addgroup -S seaweed && \
    adduser -S seaweed -G seaweed
COPY weed /usr/bin/weed
RUN chmod +x /usr/bin/weed && \
    chown seaweed:seaweed /usr/bin/weed && \
    mkdir -p /etc/seaweedfs /data/filerldb2 && \
    chown -R seaweed:seaweed /etc/seaweedfs /data && \
    chmod 755 /data /etc/seaweedfs /data/filerldb2
WORKDIR /data
USER seaweed
