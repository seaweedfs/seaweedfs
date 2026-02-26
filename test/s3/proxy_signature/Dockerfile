FROM alpine:3.20
RUN apk add --no-cache fuse curl
COPY weed /usr/bin/weed
RUN chmod +x /usr/bin/weed
RUN mkdir -p /etc/seaweedfs /data/filerldb2
WORKDIR /data
