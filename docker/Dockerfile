FROM --platform=${BUILDPLATFORM} vdm-registry.cn-hangzhou.cr.aliyuncs.com/udm-dev/golang:1.23.2-bullseye as builder
ARG TARGETARCH
ARG TARGETOS

WORKDIR /seaweedfs

ENV CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH}
ARG GOPROXY=https://goproxy.cn,direct
ARG LDFLAGS=""
ARG GOBUILDTAGS=""

COPY go.mod go.sum ./

# Get dependencies - will also be cached if we won't change mod/sum
RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go mod download

COPY weed weed

RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go build -ldflags "$LDFLAGS" -tags "$GOBUILDTAGS" -o /weed ./weed

RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go build -ldflags "$LDFLAGS" -o /weed_pub_kv ./weed/mq/client/cmd/weed_pub_kv

RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go build -ldflags "$LDFLAGS" -o /weed_pub_record ./weed/mq/client/cmd/weed_pub_record

RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go build -ldflags "$LDFLAGS" -o /weed_sub_kv ./weed/mq/client/cmd/weed_sub_kv

RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go build -ldflags "$LDFLAGS" -o /weed_sub_record ./weed/mq/client/cmd/weed_sub_record

FROM vdm-registry.cn-hangzhou.cr.aliyuncs.com/udm-dev/alpine:3.18.4 AS final

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
    # apk --no-cache add ca-certificates && \
    apk update && \
    apk add --no-cache fuse && \
    apk add --no-cache curl

# volume server grpc port
EXPOSE 18080
# volume server http port
EXPOSE 8080
# filer server grpc port
EXPOSE 18888
# filer server http port
EXPOSE 8888
# master server shared grpc port
EXPOSE 19333
# master server shared http port
EXPOSE 9333
# s3 server http port
EXPOSE 8333
# webdav server http port
EXPOSE 7333

COPY ./docker/entrypoint.sh /entrypoint.sh

COPY --from=builder /weed /usr/bin/
COPY --from=builder /weed_pub* /usr/bin/
COPY --from=builder /weed_sub* /usr/bin/

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]