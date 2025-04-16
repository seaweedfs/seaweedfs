FROM vdm-registry.cn-hangzhou.cr.aliyuncs.com/udm-dev/golang:1.24.1-bullseye as builder

RUN cp /etc/apt/sources.list /etc/apt/sources.list.bak

RUN echo "deb http://mirrors.aliyun.com/debian/ bullseye main non-free contrib" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian/ bullseye-updates main non-free contrib" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian/ bullseye-backports main non-free contrib" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/debian-security bullseye-security main" >> /etc/apt/sources.list

RUN apt-get update && \
    apt-get install -y build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libzstd-dev

ENV GOPROXY=https://goproxy.io,direct

WORKDIR /app

COPY weed weed
COPY tools tools
COPY go.mod go.sum ./

# Get dependencies - will also be cached if we won't change mod/sum
RUN --mount=type=cache,target=/go/pkg,id=go-mod-cache,sharing=locked \
    go mod download

RUN go build -o migrate -tags "tikv" ./tools/filer_store_migrate/migrate.go


FROM vdm-registry.cn-hangzhou.cr.aliyuncs.com/udm-dev/rocky-base:9.3.20231119 AS FINAL

RUN dnf install -y \
    epel-release && \
    dnf install -y \
    wget \
    vim \
    bash 

COPY --from=builder /app/migrate /app/migrate

ENTRYPOINT ["sleep"]
CMD ["36000"]
