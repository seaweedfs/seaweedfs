# Docker


## Try it out

```bash

wget https://raw.githubusercontent.com/chrislusf/seaweedfs/master/docker/seaweedfs-compose.yml

docker-compose -f seaweedfs-compose.yml -p seaweedfs up

```

## Try latest tip

```bash

wget https://raw.githubusercontent.com/chrislusf/seaweedfs/master/docker/seaweedfs-dev-compose.yml

docker-compose -f seaweedfs-dev-compose.yml -p seaweedfs up

```

## Local Development

```bash
cd $GOPATH/src/github.com/chrislusf/seaweedfs/docker
make
```

### S3 cmd

list
```
s3cmd --no-ssl --host=127.0.0.1:8333 ls s3://
```

## Build and push a multiarch build

Make sure that `docker buildx` is supported (might be an experimental docker feature)
```bash
BUILDER=$(docker buildx create --driver docker-container --use)
docker buildx build --pull --push --platform linux/386,linux/amd64,linux/arm64,linux/arm/v7,linux/arm/v6 . -t chrislusf/seaweedfs
docker buildx stop $BUILDER
```

## Minio debuging
```
mc config host add local http://127.0.0.1:9000 some_access_key1 some_secret_key1
mc admin trace --all --verbose local
```