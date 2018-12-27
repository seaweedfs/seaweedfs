# Docker


## Try it out

```bash

wget https://github.com/chrislusf/seaweedfs/blob/master/docker/docker-compose.yml

docker-compose -f docker-compose.yml up

```

## Development

```bash
cd $GOPATH/src/github.com/chrislusf/seaweedfs/docker

docker build - < Dockerfile.go_build

docker-compose -f docker-compose.yml up

```
