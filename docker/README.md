# Docker


## Try it out

```bash

wget https://raw.githubusercontent.com/chrislusf/seaweedfs/master/docker/seaweedfs-compose.yml

docker-compose -f seaweedfs-compose.yml -p seaweedfs up

```

## Development

```bash
cd $GOPATH/src/github.com/chrislusf/seaweedfs/docker

// use existing builds for git tip
docker-compose -f seaweedfs-dev-compose.yml -p seaweedfs up

// use local repo
docker-compose -f local-dev-compose.yml -p seaweedfs up

```
