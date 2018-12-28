# Docker


## Try it out

```bash

wget https://raw.githubusercontent.com/chrislusf/seaweedfs/master/docker/seaweedfs-compose.yml

docker-compose -f seaweedfs-compose.yml -p seaweedfs up

```

## Development

```bash
cd $GOPATH/src/github.com/chrislusf/seaweedfs/docker

docker-compose -f dev-compose.yml -p seaweedfs up

```
