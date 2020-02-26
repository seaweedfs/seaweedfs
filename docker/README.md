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
