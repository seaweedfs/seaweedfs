all: gen

.PHONY : gen

gen: dev

build:
	cd ../weed; GOOS=linux go build; mv weed ../docker/
	docker build --no-cache -t chrislusf/seaweedfs:local -f Dockerfile.local .
	rm ./weed

dev: build
	docker-compose -f local-dev-compose.yml -p seaweedfs up

k8s: build
	docker-compose -f local-k8s-compose.yml -p seaweedfs up

dev_registry: build
	docker-compose -f local-registry-compose.yml -p seaweedfs up

cluster: build
	docker-compose -f local-cluster-compose.yml -p seaweedfs up

clean:
	rm ./weed
