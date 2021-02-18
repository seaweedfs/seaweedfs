all: gen

.PHONY : gen

gen: dev

build:
	cd ../weed; GOOS=linux go build; mv weed ../docker/
	docker build --no-cache -t chrislusf/seaweedfs:local -f Dockerfile.local .
	rm ./weed

s3tests_build:
	docker build --no-cache -t chrislusf/ceph-s3-tests:local -f Dockerfile.s3tests .

dev: build
	docker-compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_mount: build
	docker-compose -f compose/local-mount-compose.yml -p seaweedfs up

k8s: build
	docker-compose -f compose/local-k8s-compose.yml -p seaweedfs up

dev_registry: build
	docker-compose -f compose/local-registry-compose.yml -p seaweedfs up

dev_replicate: build
	docker-compose -f compose/local-replicate-compose.yml -p seaweedfs up

cluster: build
	docker-compose -f compose/local-cluster-compose.yml -p seaweedfs up

2clusters: build
	docker-compose -f compose/local-clusters-compose.yml -p seaweedfs up

s3tests: build s3tests_build
	docker-compose -f compose/local-s3tests-compose.yml -p seaweedfs up

filer_etcd: build
	docker stack deploy -c compose/swarm-etcd.yml fs

clean:
	rm ./weed
