all: gen

.PHONY : gen

gen: dev

binary:
	export SWCOMMIT=$(shell git rev-parse --short HEAD)
	export SWLDFLAGS="-X github.com/chrislusf/seaweedfs/weed/util.COMMIT=$(SWCOMMIT)"
	cd ../weed; CGO_ENABLED=0 GOOS=linux go build -ldflags "-extldflags -static $(SWLDFLAGS)"; mv weed ../docker/

build: binary
	docker build --no-cache -t chrislusf/seaweedfs:local -f Dockerfile.local .
	rm ./weed

s3tests_build:
	docker build --no-cache -t chrislusf/ceph-s3-tests:local -f Dockerfile.s3tests .

dev: build
	docker-compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_tls: build certstrap
	ENV_FILE="tls.env" docker-compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_mount: build
	docker-compose -f compose/local-mount-compose.yml -p seaweedfs up

profile_mount: build
	docker-compose -f compose/local-mount-profile-compose.yml -p seaweedfs up

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

certstrap:
	go get github.com/square/certstrap
	certstrap --depot-path compose/tls init --passphrase "" --common-name "SeaweedFS CA" || true
	certstrap --depot-path compose/tls request-cert --passphrase "" --common-name volume01.dev || true
	certstrap --depot-path compose/tls request-cert --passphrase "" --common-name master01.dev || true
	certstrap --depot-path compose/tls request-cert --passphrase "" --common-name filer01.dev || true
	certstrap --depot-path compose/tls request-cert --passphrase "" --common-name client01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" volume01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" master01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" filer01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" client01.dev || true