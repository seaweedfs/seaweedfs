all: gen

.PHONY : gen

gen: dev

cgo ?= 0
binary:
	export SWCOMMIT=$(shell git rev-parse --short HEAD)
	export SWLDFLAGS="-X github.com/seaweedfs/seaweedfs/weed/util.COMMIT=$(SWCOMMIT)"
	cd ../weed && CGO_ENABLED=$(cgo) GOOS=linux go build $(options) -tags "$(tags)" -ldflags "-s -w -extldflags -static $(SWLDFLAGS)" && mv weed ../docker/
	cd ../weed/mq/client/cmd/weed_pub_kv && CGO_ENABLED=$(cgo) GOOS=linux go build && mv weed_pub_kv ../../../../../docker/
	cd ../weed/mq/client/cmd/weed_pub_record && CGO_ENABLED=$(cgo) GOOS=linux go build && mv weed_pub_record ../../../../../docker/
	cd ../weed/mq/client/cmd/weed_sub_kv && CGO_ENABLED=$(cgo) GOOS=linux go build && mv weed_sub_kv ../../../../../docker/
	cd ../weed/mq/client/cmd/weed_sub_record && CGO_ENABLED=$(cgo) GOOS=linux go build && mv weed_sub_record ../../../../../docker/

binary_race: options = -race
binary_race: cgo = 1
binary_race: binary

build: binary
	docker build --no-cache -t chrislusf/seaweedfs:local -f Dockerfile.local .

build_e2e: binary_race
	docker build --no-cache -t chrislusf/seaweedfs:e2e -f Dockerfile.e2e .

go_build: # make go_build tags=elastic,ydb,gocdk,hdfs,5BytesOffset
	docker build --build-arg TAGS=$(tags) --no-cache -t chrislusf/seaweedfs:go_build -f Dockerfile.go_build .

go_build_large_disk:
	docker build --build-arg TAGS=large_disk --no-cache -t chrislusf/seaweedfs:large_disk -f Dockerfile.go_build .

build_rocksdb_dev_env:
	docker build --no-cache -t chrislusf/rocksdb_dev_env -f Dockerfile.rocksdb_dev_env .

build_rocksdb_local: build_rocksdb_dev_env
	cd .. ; docker build --no-cache -t chrislusf/seaweedfs:rocksdb_local -f docker/Dockerfile.rocksdb_large_local .

build_rocksdb:
	docker build --no-cache -t chrislusf/seaweedfs:rocksdb -f Dockerfile.rocksdb_large .

s3tests_build:
	docker build --no-cache -t chrislusf/ceph-s3-tests:local -f Dockerfile.s3tests .

dev: build
	docker compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_race: binary_race
	docker compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_tls: build certstrap
	ENV_FILE="tls.env" docker compose -f compose/local-dev-compose.yml -p seaweedfs up

dev_mount: build
	docker compose -f compose/local-mount-compose.yml -p seaweedfs up

run_image: build
	docker run --rm -ti --device /dev/fuse --cap-add SYS_ADMIN --entrypoint /bin/sh chrislusf/seaweedfs:local

profile_mount: build
	docker compose -f compose/local-mount-profile-compose.yml -p seaweedfs up

k8s: build
	docker compose -f compose/local-k8s-compose.yml -p seaweedfs up

dev_registry: build
	docker compose -f compose/local-registry-compose.yml -p seaweedfs up

dev_replicate:
	docker build --build-arg TAGS=gocdk --no-cache -t chrislusf/seaweedfs:local -f Dockerfile.go_build .
	docker compose -f compose/local-replicate-compose.yml -p seaweedfs up

dev_auditlog: build
	docker compose -f compose/local-auditlog-compose.yml -p seaweedfs up

dev_nextcloud: build
	docker compose -f compose/local-nextcloud-compose.yml -p seaweedfs up

cluster: build
	docker compose -f compose/local-cluster-compose.yml -p seaweedfs up

2clusters: build
	docker compose -f compose/local-clusters-compose.yml -p seaweedfs up

2mount: build
	docker compose -f compose/local-sync-mount-compose.yml -p seaweedfs up

filer_backup: build
	docker compose -f compose/local-filer-backup-compose.yml -p seaweedfs up

hashicorp_raft: build
	docker compose -f compose/local-hashicorp-raft-compose.yml -p seaweedfs up

s3tests: build s3tests_build
	docker compose -f compose/local-s3tests-compose.yml -p seaweedfs up

brokers: build
	docker compose -f compose/local-brokers-compose.yml -p seaweedfs up

filer_etcd: build
	docker stack deploy -c compose/swarm-etcd.yml fs

test_etcd: build
	docker compose -f compose/test-etcd-filer.yml -p seaweedfs up

test_ydb: tags = ydb
test_ydb: build
	export
	docker compose -f compose/test-ydb-filer.yml -p seaweedfs up

clean:
	rm ./weed

certstrap:
	go install -v github.com/square/certstrap@latest
	certstrap --depot-path compose/tls init --curve P-256 --passphrase "" --common-name "SeaweedFS CA" || true
	certstrap --depot-path compose/tls request-cert --ou "SeaweedFS" --curve P-256 --passphrase "" --domain localhost --common-name volume01.dev || true
	certstrap --depot-path compose/tls request-cert --ou "SeaweedFS" --curve P-256 --passphrase "" --common-name master01.dev || true
	certstrap --depot-path compose/tls request-cert --ou "SeaweedFS" --curve P-256 --passphrase "" --common-name filer01.dev || true
	certstrap --depot-path compose/tls request-cert --ou "SeaweedFS" --curve P-256 --passphrase "" --common-name client01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" volume01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" master01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" filer01.dev || true
	certstrap --depot-path compose/tls sign --CA "SeaweedFS CA" client01.dev || true
