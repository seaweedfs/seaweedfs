BINARY = weed

SOURCE_DIR = .
debug ?= 0

all: install

install:
	cd weed; go install -tags "sqlite"

warp_install:
	go install github.com/minio/warp@v0.7.6

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv rclone"

server: install
	export WEED_LEVELDB2_ENABLED="false";export WEED_SQLITE_ENABLED="true"; export WEED_SQLITE_DBFILE="/tmp/filer.db"; \
	weed -v 0 server -s3.allowListRecursive=true -dir /tmp -master.volumeSizeLimitMB=1024 -s3 -filer -filer.maxMB=64 -filer.port.public=7777 -volume.max=100 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowEmptyFolder=false -s3.allowDeleteBucketNotEmpty=true -s3.config=./docker/compose/s3.json -metricsPort=9324

benchmark: install warp_install
	pkill weed || true
	pkill warp || true
	weed server -debug=$(debug) -s3 -filer -volume.max=0 -master.volumeSizeLimitMB=1024 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowEmptyFolder=false -s3.allowDeleteBucketNotEmpty=false -s3.config=./docker/compose/s3.json &
	warp client &
	while ! nc -z localhost 8000 ; do sleep 1 ; done
	warp mixed --host=127.0.0.1:8000 --access-key=some_access_key1 --secret-key=some_secret_key1 --autoterm
	pkill warp
	pkill weed

# curl -o profile "http://127.0.0.1:6060/debug/pprof/profile?debug=1"
benchmark_with_pprof: debug = 1
benchmark_with_pprof: benchmark

test:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv rclone" -v ./...
