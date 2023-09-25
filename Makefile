BINARY = weed

SOURCE_DIR = .

all: install

install:
	cd weed; go install

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv"

server: install
	weed -v 4 server -s3 -filer -volume.max=0 -master.volumeSizeLimitMB=1024 -volume.preStopSeconds=1 -s3.port=8000 -s3.allowEmptyFolder=false -s3.allowDeleteBucketNotEmpty=false -s3.config=./docker/compose/s3.json

test:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv" -v ./...
