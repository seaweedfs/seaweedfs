BINARY = weed

SOURCE_DIR = .

all: install

.PHONY : clean debug_mount

install:
	go install -ldflags="-s -w"

clean:
	go clean $(SOURCE_DIR)
	rm -f $(BINARY)

debug_shell:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- shell

debug_mount:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 mount -dir=~/tmp/mm -cacheCapacityMB=0 -filer.path=/ -umask=000

debug_server:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- server -dir=~/tmp/99 -filer -volume.port=8343 -s3 -volume.max=0 -master.volumeSizeLimitMB=1024 -volume.preStopSeconds=1

debug_volume:
	go build -tags=5BytesOffset -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- volume -dir=/Users/chrislu/tmp/x/volume_data -port 8564 -max=1 -preStopSeconds=2

debug_webdav:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 webdav

debug_s3:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 s3

debug_mq:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 mq.broker

debug_mq_agent:
	go build -gcflags="all=-N -l"
	dlv --listen=:2346 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 mq.agent -broker=localhost:17777

debug_filer_copy:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 filer.backup -filer=localhost:8888 -filerProxy -timeAgo=10h

debug_filer_remote_sync_dir:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 filer.remote.sync -filer="localhost:8888" -dir=/buckets/b2 -timeAgo=1h

debug_filer_remote_sync_buckets:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 filer.remote.sync -filer="localhost:8888" -createBucketAt=cloud1 -timeAgo=1h

debug_master_follower:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 master.follower

debug_filer_sync:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec ./weed -- -v=4 filer.sync -a 192.168.2.7:8888 -b 192.168.2.7:8889 -isActivePassive -b.debug
