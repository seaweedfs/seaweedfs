BINARY = weed

SOURCE_DIR = .

all: debug_mount

.PHONY : clean debug_mount

clean:
	go clean $(SOURCE_DIR)
	rm -f $(BINARY)

debug_shell:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- shell

debug_mount:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- mount -dir=~/tmp/mm -cacheCapacityMB=0 -filer.path=/buckets

debug_server:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- server -dir=/Volumes/mobile_disk/99 -filer -volume.port=8343 -s3 -volume.max=0 -master.volumeSizeLimitMB=1024 -volume.preStopSeconds=1

debug_volume:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- volume -dir=/Volumes/mobile_disk/100 -port 8564 -max=30 -preStopSeconds=2

debug_webdav:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- -v=4 webdav

debug_s3:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- -v=4 s3
