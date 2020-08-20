BINARY = weed

SOURCE_DIR = .

all: debug_mount

.PHONY : clean debug_mount

clean:
	go clean $(SOURCE_DIR)
	rm -f $(BINARY)

debug_mount:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- mount -dir=~/tmp/mm

debug_server:
	go build -gcflags="all=-N -l"
	dlv --listen=:2345 --headless=true --api-version=2 --accept-multiclient exec weed -- server -dir=/Volumes/mobile_disk/99 -filer -volume.port=8343 -s3 -volume.max=0
