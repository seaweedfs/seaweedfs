BINARY = weed/weed
package = github.com/chrislusf/seaweedfs/weed

GO_FLAGS = #-v
SOURCE_DIR = ./weed/

appname := weed

sources := $(wildcard *.go)

COMMIT ?= $(shell git rev-parse --short HEAD)
LDFLAGS ?= -X github.com/chrislusf/seaweedfs/weed/util.COMMIT=${COMMIT}

build = CGO_ENABLED=0 GOOS=$(1) GOARCH=$(2) go build -ldflags "-extldflags -static $(LDFLAGS)" -o build/$(appname)$(3) $(SOURCE_DIR)
tar = cd build && tar -cvzf $(1)_$(2).tar.gz $(appname)$(3) && rm $(appname)$(3)
zip = cd build && zip $(1)_$(2).zip $(appname)$(3) && rm $(appname)$(3)

build_large = CGO_ENABLED=0 GOOS=$(1) GOARCH=$(2) go build -tags 5BytesOffset -ldflags "-extldflags -static $(LDFLAGS)" -o build/$(appname)$(3) $(SOURCE_DIR)
tar_large = cd build && tar -cvzf $(1)_$(2)_large_disk.tar.gz $(appname)$(3) && rm $(appname)$(3)
zip_large = cd build && zip $(1)_$(2)_large_disk.zip $(appname)$(3) && rm $(appname)$(3)

all: build

.PHONY : clean deps build linux release windows_build darwin_build linux_build bsd_build clean

clean:
	go clean -i $(GO_FLAGS) $(SOURCE_DIR)
	rm -f $(BINARY)
	rm -rf build/

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)
	rm -rf /home/travis/gopath/src/github.com/coreos/etcd/vendor/golang.org/x/net/trace
	rm -rf /home/travis/gopath/src/go.etcd.io/etcd/vendor/golang.org/x/net/trace

build: deps
	go build $(GO_FLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY) $(SOURCE_DIR)

install: deps
	go install $(GO_FLAGS) -ldflags "$(LDFLAGS)" $(SOURCE_DIR)

linux: deps
	mkdir -p linux
	GOOS=linux GOARCH=amd64 go build $(GO_FLAGS) -ldflags "$(LDFLAGS)" -o linux/$(BINARY) $(SOURCE_DIR)

release: deps windows_build darwin_build linux_build bsd_build 5_byte_linux_build 5_byte_arm64_build 5_byte_darwin_build 5_byte_windows_build

##### LINUX BUILDS #####
5_byte_linux_build:
	$(call build_large,linux,amd64,)
	$(call tar_large,linux,amd64)

5_byte_darwin_build:
	$(call build_large,darwin,amd64,)
	$(call tar_large,darwin,amd64)

5_byte_windows_build:
	$(call build_large,windows,amd64,.exe)
	$(call zip_large,windows,amd64,.exe)

5_byte_arm_build: $(sources)
	$(call build_large,linux,arm,)
	$(call tar_large,linux,arm)

5_byte_arm64_build: $(sources)
	$(call build_large,linux,arm64,)
	$(call tar_large,linux,arm64)

linux_build: build/linux_arm.tar.gz build/linux_arm64.tar.gz build/linux_386.tar.gz build/linux_amd64.tar.gz

build/linux_386.tar.gz: $(sources)
	$(call build,linux,386,)
	$(call tar,linux,386)

build/linux_amd64.tar.gz: $(sources)
	$(call build,linux,amd64,)
	$(call tar,linux,amd64)

build/linux_arm.tar.gz: $(sources)
	$(call build,linux,arm,)
	$(call tar,linux,arm)

build/linux_arm64.tar.gz: $(sources)
	$(call build,linux,arm64,)
	$(call tar,linux,arm64)

##### DARWIN (MAC) BUILDS #####
darwin_build: build/darwin_amd64.tar.gz

build/darwin_amd64.tar.gz: $(sources)
	$(call build,darwin,amd64,)
	$(call tar,darwin,amd64)

##### WINDOWS BUILDS #####
windows_build: build/windows_386.zip build/windows_amd64.zip

build/windows_386.zip: $(sources)
	$(call build,windows,386,.exe)
	$(call zip,windows,386,.exe)

build/windows_amd64.zip: $(sources)
	$(call build,windows,amd64,.exe)
	$(call zip,windows,amd64,.exe)

##### BSD BUILDS #####
bsd_build: build/freebsd_arm.tar.gz build/freebsd_386.tar.gz build/freebsd_amd64.tar.gz \
 build/netbsd_arm.tar.gz build/netbsd_386.tar.gz build/netbsd_amd64.tar.gz \
 build/openbsd_arm.tar.gz build/openbsd_386.tar.gz build/openbsd_amd64.tar.gz

build/freebsd_386.tar.gz: $(sources)
	$(call build,freebsd,386,)
	$(call tar,freebsd,386)

build/freebsd_amd64.tar.gz: $(sources)
	$(call build,freebsd,amd64,)
	$(call tar,freebsd,amd64)

build/freebsd_arm.tar.gz: $(sources)
	$(call build,freebsd,arm,)
	$(call tar,freebsd,arm)

build/netbsd_386.tar.gz: $(sources)
	$(call build,netbsd,386,)
	$(call tar,netbsd,386)

build/netbsd_amd64.tar.gz: $(sources)
	$(call build,netbsd,amd64,)
	$(call tar,netbsd,amd64)

build/netbsd_arm.tar.gz: $(sources)
	$(call build,netbsd,arm,)
	$(call tar,netbsd,arm)

build/openbsd_386.tar.gz: $(sources)
	$(call build,openbsd,386,)
	$(call tar,openbsd,386)

build/openbsd_amd64.tar.gz: $(sources)
	$(call build,openbsd,amd64,)
	$(call tar,openbsd,amd64)

build/openbsd_arm.tar.gz: $(sources)
	$(call build,openbsd,arm,)
	$(call tar,openbsd,arm)
