BINARY = weed/weed
package = github.com/chrislusf/seaweedfs/weed

GO_FLAGS = #-v
SOURCE_DIR = ./weed/

appname := weed

sources := $(wildcard *.go)

build = GOOS=$(1) GOARCH=$(2) go build -o build/$(appname)$(3) $(SOURCE_DIR)
tar = cd build && tar -cvzf $(1)_$(2).tar.gz $(appname)$(3) && rm $(appname)$(3)
zip = cd build && zip $(1)_$(2).zip $(appname)$(3) && rm $(appname)$(3)


all: build

.PHONY : clean deps build linux release windows_build darwin_build linux_build clean

clean:
	go clean -i $(GO_FLAGS) $(SOURCE_DIR)
	rm -f $(BINARY)
	rm -rf build/

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)

build: deps
	go build $(GO_FLAGS) -o $(BINARY) $(SOURCE_DIR)

linux: deps
	mkdir -p linux
	GOOS=linux GOARCH=amd64 go build $(GO_FLAGS) -o linux/$(BINARY) $(SOURCE_DIR)

release: windows_build darwin_build linux_build

##### LINUX BUILDS #####
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