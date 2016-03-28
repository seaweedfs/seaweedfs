BINARY = weed

GO_FLAGS = -race #-v
SOURCE_DIR = ./go/weed/

all: build

.PHONY : clean deps build linux vet

clean:
	go clean -i $(GO_FLAGS) $(SOURCE_DIR)
	rm -f $(BINARY)

deps:
	go get $(GO_FLAGS) -d $(SOURCE_DIR)

fmt:
	gofmt -w -s ./go/

vet:
	go vet ./go/...

build: deps fmt
	go build $(GO_FLAGS) -o $(BINARY) $(SOURCE_DIR)

linux: deps
	mkdir -p linux
	GOOS=linux GOARCH=amd64 go build $(GO_FLAGS) -o linux/$(BINARY) $(SOURCE_DIR)
