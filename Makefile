BINARY = weed
OUT_DIR = bin

GO_FLAGS = #-race -v
SOURCE_DIR = ./weed

all: build

.PHONY : clean godep build linux vet

clean:
	go clean -i $(GO_FLAGS) $(SOURCE_DIR)
	rm -f $(BINARY)

fmt:
	gofmt -w -s $(SOURCE_DIR)

vet:
	go vet $(SOURCE_DIR)/...

build: fmt
	mkdir -p $(OUT_DIR)
	go build $(GO_FLAGS) -o $(OUT_DIR)/$(BINARY) $(SOURCE_DIR)

linux:
	mkdir -p $(OUT_DIR)/linux-amd64
	GOOS=linux GOARCH=amd64 go build $(GO_FLAGS) -o $(OUT_DIR)/linux-amd64/$(BINARY) $(SOURCE_DIR)
