
.clean:
	go clean -i -v ./go/weed/

.deps:
	go get -d ./go/weed/

.build: .deps
	go build -v ./go/weed/

all: .build
