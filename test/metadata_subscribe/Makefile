.PHONY: test test-short build-weed clean

# Build weed binary first if needed
build-weed:
	cd ../../weed && go build -o weed .

# Run all integration tests
test: build-weed
	go test -v -timeout 5m .

# Run tests in short mode (skip integration tests)
test-short:
	go test -v -short .

# Run specific test
test-basic: build-weed
	go test -v -timeout 3m -run TestMetadataSubscribeBasic .

test-stall: build-weed
	go test -v -timeout 5m -run TestMetadataSubscribeSingleFilerNoStall .

test-resume: build-weed
	go test -v -timeout 3m -run TestMetadataSubscribeResumeFromDisk .

clean:
	rm -f ../../weed/weed

