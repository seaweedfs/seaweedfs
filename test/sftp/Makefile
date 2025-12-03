.PHONY: build test test-verbose test-short clean deps

# Build the weed binary first
build:
	cd ../../weed && go build -o weed .

# Install test dependencies
deps:
	go mod download

# Run all tests
test: build deps
	go test -timeout 5m ./...

# Run tests with verbose output
test-verbose: build deps
	go test -v -timeout 5m ./...

# Run quick tests only (skip integration tests)
test-short: deps
	go test -short -v ./...

# Run specific test
test-homedir: build deps
	go test -v -timeout 5m -run TestHomeDirPathTranslation ./...

# Run tests with debug output from SeaweedFS
test-debug: build deps
	go test -v -timeout 5m ./... 2>&1 | tee test.log

# Clean up test artifacts
clean:
	rm -f test.log
	go clean -testcache

# Update go.sum
tidy:
	go mod tidy

