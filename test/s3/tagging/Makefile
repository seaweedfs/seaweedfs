# S3 Object Tagging Tests
# Tests for GitHub issue #7589: S3 object Tags query comes back empty

.PHONY: test

# Run all tagging tests
test:
	go test -v ./...

# Run specific test
test-upload:
	go test -v -run TestObjectTaggingOnUpload

test-special-chars:
	go test -v -run TestObjectTaggingOnUploadWithSpecialCharacters

test-api:
	go test -v -run TestPutObjectTaggingAPI

# Test configuration (override these for different environments)
S3_ENDPOINT ?= http://localhost:8333
S3_ACCESS_KEY ?= some_access_key1
S3_SECRET_KEY ?= some_secret_key1
