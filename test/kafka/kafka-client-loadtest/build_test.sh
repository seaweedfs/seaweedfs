#!/bin/bash
# Build the test binary locally for Linux
cd ../../../
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -ldflags="-s -w" \
  -tags "5BytesOffset" \
  -o test/kafka/kafka-client-loadtest/test_fetch_debug_linux \
  ./test/kafka/kafka-client-loadtest/test_fetch_debug.go
