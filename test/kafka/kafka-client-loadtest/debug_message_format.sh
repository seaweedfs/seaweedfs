#!/bin/bash

echo "Building debug program..."
cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest

# Build the debug program
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o debug_message_format_linux debug_message_format.go

# Run it inside the Docker network
echo "Running debug program inside Docker network..."
docker run --rm --network kafka-client-loadtest \
  -v $(pwd)/debug_message_format_linux:/debug_message_format_linux \
  alpine:3.18 \
  /debug_message_format_linux
