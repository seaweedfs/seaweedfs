#!/bin/sh

set -e

echo "Starting SeaweedFS EC Worker (gRPC)..."
echo "Worker ID: $WORKER_ID"
echo "Admin gRPC Address: $ADMIN_GRPC_ADDRESS"

# Wait for admin to be ready  
echo "Waiting for admin to be ready..."
until curl -f http://admin:9900/health > /dev/null 2>&1; do
    echo "Admin not ready, waiting..."
    sleep 5
done
echo "Admin is ready!"

# Install protobuf compiler and Go protobuf plugins
apk add --no-cache protobuf protobuf-dev

# Set up Go environment
export GOPATH=/tmp/go
export PATH=$PATH:$GOPATH/bin
mkdir -p $GOPATH/src $GOPATH/bin $GOPATH/pkg

# Install Go protobuf plugins
cd /tmp
go mod init worker-client

# Create a basic go.mod with required dependencies
cat > go.mod << 'EOF'
module worker-client

go 1.24

require (
    google.golang.org/grpc v1.65.0
    google.golang.org/protobuf v1.34.2
)
EOF

go mod tidy
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Add Go bin to PATH
export PATH=$PATH:$(go env GOPATH)/bin

# Create directory structure for protobuf
mkdir -p worker_pb

# Copy the worker client source and existing worker protobuf file
cp /worker_grpc_client.go .
cp /worker.proto .

# Generate Go code from the existing worker protobuf
echo "Generating gRPC code from worker.proto..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       worker.proto

# Build and run the worker
echo "Building worker..."
go mod tidy
go build -o worker-client worker_grpc_client.go

echo "Starting worker..."
exec ./worker-client 