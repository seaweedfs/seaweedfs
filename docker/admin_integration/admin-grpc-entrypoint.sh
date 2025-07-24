#!/bin/sh

set -e

echo "Starting SeaweedFS Admin Server (gRPC)..."
echo "Master Address: $MASTER_ADDRESS"
echo "Admin HTTP Port: $ADMIN_PORT"
echo "Admin gRPC Port: $GRPC_PORT"

# Wait for master to be ready
echo "Waiting for master to be ready..."
until wget --quiet --tries=1 --spider http://$MASTER_ADDRESS/cluster/status > /dev/null 2>&1; do
    echo "Master not ready, waiting..."
    sleep 5
done
echo "Master is ready!"

# Install protobuf compiler and Go protobuf plugins
apk add --no-cache protobuf protobuf-dev

# Set up Go environment
export GOPATH=/tmp/go
export PATH=$PATH:$GOPATH/bin
mkdir -p $GOPATH/src $GOPATH/bin $GOPATH/pkg

# Install Go protobuf plugins globally first
export GOPATH=/tmp/go
mkdir -p $GOPATH
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set up working directory for compilation
cd /tmp
mkdir -p admin-project
cd admin-project

# Create a basic go.mod with required dependencies
cat > go.mod << 'EOF'
module admin-server

go 1.24

require (
    google.golang.org/grpc v1.65.0
    google.golang.org/protobuf v1.34.2
)
EOF

go mod tidy

# Add Go bin to PATH
export PATH=$PATH:$(go env GOPATH)/bin

# Create directory structure for protobuf
mkdir -p worker_pb

# Copy the admin server source and existing worker protobuf file
cp /admin_grpc_server.go .
cp /worker.proto .

# Generate Go code from the existing worker protobuf
echo "Generating gRPC code from worker.proto..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       worker.proto

# Build and run the admin server
echo "Building admin server..."
go mod tidy
go build -o admin-server admin_grpc_server.go

echo "Starting admin server..."
exec ./admin-server 