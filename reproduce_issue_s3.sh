#!/bin/bash

# Clean up
pkill -f "weed"
rm -rf ./tmp
mkdir -p ./tmp/m ./tmp/f ./tmp/v1 ./tmp/v2 ./tmp/v3 ./tmp/v4 ./tmp/v5 ./tmp/v6

# Ensure warp is in PATH if needed (assuming user has it or we downloaded it to GOPATH/bin)
export PATH=$PATH:$(go env GOPATH)/bin

# Create S3 configuration
cat > ./tmp/s3.json <<EOF
{
  "identities": [
    {
      "name": "admin",
      "credentials": [
        {
          "accessKey": "admin",
          "secretKey": "seaweedfs"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    }
  ]
}
EOF

# Start Master
weed master -mdir=./tmp/m -port=9333 -defaultReplication=000 -ip=localhost -volumeSizeLimitMB=10 -peers=none &
MASTER_PID=$!
sleep 2

# Start Filer
weed filer -port=8888 -master=localhost:9333 &
FILER_PID=$!
sleep 2

# Start S3 with config
weed s3 -port=8333 -filer=localhost:8888 -config=./tmp/s3.json &
S3_PID=$!
sleep 2

# Start Volume Servers (6 servers to allow distribution)
weed volume -dir=./tmp/v1 -port=8081 -max=5 -mserver=localhost:9333 -ip=localhost &
V1_PID=$!
weed volume -dir=./tmp/v2 -port=8082 -max=5 -mserver=localhost:9333 -ip=localhost &
V2_PID=$!
weed volume -dir=./tmp/v3 -port=8083 -max=5 -mserver=localhost:9333 -ip=localhost &
V3_PID=$!
weed volume -dir=./tmp/v4 -port=8084 -max=5 -mserver=localhost:9333 -ip=localhost &
V4_PID=$!
weed volume -dir=./tmp/v5 -port=8085 -max=5 -mserver=localhost:9333 -ip=localhost &
V5_PID=$!
weed volume -dir=./tmp/v6 -port=8086 -max=5 -mserver=localhost:9333 -ip=localhost &
V6_PID=$!

echo "Waiting for cluster to stabilize..."
sleep 15

# Run warp to generate data (mixed mode creates objects)
echo "Generating data with warp..."
warp mixed --host=127.0.0.1:8333 --access-key=admin --secret-key=seaweedfs --duration=10s --obj.size=1MiB --concurrent=1 --bucket=sea --noclear

if [ $? -ne 0 ]; then
    echo "Warp failed to generate data. Aborting."
    kill -9 $MASTER_PID $FILER_PID $S3_PID $V1_PID $V2_PID $V3_PID $V4_PID $V5_PID $V6_PID
    exit 1
fi

sleep 5

# EC Encode
echo "lock; ec.encode -collection=sea -fullPercent=0 -quietFor=0; unlock" | weed shell -master=localhost:9333

sleep 5

# EC Balance
echo "lock; ec.balance -collection=sea -apply -shardReplicaPlacement=222; unlock" | weed shell -master=localhost:9333

sleep 5

# Stop a volume server.
echo "Stopping Volume Server 2 (localhost:8082)..."
kill -9 $V2_PID

sleep 5

# Verify with warp get --list-existing
# This ensures we are reading the objects we just created and encoded, NOT writing new ones.
echo "Attempting to read EXISTING data with warp..."
warp get --host=127.0.0.1:8333 --access-key=admin --secret-key=seaweedfs --duration=20s --concurrent=1 --bucket=sea --noclear --list-existing

if [ $? -eq 0 ]; then
    echo "Read success! (Issue NOT reproduced)"
else
    echo "Read failed! (Issue potentially REPRODUCED)"
fi

# Cleanup
echo "Cleaning up..."
kill -9 $MASTER_PID $FILER_PID $S3_PID $V1_PID $V2_PID $V3_PID $V4_PID $V5_PID $V6_PID
pkill -f "weed volume"
pkill -f "weed master"
pkill -f "weed filer"
pkill -f "weed s3"
