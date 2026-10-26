# End-to-end S3 Vectors conformance run:
# weed mini (cluster + lance catalog) + weed s3 (s3vectors port) +
# weed-vector-server (VectorService gRPC), then boto3 s3vectors calls.
WEED := ./weed
VEC := ./weed-vector-server

.PHONY: all test build clean

all: test

$(WEED):
	go build -o $(WEED) ../../weed

$(VEC):
	cargo build --release --manifest-path ../../seaweed-vector/Cargo.toml -p weed-vector-server
	cp ../../seaweed-vector/target/release/weed-vector-server $(VEC)

build: $(WEED) $(VEC)

test: build
	./run_test.sh

clean:
	rm -f $(WEED) $(VEC)
	rm -rf mini-data
