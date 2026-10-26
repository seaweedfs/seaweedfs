# End-to-end S3 Vectors conformance run:
# weed mini (cluster + lance catalog) + weed s3 (s3vectors port) +
# weed-vector-server (VectorService gRPC), then boto3 s3vectors calls.
WEED := ./weed
VEC := ./weed-vector-server

.PHONY: all test build clean FORCE

all: test

# No prerequisites: go/cargo decide for themselves whether a rebuild is needed.
$(WEED): FORCE
	go build -o $(WEED) ../../weed

$(VEC): FORCE
	cargo build --release --manifest-path ../../seaweed-vector/Cargo.toml -p weed-vector-server
	cp ../../seaweed-vector/target/release/weed-vector-server $(VEC)

FORCE:

build: $(WEED) $(VEC)

test: build
	./run_test.sh

clean:
	rm -f $(WEED) $(VEC)
	rm -rf mini-data
