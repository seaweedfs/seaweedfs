WEED := ./weed
VEC := ./weed-vector
CLI := ./weed-vector-cli
ENGINE_MANIFEST := ../../rust/sw-vector-distributed/Cargo.toml
VOLUME := ./weed-volume
LANCE_WORKER := ./weed-worker
CARGO_PROFILE ?= release
CARGO_TARGET_DIR ?= $(abspath ../../seaweed-vector/target)
override CARGO_TARGET_DIR := $(abspath $(CARGO_TARGET_DIR))
CARGO_OUTPUT := $(CARGO_TARGET_DIR)/$(if $(filter dev,$(CARGO_PROFILE)),debug,$(CARGO_PROFILE))
export CARGO_TARGET_DIR

.PHONY: all test perf build clean FORCE

all: test

$(WEED): FORCE
	go build -o $(WEED) ../../weed

$(VEC): FORCE
ifneq ($(wildcard $(ENGINE_MANIFEST)),)
	cargo build --locked --profile $(CARGO_PROFILE) --manifest-path ../../rust/Cargo.toml -p sw-vector-distributed --bin weed-vector
else
	cd ../../seaweed-vector && cargo build --locked --profile $(CARGO_PROFILE) -p weed-vector-server
endif
	cp $(CARGO_OUTPUT)/weed-vector $(VEC)

$(CLI): FORCE
	cd ../../seaweed-vector && cargo build --locked --profile $(CARGO_PROFILE) -p weed-vector-core --bin weed-vector-cli
	cp $(CARGO_OUTPUT)/weed-vector-cli $(CLI)

$(LANCE_WORKER): FORCE
	cargo build --locked --profile $(CARGO_PROFILE) --manifest-path ../../seaweed-worker/Cargo.toml -p weed-lance-worker --bin weed-worker
	cp $(CARGO_OUTPUT)/weed-worker $(LANCE_WORKER)

$(VOLUME): $(VEC) FORCE
	cargo build --locked --profile $(CARGO_PROFILE) --manifest-path ../../seaweed-volume/Cargo.toml --bin weed-volume
	cp $(CARGO_OUTPUT)/weed-volume $(VOLUME)

FORCE:

build: $(WEED) $(VEC) $(VOLUME)
ifneq ($(VECTOR_WORKER),)
build: $(CLI) $(LANCE_WORKER)
endif

test: build
	./run_test.sh

perf: build $(CLI)
	./run_perf.sh

clean:
	rm -f $(WEED) $(VEC) $(CLI) $(LANCE_WORKER) $(VOLUME)
