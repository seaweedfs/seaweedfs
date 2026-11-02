WEED := ./weed
VEC := ./weed-vector
WORKER_MANIFEST := ../../rust/sw-vector-distributed/Cargo.toml
ifneq ($(wildcard $(WORKER_MANIFEST)),)
WORKER := ./weed-vector-worker
endif
VOLUME := ./weed-volume
CARGO_PROFILE ?= release
CARGO_TARGET_DIR ?= $(abspath ../../seaweed-vector/target)
CARGO_OUTPUT := $(CARGO_TARGET_DIR)/$(if $(filter dev,$(CARGO_PROFILE)),debug,$(CARGO_PROFILE))
export CARGO_TARGET_DIR

.PHONY: all test perf build clean FORCE

all: test

$(WEED): FORCE
	go build -o $(WEED) ../../weed

$(VEC): FORCE
	cd ../../seaweed-vector && cargo build --locked --profile $(CARGO_PROFILE) -p weed-vector-server
	cp $(CARGO_OUTPUT)/weed-vector $(VEC)

ifneq ($(WORKER),)
$(WORKER): $(VEC) FORCE
	cargo build --locked --profile $(CARGO_PROFILE) --manifest-path ../../rust/Cargo.toml -p sw-vector-distributed --bin weed-vector
	cp $(CARGO_OUTPUT)/weed-vector $(WORKER)
endif

$(VOLUME): $(VEC) $(WORKER) FORCE
	cargo build --locked --profile $(CARGO_PROFILE) --manifest-path ../../seaweed-volume/Cargo.toml --bin weed-volume
	cp $(CARGO_OUTPUT)/weed-volume $(VOLUME)

FORCE:

build: $(WEED) $(VEC) $(WORKER) $(VOLUME)

test: build
	./run_test.sh

perf: build
	./run_perf.sh

clean:
	rm -f $(WEED) $(VEC) ./weed-vector-worker $(VOLUME)
