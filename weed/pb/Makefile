.PHONY: help ensure-PB_NAME gen-all gen-protobuf fbs
all: gen-all

help:  ## Display help text
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_\-.*]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# basename of the protobuf that is used in generation (ex. 'master')
PB_NAME ?=

###########
# Tooling #
###########

ensure-PB_NAME:
	@if [ -z "$(PB_NAME)" ] ; then \
		echo "PB_NAME must be specified (ex. PB_NAME=master)"; \
		exit 1; \
	fi

ensure-protoc-gen-openapi:
	@if [ ! $$(command -v protoc-gen-openapi) ] ; then \
		echo "protoc-gen-openapi is not installed (see: https://github.com/google/gnostic/tree/main/cmd/protoc-gen-openapi)"; \
		exit 1; \
	fi

#######################
# Protobuf Generation #
#######################

gen-protobuf: ensure-PB_NAME ## Generate probouf for a consistently named sub-API
	protoc $(PB_NAME).proto \
		--go_out=./$(PB_NAME)_pb \
		--go-grpc_out=./$(PB_NAME)_pb \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative

gen-all: ## Generate all
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=master
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=volume_server
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=filer
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=remote
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=iam
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=mount
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=s3
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=mq
	@$(MAKE) --no-print-dir gen-protobuf PB_NAME=public
# protoc filer.proto --java_out=../../other/java/client/src/main/java
	cp filer.proto ../../other/java/client/src/main/proto

fbs: ## Generate flatbuffers
	flatc --go -o . --go-namespace message_fbs message.fbs
