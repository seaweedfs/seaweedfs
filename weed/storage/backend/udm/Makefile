API_PROTO_FILES=$(shell find api -name *.proto)

.PHONY: api
# generate api proto
api:
	protoc --proto_path=./api \
           --proto_path=./third_party \
           --go_out=./api --go_opt=paths=source_relative \
           --go-grpc_out=./api --go-grpc_opt=paths=source_relative \
           $(API_PROTO_FILES)