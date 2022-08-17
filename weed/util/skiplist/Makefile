all: gen

.PHONY : gen

gen:
	protoc skiplist.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
