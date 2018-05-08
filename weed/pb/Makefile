all: gen

.PHONY : gen

gen:
	protoc seaweed.proto --go_out=plugins=grpc:.
	protoc filer.proto --go_out=plugins=grpc:../filer
