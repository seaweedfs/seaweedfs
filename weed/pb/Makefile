all: gen

.PHONY : gen

gen:
	protoc seaweed.proto --go_out=plugins=grpc:./master_pb
	protoc filer.proto --go_out=plugins=grpc:../filer
