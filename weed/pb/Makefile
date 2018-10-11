all: gen

.PHONY : gen

gen:
	protoc master.proto --go_out=plugins=grpc:./master_pb
	protoc filer.proto --go_out=plugins=grpc:./filer_pb
