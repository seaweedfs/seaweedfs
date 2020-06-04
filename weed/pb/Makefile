all: gen

.PHONY : gen

gen:
	protoc master.proto --go_out=plugins=grpc:./master_pb
	protoc volume_server.proto --go_out=plugins=grpc:./volume_server_pb
	protoc filer.proto --go_out=plugins=grpc:./filer_pb
	protoc iam.proto --go_out=plugins=grpc:./iam_pb
	protoc messaging.proto --go_out=plugins=grpc:./messaging_pb
	# protoc filer.proto --java_out=../../other/java/client/src/main/java
	cp filer.proto ../../other/java/client/src/main/proto
