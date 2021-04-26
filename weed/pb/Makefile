all: gen

.PHONY : gen

gen:
	protoc master.proto --go_out=plugins=grpc:./master_pb --go_opt=paths=source_relative
	protoc volume_server.proto --go_out=plugins=grpc:./volume_server_pb --go_opt=paths=source_relative
	protoc filer.proto --go_out=plugins=grpc:./filer_pb --go_opt=paths=source_relative
	protoc iam.proto --go_out=plugins=grpc:./iam_pb --go_opt=paths=source_relative
	protoc messaging.proto --go_out=plugins=grpc:./messaging_pb --go_opt=paths=source_relative
	# protoc filer.proto --java_out=../../other/java/client/src/main/java
	cp filer.proto ../../other/java/client/src/main/proto
