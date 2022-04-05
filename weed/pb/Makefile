all: gen

.PHONY : gen

gen:
	protoc master.proto --go_out=./master_pb --go-grpc_out=./master_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc volume_server.proto --go_out=./volume_server_pb --go-grpc_out=./volume_server_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc filer.proto --go_out=./filer_pb --go-grpc_out=./filer_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc remote.proto --go_out=./remote_pb --go-grpc_out=./remote_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc iam.proto --go_out=./iam_pb --go-grpc_out=./iam_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc mount.proto --go_out=./mount_pb --go-grpc_out=./mount_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc messaging.proto --go_out=./messaging_pb --go-grpc_out=./messaging_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	# protoc filer.proto --java_out=../../other/java/client/src/main/java
	cp filer.proto ../../other/java/client/src/main/proto
