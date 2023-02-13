all: gen

.PHONY : gen

gen:
	protoc columnar.proto --go_out=./columnar_pb --go-grpc_out=./columnar_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
