all: gen

.PHONY : gen

gen:
	protoc bptree.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative
