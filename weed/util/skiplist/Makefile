all: gen

.PHONY : gen

gen:
	protoc skiplist.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative
