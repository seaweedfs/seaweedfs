package filer_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestFileIdSize(t *testing.T) {
	fileIdStr := "11745,0293434534cbb9892b"

	fid, _ := ToFileIdObject(fileIdStr)
	bytes, _ := proto.Marshal(fid)

	println(len(fileIdStr))
	println(len(bytes))
}
