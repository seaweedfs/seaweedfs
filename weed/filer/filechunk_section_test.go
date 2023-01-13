package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"testing"
)

func Test_removeGarbageChunks(t *testing.T) {
	section := NewFileChunkSection(0)
	section.chunks = append(section.chunks, &filer_pb.FileChunk{
		FileId:       "0",
		Offset:       0,
		Size:         1,
		ModifiedTsNs: 0,
	})
	section.chunks = append(section.chunks, &filer_pb.FileChunk{
		FileId:       "1",
		Offset:       1,
		Size:         1,
		ModifiedTsNs: 1,
	})
	section.chunks = append(section.chunks, &filer_pb.FileChunk{
		FileId:       "2",
		Offset:       2,
		Size:         1,
		ModifiedTsNs: 2,
	})
	section.chunks = append(section.chunks, &filer_pb.FileChunk{
		FileId:       "3",
		Offset:       3,
		Size:         1,
		ModifiedTsNs: 3,
	})
	section.chunks = append(section.chunks, &filer_pb.FileChunk{
		FileId:       "4",
		Offset:       4,
		Size:         1,
		ModifiedTsNs: 4,
	})
	garbageFileIds := make(map[string]struct{})
	garbageFileIds["0"] = struct{}{}
	garbageFileIds["2"] = struct{}{}
	garbageFileIds["4"] = struct{}{}
	removeGarbageChunks(section, garbageFileIds)
	if len(section.chunks) != 2 {
		t.Errorf("remove chunk 2 failed")
	}
}
