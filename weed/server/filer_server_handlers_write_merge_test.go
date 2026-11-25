package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestMaybeMergeChunksSkipsSparseFile(t *testing.T) {
	fs := &FilerServer{option: &FilerOption{MaxMB: 4}}
	var chunks []*filer_pb.FileChunk
	offset := int64(0)
	for i := 0; i < MergeChunkMinCount+1; i++ {
		if i == MergeChunkMinCount/2 {
			offset += 1 << 20
		}
		chunks = append(chunks, &filer_pb.FileChunk{Fid: &filer_pb.FileId{VolumeId: 1, FileKey: uint64(i + 1), Cookie: 1}, Offset: offset, Size: 1024, ModifiedTsNs: int64(i + 1)})
		offset += 1024
	}

	merged, err := fs.maybeMergeChunks(context.Background(), &operation.StorageOption{}, chunks)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != len(chunks) {
		t.Fatalf("merged a sparse file into %d chunks, want its %d chunks unchanged", len(merged), len(chunks))
	}
}
