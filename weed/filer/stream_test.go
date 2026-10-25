package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

// IsSameData must not reorder the caller's chunk slices: filer.remote.sync
// stamps the filer entry an event described under an IF_ENTRY_EQUAL guard,
// and an in-place sort leaves the event entry ordered differently from the
// stored one, so the stamp never matches.
func TestIsSameDataLeavesChunkOrderAlone(t *testing.T) {
	chunkA := &filer_pb.FileChunk{FileId: "1,aaa", ETag: "zzzz", Size: 4}
	chunkB := &filer_pb.FileChunk{FileId: "2,bbb", ETag: "aaaa", Size: 2}
	a := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{chunkA, chunkB}}
	b := &filer_pb.Entry{Chunks: []*filer_pb.FileChunk{
		{FileId: "3,ccc", ETag: "zzzz", Size: 4},
		{FileId: "4,ddd", ETag: "aaaa", Size: 2},
	}}

	assert.True(t, IsSameData(a, b))
	assert.Equal(t, "1,aaa", a.Chunks[0].FileId)
	assert.Equal(t, "2,bbb", a.Chunks[1].FileId)
	assert.Equal(t, "3,ccc", b.Chunks[0].FileId)
	assert.Equal(t, "4,ddd", b.Chunks[1].FileId)
}
