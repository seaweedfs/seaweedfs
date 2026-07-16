package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

func TestMergeRemoteContentEncoding(t *testing.T) {
	merged := MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: "zstd"}, nil)
	assert.Equal(t, []byte("zstd"), merged["Content-Encoding"])

	// existing keys are preserved, the encoding is overwritten from remote
	merged = MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: "gzip"}, map[string][]byte{
		"Content-Encoding": []byte("zstd"),
		"x-amz-meta-foo":   []byte("bar"),
	})
	assert.Equal(t, []byte("gzip"), merged["Content-Encoding"])
	assert.Equal(t, []byte("bar"), merged["x-amz-meta-foo"])

	// a remote without encoding does not remove a locally set value
	local := map[string][]byte{"Content-Encoding": []byte("zstd")}
	merged = MergeRemoteContentEncoding(&filer_pb.RemoteEntry{}, local)
	assert.Equal(t, []byte("zstd"), merged["Content-Encoding"])

	assert.Nil(t, MergeRemoteContentEncoding(nil, nil))
	assert.Nil(t, MergeRemoteContentEncoding(&filer_pb.RemoteEntry{}, nil))
}
