package filer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestMergeRemoteContentEncoding(t *testing.T) {
	merged := MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: proto.String("zstd")}, nil)
	assert.Equal(t, []byte("zstd"), merged["Content-Encoding"])

	// existing keys are preserved, the encoding is overwritten from remote
	merged = MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: proto.String("gzip")}, map[string][]byte{
		"Content-Encoding": []byte("zstd"),
		"x-amz-meta-foo":   []byte("bar"),
	})
	assert.Equal(t, []byte("gzip"), merged["Content-Encoding"])
	assert.Equal(t, []byte("bar"), merged["x-amz-meta-foo"])

	// an unreported encoding (S3 listings) leaves a locally set value alone
	local := map[string][]byte{"Content-Encoding": []byte("zstd")}
	merged = MergeRemoteContentEncoding(&filer_pb.RemoteEntry{}, local)
	assert.Equal(t, []byte("zstd"), merged["Content-Encoding"])

	// an authoritatively empty one removes it
	merged = MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: proto.String("")}, local)
	assert.NotContains(t, merged, "Content-Encoding")

	assert.Nil(t, MergeRemoteContentEncoding(nil, nil))
	assert.Nil(t, MergeRemoteContentEncoding(&filer_pb.RemoteEntry{RemoteContentEncoding: proto.String("")}, nil))
}
