package filer

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
)

// Regression test for #11286: stream preparation must fail when chunk
// manifest resolution fails, instead of serving a zero-filled stream.
func TestPrepareStreamContent_ManifestResolveFailure(t *testing.T) {
	master := &testMasterClient{} // no urls registered: every lookup fails

	chunks := []*filer_pb.FileChunk{
		{FileId: "1,1879011dc64abd40", IsChunkManifest: true, Offset: 0, Size: 1 << 20},
	}

	_, err := PrepareStreamContentWithThrottler(context.Background(), master, noopJwt, chunks, 0, 1<<20, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail to read manifest")

	_, err = PrepareStreamContentWithPrefetch(context.Background(), master, noopJwt, chunks, 0, 1<<20, 0, 4)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fail to read manifest")
}
