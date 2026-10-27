package filer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
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

// A client hanging up mid-read must not evict or re-look-up the volume's locations.
func TestCancelledFetchKeepsCachedLocations(t *testing.T) {
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(volume.Close)

	inv := &countingInvalidator{}
	var lookups atomic.Int32
	lookup := func(ctx context.Context, fileId string) ([]string, error) {
		lookups.Add(1)
		return nil, ctx.Err()
	}

	fileId := "4,12c8f6c18f"
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := util_http.RetriedFetchChunkData(ctx, make([]byte, 8), []string{volume.URL + "/" + fileId}, nil, false, true, 0, fileId, refreshUrls(ctx, inv, lookup, fileId))

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(0), inv.invalidations.Load(), "a cancelled read must not evict the volume's locations")
	assert.Equal(t, int32(0), lookups.Load(), "a cancelled read must not look the volume up again")
}
