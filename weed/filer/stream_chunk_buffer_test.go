package filer

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Interleaved ReadAt calls across chunks must not refetch a chunk every time
// the read window moves: parallel consumers (e.g. multipart upload parts read
// through io.SectionReader) share the reader and would otherwise keep evicting
// one another's single buffer, refetching whole chunks on nearly every read.
func TestChunkStreamReaderReadAtReusesFetchedChunks(t *testing.T) {
	contentA := bytes.Repeat([]byte("a"), 4096)
	contentB := bytes.Repeat([]byte("b"), 4096)
	var fetches atomic.Int32
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		switch strings.TrimPrefix(r.URL.Path, "/") {
		case "1,aaaaaaaa":
			_, _ = w.Write(contentA)
		case "2,bbbbbbbb":
			_, _ = w.Write(contentB)
		}
	}))
	t.Cleanup(volume.Close)

	lookup := func(ctx context.Context, fileId string) ([]string, error) {
		return []string{volume.URL + "/" + fileId}, nil
	}
	chunks := []*filer_pb.FileChunk{
		{FileId: "1,aaaaaaaa", Offset: 0, Size: uint64(len(contentA))},
		{FileId: "2,bbbbbbbb", Offset: int64(len(contentA)), Size: uint64(len(contentB))},
	}
	reader := NewChunkStreamReaderFromLookup(context.Background(), lookup, chunks)
	t.Cleanup(func() { _ = reader.Close() })

	buf := make([]byte, 64)
	for i := 0; i < 4; i++ {
		for _, off := range []int64{0, int64(len(contentA))} {
			_, err := reader.ReadAt(buf, off)
			require.NoError(t, err)
		}
	}
	assert.Equal(t, int32(2), fetches.Load(), "each chunk is fetched once, not once per read")

	_, err := reader.ReadAt(buf, 8)
	require.NoError(t, err)
	assert.Equal(t, contentA[8:72], buf)
}
