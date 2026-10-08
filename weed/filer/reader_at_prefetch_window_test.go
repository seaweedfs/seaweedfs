package filer

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// A sequential reader that consumes a chunk in several small ReadAt calls, as
// the S3 GET path does with its 256 KiB copy buffer over 4 MiB chunks, must
// keep its read-ahead at prefetchCount chunks. Each ReadAt triggers a
// prefetch; if chunks already being fetched are not counted toward the
// window, every call starts prefetchCount more downloads and the read-ahead
// runs to the ReaderCache downloader limit, buffering most of the object.
func TestChunkReadAtSequentialPrefetchStaysWithinWindow(t *testing.T) {
	const chunkSize = 64 << 10
	const chunkCount = 64
	const sliceSize = 16 << 10

	var readerChunk atomic.Int64
	var maxLead atomic.Int64
	rc := NewReaderCache(256, (*chunk_cache.TieredChunkCache)(nil), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
		idx, err := strconv.Atoi(strings.TrimPrefix(fileId, "chunk"))
		if err != nil {
			return 0, err
		}
		lead := int64(idx) - readerChunk.Load()
		for {
			cur := maxLead.Load()
			if lead <= cur || maxLead.CompareAndSwap(cur, lead) {
				break
			}
		}
		return len(buffer), nil
	}

	views := NewIntervalList[*ChunkView]()
	for i := 0; i < chunkCount; i++ {
		views.AppendInterval(&Interval[*ChunkView]{
			StartOffset: int64(i * chunkSize),
			StopOffset:  int64((i + 1) * chunkSize),
			Value: &ChunkView{
				FileId:     fmt.Sprintf("chunk%d", i),
				ViewSize:   chunkSize,
				ViewOffset: int64(i * chunkSize),
				ChunkSize:  chunkSize,
			},
		})
	}
	reader := NewChunkReaderAtFromClient(context.Background(), rc, views, chunkSize*chunkCount, DefaultPrefetchCount)
	defer reader.ReleaseStream()

	buf := make([]byte, sliceSize)
	for off := int64(0); off < chunkSize*chunkCount; off += sliceSize {
		readerChunk.Store(off / chunkSize)
		if _, err := reader.ReadAt(buf, off); err != nil && off+sliceSize < chunkSize*chunkCount {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}

	if got := maxLead.Load(); got > DefaultPrefetchCount {
		t.Fatalf("read-ahead reached %d chunks past the reader, want at most %d", got, DefaultPrefetchCount)
	}
}
