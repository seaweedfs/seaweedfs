package filer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// Two streams (e.g. two S3 GETs) reading the same object through one shared
// ReaderCache, interleaved in small slices, must share each chunk download:
// one stream finishing a chunk, or moving on to the next one, must not drop a
// buffer the other stream is still reading.
func TestChunkReadAtConcurrentStreamsShareChunks(t *testing.T) {
	const chunkSize = 64 << 10
	const chunkCount = 3
	const sliceSize = 16 << 10

	var mu sync.Mutex
	fetches := map[string]int{}
	rc := NewReaderCache(64, (*chunk_cache.TieredChunkCache)(nil), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
		mu.Lock()
		fetches[fileId]++
		mu.Unlock()
		for i := range buffer {
			buffer[i] = fileId[len(fileId)-1]
		}
		return len(buffer), nil
	}

	newStream := func() *ChunkReadAt {
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
		return NewChunkReaderAtFromClient(context.Background(), rc, views, chunkSize*chunkCount, 0)
	}
	streams := []*ChunkReadAt{newStream(), newStream()}

	// The streams alternate slice by slice, the second one lagging one slice
	// behind, so the leader always finishes a chunk while the follower is
	// still inside it.
	offsets := make([]int64, len(streams))
	offsets[1] = -sliceSize
	for offsets[1] < chunkSize*chunkCount {
		for i, stream := range streams {
			if offsets[i] < 0 || offsets[i] >= chunkSize*chunkCount {
				offsets[i] += sliceSize
				continue
			}
			buf := make([]byte, sliceSize)
			n, err := stream.ReadAt(buf, offsets[i])
			if (err != nil && err != io.EOF) || n != sliceSize {
				t.Fatalf("stream %d at %d: n=%d err=%v", i, offsets[i], n, err)
			}
			if want := byte('0' + offsets[i]/chunkSize); buf[0] != want || buf[n-1] != want {
				t.Fatalf("stream %d at %d: got %q, want %q", i, offsets[i], buf[0], want)
			}
			offsets[i] += sliceSize
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < chunkCount; i++ {
		fileId := fmt.Sprintf("chunk%d", i)
		if fetches[fileId] != 1 {
			t.Errorf("%s fetched %d times for two concurrent streams, want 1", fileId, fetches[fileId])
		}
	}
}

func newStreamTestReaderCache(chunkCache chunk_cache.ChunkCache) *ReaderCache {
	rc := NewReaderCache(64, chunkCache, func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		return len(buffer), nil
	}
	return rc
}

func isRetained(rc *ReaderCache, fileId string) bool {
	rc.Lock()
	defer rc.Unlock()
	_, found := rc.downloaders[fileId]
	return found
}

// A stream moving on to a chunk served from the chunk cache must still
// release the chunk it was positioned in before.
func TestChunkStreamReleasesPreviousChunkOnCacheHit(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	cache.SetChunk("chunk1", make([]byte, 4<<10))
	rc := newStreamTestReaderCache(cache)
	defer rc.destroy()

	stream := &chunkStream{}
	if _, err := rc.readChunkAt(context.Background(), stream, make([]byte, 1<<10), "chunk0", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	if n, err := rc.readChunkAt(context.Background(), stream, make([]byte, 1<<10), "chunk1", nil, false, 0, 4<<10, true); err != nil || n == 0 {
		t.Fatalf("cache hit read: n=%d err=%v", n, err)
	}
	if isRetained(rc, "chunk0") {
		t.Fatal("chunk0 still retained after the stream moved on to a cached chunk")
	}
}

// A chunk the stream leaves while another read is in flight must be dropped
// once that read ends, even if it did not read the chunk to the end.
func TestChunkStreamDropsLeftChunkAfterInFlightRead(t *testing.T) {
	rc := newStreamTestReaderCache(newMockChunkCacheForReaderCache())
	defer rc.destroy()

	stream := &chunkStream{}
	if _, err := rc.readChunkAt(context.Background(), stream, make([]byte, 1<<10), "chunk0", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	// Another reader is in the middle of a partial read of chunk0.
	rc.Lock()
	other := rc.downloaders["chunk0"]
	other.wg.Add(1)
	atomic.AddInt32(&other.readers, 1)
	rc.Unlock()

	if _, err := rc.readChunkAt(context.Background(), stream, make([]byte, 1<<10), "chunk1", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	if !isRetained(rc, "chunk0") {
		t.Fatal("chunk0 dropped while a read was still in flight")
	}

	// The in-flight read ends without reaching the end of the chunk.
	other.wg.Done()
	atomic.AddInt32(&other.readers, -1)
	rc.removeConsumed(other)
	if isRetained(rc, "chunk0") {
		t.Fatal("chunk0 retained after the stream left it and the last read ended")
	}
}

// Concurrent ReadAt calls on one ChunkReadAt (as mount does) share its stream
// pin; they must neither race on it nor leak or double-release pins.
func TestChunkStreamConcurrentReadsOnOneReader(t *testing.T) {
	const chunkSize = 16 << 10
	const chunkCount = 4
	const sliceSize = 4 << 10
	rc := newStreamTestReaderCache((*chunk_cache.TieredChunkCache)(nil))
	defer rc.destroy()

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
	reader := NewChunkReaderAtFromClient(context.Background(), rc, views, chunkSize*chunkCount, 0)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for offset := int64(0); offset < chunkSize*chunkCount; offset += sliceSize {
				if _, err := reader.ReadAt(make([]byte, sliceSize), offset); err != nil && err != io.EOF {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// Finishing the last chunk releases the stream's final pin.
	if _, err := reader.ReadAt(make([]byte, sliceSize), chunkSize*chunkCount-sliceSize); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	rc.Lock()
	defer rc.Unlock()
	for fileId, cacher := range rc.downloaders {
		t.Errorf("%s retained after all reads finished: pins=%d readers=%d", fileId, atomic.LoadInt32(&cacher.pins), atomic.LoadInt32(&cacher.readers))
	}
}

// A chunk a stream is positioned in must outlast downloader-limit eviction:
// otherwise a busy cache drops the buffer mid-stream and forces a refetch.
func TestChunkReadAtPinnedChunkSurvivesEviction(t *testing.T) {
	const chunkSize = 64 << 10

	var fetches int32
	rc := NewReaderCache(2, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, fileId string, _ util_http.RefreshUrlsFunc) (int, error) {
		if fileId == "chunk0" {
			atomic.AddInt32(&fetches, 1)
		}
		return len(buffer), nil
	}

	stream := &chunkStream{}
	buf := make([]byte, 16<<10)
	// One slice in: the stream is positioned in chunk0 but has not left it.
	if _, err := rc.readChunkAt(context.Background(), stream, buf, "chunk0", nil, false, 0, chunkSize, false); err != nil {
		t.Fatal(err)
	}

	// Fill the downloader map past its limit with unpinned chunks.
	for _, fileId := range []string{"chunk1", "chunk2", "chunk3"} {
		if _, err := rc.ReadChunkAt(context.Background(), buf, fileId, nil, false, 0, chunkSize, true); err != nil {
			t.Fatalf("read %s: %v", fileId, err)
		}
	}

	// The stream's next slice must come from the still-pinned buffer.
	if _, err := rc.readChunkAt(context.Background(), stream, buf, "chunk0", nil, false, 16<<10, chunkSize, false); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&fetches); got != 1 {
		t.Errorf("chunk0 fetched %d times, want 1", got)
	}
}
