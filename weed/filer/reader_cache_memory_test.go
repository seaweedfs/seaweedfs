package filer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestChunkGroupReaderCacheMemory(t *testing.T) {
	budget := NewReaderCacheBudget(8 << 10)
	groups := make([]*ChunkGroup, 32)
	for i := range groups {
		group, err := NewChunkGroup(func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }, newMockChunkCacheForReaderCache(), nil, 128, nil, nil, budget)
		if err != nil {
			t.Fatal(err)
		}
		groups[i] = group
		defer group.Close()
		group.readerCache.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
			buffer[0] = 42
			return len(buffer), nil
		}
		buffer := make([]byte, 1)
		n, err := group.readerCache.ReadChunkAt(context.Background(), buffer, fmt.Sprint(i), nil, false, 0, 3<<10, false)
		if err != nil || n != 1 || buffer[0] != 42 {
			t.Fatalf("read %d: n=%d data=%v err=%v", i, n, buffer, err)
		}
		budget.Lock()
		used := budget.used
		budget.Unlock()
		if used > 8<<10 {
			t.Fatalf("shared budget used %d bytes", used)
		}
	}
	groups[0].readerCache.Lock()
	retained := len(groups[0].readerCache.downloaders)
	groups[0].readerCache.Unlock()
	if retained != 0 {
		t.Fatalf("first file still retains %d downloaders", retained)
	}
	for _, group := range groups {
		_ = group.Close()
	}
	budget.Lock()
	defer budget.Unlock()
	if budget.used != 0 {
		t.Fatalf("closed files still reserve %d bytes", budget.used)
	}
}

func TestReaderCacheBudgetInFlight(t *testing.T) {
	for _, prefetch := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefetch=%t", prefetch), func(t *testing.T) {
			budget := NewReaderCacheBudget(8 << 10)
			started := make(chan struct{}, 4)
			gate := make(chan struct{})
			var readers sync.WaitGroup
			var caches []*ReaderCache
			for i := 0; i < 4; i++ {
				rc := NewReaderCache(256, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }, nil, budget)
				caches = append(caches, rc)
				defer rc.destroy()
				rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
					started <- struct{}{}
					<-gate
					buffer[0] = 42
					return len(buffer), nil
				}
				if prefetch {
					rc.MaybeCache(&Interval[*ChunkView]{Value: &ChunkView{FileId: "chunk", ChunkSize: 3 << 10}}, 1)
				} else {
					readers.Add(1)
					go func() {
						defer readers.Done()
						buffer := make([]byte, 1)
						n, err := rc.ReadChunkAt(context.Background(), buffer, "chunk", nil, false, 0, 3<<10, false)
						if err != nil || n != 1 || buffer[0] != 42 {
							t.Errorf("read: n=%d data=%v err=%v", n, buffer, err)
						}
					}()
				}
			}
			for i := 0; i < 2; i++ {
				<-started
			}
			select {
			case <-started:
				t.Error("third download allocated before budget was released")
			case <-time.After(50 * time.Millisecond):
			}
			budget.Lock()
			used := budget.used
			budget.Unlock()
			if used != 8<<10 {
				t.Errorf("in-flight reservations = %d, want 8192", used)
			}
			close(gate)
			readers.Wait()
			for i := 0; i < 2; i++ {
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("download did not resume after eviction")
				}
			}
			for _, rc := range caches {
				rc.destroy()
			}
			budget.Lock()
			defer budget.Unlock()
			if budget.used != 0 {
				t.Errorf("reservations leaked: %d", budget.used)
			}
		})
	}
}

func TestReaderCacheBudgetOversizedChunk(t *testing.T) {
	rc := NewReaderCache(256, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		t.Error("oversized chunk performed lookup")
		return nil, nil
	}, nil, NewReaderCacheBudget(3<<10))
	defer rc.destroy()
	_, err := rc.ReadChunkAt(context.Background(), make([]byte, 1), "chunk", nil, false, 0, 3<<10, false)
	if err == nil {
		t.Fatal("expected pooled buffer larger than budget to be rejected")
	}
}

func TestReaderCacheEvictionDoesNotHoldCacheLock(t *testing.T) {
	rc := NewReaderCache(2, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) { return []string{"unused"}, nil }, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		return len(buffer), nil
	}
	if _, err := rc.ReadChunkAt(context.Background(), make([]byte, 1), "chunk", nil, false, 0, 1024, false); err != nil {
		t.Fatal(err)
	}
	rc.Lock()
	downloader := rc.downloaders["chunk"]
	downloader.wg.Add(1)
	rc.Unlock()
	evicted := make(chan struct{})
	go func() { rc.UnCache("chunk"); close(evicted) }()
	deadline := time.Now().Add(5 * time.Second)
	available := false
	for time.Now().Before(deadline) {
		if rc.TryLock() {
			available = rc.downloaders["chunk"] == nil
			rc.Unlock()
			if available {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	downloader.wg.Done()
	<-evicted
	if !available {
		t.Fatal("cache lock held while eviction waited for a reader")
	}
}

func TestReaderCacheFailedPrefetchReleasesBudget(t *testing.T) {
	for _, lookupFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("lookupFailure=%t", lookupFailure), func(t *testing.T) {
			budget := NewReaderCacheBudget(1024)
			rc := NewReaderCache(1, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
				if lookupFailure {
					return nil, fmt.Errorf("lookup failed")
				}
				return []string{"unused"}, nil
			}, nil, budget)
			defer rc.destroy()
			rc.fetchChunkDataFn = func(_ context.Context, _ []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
				return 0, fmt.Errorf("fetch failed")
			}
			rc.MaybeCache(&Interval[*ChunkView]{Value: &ChunkView{FileId: "failed", ChunkSize: 1024}}, 1)
			deadline := time.Now().Add(5 * time.Second)
			for {
				rc.Lock()
				count := len(rc.downloaders)
				rc.Unlock()
				budget.Lock()
				used := budget.used
				budget.Unlock()
				if count == 0 && used == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("failed prefetch retains %d slots and %d bytes", count, used)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestReaderCacheUnboundedWithoutBudget(t *testing.T) {
	const readers = 96 // more than 256MiB / 4MiB = 64 buffers
	started := make(chan struct{}, readers)
	gate := make(chan struct{})
	rc := NewReaderCache(256, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		started <- struct{}{}
		<-gate
		buffer[0] = 42
		return len(buffer), nil
	}
	var readersWg sync.WaitGroup
	for i := 0; i < readers; i++ {
		readersWg.Add(1)
		go func(i int) {
			defer readersWg.Done()
			rc.ReadChunkAt(context.Background(), make([]byte, 1), fmt.Sprint(i), nil, false, 0, 4<<20, false)
		}(i)
	}
	for i := 0; i < readers; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			close(gate)
			readersWg.Wait()
			t.Fatalf("only %d of %d downloads started; an implicit memory budget throttled the reader cache", i, readers)
		}
	}
	close(gate)
	readersWg.Wait()
}

func TestReaderCacheDropsConsumedChunks(t *testing.T) {
	var fetchCount int32
	rc := NewReaderCache(10, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		atomic.AddInt32(&fetchCount, 1)
		return len(buffer), nil
	}
	buf := make([]byte, 4<<10)
	if _, err := rc.ReadChunkAt(context.Background(), buf, "chunk", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	rc.Lock()
	_, retained := rc.downloaders["chunk"]
	rc.Unlock()
	if retained {
		t.Fatal("fully consumed chunk buffer still retained")
	}
	if _, err := rc.ReadChunkAt(context.Background(), buf, "chunk", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&fetchCount); got != 2 {
		t.Fatalf("consumed chunk was not refetched, fetchCount=%d", got)
	}
}

// A prefetched chunk must survive until its reader arrives, then be dropped
// once consumed: the read hits the prefetched buffer (no second fetch) and a
// later read fetches again.
func TestReaderCachePrefetchBufferDroppedAfterRead(t *testing.T) {
	var fetchCount int32
	rc := NewReaderCache(10, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		atomic.AddInt32(&fetchCount, 1)
		buffer[0] = 42
		return len(buffer), nil
	}
	rc.MaybeCache(&Interval[*ChunkView]{Value: &ChunkView{FileId: "chunk", ChunkSize: 4 << 10}}, 1)

	buf := make([]byte, 4<<10)
	n, err := rc.ReadChunkAt(context.Background(), buf, "chunk", nil, false, 0, 4<<10, false)
	if err != nil || n != len(buf) || buf[0] != 42 {
		t.Fatalf("read of prefetched chunk: n=%d data=%d err=%v", n, buf[0], err)
	}
	if got := atomic.LoadInt32(&fetchCount); got != 1 {
		t.Fatalf("read did not hit the prefetched buffer, fetchCount=%d", got)
	}
	rc.Lock()
	_, retained := rc.downloaders["chunk"]
	rc.Unlock()
	if retained {
		t.Fatal("consumed prefetch buffer still retained")
	}
	if _, err := rc.ReadChunkAt(context.Background(), buf, "chunk", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&fetchCount); got != 2 {
		t.Fatalf("consumed chunk was not refetched, fetchCount=%d", got)
	}
}

// A consumed chunk survives while another reader is still attached; it is
// dropped once no readers remain, regardless of which reader reached the end.
func TestReaderCacheConsumedBufferSurvivesAttachedReader(t *testing.T) {
	var fetchCount int32
	rc := NewReaderCache(10, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		atomic.AddInt32(&fetchCount, 1)
		buffer[0] = 42
		return len(buffer), nil
	}

	// A partial read primes the cacher, then a second reader attaches.
	rc.ReadChunkAt(context.Background(), make([]byte, 1), "chunk", nil, false, 0, 4<<10, false)
	rc.Lock()
	downloader := rc.downloaders["chunk"]
	if downloader == nil {
		rc.Unlock()
		t.Fatal("cacher missing before attach")
	}
	downloader.wg.Add(1)
	atomic.AddInt32(&downloader.readers, 1)
	rc.Unlock()

	buf := make([]byte, 4<<10)
	if n, err := rc.ReadChunkAt(context.Background(), buf, "chunk", nil, false, 0, 4<<10, false); err != nil || n != len(buf) {
		t.Fatalf("full read: n=%d err=%v", n, err)
	}
	rc.Lock()
	_, retained := rc.downloaders["chunk"]
	rc.Unlock()
	if !retained {
		t.Fatal("consumed chunk dropped while a reader was still attached")
	}

	// The attached reader detaches without reading: since the buffer was
	// already consumed, the last detach drops it.
	downloader.wg.Done()
	atomic.AddInt32(&downloader.readers, -1)
	rc.removeConsumed(downloader)
	rc.Lock()
	_, retained = rc.downloaders["chunk"]
	rc.Unlock()
	if retained {
		t.Fatal("consumed chunk retained after last reader detached")
	}
	if got := atomic.LoadInt32(&fetchCount); got != 1 {
		t.Fatalf("attached readers fetched more than once, fetchCount=%d", got)
	}
}

func TestReaderCacheConsumedChunkReleasesBudget(t *testing.T) {
	budget := NewReaderCacheBudget(4 << 10)
	rc := NewReaderCache(10, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil, budget)
	defer rc.destroy()
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		return len(buffer), nil
	}
	if _, err := rc.ReadChunkAt(context.Background(), make([]byte, 4<<10), "chunk", nil, false, 0, 4<<10, false); err != nil {
		t.Fatal(err)
	}
	budget.Lock()
	used := budget.used
	budget.Unlock()
	if used != 0 {
		t.Fatalf("consumed chunk still reserves %d bytes", used)
	}
}

// TestReaderCacheReReadAfterEviction verifies that a chunk evicted by budget
// pressure is transparently re-downloaded on the next read and returns the
// correct data. This is the core correctness property of eviction: a reader
// must never observe missing or stale data after a chunk has been evicted.
func TestReaderCacheReReadAfterEviction(t *testing.T) {
	budget := NewReaderCacheBudget(4 << 10) // fits exactly one 4 KiB pooled chunk
	rc := NewReaderCache(256, newMockChunkCacheForReaderCache(), func(context.Context, string) ([]string, error) {
		return []string{"unused"}, nil
	}, nil, budget)
	defer rc.destroy()

	var fetchCount int32
	rc.fetchChunkDataFn = func(_ context.Context, buffer []byte, _ []string, _ []byte, _ bool, _ bool, _ int64, _ string, _ util_http.RefreshUrlsFunc) (int, error) {
		n := atomic.AddInt32(&fetchCount, 1)
		buffer[0] = byte(n) // each download writes a distinct value
		return len(buffer), nil
	}

	// Read chunk "a": triggers download #1, fills the budget.
	buf := make([]byte, 1)
	if n, err := rc.ReadChunkAt(context.Background(), buf, "a", nil, false, 0, 4<<10, false); err != nil || n != 1 || buf[0] != 1 {
		t.Fatalf("first read of 'a': n=%d data=%d err=%v", n, buf[0], err)
	}

	// Read chunk "b": budget only fits one chunk, so "a" is evicted to make room.
	if n, err := rc.ReadChunkAt(context.Background(), buf, "b", nil, false, 0, 4<<10, false); err != nil || n != 1 || buf[0] != 2 {
		t.Fatalf("read of 'b': n=%d data=%d err=%v", n, buf[0], err)
	}

	// "a" should no longer be in the cache.
	rc.Lock()
	_, stillCached := rc.downloaders["a"]
	rc.Unlock()
	if stillCached {
		t.Fatal("chunk 'a' was not evicted by budget pressure")
	}

	// Re-read "a": must trigger download #3 and return the fresh value.
	if n, err := rc.ReadChunkAt(context.Background(), buf, "a", nil, false, 0, 4<<10, false); err != nil || n != 1 || buf[0] != 3 {
		t.Fatalf("re-read of 'a': n=%d data=%d err=%v (expected re-download with value 3)", n, buf[0], err)
	}

	if got := atomic.LoadInt32(&fetchCount); got != 3 {
		t.Fatalf("fetchCount=%d, want 3 (a, b, a-re-read)", got)
	}
}
