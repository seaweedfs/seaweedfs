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
