package filer

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func logEntriesAt(tsNs ...int64) []*filer_pb.LogEntry {
	out := make([]*filer_pb.LogEntry, 0, len(tsNs))
	for _, ts := range tsNs {
		out = append(out, &filer_pb.LogEntry{TsNs: ts, Data: []byte("x")})
	}
	return out
}

func TestPersistedLogCacheHitMiss(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2, 3), true, nil
	}

	e1, err := c.getOrLoad("k", "fp", load)
	if err != nil || len(e1) != 3 {
		t.Fatalf("first getOrLoad: err=%v len=%d", err, len(e1))
	}
	e2, err := c.getOrLoad("k", "fp", load)
	if err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&loads); n != 1 {
		t.Fatalf("expected loader called once, got %d", n)
	}
	if &e1[0] != &e2[0] {
		t.Fatal("cache should return the same shared slice")
	}
}

func TestPersistedLogCacheFingerprintReload(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	loadV1 := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2), true, nil
	}
	loadV2 := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2, 3), true, nil // a delayed append grew the file
	}

	if e, _ := c.getOrLoad("k", "fp1", loadV1); len(e) != 2 {
		t.Fatalf("v1 len=%d", len(e))
	}
	// a later replay observes the grown file (new fingerprint): it must reload,
	// not serve the stale truncated snapshot.
	e, err := c.getOrLoad("k", "fp2", loadV2)
	if err != nil {
		t.Fatal(err)
	}
	if len(e) != 3 {
		t.Fatalf("expected reload to 3 entries, got %d", len(e))
	}
	if n := atomic.LoadInt32(&loads); n != 2 {
		t.Fatalf("expected one reload (2 loads), got %d", n)
	}
	// the cache now holds the new version; the matching fingerprint hits
	if _, err := c.getOrLoad("k", "fp2", loadV2); err != nil {
		t.Fatal(err)
	}
	if n := atomic.LoadInt32(&loads); n != 2 {
		t.Fatalf("expected fp2 to hit (still 2 loads), got %d", n)
	}
}

func TestPersistedLogCacheNotCachedWhenUncacheable(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	// cacheable=false models a chunk-not-found stop: deliver the prefix this
	// round but never pin it, so a transient outage is re-probed next replay.
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		return logEntriesAt(1, 2), false, nil
	}

	if e, err := c.getOrLoad("k", "fp", load); err != nil || len(e) != 2 {
		t.Fatalf("first: err=%v len=%d", err, len(e))
	}
	if e, err := c.getOrLoad("k", "fp", load); err != nil || len(e) != 2 {
		t.Fatalf("second: err=%v len=%d", err, len(e))
	}
	if n := atomic.LoadInt32(&loads); n != 2 {
		t.Fatalf("uncacheable result must re-load every time, got %d loads", n)
	}
}

func TestChunksFingerprintChangesOnGrowth(t *testing.T) {
	c1 := []*filer_pb.FileChunk{{FileId: "3,01", Size: 100}}
	c2 := []*filer_pb.FileChunk{{FileId: "3,01", Size: 100}, {FileId: "3,02", Size: 50}}
	if chunksFingerprint(c1) == chunksFingerprint(c2) {
		t.Fatal("fingerprint must change when a chunk is appended")
	}
}

func TestPersistedLogCacheSingleFlight(t *testing.T) {
	c := newPersistedLogCache(persistedLogCacheMaxBytes)
	var loads int32
	release := make(chan struct{})
	load := func() ([]*filer_pb.LogEntry, bool, error) {
		atomic.AddInt32(&loads, 1)
		<-release // hold the flight open so concurrent callers coalesce
		return logEntriesAt(1), true, nil
	}

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.getOrLoad("k", "fp", load); err != nil {
				t.Error(err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("expected 1 coalesced load across %d callers, got %d", n, got)
	}
}

func TestPersistedLogCacheEviction(t *testing.T) {
	// One entry estimates to 128 + 100 + 16 = 244 bytes; budget holds one, not two.
	c := newPersistedLogCache(300)
	mk := func(ts int64) func() ([]*filer_pb.LogEntry, bool, error) {
		return func() ([]*filer_pb.LogEntry, bool, error) {
			return []*filer_pb.LogEntry{{TsNs: ts, Data: make([]byte, 100)}}, true, nil
		}
	}

	if _, err := c.getOrLoad("a", "fp", mk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.getOrLoad("b", "fp", mk(2)); err != nil { // pushes over budget, evicts LRU "a"
		t.Fatal(err)
	}

	c.mu.Lock()
	_, hasA := c.index["a"]
	_, hasB := c.index["b"]
	c.mu.Unlock()
	if hasA {
		t.Error("least-recently-used entry a should have been evicted")
	}
	if !hasB {
		t.Error("most-recently-used entry b should remain")
	}
}

func TestLogFileIsCacheable(t *testing.T) {
	now := time.Now()
	if logFileIsCacheable(now.UnixNano()) {
		t.Error("a current-minute file must not be cacheable")
	}
	old := now.Add(-persistedLogCacheMinAge - time.Minute).UnixNano()
	if !logFileIsCacheable(old) {
		t.Error("a file older than the min age should be cacheable")
	}
}

func TestLogFileIteratorCachedFiltering(t *testing.T) {
	iter := &LogFileIterator{
		cached:    logEntriesAt(10, 20, 30, 40),
		startTsNs: 15,
		stopTsNs:  35,
	}
	var got []int64
	for {
		e, err := iter.getNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e.TsNs)
	}
	want := []int64{20, 30} // 10 filtered by startTsNs, 40 cut off by stopTsNs
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("cached filtering: got %v, want %v", got, want)
	}
}

func TestLogFileIteratorCachedLoadError(t *testing.T) {
	iter := &LogFileIterator{loadErr: fmt.Errorf("boom")}
	if _, err := iter.getNext(); err == nil || err.Error() != "boom" {
		t.Fatalf("expected load error surfaced on first getNext, got %v", err)
	}
}

func TestLogFileIteratorCachedYieldsBeforeError(t *testing.T) {
	iter := &LogFileIterator{cached: logEntriesAt(10, 20), loadErr: fmt.Errorf("boom")}
	var got []int64
	for {
		e, err := iter.getNext()
		if err != nil {
			if err.Error() != "boom" {
				t.Fatalf("expected boom after entries, got %v", err)
			}
			break
		}
		got = append(got, e.TsNs)
	}
	if fmt.Sprint(got) != fmt.Sprint([]int64{10, 20}) {
		t.Fatalf("partial read must yield entries before the error, got %v", got)
	}
}
