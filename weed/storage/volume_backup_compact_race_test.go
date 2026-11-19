package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

// The append-time search (tail and incremental copy) must stay correct while a
// vacuum commit swaps the volume's files: it takes the index size once and
// then probes .idx rows and .dat records, so a commit that lands mid-search
// must not turn into a read error (EOF past the new, smaller files).
func TestBinarySearchByAppendAtNsDuringCompactionCommit(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	next := uint64(1)
	write := func(n int) {
		for i := 0; i < n; i++ {
			if _, _, _, err := v.writeNeedle2(newRandomNeedle(next), true, false, false); err != nil {
				t.Fatalf("write %d: %v", next, err)
			}
			next++
		}
	}
	write(2000)

	var (
		stop     atomic.Bool
		searches atomic.Int64
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			// a timestamp inside the volume's history: the search probes real rows
			if _, _, err := v.BinarySearchByAppendAtNs(uint64(time.Now().UnixNano()) - uint64(time.Second)); err != nil {
				mu.Lock()
				failures = append(failures, err)
				mu.Unlock()
			}
			searches.Add(1)
		}
	}()

	deadline := time.Now().Add(3 * time.Second)
	rounds := 0
	for time.Now().Before(deadline) {
		// shrink the volume on every commit: delete most live needles, add a few
		for id := next - 1; id > 0 && id+400 > next; id-- {
			v.deleteNeedle2(newEmptyNeedle(id), false)
		}
		write(400)
		if err := v.CompactByIndex(nil); err != nil {
			t.Fatalf("compact round %d: %v", rounds, err)
		}
		if err := v.CommitCompact(); err != nil {
			t.Fatalf("commit round %d: %v", rounds, err)
		}
		rounds++
	}
	stop.Store(true)
	wg.Wait()

	t.Logf("%d compaction commits, %d searches, %d failed", rounds, searches.Load(), len(failures))
	if len(failures) > 0 {
		t.Fatalf("append-time search failed during compaction commits (first of %d): %v", len(failures), failures[0])
	}
}
