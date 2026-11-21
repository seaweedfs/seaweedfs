package storage

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// startSearcher runs fn in a loop until the returned stop is called. stop is
// failure-safe (it runs from a defer, also after t.Fatal) and joins the
// goroutine with a bound; it reports whether the goroutine ended.
func startSearcher(t *testing.T, fn func()) (stop func() bool) {
	var halt atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !halt.Load() {
			fn()
		}
	}()
	return func() bool {
		halt.Store(true)
		select {
		case <-done:
			return true
		case <-time.After(30 * time.Second):
			t.Errorf("searcher did not stop within 30s")
			return false
		}
	}
}

// The append-time search (tail and incremental copy) must stay correct while a
// vacuum commit swaps the volume's files: it takes the index size once and
// then probes .idx rows and .dat records, so a commit that lands mid-search
// must not turn into a read error (EOF past the new, smaller files) or a
// wrong answer.
func TestBinarySearchByAppendAtNsDuringCompactionCommit(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	joined := true
	defer func() {
		if joined { // never close the volume under a searcher that is still running
			v.Close()
		}
	}()

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
		inCommit     atomic.Bool
		commitsBegun atomic.Int64
		searches     atomic.Int64
		overlapped   atomic.Int64
		kind         atomic.Int64
		mu           sync.Mutex
		failures     []string
	)
	fail := func(format string) {
		mu.Lock()
		failures = append(failures, format)
		mu.Unlock()
	}
	stop := startSearcher(t, func() {
		before, busy := commitsBegun.Load(), inCommit.Load()
		k := kind.Add(1) % 3
		var since uint64
		switch k {
		case 0:
			since = 0 // before every record: the first record, never "last"
		case 1:
			since = uint64(time.Now().Add(time.Hour).UnixNano()) // after every record: "last"
		default:
			since = uint64(time.Now().UnixNano()) - uint64(time.Second) // inside the history
		}
		offset, isLast, err := v.BinarySearchByAppendAtNs(since)
		if busy || inCommit.Load() || commitsBegun.Load() != before {
			overlapped.Add(1)
		}
		searches.Add(1)
		switch {
		case err != nil:
			fail("search error: " + err.Error())
		case k == 0 && (isLast || offset.IsZero()):
			fail("search from 0 did not return the first record")
		case k == 1 && !isLast:
			fail("search from the future did not report the end")
		}
	})
	defer func() { joined = stop() }()

	deadline := time.Now().Add(3 * time.Second)
	rounds := 0
	for time.Now().Before(deadline) {
		// shrink the volume on every commit: delete the newest live needles, add a few
		for id := next - 1; id > 0 && id+400 > next; id-- {
			if _, err := v.deleteNeedle2(newEmptyNeedle(id), false); err != nil {
				t.Fatalf("delete %d: %v", id, err)
			}
		}
		write(400)
		if err := v.CompactByIndex(nil); err != nil {
			t.Fatalf("compact round %d: %v", rounds, err)
		}
		inCommit.Store(true)
		commitsBegun.Add(1)
		err := v.CommitCompact()
		inCommit.Store(false)
		if err != nil {
			t.Fatalf("commit round %d: %v", rounds, err)
		}
		rounds++
	}
	if joined = stop(); !joined {
		return // the searcher may still be appending to failures; stop already reported it
	}

	mu.Lock()
	failed := append([]string(nil), failures...)
	mu.Unlock()
	t.Logf("%d compaction commits, %d searches (%d overlapping a commit), %d failed", rounds, searches.Load(), overlapped.Load(), len(failed))
	if overlapped.Load() == 0 {
		t.Fatalf("no search overlapped a commit; the test did not exercise the race")
	}
	if len(failed) > 0 {
		t.Fatalf("append-time search failed during compaction commits (first of %d): %s", len(failed), failed[0])
	}
}

// The search holds the data-file read lock for its whole run, so its length
// bounds how long a foreground write can wait. The slowest search is one whose
// probe lands in a long run of index-only tombstones (offset zero): it scans
// left and right row by row. With such a run of searchLockRunRows rows, a
// search and a foreground write must stay within searchLockBudget. Every
// search scans the run, so a slow search shows up in every round, while a
// stall of a loaded host (a goroutine not scheduled for a while) hits one
// round: the check passes once a round stays within the budget. The budget
// is a Linux figure (15-40 ms measured there); other platforms only log the
// times, since per-row file reads are much slower there (about 0.8 s per search
// measured on Windows). Correctness (no search error) is checked everywhere.
const (
	searchLockRunRows = 20000
	searchLockBudget  = 250 * time.Millisecond
	searchLockRounds  = 5
	searchLockRound   = 500 * time.Millisecond
)

func TestBinarySearchByAppendAtNsLockHoldBudget(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	joined := true
	defer func() {
		if joined {
			v.Close()
		}
	}()

	next := uint64(1)
	write := func(n int) {
		for i := 0; i < n; i++ {
			if _, _, _, err := v.writeNeedle2(newRandomNeedle(next), true, false, false); err != nil {
				t.Fatalf("write %d: %v", next, err)
			}
			next++
		}
	}
	write(500)
	mid := uint64(time.Now().UnixNano())
	// a long run of index-only tombstones (as remote-tier deletes write)
	for i := 0; i < searchLockRunRows; i++ {
		if err := v.nm.Delete(NeedleId(1_000_000+i), Offset{}); err != nil {
			t.Fatalf("index-only tombstone %d: %v", i, err)
		}
	}
	write(500)

	measure := func() (searches int64, maxSearch, maxWrite time.Duration) {
		var slowest, count atomic.Int64
		stop := startSearcher(t, func() {
			start := time.Now()
			if _, _, err := v.BinarySearchByAppendAtNs(mid); err != nil {
				t.Errorf("search: %v", err)
			}
			if d := int64(time.Since(start)); d > slowest.Load() {
				slowest.Store(d)
			}
			count.Add(1)
		})
		defer func() { joined = stop() }()

		deadline := time.Now().Add(searchLockRound)
		for time.Now().Before(deadline) {
			start := time.Now()
			write(1)
			if d := time.Since(start); d > maxWrite {
				maxWrite = d
			}
		}
		if joined = stop(); !joined {
			return
		}
		return count.Load(), time.Duration(slowest.Load()), maxWrite
	}

	searched := false
	for round := 1; round <= searchLockRounds; round++ {
		searches, maxSearch, maxWrite := measure()
		if !joined {
			return
		}
		t.Logf("round %d: %d searches over a %d-row tombstone run: slowest search %v, slowest foreground write %v (budget %v)",
			round, searches, searchLockRunRows, maxSearch, maxWrite, searchLockBudget)
		if searches == 0 {
			continue
		}
		searched = true
		if runtime.GOOS != "linux" || (maxSearch <= searchLockBudget && maxWrite <= searchLockBudget) {
			return
		}
	}
	if !searched {
		t.Fatal("no search ran")
	}
	t.Errorf("a search or a foreground write went over the %v budget in every one of %d rounds", searchLockBudget, searchLockRounds)
}
