package blockvol

import (
	"sync"
	"testing"
)

func TestDirtyMap(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "dirty_put_get", run: testDirtyPutGet},
		{name: "dirty_overwrite", run: testDirtyOverwrite},
		{name: "dirty_delete", run: testDirtyDelete},
		{name: "dirty_range_query", run: testDirtyRangeQuery},
		{name: "dirty_empty", run: testDirtyEmpty},
		{name: "dirty_concurrent_rw", run: testDirtyConcurrentRW},
		{name: "dirty_range_modify", run: testDirtyRangeModify},
		// Phase 3 Task 1.3: Sharded DirtyMap tests.
		{name: "sharded_put_get", run: testShardedPutGet},
		{name: "sharded_cross_shard", run: testShardedCrossShard},
		{name: "sharded_concurrent_rw_256", run: testShardedConcurrentRW256},
		{name: "sharded_snapshot_all_shards", run: testShardedSnapshotAllShards},
		{name: "sharded_range_cross_shard", run: testShardedRangeCrossShard},
		{name: "sharded_1_shard_compat", run: testSharded1ShardCompat},
		{name: "sharded_delete", run: testShardedDelete},
		{name: "sharded_overwrite", run: testShardedOverwrite},
		{name: "sharded_concurrent_range_during_write", run: testShardedConcurrentRangeDuringWrite},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testDirtyPutGet(t *testing.T) {
	dm := NewDirtyMap(1)
	dm.Put(100, 5000, 1, 4096)

	off, lsn, length, ok := dm.Get(100)
	if !ok {
		t.Fatal("Get(100) returned not-found")
	}
	if off != 5000 {
		t.Errorf("walOffset = %d, want 5000", off)
	}
	if lsn != 1 {
		t.Errorf("lsn = %d, want 1", lsn)
	}
	if length != 4096 {
		t.Errorf("length = %d, want 4096", length)
	}
}

func testDirtyOverwrite(t *testing.T) {
	dm := NewDirtyMap(1)
	dm.Put(100, 5000, 1, 4096)
	dm.Put(100, 9000, 2, 4096)

	off, lsn, _, ok := dm.Get(100)
	if !ok {
		t.Fatal("Get(100) returned not-found")
	}
	if off != 9000 {
		t.Errorf("walOffset = %d, want 9000 (latest)", off)
	}
	if lsn != 2 {
		t.Errorf("lsn = %d, want 2 (latest)", lsn)
	}
}

func testDirtyDelete(t *testing.T) {
	dm := NewDirtyMap(1)
	dm.Put(100, 5000, 1, 4096)
	dm.Delete(100)

	_, _, _, ok := dm.Get(100)
	if ok {
		t.Error("Get(100) should return not-found after delete")
	}
}

func testDirtyRangeQuery(t *testing.T) {
	dm := NewDirtyMap(1)
	for i := uint64(100); i < 110; i++ {
		dm.Put(i, i*1000, i, 4096)
	}

	found := make(map[uint64]bool)
	dm.Range(100, 10, func(lba, walOffset, lsn uint64, length uint32) {
		found[lba] = true
	})

	if len(found) != 10 {
		t.Errorf("Range returned %d entries, want 10", len(found))
	}
	for i := uint64(100); i < 110; i++ {
		if !found[i] {
			t.Errorf("Range missing LBA %d", i)
		}
	}
}

func testDirtyEmpty(t *testing.T) {
	dm := NewDirtyMap(1)

	_, _, _, ok := dm.Get(0)
	if ok {
		t.Error("empty map: Get(0) should return not-found")
	}
	_, _, _, ok = dm.Get(999)
	if ok {
		t.Error("empty map: Get(999) should return not-found")
	}
	if dm.Len() != 0 {
		t.Errorf("empty map: Len() = %d, want 0", dm.Len())
	}
}

func testDirtyConcurrentRW(t *testing.T) {
	dm := NewDirtyMap(1)
	const goroutines = 16
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			base := uint64(id * opsPerGoroutine)
			for i := uint64(0); i < opsPerGoroutine; i++ {
				lba := base + i
				dm.Put(lba, lba*10, lba, 4096)
				dm.Get(lba)
				if i%3 == 0 {
					dm.Delete(lba)
				}
			}
		}(g)
	}
	wg.Wait()

	// No assertion on final state -- the test passes if no race detector fires.
}

func testDirtyRangeModify(t *testing.T) {
	// Verify that Range callback can safely call Delete without deadlock.
	dm := NewDirtyMap(1)
	for i := uint64(0); i < 10; i++ {
		dm.Put(i, i*1000, i+1, 4096)
	}

	// Delete every entry from within the Range callback.
	dm.Range(0, 10, func(lba, walOffset, lsn uint64, length uint32) {
		dm.Delete(lba)
	})

	if dm.Len() != 0 {
		t.Errorf("after Range+Delete: Len() = %d, want 0", dm.Len())
	}
}

// --- Phase 3 Task 1.3: Sharded DirtyMap tests ---

func testShardedPutGet(t *testing.T) {
	dm := NewDirtyMap(256)
	dm.Put(100, 5000, 1, 4096)

	off, lsn, length, ok := dm.Get(100)
	if !ok {
		t.Fatal("Get(100) returned not-found")
	}
	if off != 5000 || lsn != 1 || length != 4096 {
		t.Errorf("got (%d, %d, %d), want (5000, 1, 4096)", off, lsn, length)
	}
}

func testShardedCrossShard(t *testing.T) {
	dm := NewDirtyMap(4) // 4 shards: mask = 3
	// LBAs 0,1,2,3 each go to different shards.
	for i := uint64(0); i < 4; i++ {
		dm.Put(i, i*1000, i+1, 4096)
	}
	if dm.Len() != 4 {
		t.Errorf("Len() = %d, want 4", dm.Len())
	}
	for i := uint64(0); i < 4; i++ {
		off, lsn, _, ok := dm.Get(i)
		if !ok {
			t.Fatalf("Get(%d) not found", i)
		}
		if off != i*1000 || lsn != i+1 {
			t.Errorf("LBA %d: got (%d, %d), want (%d, %d)", i, off, lsn, i*1000, i+1)
		}
	}
	// Delete from specific shards.
	dm.Delete(1)
	dm.Delete(3)
	if dm.Len() != 2 {
		t.Errorf("after delete: Len() = %d, want 2", dm.Len())
	}
}

func testShardedConcurrentRW256(t *testing.T) {
	dm := NewDirtyMap(256)
	const goroutines = 32
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			base := uint64(id * opsPerGoroutine)
			for i := uint64(0); i < opsPerGoroutine; i++ {
				lba := base + i
				dm.Put(lba, lba*10, lba, 4096)
				dm.Get(lba)
				if i%3 == 0 {
					dm.Delete(lba)
				}
			}
		}(g)
	}
	wg.Wait()
	// Test passes if no race/panic.
}

func testShardedSnapshotAllShards(t *testing.T) {
	dm := NewDirtyMap(16)
	// Insert entries that span all 16 shards.
	for i := uint64(0); i < 100; i++ {
		dm.Put(i, i*100, i+1, 4096)
	}

	snap := dm.Snapshot()
	if len(snap) != 100 {
		t.Errorf("Snapshot len = %d, want 100", len(snap))
	}

	// Verify all entries present.
	seen := make(map[uint64]bool)
	for _, e := range snap {
		seen[e.Lba] = true
	}
	for i := uint64(0); i < 100; i++ {
		if !seen[i] {
			t.Errorf("Snapshot missing LBA %d", i)
		}
	}
}

func testShardedRangeCrossShard(t *testing.T) {
	dm := NewDirtyMap(8)
	// LBAs 10-19 spread across shards.
	for i := uint64(10); i < 20; i++ {
		dm.Put(i, i*100, i, 4096)
	}
	// Also put entries outside range.
	dm.Put(0, 0, 1, 4096)
	dm.Put(100, 100, 1, 4096)

	found := make(map[uint64]bool)
	dm.Range(10, 10, func(lba, walOffset, lsn uint64, length uint32) {
		found[lba] = true
	})
	if len(found) != 10 {
		t.Errorf("Range returned %d entries, want 10", len(found))
	}
	for i := uint64(10); i < 20; i++ {
		if !found[i] {
			t.Errorf("Range missing LBA %d", i)
		}
	}
}

func testShardedDelete(t *testing.T) {
	dm := NewDirtyMap(256)
	dm.Put(100, 5000, 1, 4096)
	dm.Delete(100)

	_, _, _, ok := dm.Get(100)
	if ok {
		t.Error("Get(100) should return not-found after delete with 256 shards")
	}
	if dm.Len() != 0 {
		t.Errorf("Len() = %d, want 0", dm.Len())
	}
}

func testShardedOverwrite(t *testing.T) {
	dm := NewDirtyMap(256)
	dm.Put(100, 5000, 1, 4096)
	dm.Put(100, 9000, 2, 4096)

	off, lsn, _, ok := dm.Get(100)
	if !ok {
		t.Fatal("Get(100) returned not-found after overwrite")
	}
	if off != 9000 {
		t.Errorf("walOffset = %d, want 9000 (latest)", off)
	}
	if lsn != 2 {
		t.Errorf("lsn = %d, want 2 (latest)", lsn)
	}
	if dm.Len() != 1 {
		t.Errorf("Len() = %d, want 1", dm.Len())
	}
}

func testShardedConcurrentRangeDuringWrite(t *testing.T) {
	dm := NewDirtyMap(256)
	// Pre-populate with entries.
	for i := uint64(0); i < 100; i++ {
		dm.Put(i, i*10, i+1, 4096)
	}

	var wg sync.WaitGroup
	// Writers: continuously Put new entries.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(100); i < 1100; i++ {
			dm.Put(i, i*10, i+1, 4096)
		}
	}()

	// Concurrent Range reader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 50; round++ {
			count := 0
			dm.Range(0, 200, func(lba, walOffset, lsn uint64, length uint32) {
				count++
			})
			// Range should return some entries (at least the pre-populated ones).
			if count == 0 {
				// Shouldn't happen, but don't fatal from goroutine.
				return
			}
		}
	}()
	wg.Wait()
	// Test passes if no race/panic.
}

func testSharded1ShardCompat(t *testing.T) {
	// 1-shard map should behave identically to old single-mutex map.
	dm := NewDirtyMap(1)
	for i := uint64(0); i < 50; i++ {
		dm.Put(i, i*10, i+1, 4096)
	}
	if dm.Len() != 50 {
		t.Errorf("Len() = %d, want 50", dm.Len())
	}
	snap := dm.Snapshot()
	if len(snap) != 50 {
		t.Errorf("Snapshot len = %d, want 50", len(snap))
	}
	dm.Range(10, 10, func(lba, walOffset, lsn uint64, length uint32) {
		dm.Delete(lba)
	})
	if dm.Len() != 40 {
		t.Errorf("after Range+Delete: Len() = %d, want 40", dm.Len())
	}
}
