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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func testDirtyPutGet(t *testing.T) {
	dm := NewDirtyMap()
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
	dm := NewDirtyMap()
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
	dm := NewDirtyMap()
	dm.Put(100, 5000, 1, 4096)
	dm.Delete(100)

	_, _, _, ok := dm.Get(100)
	if ok {
		t.Error("Get(100) should return not-found after delete")
	}
}

func testDirtyRangeQuery(t *testing.T) {
	dm := NewDirtyMap()
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
	dm := NewDirtyMap()

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
	dm := NewDirtyMap()
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
	dm := NewDirtyMap()
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
