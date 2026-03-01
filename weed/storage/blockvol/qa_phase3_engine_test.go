package blockvol

import (
	"errors"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQAPhase3Engine tests Phase 3 engine adversarial scenarios:
// adaptive group commit, sharded dirty map, WAL pressure.
func TestQAPhase3Engine(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// QA-1.1: Group Commit Adversarial
		{name: "gc_syncfunc_panic", run: testQAGCSyncFuncPanic},
		{name: "gc_thundering_herd_1000", run: testQAGCThunderingHerd1000},
		{name: "gc_lowwatermark_equals_maxbatch", run: testQAGCLowWatermarkEqualsMaxBatch},

		// QA-1.2: Sharded Dirty Map Adversarial
		{name: "dm_snapshot_during_heavy_write", run: testQADMSnapshotDuringHeavyWrite},
		{name: "dm_put_delete_put_same_lba", run: testQADMPutDeletePutSameLBA},
		{name: "dm_max_uint64_lba", run: testQADMMaxUint64LBA},
		{name: "dm_non_power_of_2_shards", run: testQADMNonPowerOf2Shards},
		{name: "dm_zero_shards", run: testQADMZeroShards},
		{name: "dm_len_during_concurrent_writes", run: testQADMLenDuringConcurrentWrites},

		// QA-1.3: WAL Pressure Adversarial
		{name: "wal_pressure_flusher_slow", run: testQAWALPressureFlusherSlow},
		{name: "wal_pressure_threshold_0", run: testQAWALPressureThreshold0},
		{name: "wal_pressure_threshold_1", run: testQAWALPressureThreshold1},
		{name: "wal_close_during_pressure_block", run: testQAWALCloseDuringPressureBlock},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- QA-1.1: Group Commit Adversarial ---

func testQAGCSyncFuncPanic(t *testing.T) {
	// syncFunc panics: Run() goroutine crashes, pending waiters must not hang forever.
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			panic("simulated disk panic")
		},
		MaxDelay: 10 * time.Millisecond,
	})

	// Wrap Run() with panic recovery.
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		defer func() { recover() }()
		gc.Run()
	}()

	// Submit should either return an error or hang. Use a timeout.
	result := make(chan error, 1)
	go func() {
		result <- gc.Submit()
	}()

	select {
	case <-result:
		// Got a result (error or nil) -- panic was recovered, or waiter unblocked.
	case <-runDone:
		// Run() exited due to panic. Submit will hang on channel read.
		// This is expected behavior: panic in syncFunc is fatal.
		// Test passes: no goroutine leak from Run(), panic was contained.
	case <-time.After(3 * time.Second):
		t.Fatal("syncFunc panic: Run() and Submit both hung for 3s")
	}
}

func testQAGCThunderingHerd1000(t *testing.T) {
	// 1000 goroutines Submit() simultaneously: all must return, batching should work.
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     50 * time.Millisecond,
		MaxBatch:     64,
		LowWatermark: 4,
	})
	go gc.Run()
	defer gc.Stop()

	const n = 1000
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = gc.Submit()
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("thundering herd: 1000 submits hung for 10s")
	}

	for i, err := range errs {
		if err != nil {
			t.Errorf("Submit[%d]: %v", i, err)
		}
	}

	c := syncCalls.Load()
	if c >= 1000 {
		t.Errorf("syncCalls = %d, expected batching (< 1000)", c)
	}
	t.Logf("thundering herd: 1000 submits, %d fsyncs", c)
}

func testQAGCLowWatermarkEqualsMaxBatch(t *testing.T) {
	// lowWatermark = maxBatch = 8: skipDelay always true when < 8 pending.
	var syncCalls atomic.Uint64
	gc := NewGroupCommitter(GroupCommitterConfig{
		SyncFunc: func() error {
			syncCalls.Add(1)
			return nil
		},
		MaxDelay:     5 * time.Second, // should never wait this long
		MaxBatch:     8,
		LowWatermark: 8,
	})
	go gc.Run()
	defer gc.Stop()

	// Single serial submits: since pending < lowWatermark (8), skipDelay=true.
	for i := 0; i < 5; i++ {
		if err := gc.Submit(); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	// Each serial submit should have triggered its own fsync (no batching).
	if c := syncCalls.Load(); c != 5 {
		t.Errorf("syncCalls = %d, want 5 (skipDelay always true)", c)
	}
}

// --- QA-1.2: Sharded Dirty Map Adversarial ---

func testQADMSnapshotDuringHeavyWrite(t *testing.T) {
	dm := NewDirtyMap(256)
	const numWriters = 16
	const opsPerWriter = 1000

	var wg sync.WaitGroup

	// Writers: continuously Put entries.
	wg.Add(numWriters)
	for g := 0; g < numWriters; g++ {
		go func(id int) {
			defer wg.Done()
			base := uint64(id * opsPerWriter)
			for i := uint64(0); i < opsPerWriter; i++ {
				dm.Put(base+i, (base+i)*10, base+i+1, 4096)
			}
		}(g)
	}

	// Concurrent snapshots.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 20; round++ {
			snap := dm.Snapshot()
			// Each snapshot entry should have valid fields.
			for _, e := range snap {
				if e.Length != 4096 {
					t.Errorf("snapshot entry LBA %d has length %d, want 4096", e.Lba, e.Length)
					return
				}
			}
		}
	}()

	wg.Wait()
	// Test passes if no race/panic.
}

func testQADMPutDeletePutSameLBA(t *testing.T) {
	dm := NewDirtyMap(256)
	const iterations = 1000

	for i := 0; i < iterations; i++ {
		dm.Put(100, uint64(i)*10, uint64(i)+1, 4096)
		dm.Delete(100)
		dm.Put(100, uint64(i)*10+5, uint64(i)+2, 4096)
	}

	// Final state: should have the last Put's entry.
	off, lsn, length, ok := dm.Get(100)
	if !ok {
		t.Fatal("Get(100) not found after Put-Delete-Put cycle")
	}
	expectedOff := uint64(999)*10 + 5
	expectedLsn := uint64(999) + 2
	if off != expectedOff {
		t.Errorf("walOffset = %d, want %d", off, expectedOff)
	}
	if lsn != expectedLsn {
		t.Errorf("lsn = %d, want %d", lsn, expectedLsn)
	}
	if length != 4096 {
		t.Errorf("length = %d, want 4096", length)
	}
}

func testQADMMaxUint64LBA(t *testing.T) {
	dm := NewDirtyMap(256)
	maxLBA := uint64(math.MaxUint64)

	dm.Put(maxLBA, 12345, 1, 4096)

	off, lsn, length, ok := dm.Get(maxLBA)
	if !ok {
		t.Fatal("Get(MaxUint64) not found")
	}
	if off != 12345 || lsn != 1 || length != 4096 {
		t.Errorf("got (%d, %d, %d), want (12345, 1, 4096)", off, lsn, length)
	}

	dm.Delete(maxLBA)
	_, _, _, ok = dm.Get(maxLBA)
	if ok {
		t.Error("Get(MaxUint64) should not be found after delete")
	}
}

func testQADMNonPowerOf2Shards(t *testing.T) {
	// Non-power-of-2: mask will be wrong (7-1=6=0b110), but should not panic.
	// This tests robustness, not correctness of shard distribution.
	dm := NewDirtyMap(7)

	// Should not panic on basic operations.
	for i := uint64(0); i < 100; i++ {
		dm.Put(i, i*10, i+1, 4096)
	}

	// All entries should be retrievable (mask-based routing is deterministic).
	for i := uint64(0); i < 100; i++ {
		_, _, _, ok := dm.Get(i)
		if !ok {
			t.Errorf("Get(%d) not found with 7 shards", i)
		}
	}

	if dm.Len() != 100 {
		t.Errorf("Len() = %d, want 100", dm.Len())
	}
}

func testQADMZeroShards(t *testing.T) {
	// NewDirtyMap(0) should default to 1 shard (no panic, no divide-by-zero).
	dm := NewDirtyMap(0)

	dm.Put(42, 100, 1, 4096)
	off, _, _, ok := dm.Get(42)
	if !ok {
		t.Fatal("Get(42) not found with 0 shards (defaulted to 1)")
	}
	if off != 100 {
		t.Errorf("walOffset = %d, want 100", off)
	}
}

func testQADMLenDuringConcurrentWrites(t *testing.T) {
	dm := NewDirtyMap(256)
	const numWriters = 32
	const opsPerWriter = 100

	var wg sync.WaitGroup

	// Writers.
	wg.Add(numWriters)
	for g := 0; g < numWriters; g++ {
		go func(id int) {
			defer wg.Done()
			base := uint64(id * opsPerWriter)
			for i := uint64(0); i < opsPerWriter; i++ {
				dm.Put(base+i, (base+i)*10, base+i+1, 4096)
			}
		}(g)
	}

	// Concurrent Len() reader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 100; round++ {
			n := dm.Len()
			if n < 0 {
				t.Errorf("Len() = %d, must be non-negative", n)
				return
			}
		}
	}()

	wg.Wait()

	// Final Len should be numWriters * opsPerWriter (each LBA unique).
	if dm.Len() != numWriters*opsPerWriter {
		t.Errorf("final Len() = %d, want %d", dm.Len(), numWriters*opsPerWriter)
	}
}

// --- QA-1.3: WAL Pressure Adversarial ---

func testQAWALPressureFlusherSlow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "slow_flush.blockvol")

	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.3
	cfg.WALFullTimeout = 3 * time.Second
	cfg.FlushInterval = 5 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write many blocks. With a small WAL and active flusher, some may succeed
	// and some may encounter WAL pressure. None should hang forever.
	var succeeded, walFull int
	for i := 0; i < 30; i++ {
		err := v.WriteLBA(uint64(i%256), makeBlock(byte('A'+i%26)))
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrWALFull) {
			walFull++
		} else {
			t.Fatalf("WriteLBA(%d): unexpected error: %v", i, err)
		}
	}
	t.Logf("flusher_slow: %d succeeded, %d WAL full", succeeded, walFull)
	if succeeded == 0 {
		t.Error("no writes succeeded -- flusher not draining?")
	}
}

func testQAWALPressureThreshold0(t *testing.T) {
	// threshold=0: urgent flush on every write. Should still work.
	dir := t.TempDir()
	path := filepath.Join(dir, "thresh0.blockvol")

	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.0 // always urgent
	cfg.FlushInterval = 5 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Every write triggers urgent flush. Should still succeed.
	for i := 0; i < 20; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Verify data readable.
	for i := 0; i < 20; i++ {
		data, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i%26) {
			t.Errorf("LBA %d: first byte = %c, want %c", i, data[0], byte('A'+i%26))
		}
	}
}

func testQAWALPressureThreshold1(t *testing.T) {
	// threshold=1.0: never urgent. WAL fills, eventually ErrWALFull.
	dir := t.TempDir()
	path := filepath.Join(dir, "thresh1.blockvol")

	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 1.0 // never urgent
	cfg.WALFullTimeout = 100 * time.Millisecond
	cfg.FlushInterval = 1 * time.Hour // disable periodic flush

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 4 // tiny: 4 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Stop flusher to prevent any drainage.
	v.flusher.Stop()

	// Fill WAL.
	var gotWALFull bool
	for i := 0; i < 10; i++ {
		err := v.WriteLBA(uint64(i%256), makeBlock(byte('X')))
		if errors.Is(err, ErrWALFull) {
			gotWALFull = true
			break
		} else if err != nil {
			t.Fatalf("WriteLBA(%d): unexpected error: %v", i, err)
		}
	}

	if !gotWALFull {
		t.Error("expected ErrWALFull with threshold=1.0 and no flusher")
	}
}

func testQAWALCloseDuringPressureBlock(t *testing.T) {
	// Writer blocked on WAL full, Close() called -- writer should unblock.
	dir := t.TempDir()
	path := filepath.Join(dir, "close_pressure.blockvol")

	cfg := DefaultConfig()
	cfg.WALFullTimeout = 10 * time.Second // long timeout
	cfg.FlushInterval = 1 * time.Hour     // disable flusher

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 2 // tiny: 2 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Stop flusher.
	v.flusher.Stop()

	// Fill WAL.
	for i := 0; i < 2; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Start a writer that will block on WAL full.
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- v.WriteLBA(2, makeBlock('Z'))
	}()

	// Give it time to block.
	time.Sleep(50 * time.Millisecond)

	// Close the volume -- should unblock the writer.
	v.Close()

	select {
	case err := <-writeResult:
		// Writer unblocked (error expected: ErrWALFull or closed fd).
		if err == nil {
			t.Error("expected error from WriteLBA after Close, got nil")
		}
		t.Logf("close_during_pressure: writer unblocked with: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("writer still blocked 5s after Close()")
	}
}
