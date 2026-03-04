package blockvol

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQASnapshot runs adversarial tests for CP5-2 CoW snapshots.
func TestQASnapshot(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// Group A: Race conditions and concurrency
		{name: "concurrent_create_same_id", run: testQASnap_ConcurrentCreateSameID},
		{name: "concurrent_create_different_ids", run: testQASnap_ConcurrentCreateDifferentIDs},
		{name: "delete_during_flush_cow", run: testQASnap_DeleteDuringFlushCoW},
		{name: "concurrent_read_snapshot_during_cow", run: testQASnap_ConcurrentReadDuringCoW},
		{name: "create_snapshot_during_close", run: testQASnap_CreateDuringClose},

		// Group B: Role and state rejection
		{name: "create_on_replica_rejected", run: testQASnap_CreateOnReplicaRejected},
		{name: "create_on_stale_rejected", run: testQASnap_CreateOnStaleRejected},
		{name: "create_on_rebuilding_rejected", run: testQASnap_CreateOnRebuildingRejected},
		{name: "create_duplicate_id_rejected", run: testQASnap_CreateDuplicateIDRejected},

		// Group C: Edge cases
		{name: "read_unmodified_block_returns_zeros", run: testQASnap_ReadUnmodifiedBlockZeros},
		{name: "cow_trim_block_preserves_old_data", run: testQASnap_CoWTrimBlockPreservesOldData},
		{name: "delete_nonexistent_snapshot", run: testQASnap_DeleteNonexistent},
		{name: "read_nonexistent_snapshot", run: testQASnap_ReadNonexistent},
		{name: "restore_nonexistent_snapshot", run: testQASnap_RestoreNonexistent},
		{name: "snapshot_at_boundary_lba", run: testQASnap_BoundaryLBA},

		// Group D: Stress and lifecycle
		{name: "create_delete_many_snapshots", run: testQASnap_CreateDeleteMany},
		{name: "restore_then_new_snapshot", run: testQASnap_RestoreThenNewSnapshot},
		{name: "multiple_flush_cycles_cow_idempotent", run: testQASnap_MultiFlushCoWIdempotent},
		{name: "snapshot_recovery_after_post_cow_writes", run: testQASnap_RecoveryAfterPostCoWWrites},

		// Group E: Restore correctness
		{name: "restore_with_multiple_snapshots", run: testQASnap_RestoreWithMultiple},
		{name: "restore_volume_writable_after", run: testQASnap_RestoreWritableAfter},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// --- Group A: Race conditions and concurrency ---

// testQASnap_ConcurrentCreateSameID: two goroutines race to create
// the same snapshot ID. Exactly one must succeed, the other must get
// ErrSnapshotExists (or O_EXCL file error). No double-registration.
func testQASnap_ConcurrentCreateSameID(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	var wg sync.WaitGroup
	var successes atomic.Int32
	var failures atomic.Int32

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := v.CreateSnapshot(42)
			if err == nil {
				successes.Add(1)
			} else {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()

	// Exactly one should succeed.
	if s := successes.Load(); s != 1 {
		t.Fatalf("expected exactly 1 success, got %d", s)
	}

	// Snapshot map should have exactly one entry.
	v.snapMu.RLock()
	n := len(v.snapshots)
	v.snapMu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 snapshot in map, got %d", n)
	}

	// Delta file should exist exactly once.
	deltaPath := deltaFilePath(v.path, 42)
	if _, err := os.Stat(deltaPath); err != nil {
		t.Fatalf("delta file missing: %v", err)
	}
}

// testQASnap_ConcurrentCreateDifferentIDs: multiple goroutines create
// different snapshot IDs concurrently. All should succeed.
func testQASnap_ConcurrentCreateDifferentIDs(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	const n = 5
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			errs[id] = v.CreateSnapshot(uint32(id + 1))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("snapshot %d failed: %v", i+1, err)
		}
	}

	infos := v.ListSnapshots()
	if len(infos) != n {
		t.Fatalf("expected %d snapshots, got %d", n, len(infos))
	}
}

// testQASnap_DeleteDuringFlushCoW: tries to trigger the race where
// the flusher copies snapshots into a local slice, then DeleteSnapshot
// closes the fd while flusher is still using it.
func testQASnap_DeleteDuringFlushCoW(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write initial data and create snapshot.
	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// Write new data to trigger CoW on next flush.
	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()

	// Race: flush (which does CoW) vs delete.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		v.flusher.FlushOnce()
	}()
	go func() {
		defer wg.Done()
		// Small delay to let flush start.
		time.Sleep(100 * time.Microsecond)
		v.DeleteSnapshot(1)
	}()
	wg.Wait()

	// Volume should still be functional (no panic, no EBADF crash).
	if err := v.WriteLBA(0, makeBlock('C')); err != nil {
		t.Fatalf("write after race: %v", err)
	}
}

// testQASnap_ConcurrentReadDuringCoW: reads from a snapshot while the
// flusher is actively performing CoW on other blocks.
func testQASnap_ConcurrentReadDuringCoW(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write blocks 0-9.
	for i := uint64(0); i < 10; i++ {
		v.WriteLBA(i, makeBlock(byte('A'+i)))
	}
	v.SyncCache()
	v.CreateSnapshot(1)

	// Modify blocks 5-9 to trigger CoW.
	for i := uint64(5); i < 10; i++ {
		v.WriteLBA(i, makeBlock('Z'))
	}
	v.SyncCache()

	// Race: flush (CoW) vs snapshot read.
	var wg sync.WaitGroup
	var readErr error
	var readData []byte

	wg.Add(2)
	go func() {
		defer wg.Done()
		v.flusher.FlushOnce()
	}()
	go func() {
		defer wg.Done()
		// Read block 0 from snapshot (not being CoW'd).
		readData, readErr = v.ReadSnapshot(1, 0, 4096)
	}()
	wg.Wait()

	if readErr != nil {
		t.Fatalf("snapshot read during CoW: %v", readErr)
	}
	if readData[0] != 'A' {
		t.Fatalf("snapshot read: got %c, want A", readData[0])
	}
}

// testQASnap_CreateDuringClose: CreateSnapshot should fail cleanly on
// a closed volume, not panic or deadlock.
func testQASnap_CreateDuringClose(t *testing.T) {
	v := createTestVol(t)

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	// Close the volume.
	v.Close()

	// Creating a snapshot on a closed volume should return error.
	err := v.CreateSnapshot(1)
	if err == nil {
		t.Fatal("expected error creating snapshot on closed volume")
	}
}

// --- Group B: Role and state rejection ---

func testQASnap_CreateOnReplicaRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Transition: None -> Replica
	v.SetRole(RoleReplica)

	err := v.CreateSnapshot(1)
	if err != ErrSnapshotRoleReject {
		t.Fatalf("expected ErrSnapshotRoleReject, got %v", err)
	}
}

func testQASnap_CreateOnStaleRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// None -> Replica -> (need to get to Stale)
	// Stale is only reachable from Draining. Use role.Store directly for test.
	v.role.Store(uint32(RoleStale))

	err := v.CreateSnapshot(1)
	if err != ErrSnapshotRoleReject {
		t.Fatalf("expected ErrSnapshotRoleReject, got %v", err)
	}
}

func testQASnap_CreateOnRebuildingRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Force to Rebuilding for test.
	v.role.Store(uint32(RoleRebuilding))

	err := v.CreateSnapshot(1)
	if err != ErrSnapshotRoleReject {
		t.Fatalf("expected ErrSnapshotRoleReject, got %v", err)
	}
}

func testQASnap_CreateDuplicateIDRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	err := v.CreateSnapshot(1)
	if err != ErrSnapshotExists {
		t.Fatalf("expected ErrSnapshotExists, got %v", err)
	}
}

// --- Group C: Edge cases ---

// testQASnap_ReadUnmodifiedBlockZeros: reading a block from a snapshot
// that was never written to should return zeros (from extent).
func testQASnap_ReadUnmodifiedBlockZeros(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Create snapshot without writing anything.
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// Read block 100 (never written).
	data, err := v.ReadSnapshot(1, 100, 4096)
	if err != nil {
		t.Fatal(err)
	}

	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte %d: got %d, want 0", i, b)
		}
	}
}

// testQASnap_CoWTrimBlockPreservesOldData: snapshot preserves data that
// was present before a TRIM operation.
func testQASnap_CoWTrimBlockPreservesOldData(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 'X' to LBA 3.
	v.WriteLBA(3, makeBlock('X'))
	v.SyncCache()
	v.flusher.FlushOnce() // flush to extent

	// Snapshot captures 'X' at LBA 3.
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// TRIM LBA 3 (zeros it).
	v.Trim(3, 4096)
	v.SyncCache()
	v.flusher.FlushOnce() // CoW 'X' to delta, then zero extent

	// Live read should be zeros.
	live, err := v.ReadLBA(3, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live[0] != 0 {
		t.Fatalf("live after TRIM: got %d, want 0", live[0])
	}

	// Snapshot should still see 'X'.
	snap, err := v.ReadSnapshot(1, 3, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if snap[0] != 'X' {
		t.Fatalf("snapshot after TRIM: got %c, want X", snap[0])
	}
}

func testQASnap_DeleteNonexistent(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	err := v.DeleteSnapshot(999)
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

func testQASnap_ReadNonexistent(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	_, err := v.ReadSnapshot(999, 0, 4096)
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

func testQASnap_RestoreNonexistent(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	err := v.RestoreSnapshot(999)
	if err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
}

// testQASnap_BoundaryLBA: test CoW at LBA 0 and the last valid LBA.
func testQASnap_BoundaryLBA(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Volume is 1MB = 256 blocks (LBA 0..255).
	lastLBA := uint64(255)

	v.WriteLBA(0, makeBlock('F'))
	v.WriteLBA(lastLBA, makeBlock('L'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Overwrite both boundaries.
	v.WriteLBA(0, makeBlock('f'))
	v.WriteLBA(lastLBA, makeBlock('l'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Snapshot should see originals.
	s0, _ := v.ReadSnapshot(1, 0, 4096)
	sL, _ := v.ReadSnapshot(1, lastLBA, 4096)
	if s0[0] != 'F' {
		t.Fatalf("LBA 0: got %c, want F", s0[0])
	}
	if sL[0] != 'L' {
		t.Fatalf("LBA %d: got %c, want L", lastLBA, sL[0])
	}
}

// --- Group D: Stress and lifecycle ---

// testQASnap_CreateDeleteMany: create and delete many snapshots to
// stress the lifecycle. No leaks, no stale entries.
func testQASnap_CreateDeleteMany(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	for i := uint32(1); i <= 20; i++ {
		if err := v.CreateSnapshot(i); err != nil {
			t.Fatalf("create snapshot %d: %v", i, err)
		}
		// Write after each snapshot to force CoW.
		v.WriteLBA(0, makeBlock(byte('A'+i%26)))
		v.SyncCache()
		v.flusher.FlushOnce()
	}

	if len(v.ListSnapshots()) != 20 {
		t.Fatalf("expected 20 snapshots, got %d", len(v.ListSnapshots()))
	}

	// Delete odd-numbered snapshots.
	for i := uint32(1); i <= 20; i += 2 {
		if err := v.DeleteSnapshot(i); err != nil {
			t.Fatalf("delete snapshot %d: %v", i, err)
		}
	}

	if len(v.ListSnapshots()) != 10 {
		t.Fatalf("expected 10 snapshots after deletion, got %d", len(v.ListSnapshots()))
	}

	// Verify delta files of deleted snapshots are gone.
	for i := uint32(1); i <= 20; i += 2 {
		p := deltaFilePath(v.path, i)
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("delta file for snapshot %d still exists", i)
		}
	}

	// Verify remaining snapshots are readable.
	for i := uint32(2); i <= 20; i += 2 {
		if _, err := v.ReadSnapshot(i, 0, 4096); err != nil {
			t.Fatalf("read snapshot %d: %v", i, err)
		}
	}
}

// testQASnap_RestoreThenNewSnapshot: after restore, creating a new
// snapshot should work cleanly (no leftover state).
func testQASnap_RestoreThenNewSnapshot(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Restore back to 'A'.
	v.RestoreSnapshot(1)

	// All snapshots gone.
	if len(v.ListSnapshots()) != 0 {
		t.Fatal("snapshots should be empty after restore")
	}

	// Create a new snapshot on the restored state.
	if err := v.CreateSnapshot(10); err != nil {
		t.Fatalf("create after restore: %v", err)
	}

	// Write new data.
	v.WriteLBA(0, makeBlock('C'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// New snapshot should see 'A' (the restored state).
	data, err := v.ReadSnapshot(10, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'A' {
		t.Fatalf("snapshot after restore: got %c, want A", data[0])
	}

	// Live should see 'C'.
	live, _ := v.ReadLBA(0, 4096)
	if live[0] != 'C' {
		t.Fatalf("live after restore+write: got %c, want C", live[0])
	}
}

// testQASnap_MultiFlushCoWIdempotent: flushing multiple times after a
// snapshot should only CoW each block once (bitmap prevents double CoW).
func testQASnap_MultiFlushCoWIdempotent(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Write 'B', flush (CoW 'A' to delta).
	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Write 'C', flush (should NOT re-CoW -- bitmap[0] already set).
	v.WriteLBA(0, makeBlock('C'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Write 'D', flush again.
	v.WriteLBA(0, makeBlock('D'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Snapshot should still see 'A' (the first CoW), not 'B' or 'C'.
	data, err := v.ReadSnapshot(1, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'A' {
		t.Fatalf("snapshot after multi-flush: got %c, want A", data[0])
	}

	// Bitmap should have exactly 1 bit set.
	v.snapMu.RLock()
	cowCount := v.snapshots[1].bitmap.CountSet()
	v.snapMu.RUnlock()
	if cowCount != 1 {
		t.Fatalf("CoW count = %d, want 1 (idempotent)", cowCount)
	}
}

// testQASnap_RecoveryAfterPostCoWWrites: create snapshot, do CoW writes,
// close, reopen. Verify snapshot data and live data are both correct.
func testQASnap_RecoveryAfterPostCoWWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write multiple blocks.
	v.WriteLBA(0, makeBlock('A'))
	v.WriteLBA(1, makeBlock('B'))
	v.WriteLBA(2, makeBlock('C'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Overwrite all three blocks.
	v.WriteLBA(0, makeBlock('X'))
	v.WriteLBA(1, makeBlock('Y'))
	v.WriteLBA(2, makeBlock('Z'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Write even more data.
	v.WriteLBA(0, makeBlock('1'))
	v.SyncCache()
	v.flusher.FlushOnce()

	v.Close()

	// Reopen.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// Snapshot should see original A, B, C.
	for i, expected := range []byte{'A', 'B', 'C'} {
		data, err := v2.ReadSnapshot(1, uint64(i), 4096)
		if err != nil {
			t.Fatalf("LBA %d: %v", i, err)
		}
		if data[0] != expected {
			t.Fatalf("LBA %d snapshot: got %c, want %c", i, data[0], expected)
		}
	}

	// Live should see '1', 'Y', 'Z'.
	for i, expected := range []byte{'1', 'Y', 'Z'} {
		data, err := v2.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("LBA %d: %v", i, err)
		}
		if data[0] != expected {
			t.Fatalf("LBA %d live: got %c, want %c", i, data[0], expected)
		}
	}
}

// --- Group E: Restore correctness ---

// testQASnap_RestoreWithMultiple: with two snapshots S1 and S2, restoring
// S1 should revert to S1's state and remove both snapshots.
func testQASnap_RestoreWithMultiple(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// A -> S1 -> B -> S2 -> C
	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()
	v.CreateSnapshot(2)

	v.WriteLBA(0, makeBlock('C'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Restore S1 (should revert to 'A').
	if err := v.RestoreSnapshot(1); err != nil {
		t.Fatal(err)
	}

	data, _ := v.ReadLBA(0, 4096)
	if data[0] != 'A' {
		t.Fatalf("after restore S1: got %c, want A", data[0])
	}

	// Both snapshots should be removed.
	if len(v.ListSnapshots()) != 0 {
		t.Fatal("all snapshots should be removed after restore")
	}

	// Delta files for both should be gone.
	for _, id := range []uint32{1, 2} {
		p := deltaFilePath(v.path, id)
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("delta file for snapshot %d still exists", id)
		}
	}
}

// testQASnap_RestoreWritableAfter: after restore, verify the volume can
// handle a full write-read-flush cycle and nextLSN is correct.
func testQASnap_RestoreWritableAfter(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Many writes to advance LSN.
	for i := 0; i < 50; i++ {
		v.WriteLBA(0, makeBlock(byte('0'+i%10)))
	}
	v.SyncCache()
	v.flusher.FlushOnce()

	v.RestoreSnapshot(1)

	// nextLSN should be baseLSN+1 (low number, reset).
	currentLSN := v.nextLSN.Load()
	if currentLSN > 10 {
		t.Fatalf("nextLSN after restore too high: %d (expected reset to ~baseLSN+1)", currentLSN)
	}

	// Write-read cycle should work.
	if err := v.WriteLBA(0, makeBlock('W')); err != nil {
		t.Fatalf("write after restore: %v", err)
	}
	v.SyncCache()
	v.flusher.FlushOnce()

	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'W' {
		t.Fatalf("read after restore write: got %c, want W", data[0])
	}
}
