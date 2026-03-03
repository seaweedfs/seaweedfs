package blockvol

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSnapshots(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "bitmap_get_set", run: testSnap_BitmapGetSet},
		{name: "bitmap_persist_reload", run: testSnap_BitmapPersistReload},
		{name: "header_roundtrip", run: testSnap_HeaderRoundtrip},
		{name: "create_and_read", run: testSnap_CreateAndRead},
		{name: "multiple_snapshots", run: testSnap_MultipleSnapshots},
		{name: "delete_removes_file", run: testSnap_DeleteRemovesFile},
		{name: "cow_only_touched_blocks", run: testSnap_CoWOnlyTouchedBlocks},
		{name: "during_active_writes", run: testSnap_DuringActiveWrites},
		{name: "survives_recovery", run: testSnap_SurvivesRecovery},
		{name: "restore_rewinds", run: testSnap_RestoreRewinds},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testSnap_BitmapGetSet(t *testing.T) {
	bm := NewSnapshotBitmap(1024)

	// All bits start as zero.
	for i := uint64(0); i < 1024; i++ {
		if bm.Get(i) {
			t.Fatalf("bit %d should be 0", i)
		}
	}

	// Set some bits.
	bm.Set(0)
	bm.Set(7)
	bm.Set(8)
	bm.Set(1023)

	if !bm.Get(0) {
		t.Fatal("bit 0 not set")
	}
	if !bm.Get(7) {
		t.Fatal("bit 7 not set")
	}
	if !bm.Get(8) {
		t.Fatal("bit 8 not set")
	}
	if !bm.Get(1023) {
		t.Fatal("bit 1023 not set")
	}
	if bm.Get(1) {
		t.Fatal("bit 1 should not be set")
	}

	if bm.CountSet() != 4 {
		t.Fatalf("CountSet = %d, want 4", bm.CountSet())
	}

	// Out-of-range: Get returns false, Set is a no-op.
	if bm.Get(1024) {
		t.Fatal("out-of-range Get should return false")
	}
	bm.Set(1024) // no panic

	// ByteSize.
	if bm.ByteSize() != 128 { // 1024/8 = 128
		t.Fatalf("ByteSize = %d, want 128", bm.ByteSize())
	}
}

func testSnap_BitmapPersistReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bitmap.dat")

	bm := NewSnapshotBitmap(256)
	bm.Set(0)
	bm.Set(100)
	bm.Set(255)

	// Write to file.
	fd, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := bm.WriteTo(fd, 0); err != nil {
		t.Fatal(err)
	}
	fd.Close()

	// Read back.
	fd2, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer fd2.Close()

	bm2 := NewSnapshotBitmap(256)
	if err := bm2.ReadFrom(fd2, 0); err != nil {
		t.Fatal(err)
	}

	if !bm2.Get(0) || !bm2.Get(100) || !bm2.Get(255) {
		t.Fatal("reloaded bitmap missing set bits")
	}
	if bm2.Get(1) || bm2.Get(99) || bm2.Get(254) {
		t.Fatal("reloaded bitmap has spurious bits")
	}
	if bm2.CountSet() != 3 {
		t.Fatalf("CountSet = %d, want 3", bm2.CountSet())
	}
}

func testSnap_HeaderRoundtrip(t *testing.T) {
	hdr := SnapshotHeader{
		Version:    SnapVersion,
		SnapshotID: 42,
		BaseLSN:    1000,
		VolumeSize: 1 << 30,
		BlockSize:  4096,
		BitmapSize: 32768,
		DataOffset: 36864,
		CreatedAt:  uint64(time.Now().Unix()),
	}
	copy(hdr.Magic[:], SnapMagic)
	copy(hdr.ParentUUID[:], "0123456789abcdef")

	var buf bytes.Buffer
	if _, err := hdr.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != SnapHeaderSize {
		t.Fatalf("header size = %d, want %d", buf.Len(), SnapHeaderSize)
	}

	hdr2, err := ReadSnapshotHeader(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if hdr2.SnapshotID != 42 {
		t.Fatalf("SnapshotID = %d, want 42", hdr2.SnapshotID)
	}
	if hdr2.BaseLSN != 1000 {
		t.Fatalf("BaseLSN = %d, want 1000", hdr2.BaseLSN)
	}
	if hdr2.VolumeSize != 1<<30 {
		t.Fatalf("VolumeSize = %d, want %d", hdr2.VolumeSize, 1<<30)
	}
	if hdr2.BlockSize != 4096 {
		t.Fatalf("BlockSize = %d, want 4096", hdr2.BlockSize)
	}
	if hdr2.BitmapSize != 32768 {
		t.Fatalf("BitmapSize = %d", hdr2.BitmapSize)
	}
	if hdr2.DataOffset != 36864 {
		t.Fatalf("DataOffset = %d", hdr2.DataOffset)
	}
	if hdr2.ParentUUID != hdr.ParentUUID {
		t.Fatalf("ParentUUID mismatch")
	}
}

func testSnap_CreateAndRead(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 'A' to LBA 0.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Create snapshot 1.
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// Write 'B' to LBA 0 (after snapshot).
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatal(err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatal(err)
	}
	// Force flush so CoW happens.
	v.flusher.FlushOnce()

	// Live read should see 'B'.
	live, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live[0] != 'B' {
		t.Fatalf("live read: got %c, want B", live[0])
	}

	// Snapshot read should see 'A'.
	snapData, err := v.ReadSnapshot(1, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if snapData[0] != 'A' {
		t.Fatalf("snapshot read: got %c, want A", snapData[0])
	}
}

func testSnap_MultipleSnapshots(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 'A' to LBA 5, snapshot S1.
	if err := v.WriteLBA(5, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	v.SyncCache()
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// Write 'B' to LBA 5, snapshot S2.
	if err := v.WriteLBA(5, makeBlock('B')); err != nil {
		t.Fatal(err)
	}
	v.SyncCache()
	v.flusher.FlushOnce() // CoW 'A' to S1
	if err := v.CreateSnapshot(2); err != nil {
		t.Fatal(err)
	}

	// Write 'C' to LBA 5.
	if err := v.WriteLBA(5, makeBlock('C')); err != nil {
		t.Fatal(err)
	}
	v.SyncCache()
	v.flusher.FlushOnce() // CoW 'B' to S2, S1 already done

	// S1 sees 'A', S2 sees 'B', live sees 'C'.
	s1, err := v.ReadSnapshot(1, 5, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if s1[0] != 'A' {
		t.Fatalf("S1: got %c, want A", s1[0])
	}

	s2, err := v.ReadSnapshot(2, 5, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if s2[0] != 'B' {
		t.Fatalf("S2: got %c, want B", s2[0])
	}

	live, err := v.ReadLBA(5, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live[0] != 'C' {
		t.Fatalf("live: got %c, want C", live[0])
	}

	// List should show 2 snapshots.
	infos := v.ListSnapshots()
	if len(infos) != 2 {
		t.Fatalf("ListSnapshots: got %d, want 2", len(infos))
	}
}

func testSnap_DeleteRemovesFile(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.WriteLBA(0, makeBlock('X'))
	v.SyncCache()
	v.CreateSnapshot(1)

	deltaPath := deltaFilePath(v.path, 1)
	if _, err := os.Stat(deltaPath); err != nil {
		t.Fatalf("delta file should exist: %v", err)
	}

	if err := v.DeleteSnapshot(1); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(deltaPath); !os.IsNotExist(err) {
		t.Fatalf("delta file should be removed, got err: %v", err)
	}

	// Reading deleted snapshot returns error.
	if _, err := v.ReadSnapshot(1, 0, 4096); err != ErrSnapshotNotFound {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}

	if len(v.ListSnapshots()) != 0 {
		t.Fatal("ListSnapshots should be empty")
	}
}

func testSnap_CoWOnlyTouchedBlocks(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write LBAs 0, 1, 2.
	for i := uint64(0); i < 3; i++ {
		v.WriteLBA(i, makeBlock(byte('A'+i)))
	}
	v.SyncCache()
	v.CreateSnapshot(1)

	// Only modify LBA 1.
	v.WriteLBA(1, makeBlock('Z'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Check bitmap: only LBA 1 should be CoW'd.
	v.snapMu.RLock()
	snap := v.snapshots[1]
	cowCount := snap.bitmap.CountSet()
	v.snapMu.RUnlock()

	if cowCount != 1 {
		t.Fatalf("CoW count = %d, want 1", cowCount)
	}

	// Snapshot should still see original values.
	for i := uint64(0); i < 3; i++ {
		data, err := v.ReadSnapshot(1, i, 4096)
		if err != nil {
			t.Fatal(err)
		}
		expected := byte('A' + i)
		if data[0] != expected {
			t.Fatalf("LBA %d: got %c, want %c", i, data[0], expected)
		}
	}
}

func testSnap_DuringActiveWrites(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write initial data.
	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()

	// Start concurrent writes and create snapshot.
	var wg sync.WaitGroup
	errCh := make(chan error, 20)

	// Writer goroutine: continuously write to LBA 10.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err := v.WriteLBA(10, makeBlock(byte('0'+i))); err != nil {
				errCh <- err
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Snapshot creation mid-writes.
	time.Sleep(5 * time.Millisecond)
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write error: %v", err)
	}

	// Snapshot should exist and be readable.
	_, err := v.ReadSnapshot(1, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
}

func testSnap_SurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")

	// Create volume, write, snapshot.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	v.WriteLBA(0, makeBlock('A'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Write new data after snapshot.
	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Close and reopen.
	v.Close()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()

	// Snapshot should survive.
	if len(v2.ListSnapshots()) != 1 {
		t.Fatalf("expected 1 snapshot after recovery, got %d", len(v2.ListSnapshots()))
	}

	// Snapshot data should read 'A'.
	snapData, err := v2.ReadSnapshot(1, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if snapData[0] != 'A' {
		t.Fatalf("snapshot after recovery: got %c, want A", snapData[0])
	}

	// Live data should read 'B'.
	live, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live[0] != 'B' {
		t.Fatalf("live after recovery: got %c, want B", live[0])
	}
}

func testSnap_RestoreRewinds(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 'A' to LBA 0 and 'X' to LBA 1.
	v.WriteLBA(0, makeBlock('A'))
	v.WriteLBA(1, makeBlock('X'))
	v.SyncCache()
	v.CreateSnapshot(1)

	// Write 'B' to LBA 0 (overwrites 'A').
	v.WriteLBA(0, makeBlock('B'))
	v.SyncCache()
	v.flusher.FlushOnce()

	// Live should be 'B'.
	live, _ := v.ReadLBA(0, 4096)
	if live[0] != 'B' {
		t.Fatalf("pre-restore live: got %c, want B", live[0])
	}

	// Restore snapshot 1.
	if err := v.RestoreSnapshot(1); err != nil {
		t.Fatal(err)
	}

	// Live should now be 'A' (reverted).
	live2, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live2[0] != 'A' {
		t.Fatalf("post-restore LBA 0: got %c, want A", live2[0])
	}

	// LBA 1 should still be 'X' (unchanged, not CoW'd).
	live3, err := v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if live3[0] != 'X' {
		t.Fatalf("post-restore LBA 1: got %c, want X", live3[0])
	}

	// All snapshots should be gone.
	if len(v.ListSnapshots()) != 0 {
		t.Fatalf("snapshots should be empty after restore, got %d", len(v.ListSnapshots()))
	}

	// Volume should still be writable.
	if err := v.WriteLBA(0, makeBlock('C')); err != nil {
		t.Fatalf("write after restore: %v", err)
	}
	v.SyncCache()
	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if data[0] != 'C' {
		t.Fatalf("read after restore write: got %c, want C", data[0])
	}
}
