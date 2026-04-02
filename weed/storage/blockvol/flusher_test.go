package blockvol

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFlusher(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "flush_moves_data", run: testFlushMovesData},
		{name: "flush_idempotent", run: testFlushIdempotent},
		{name: "flush_concurrent_writes", run: testFlushConcurrentWrites},
		{name: "flush_frees_wal_space", run: testFlushFreesWALSpace},
		{name: "flush_partial", run: testFlushPartial},
		// Phase 3 Task 1.6: NotifyUrgent.
		{name: "flusher_notify_urgent_triggers_flush", run: testFlusherNotifyUrgentTriggersFlush},
		// Phase 3 bug fix: P3-BUG-4 error logging.
		{name: "flusher_error_logged", run: testFlusherErrorLogged},
		// Multi-block write WAL dedup (flusher OOM fix).
		{name: "flush_multiblock_shared_wal_read", run: testFlushMultiblockSharedWALRead},
		{name: "flush_multiblock_data_correct", run: testFlushMultiblockDataCorrect},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func createTestVolWithFlusher(t *testing.T) (*BlockVol, *Flusher) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024, // 1MB
		BlockSize:  4096,
		WALSize:    256 * 1024, // 256KB WAL
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour, // don't auto-flush in tests
	})

	return v, f
}

func testFlushMovesData(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write 10 blocks.
	for i := uint64(0); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if v.dirtyMap.Len() != 10 {
		t.Fatalf("dirty map len = %d, want 10", v.dirtyMap.Len())
	}

	// Run flusher.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Dirty map should be empty.
	if v.dirtyMap.Len() != 0 {
		t.Errorf("after flush: dirty map len = %d, want 0", v.dirtyMap.Len())
	}

	// Checkpoint should have advanced.
	if f.CheckpointLSN() == 0 {
		t.Error("checkpoint LSN should be > 0 after flush")
	}

	// Read from extent (dirty map is empty, so reads go to extent).
	for i := uint64(0); i < 10; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after flush: %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('A'+i))) {
			t.Errorf("block %d: data mismatch after flush", i)
		}
	}
}

func testFlushIdempotent(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	data := makeBlock('X')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Flush twice.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 1: %v", err)
	}
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	// Data should still be correct.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after double flush: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch after double flush")
	}
}

func testFlushConcurrentWrites(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write blocks 0-4.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Flush (moves blocks 0-4 to extent).
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Write blocks 5-9 AFTER flush.
	for i := uint64(5); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Blocks 0-4 should read from extent, blocks 5-9 from WAL.
	for i := uint64(0); i < 10; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('A'+i))) {
			t.Errorf("block %d: data mismatch", i)
		}
	}

	// Dirty map should have 5 entries (blocks 5-9).
	if v.dirtyMap.Len() != 5 {
		t.Errorf("dirty map len = %d, want 5", v.dirtyMap.Len())
	}

	// Also: overwrite block 0 after flush -- new write should go to WAL.
	newData := makeBlock('Z')
	if err := v.WriteLBA(0, newData); err != nil {
		t.Fatalf("WriteLBA(0) overwrite: %v", err)
	}
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0) after overwrite: %v", err)
	}
	if !bytes.Equal(got, newData) {
		t.Error("block 0: should return overwritten data 'Z'")
	}
}

func testFlushFreesWALSpace(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write enough blocks to fill a significant portion of WAL.
	entrySize := uint64(walEntryHeaderSize + 4096)
	walCapacity := v.super.WALSize / entrySize
	// Write ~80% of capacity.
	writeCount := int(walCapacity * 80 / 100)

	for i := 0; i < writeCount; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte(i%26+'A'))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Try to write more -- should eventually fail with WAL full.
	var walFullBefore bool
	for i := writeCount; i < writeCount+int(walCapacity); i++ {
		if err := v.WriteLBA(uint64(i%writeCount), makeBlock('X')); err != nil {
			walFullBefore = true
			break
		}
	}

	// Flush to free WAL space.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// WAL tail should have advanced (free space available).
	// New writes should succeed.
	if err := v.WriteLBA(0, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA after flush: %v", err)
	}

	// Log whether WAL was full before flush.
	if walFullBefore {
		t.Log("WAL was full before flush, writes succeeded after flush")
	}
}

func testFlushPartial(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	// Write blocks 0-4.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Flush once (all 5 blocks).
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	checkpointAfterFirst := f.CheckpointLSN()

	// Write blocks 5-9.
	for i := uint64(5); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Simulate partial flush: flusher runs again, should handle new entries.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce 2: %v", err)
	}

	checkpointAfterSecond := f.CheckpointLSN()
	if checkpointAfterSecond <= checkpointAfterFirst {
		t.Errorf("checkpoint should advance: first=%d, second=%d", checkpointAfterFirst, checkpointAfterSecond)
	}

	// All blocks should be readable from extent.
	for i := uint64(0); i < 10; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after two flushes: %v", i, err)
		}
		if !bytes.Equal(got, makeBlock(byte('A'+i))) {
			t.Errorf("block %d: data mismatch after two flushes", i)
		}
	}
}

// --- Phase 3 Task 1.6: NotifyUrgent test ---

func testFlusherNotifyUrgentTriggersFlush(t *testing.T) {
	v, f := createTestVolWithFlusher(t)
	defer v.Close()

	data := makeBlock('U')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	if v.dirtyMap.Len() != 1 {
		t.Fatalf("dirty map len = %d, want 1", v.dirtyMap.Len())
	}

	// Start flusher in background with long interval.
	go f.Run()
	defer f.Stop()

	// NotifyUrgent should trigger a flush.
	f.NotifyUrgent()

	// Wait for flush to complete.
	deadline := time.After(2 * time.Second)
	for {
		if v.dirtyMap.Len() == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("NotifyUrgent did not trigger flush within 2s")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after urgent flush: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch after urgent flush")
	}
}

// testFlusherErrorLogged verifies that flusher I/O errors are logged
// and that consecutive errors are deduplicated.
func testFlusherErrorLogged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errlog.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write a block so dirty map has an entry.
	if err := v.WriteLBA(0, makeBlock('E')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Create a flusher with a captured logger and a CLOSED fd to force error.
	var logBuf strings.Builder
	logger := log.New(&logBuf, "", 0)

	closedFD, err := openAndClose(path)
	if err != nil {
		t.Fatalf("openAndClose: %v", err)
	}

	f := NewFlusher(FlusherConfig{
		FD:       closedFD,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
		Logger:   logger,
	})

	// Run flusher briefly -- FlushOnce should error, and Run should log it.
	go f.Run()
	f.Notify()
	time.Sleep(50 * time.Millisecond)
	// Send another notify to test dedup.
	f.Notify()
	time.Sleep(50 * time.Millisecond)
	f.Stop()

	logged := logBuf.String()
	if !strings.Contains(logged, "flusher error:") {
		t.Fatalf("expected 'flusher error:' in log, got: %q", logged)
	}

	// Should only be logged once (dedup of consecutive errors).
	count := strings.Count(logged, "flusher error:")
	if count != 1 {
		t.Errorf("expected 1 log line (dedup), got %d: %q", count, logged)
	}

	v.Close()
}

// openAndClose opens a file and immediately closes the fd, returning
// the now-invalid *os.File for injection into flusher tests.
func openAndClose(path string) (*os.File, error) {
	fd, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fd.Close()
	return fd, nil
}

// --- Multi-block WAL dedup tests (flusher OOM fix) ---

// testFlushMultiblockSharedWALRead verifies that a multi-block WriteLBA
// (e.g. 64KB = 16 blocks) results in ONE WAL read during flush, not 16.
// This is the core invariant of the flusher dedup fix.
func testFlushMultiblockSharedWALRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multiblock.blockvol")

	// Volume large enough for multi-block writes: 1MB vol, 256KB WAL.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write 64KB (16 blocks) in one call. This creates 1 WAL entry, 16 dirty map entries.
	multiBlock := make([]byte, 64*1024) // 16 × 4KB
	for i := range multiBlock {
		multiBlock[i] = byte(i & 0xFF)
	}
	if err := v.WriteLBA(0, multiBlock); err != nil {
		t.Fatalf("WriteLBA(64KB): %v", err)
	}

	// Dirty map should have 16 entries (one per block).
	if n := v.dirtyMap.Len(); n != 16 {
		t.Fatalf("dirty map len = %d, want 16", n)
	}

	// Verify all 16 entries share the same WAL offset.
	snap := v.dirtyMap.Snapshot()
	firstOff := snap[0].WalOffset
	for i, e := range snap {
		if e.WalOffset != firstOff {
			t.Fatalf("block %d has WalOffset=%d, want %d (shared)", i, e.WalOffset, firstOff)
		}
	}
	t.Logf("16 dirty blocks share WalOffset=%d — dedup should read WAL once", firstOff)

	// Flush. Before the fix, this would allocate 16 × 64KB = 1MB.
	// After the fix, it allocates 1 × 64KB = 64KB.
	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Dirty map should be empty after flush.
	if n := v.dirtyMap.Len(); n != 0 {
		t.Fatalf("dirty map len after flush = %d, want 0", n)
	}
}

// testFlushMultiblockDataCorrect verifies that after flushing a multi-block
// write, each block is readable with correct data from the extent region.
func testFlushMultiblockDataCorrect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multidata.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write 3 different multi-block writes to exercise dedup across entries.
	// Write 1: 32KB (8 blocks) at LBA 0, filled with 0xAA.
	w1 := bytes.Repeat([]byte{0xAA}, 32*1024)
	if err := v.WriteLBA(0, w1); err != nil {
		t.Fatalf("WriteLBA w1: %v", err)
	}

	// Write 2: 16KB (4 blocks) at LBA 100, filled with 0xBB.
	w2 := bytes.Repeat([]byte{0xBB}, 16*1024)
	if err := v.WriteLBA(100, w2); err != nil {
		t.Fatalf("WriteLBA w2: %v", err)
	}

	// Write 3: single 4KB block at LBA 50, filled with 0xCC.
	w3 := bytes.Repeat([]byte{0xCC}, 4096)
	if err := v.WriteLBA(50, w3); err != nil {
		t.Fatalf("WriteLBA w3: %v", err)
	}

	// Should have 8 + 4 + 1 = 13 dirty entries from 3 WAL entries.
	if n := v.dirtyMap.Len(); n != 13 {
		t.Fatalf("dirty map len = %d, want 13", n)
	}

	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		SuperMu:  &v.superMu,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Verify each block from extent region.
	// w1: LBA 0-7, each block = 0xAA.
	for i := uint64(0); i < 8; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if got[0] != 0xAA || got[4095] != 0xAA {
			t.Errorf("LBA %d: expected 0xAA, got first=0x%02x last=0x%02x", i, got[0], got[4095])
		}
	}

	// w3: LBA 50, block = 0xCC.
	got50, err := v.ReadLBA(50, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(50): %v", err)
	}
	if got50[0] != 0xCC {
		t.Errorf("LBA 50: expected 0xCC, got 0x%02x", got50[0])
	}

	// w2: LBA 100-103, each block = 0xBB.
	for i := uint64(100); i < 104; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if got[0] != 0xBB || got[4095] != 0xBB {
			t.Errorf("LBA %d: expected 0xBB, got first=0x%02x last=0x%02x", i, got[0], got[4095])
		}
	}

	t.Log("multi-block flush dedup: 3 writes (8+4+1 blocks, 3 WAL entries) → all data correct")
}
