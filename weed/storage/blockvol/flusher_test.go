package blockvol

import (
	"bytes"
	"path/filepath"
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
