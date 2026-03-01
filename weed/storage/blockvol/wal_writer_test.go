package blockvol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriter(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "wal_writer_append_read_back", run: testWALWriterAppendReadBack},
		{name: "wal_writer_multiple_entries", run: testWALWriterMultipleEntries},
		{name: "wal_writer_wrap_around", run: testWALWriterWrapAround},
		{name: "wal_writer_full", run: testWALWriterFull},
		{name: "wal_writer_advance_tail_frees_space", run: testWALWriterAdvanceTailFreesSpace},
		{name: "wal_writer_fill_no_flusher", run: testWALWriterFillNoFlusher},
		// Phase 3 Task 1.5: WAL UsedFraction.
		{name: "wal_fraction_empty", run: testWALFractionEmpty},
		{name: "wal_fraction_after_writes", run: testWALFractionAfterWrites},
		{name: "wal_fraction_after_advance_tail", run: testWALFractionAfterAdvanceTail},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// createTestWAL creates a temp file with a WAL region at the given offset and size.
func createTestWAL(t *testing.T, walOffset, walSize uint64) (*os.File, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	// Extend file to cover superblock + WAL region.
	totalSize := int64(walOffset + walSize)
	if err := fd.Truncate(totalSize); err != nil {
		fd.Close()
		t.Fatalf("truncate: %v", err)
	}
	return fd, func() { fd.Close() }
}

func testWALWriterAppendReadBack(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	walSize := uint64(64 * 1024) // 64KB WAL
	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	data := []byte("hello WAL writer test!")
	// Pad to block size for a realistic entry.
	padded := make([]byte, 4096)
	copy(padded, data)

	entry := &WALEntry{
		LSN:    1,
		Type:   EntryTypeWrite,
		LBA:    0,
		Length: uint32(len(padded)),
		Data:   padded,
	}

	off, err := w.Append(entry)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != 0 {
		t.Errorf("first entry offset = %d, want 0", off)
	}

	// Read back from file and decode.
	buf := make([]byte, entry.EntrySize)
	if _, err := fd.ReadAt(buf, int64(walOffset+off)); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	decoded, err := DecodeWALEntry(buf)
	if err != nil {
		t.Fatalf("DecodeWALEntry: %v", err)
	}
	if decoded.LSN != 1 {
		t.Errorf("decoded LSN = %d, want 1", decoded.LSN)
	}
}

func testWALWriterMultipleEntries(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	walSize := uint64(64 * 1024)
	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	for i := uint64(1); i <= 5; i++ {
		entry := &WALEntry{
			LSN:    i,
			Type:   EntryTypeWrite,
			LBA:    i * 10,
			Length: 4096,
			Data:   make([]byte, 4096),
		}
		_, err := w.Append(entry)
		if err != nil {
			t.Fatalf("Append entry %d: %v", i, err)
		}
	}

	expectedHead := uint64(5 * (walEntryHeaderSize + 4096))
	if w.Head() != expectedHead {
		t.Errorf("head = %d, want %d", w.Head(), expectedHead)
	}
}

func testWALWriterWrapAround(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	// Small WAL: 2.5x entry size. After writing 2 entries (head at 2*entrySize),
	// remaining (0.5*entrySize) is too small for entry 3, so it wraps to offset 0.
	// Tail must be advanced past both entries so there's free space after wrap.
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize*2 + entrySize/2

	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	// Write 2 entries to fill most of the WAL.
	for i := uint64(1); i <= 2; i++ {
		entry := &WALEntry{LSN: i, Type: EntryTypeWrite, LBA: i, Length: 4096, Data: make([]byte, 4096)}
		if _, err := w.Append(entry); err != nil {
			t.Fatalf("Append entry %d: %v", i, err)
		}
	}

	// Advance tail past both entries (simulates flusher flushed them).
	w.AdvanceTail(entrySize * 2)

	// Write a 3rd entry -- should wrap around.
	entry3 := &WALEntry{LSN: 3, Type: EntryTypeWrite, LBA: 3, Length: 4096, Data: make([]byte, 4096)}
	off, err := w.Append(entry3)
	if err != nil {
		t.Fatalf("Append entry 3 (wrap): %v", err)
	}

	// After wrap, entry should be written at offset 0.
	if off != 0 {
		t.Errorf("wrapped entry offset = %d, want 0", off)
	}
}

func testWALWriterFull(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 2 // fits exactly 2 entries

	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	// Fill the WAL with 2 entries (exact fit).
	for i := uint64(1); i <= 2; i++ {
		entry := &WALEntry{LSN: i, Type: EntryTypeWrite, LBA: i - 1, Length: 4096, Data: make([]byte, 4096)}
		if _, err := w.Append(entry); err != nil {
			t.Fatalf("Append entry %d: %v", i, err)
		}
	}

	// Third entry should fail -- tail hasn't moved, so no free space.
	entry3 := &WALEntry{LSN: 3, Type: EntryTypeWrite, LBA: 2, Length: 4096, Data: make([]byte, 4096)}
	_, err := w.Append(entry3)
	if err == nil {
		t.Fatal("expected ErrWALFull when WAL is full")
	}
}

func testWALWriterAdvanceTailFreesSpace(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 2

	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	// Fill the WAL with 2 entries.
	for i := uint64(1); i <= 2; i++ {
		entry := &WALEntry{LSN: i, Type: EntryTypeWrite, LBA: i - 1, Length: 4096, Data: make([]byte, 4096)}
		if _, err := w.Append(entry); err != nil {
			t.Fatalf("Append entry %d: %v", i, err)
		}
	}

	// WAL is full. Third entry should fail.
	entry3 := &WALEntry{LSN: 3, Type: EntryTypeWrite, LBA: 2, Length: 4096, Data: make([]byte, 4096)}
	if _, err := w.Append(entry3); err == nil {
		t.Fatal("expected ErrWALFull before AdvanceTail")
	}

	// Advance tail to free space for 1 entry.
	w.AdvanceTail(entrySize)

	// Now entry 3 should succeed.
	if _, err := w.Append(entry3); err != nil {
		t.Fatalf("Append after AdvanceTail: %v", err)
	}
}

// --- Phase 3 Task 1.5: WAL UsedFraction tests ---

func testWALFractionEmpty(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	walSize := uint64(64 * 1024)
	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)
	frac := w.UsedFraction()
	if frac != 0 {
		t.Errorf("UsedFraction on empty WAL = %f, want 0", frac)
	}
}

func testWALFractionAfterWrites(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	walSize := uint64(64 * 1024)
	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	entry := &WALEntry{LSN: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	if _, err := w.Append(entry); err != nil {
		t.Fatalf("Append: %v", err)
	}

	frac := w.UsedFraction()
	if frac <= 0 || frac > 1 {
		t.Errorf("UsedFraction after write = %f, want (0, 1]", frac)
	}

	entrySize := float64(walEntryHeaderSize + 4096)
	expected := entrySize / float64(walSize)
	if diff := frac - expected; diff > 0.001 || diff < -0.001 {
		t.Errorf("UsedFraction = %f, want ~%f", frac, expected)
	}
}

func testWALFractionAfterAdvanceTail(t *testing.T) {
	walOffset := uint64(SuperblockSize)
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 4
	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	// Write 2 entries.
	for i := uint64(1); i <= 2; i++ {
		entry := &WALEntry{LSN: i, Type: EntryTypeWrite, LBA: i, Length: 4096, Data: make([]byte, 4096)}
		if _, err := w.Append(entry); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	fracBefore := w.UsedFraction()

	// Advance tail past first entry.
	w.AdvanceTail(entrySize)

	fracAfter := w.UsedFraction()
	if fracAfter >= fracBefore {
		t.Errorf("UsedFraction should decrease after AdvanceTail: before=%f, after=%f", fracBefore, fracAfter)
	}
}

func testWALWriterFillNoFlusher(t *testing.T) {
	// QA-001 regression: fill WAL without flusher (tail stays at 0).
	// After wrap, head=0 and tail=0 must NOT be treated as "empty".
	walOffset := uint64(SuperblockSize)
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 10 // room for ~10 entries

	fd, cleanup := createTestWAL(t, walOffset, walSize)
	defer cleanup()

	w := NewWALWriter(fd, walOffset, walSize, 0, 0)

	// Fill the WAL completely -- tail never moves (no flusher).
	written := 0
	for i := uint64(1); ; i++ {
		entry := &WALEntry{LSN: i, Type: EntryTypeWrite, LBA: i, Length: 4096, Data: make([]byte, 4096)}
		_, err := w.Append(entry)
		if err != nil {
			// Should eventually get ErrWALFull, NOT wrap and overwrite.
			break
		}
		written++
		if written > 20 {
			t.Fatalf("wrote %d entries to a 10-entry WAL without ErrWALFull -- wrap overwrote live entries (QA-001)", written)
		}
	}

	if written == 0 {
		t.Fatal("should have written at least 1 entry")
	}
	t.Logf("correctly wrote %d entries then got ErrWALFull", written)
}
