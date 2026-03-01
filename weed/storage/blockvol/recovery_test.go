package blockvol

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func TestRecovery(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "recover_empty_wal", run: testRecoverEmptyWAL},
		{name: "recover_one_entry", run: testRecoverOneEntry},
		{name: "recover_many_entries", run: testRecoverManyEntries},
		{name: "recover_torn_write", run: testRecoverTornWrite},
		{name: "recover_after_checkpoint", run: testRecoverAfterCheckpoint},
		{name: "recover_idempotent", run: testRecoverIdempotent},
		{name: "recover_wal_full", run: testRecoverWALFull},
		{name: "recover_barrier_only", run: testRecoverBarrierOnly},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// simulateCrash closes the volume without syncing and returns the path.
// simulateCrash stops background goroutines and closes the fd without a clean
// shutdown. The caller is responsible for persisting the superblock AFTER
// stopping the flusher (to avoid concurrent superblock writes).
// For the common case, use simulateCrashWithSuper instead.
func simulateCrash(v *BlockVol) string {
	path := v.Path()
	v.groupCommit.Stop()
	v.flusher.Stop()
	v.fd.Close()
	return path
}

// simulateCrashWithSuper is the safe default: stops background goroutines,
// writes the superblock with current WAL positions, then closes the fd.
func simulateCrashWithSuper(v *BlockVol) string {
	path := v.Path()
	v.groupCommit.Stop()
	v.flusher.Stop()
	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()
	v.fd.Close()
	return path
}

func testRecoverEmptyWAL(t *testing.T) {
	v := createTestVol(t)
	// Sync to make superblock durable.
	v.fd.Sync()
	path := simulateCrash(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Empty volume: read should return zeros.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("expected zeros from empty volume")
	}
}

func testRecoverOneEntry(t *testing.T) {
	v := createTestVol(t)
	data := makeBlock('A')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Sync WAL to make the entry durable.
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path := simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after recovery: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data not recovered")
	}
}

func testRecoverManyEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "many.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 4 * 1024 * 1024, // 4MB
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024, // 2MB WAL -- enough for ~500 entries
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	const numWrites = 400
	for i := uint64(0); i < numWrites; i++ {
		if err := v.WriteLBA(i, makeBlock(byte(i%26+'A'))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := uint64(0); i < numWrites; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after recovery: %v", i, err)
		}
		expected := makeBlock(byte(i%26 + 'A'))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch after recovery", i)
		}
	}
}

func testRecoverTornWrite(t *testing.T) {
	v := createTestVol(t)

	// Write 2 entries.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(0): %v", err)
	}
	if err := v.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(1): %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Stop background goroutines before manual superblock write.
	v.groupCommit.Stop()
	v.flusher.Stop()

	// Save superblock with correct WAL state.
	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALTail = v.wal.LogicalTail()
	v.fd.Seek(0, 0)
	v.super.WriteTo(v.fd)
	v.fd.Sync()

	// Corrupt the last 2 bytes of the second entry to simulate torn write.
	entrySize := uint64(walEntryHeaderSize + 4096)
	secondEntryEnd := v.super.WALOffset + entrySize*2
	corruptOff := int64(secondEntryEnd - 2)
	v.fd.WriteAt([]byte{0xFF, 0xFF}, corruptOff)
	v.fd.Sync()

	v.fd.Close()
	path := v.Path()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// First entry should be recovered.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0) after torn recovery: %v", err)
	}
	if !bytes.Equal(got, makeBlock('A')) {
		t.Error("block 0 should be recovered")
	}

	// Second entry was torn -- should NOT be in dirty map.
	// Read returns zeros (from extent).
	got, err = v2.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1) after torn recovery: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("block 1 (torn) should return zeros")
	}
}

func testRecoverAfterCheckpoint(t *testing.T) {
	v := createTestVol(t)

	// Write blocks 0-9.
	for i := uint64(0); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Flush first 5 blocks (flusher moves them to extent, advances checkpoint).
	f := NewFlusher(FlusherConfig{
		FD:       v.fd,
		Super:    &v.super,
		WAL:      v.wal,
		DirtyMap: v.dirtyMap,
		Interval: 1 * time.Hour,
	})

	// Flush all (flusher takes all dirty entries).
	// To simulate partial flush, we'll manually set checkpoint to midpoint.
	if err := f.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Now write 5 more blocks (these will need replay after crash).
	for i := uint64(10); i < 15; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path := simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Blocks 0-9 should be in extent (flushed). Blocks 10-14 replayed from WAL.
	for i := uint64(0); i < 15; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch after checkpoint recovery", i)
		}
	}
}

func testRecoverIdempotent(t *testing.T) {
	v := createTestVol(t)
	data := makeBlock('X')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path := simulateCrashWithSuper(v)

	// First recovery.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol 1: %v", err)
	}

	// Verify data.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA 1: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("first recovery: data mismatch")
	}

	// Close and recover again (should be idempotent).
	path2 := simulateCrash(v2)

	v3, err := OpenBlockVol(path2)
	if err != nil {
		t.Fatalf("OpenBlockVol 2: %v", err)
	}
	defer v3.Close()

	got, err = v3.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA 2: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("second recovery: data mismatch")
	}
}

func testRecoverWALFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "full.blockvol")

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 5 // room for exactly 5 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write exactly 5 entries to fill WAL.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path = simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	for i := uint64(0); i < 5; i++ {
		got, err := v2.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch after full WAL recovery", i)
		}
	}
}

func testRecoverBarrierOnly(t *testing.T) {
	v := createTestVol(t)

	// Write a barrier entry.
	lsn := v.nextLSN.Add(1) - 1
	entry := &WALEntry{
		LSN:  lsn,
		Type: EntryTypeBarrier,
		LBA:  0,
	}
	if _, err := v.wal.Append(entry); err != nil {
		t.Fatalf("Append barrier: %v", err)
	}

	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	path := simulateCrashWithSuper(v)

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// No data changes from barrier.
	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("barrier-only WAL should leave data as zeros")
	}
}
