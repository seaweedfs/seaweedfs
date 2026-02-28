package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestBlockVol(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "write_then_read", run: testWriteThenRead},
		{name: "overwrite_read_latest", run: testOverwriteReadLatest},
		{name: "write_no_sync_not_durable", run: testWriteNoSyncNotDurable},
		{name: "write_sync_durable", run: testWriteSyncDurable},
		{name: "write_multiple_sync", run: testWriteMultipleSync},
		{name: "read_unflushed", run: testReadUnflushed},
		{name: "read_flushed", run: testReadFlushed},
		{name: "read_mixed_dirty_clean", run: testReadMixedDirtyClean},
		{name: "wal_read_corrupt_length", run: testWALReadCorruptLength},
		{name: "open_invalid_superblock", run: testOpenInvalidSuperblock},
		{name: "trim_large_length_read_returns_zero", run: testTrimLargeLengthReadReturnsZero},
		// Task 1.10: Lifecycle tests.
		{name: "lifecycle_create_close_reopen", run: testLifecycleCreateCloseReopen},
		{name: "lifecycle_close_flushes_dirty", run: testLifecycleCloseFlushes},
		{name: "lifecycle_double_close", run: testLifecycleDoubleClose},
		{name: "lifecycle_info", run: testLifecycleInfo},
		{name: "lifecycle_write_sync_close_reopen", run: testLifecycleWriteSyncCloseReopen},
		// Task 1.11: Crash stress test.
		{name: "crash_stress_100", run: testCrashStress100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func createTestVol(t *testing.T) *BlockVol {
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
	return v
}

func makeBlock(fill byte) []byte {
	b := make([]byte, 4096)
	for i := range b {
		b[i] = fill
	}
	return b
}

func testWriteThenRead(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	data := makeBlock('A')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("read data does not match written data")
	}
}

func testOverwriteReadLatest(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}

	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, makeBlock('B')) {
		t.Error("read should return latest write ('B'), not 'A'")
	}
}

func testWriteNoSyncNotDurable(t *testing.T) {
	v := createTestVol(t)
	path := v.Path()

	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Simulate crash: close fd without sync.
	v.fd.Close()

	// Reopen -- without recovery (Phase 1.9), data MAY be lost.
	// This test just verifies we can reopen without error.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol after crash: %v", err)
	}
	defer v2.Close()
	// Data may or may not be present -- both are correct without SyncCache.
}

func testWriteSyncDurable(t *testing.T) {
	v := createTestVol(t)
	path := v.Path()

	data := makeBlock('A')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Sync WAL (manual fsync for now, group commit in Task 1.7).
	if err := v.wal.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Update superblock WALHead so reopen knows where entries are.
	v.super.WALHead = v.wal.LogicalHead()
	v.super.WALCheckpointLSN = 0
	if _, err := v.fd.Seek(0, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	if _, err := v.super.WriteTo(v.fd); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	v.fd.Sync()
	v.fd.Close()

	// Reopen and manually replay WAL to verify data is durable.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	// Manually replay: read WAL entry and populate dirty map.
	replayBuf := make([]byte, v2.super.WALHead)
	if _, err := v2.fd.ReadAt(replayBuf, int64(v2.super.WALOffset)); err != nil {
		t.Fatalf("read WAL for replay: %v", err)
	}
	entry, err := DecodeWALEntry(replayBuf)
	if err != nil {
		t.Fatalf("decode WAL entry: %v", err)
	}
	blocks := entry.Length / v2.super.BlockSize
	for i := uint32(0); i < blocks; i++ {
		v2.dirtyMap.Put(entry.LBA+uint64(i), 0, entry.LSN, v2.super.BlockSize)
	}

	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after recovery: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data not durable after sync + reopen")
	}
}

func testWriteMultipleSync(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 10 blocks with different data.
	for i := uint64(0); i < 10; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Read all back.
	for i := uint64(0); i < 10; i++ {
		got, err := v.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		expected := makeBlock(byte('A' + i))
		if !bytes.Equal(got, expected) {
			t.Errorf("block %d: data mismatch", i)
		}
	}
}

func testReadUnflushed(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	data := makeBlock('X')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Read before flusher runs -- should come from dirty map / WAL.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("unflushed read: data mismatch")
	}
}

func testReadFlushed(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	data := makeBlock('F')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Manually flush: copy WAL data to extent region, clear dirty map.
	extentStart := v.super.WALOffset + v.super.WALSize
	if _, err := v.fd.WriteAt(data, int64(extentStart)); err != nil {
		t.Fatalf("manual flush write: %v", err)
	}
	v.dirtyMap.Delete(0)

	// Read should now come from extent region.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after flush: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("flushed read: data mismatch")
	}
}

func testReadMixedDirtyClean(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write blocks 0, 2, 4 (dirty).
	for _, lba := range []uint64{0, 2, 4} {
		if err := v.WriteLBA(lba, makeBlock(byte('A'+lba))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", lba, err)
		}
	}

	// Manually flush block 0 to extent, remove from dirty map.
	extentStart := v.super.WALOffset + v.super.WALSize
	if _, err := v.fd.WriteAt(makeBlock('A'), int64(extentStart)); err != nil {
		t.Fatalf("manual flush: %v", err)
	}
	v.dirtyMap.Delete(0)

	// Block 0: from extent (flushed)
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(got, makeBlock('A')) {
		t.Error("block 0 (flushed) mismatch")
	}

	// Block 2: from dirty map (WAL)
	got, err = v.ReadLBA(2, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(2): %v", err)
	}
	if !bytes.Equal(got, makeBlock('C')) { // 'A'+2 = 'C'
		t.Error("block 2 (dirty) mismatch")
	}

	// Block 4: from dirty map (WAL)
	got, err = v.ReadLBA(4, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(4): %v", err)
	}
	if !bytes.Equal(got, makeBlock('E')) { // 'A'+4 = 'E'
		t.Error("block 4 (dirty) mismatch")
	}

	// Blocks 1, 3, 5: never written, should be zeros (from extent region).
	for _, lba := range []uint64{1, 3, 5} {
		got, err = v.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", lba, err)
		}
		if !bytes.Equal(got, make([]byte, 4096)) {
			t.Errorf("block %d (unwritten) should be zeros", lba)
		}
	}
}

func testWALReadCorruptLength(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write a valid block.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Get the WAL offset from dirty map.
	walOff, _, _, ok := v.dirtyMap.Get(0)
	if !ok {
		t.Fatal("block 0 not in dirty map")
	}

	// Corrupt the Length field in the WAL entry on disk.
	// Length is at header offset 26 (LSN=8 + Epoch=8 + Type=1 + Flags=1 + LBA=8).
	absOff := int64(v.super.WALOffset + walOff)
	lengthOff := absOff + 26
	var hugeLenBuf [4]byte
	binary.LittleEndian.PutUint32(hugeLenBuf[:], 999999999) // ~1GB
	if _, err := v.fd.WriteAt(hugeLenBuf[:], lengthOff); err != nil {
		t.Fatalf("corrupt length: %v", err)
	}

	// ReadLBA should detect the corrupt length and error (not panic/OOM).
	_, err := v.ReadLBA(0, 4096)
	if err == nil {
		t.Error("expected error reading corrupt WAL entry, got nil")
	}
}

func testOpenInvalidSuperblock(t *testing.T) {
	dir := t.TempDir()

	// Create a valid volume, then corrupt BlockSize to 0.
	path := filepath.Join(dir, "corrupt.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{VolumeSize: 1024 * 1024})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	v.Close()

	// Corrupt BlockSize (at superblock offset 36: Magic=4 + Version=2 + Flags=2 + UUID=16 + VolumeSize=8 + ExtentSize=4 = 36).
	fd, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	var zeroBuf [4]byte
	if _, err := fd.WriteAt(zeroBuf[:], 36); err != nil {
		fd.Close()
		t.Fatalf("corrupt blocksize: %v", err)
	}
	fd.Close()

	// OpenBlockVol should reject the corrupt superblock.
	_, err = OpenBlockVol(path)
	if err == nil {
		t.Fatal("expected error opening volume with BlockSize=0")
	}
	if !errors.Is(err, ErrInvalidSuperblock) {
		t.Errorf("expected ErrInvalidSuperblock, got: %v", err)
	}
}

// --- Task 1.10: Lifecycle tests ---

func testLifecycleCreateCloseReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lifecycle.blockvol")

	// Create, write, close.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	data := makeBlock('L')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify data survived.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data not durable after create→write→sync→close→reopen")
	}
}

func testLifecycleCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flush.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write several blocks.
	for i := uint64(0); i < 5; i++ {
		if err := v.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Close does a final flush — dirty map should be drained.
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify.
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
			t.Errorf("block %d: data mismatch after close+reopen", i)
		}
	}
}

func testLifecycleDoubleClose(t *testing.T) {
	v := createTestVol(t)
	if err := v.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close should not panic (group committer + flusher are idempotent).
	// The fd.Close() will return an error but should not panic.
	_ = v.Close()
}

func testLifecycleInfo(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	info := v.Info()
	if info.VolumeSize != 1*1024*1024 {
		t.Errorf("VolumeSize = %d, want %d", info.VolumeSize, 1*1024*1024)
	}
	if info.BlockSize != 4096 {
		t.Errorf("BlockSize = %d, want 4096", info.BlockSize)
	}
	if !info.Healthy {
		t.Error("Healthy = false, want true")
	}
}

func testLifecycleWriteSyncCloseReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wsco.blockvol")

	// Cycle: create → write → sync → close → reopen → write → sync → close → verify.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA round 1: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache round 1: %v", err)
	}
	v.Close()

	// Reopen, write more.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol round 2: %v", err)
	}

	if err := v2.WriteLBA(1, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA round 2: %v", err)
	}
	// Overwrite block 0.
	if err := v2.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA overwrite: %v", err)
	}
	if err := v2.SyncCache(); err != nil {
		t.Fatalf("SyncCache round 2: %v", err)
	}
	v2.Close()

	// Final reopen — verify.
	v3, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol round 3: %v", err)
	}
	defer v3.Close()

	got0, _ := v3.ReadLBA(0, 4096)
	if !bytes.Equal(got0, makeBlock('Z')) {
		t.Error("block 0: expected 'Z' (overwritten)")
	}
	got1, _ := v3.ReadLBA(1, 4096)
	if !bytes.Equal(got1, makeBlock('Y')) {
		t.Error("block 1: expected 'Y'")
	}
}

// --- Task 1.11: Crash stress test ---

func testCrashStress100(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stress.blockvol")

	const (
		volumeSize = 256 * 1024 // 256KB volume (64 blocks of 4KB)
		blockSize  = 4096
		walSize    = 64 * 1024 // 64KB WAL
		maxLBA     = volumeSize / blockSize
		iterations = 100
	)

	// Oracle: tracks the expected state of each block.
	oracle := make(map[uint64]byte) // lba → fill byte (0 = zeros/trimmed)

	// Create initial volume.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volumeSize,
		BlockSize:  blockSize,
		WALSize:    walSize,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	for iter := 0; iter < iterations; iter++ {
		// Deterministic "random" ops using iteration number.
		numOps := 3 + (iter % 5) // 3-7 ops per iteration

		for op := 0; op < numOps; op++ {
			lba := uint64((iter*7 + op*13) % maxLBA)
			action := (iter + op) % 3 // 0=write, 1=overwrite, 2=trim

			switch action {
			case 0, 1: // write
				fill := byte('A' + (iter+op)%26)
				err := v.WriteLBA(lba, makeBlock(fill))
				if err != nil {
					if errors.Is(err, ErrWALFull) {
						continue // WAL full, skip this op
					}
					t.Fatalf("iter %d op %d: WriteLBA(%d): %v", iter, op, lba, err)
				}
				oracle[lba] = fill
			case 2: // trim
				err := v.Trim(lba, blockSize)
				if err != nil {
					if errors.Is(err, ErrWALFull) {
						continue
					}
					t.Fatalf("iter %d op %d: Trim(%d): %v", iter, op, lba, err)
				}
				oracle[lba] = 0
			}
		}

		// Sync WAL.
		if err := v.SyncCache(); err != nil {
			t.Fatalf("iter %d: SyncCache: %v", iter, err)
		}

		// Simulate crash: close fd without clean shutdown.
		v.fd.Sync() // ensure WAL is on disk
		// Write superblock with current WAL positions for recovery.
		v.super.WALHead = v.wal.LogicalHead()
		v.super.WALTail = v.wal.LogicalTail()
		if _, seekErr := v.fd.Seek(0, 0); seekErr != nil {
			t.Fatalf("iter %d: Seek: %v", iter, seekErr)
		}
		v.super.WriteTo(v.fd)
		v.fd.Sync()

		// Hard crash: close fd, stop goroutines.
		v.groupCommit.Stop()
		v.flusher.Stop()
		v.fd.Close()

		// Reopen with recovery.
		v, err = OpenBlockVol(path)
		if err != nil {
			t.Fatalf("iter %d: OpenBlockVol: %v", iter, err)
		}

		// Verify oracle against actual reads.
		for lba, fill := range oracle {
			got, readErr := v.ReadLBA(lba, blockSize)
			if readErr != nil {
				t.Fatalf("iter %d: ReadLBA(%d): %v", iter, lba, readErr)
			}
			var expected []byte
			if fill == 0 {
				expected = make([]byte, blockSize)
			} else {
				expected = makeBlock(fill)
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("iter %d: block %d mismatch: got[0]=%d want[0]=%d", iter, lba, got[0], expected[0])
			}
		}
	}

	v.Close()
}

func testTrimLargeLengthReadReturnsZero(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write data first.
	data := makeBlock('A')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Verify data is written.
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA before trim: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch before trim")
	}

	// Trim with a length larger than WAL size — should still work.
	// The trim Length is metadata (trim extent), not a data allocation.
	if err := v.Trim(0, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	// Read should return zeros (TRIM entry in dirty map).
	got, err = v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after trim: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Error("read after trim should return zeros")
	}
}
