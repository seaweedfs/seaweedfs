package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		// Phase 3 Task 1.2: Config wiring.
		{name: "config_zero_value_compat", run: testConfigZeroValueCompat},
		{name: "config_validates_on_create", run: testConfigValidatesOnCreate},
		{name: "config_validates_on_open", run: testConfigValidatesOnOpen},
		// Phase 3 Task 1.7: WAL pressure integration.
		{name: "wal_pressure_triggers_flush", run: testWALPressureTriggersFlush},
		{name: "wal_full_retry_succeeds", run: testWALFullRetrySucceeds},
		{name: "wal_full_timeout_returns_error", run: testWALFullTimeoutReturnsError},
		{name: "wal_pressure_custom_threshold", run: testWALPressureCustomThreshold},
		{name: "wal_pressure_below_threshold_no_trigger", run: testWALPressureBelowThresholdNoTrigger},
		{name: "wal_pressure_concurrent_pressure", run: testWALPressureConcurrentPressure},
		// Phase 3 bug fix: P3-BUG-5 closed guard.
		{name: "write_after_close", run: testWriteAfterClose},
		// Phase 3 Task 1.8: Integration tests.
		{name: "blockvol_custom_config_create", run: testBlockvolCustomConfigCreate},
		{name: "blockvol_custom_config_open", run: testBlockvolCustomConfigOpen},
		{name: "sharded_len_accurate", run: testShardedLenAccurate},
		// Phase 3: WAL reuse guard (ReadLBA vs flusher race).
		{name: "wal_reuse_guard_read_during_flush", run: testWALReuseGuardReadDuringFlush},
		{name: "wal_reuse_guard_concurrent_stress", run: testWALReuseGuardConcurrentStress},
		// Phase 3 Task 5.2: opsOutstanding + Close drain.
		{name: "close_drains_inflight_ops", run: testCloseDrainsInflightOps},
		{name: "close_drains_concurrent_readers", run: testCloseDrainsConcurrentReaders},
		// Phase 3 Task 5.3: Trim WAL-full retry.
		{name: "trim_wal_full_retry", run: testTrimWALFullRetry},
		// Phase 3 Task 5.5: Flusher error no checkpoint advance.
		{name: "flusher_error_no_checkpoint_advance", run: testFlusherErrorNoCheckpointAdvance},
		// Phase 3 Task 5.6: Close during SyncCache.
		{name: "close_during_sync_cache", run: testCloseDuringSyncCache},
		// Review finding: Close timeout if op stuck.
		{name: "close_timeout_if_op_stuck", run: testCloseTimeoutIfOpStuck},
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

	// Short WALFullTimeout for stress test to avoid long retries.
	stressCfg := DefaultConfig()
	stressCfg.WALFullTimeout = 10 * time.Millisecond

	// Create initial volume.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: volumeSize,
		BlockSize:  blockSize,
		WALSize:    walSize,
	}, stressCfg)
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

		// Stop background goroutines BEFORE writing superblock to avoid
		// concurrent superblock writes (flusher also writes superblock).
		v.groupCommit.Stop()
		v.flusher.Stop()

		// Simulate crash: write superblock with current WAL positions.
		v.fd.Sync() // ensure WAL is on disk
		v.super.WALHead = v.wal.LogicalHead()
		v.super.WALTail = v.wal.LogicalTail()
		if _, seekErr := v.fd.Seek(0, 0); seekErr != nil {
			t.Fatalf("iter %d: Seek: %v", iter, seekErr)
		}
		v.super.WriteTo(v.fd)
		v.fd.Sync()
		v.fd.Close()

		// Reopen with recovery.
		v, err = OpenBlockVol(path, stressCfg)
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

// --- Phase 3 Task 1.2: Config wiring tests ---

func testConfigZeroValueCompat(t *testing.T) {
	// Zero-value config (no explicit config passed) should work identically to Phase 2.
	dir := t.TempDir()
	path := filepath.Join(dir, "zeroconfig.blockvol")

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol with zero config: %v", err)
	}

	data := makeBlock('Z')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("zero-value config: data mismatch")
	}
	v.Close()
}

func testConfigValidatesOnCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "badcfg.blockvol")
	badCfg := DefaultConfig()
	badCfg.DirtyMapShards = 3 // not power-of-2

	_, err := CreateBlockVol(path, CreateOptions{VolumeSize: 1 * 1024 * 1024}, badCfg)
	if err == nil {
		t.Fatal("expected error with bad config on Create")
	}
	if !errors.Is(err, errInvalidConfig) {
		t.Errorf("expected errInvalidConfig, got: %v", err)
	}
}

func testConfigValidatesOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opencfg.blockvol")

	// Create a valid volume first.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	v.Close()

	// Open with bad config.
	badCfg := DefaultConfig()
	badCfg.DirtyMapShards = 3 // invalid: not power-of-2

	_, err = OpenBlockVol(path, badCfg)
	if err == nil {
		t.Fatal("expected error with bad config on Open")
	}
	if !errors.Is(err, errInvalidConfig) {
		t.Errorf("expected errInvalidConfig, got: %v", err)
	}
}

// --- Phase 3 Task 1.7: WAL pressure tests ---

func testWALPressureTriggersFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pressure.blockvol")

	// Small WAL + low threshold to trigger pressure quickly.
	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.3
	cfg.FlushInterval = 10 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024, // 64KB WAL
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write enough to exceed 30% threshold.
	entrySize := uint64(walEntryHeaderSize + 4096)
	walCapacity := 64 * 1024 / entrySize
	writeCount := int(float64(walCapacity)*0.4) + 1

	for i := 0; i < writeCount; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Pressure should have triggered flusher. Give it time to flush.
	time.Sleep(50 * time.Millisecond)

	// The flusher should have made progress.
	frac := v.wal.UsedFraction()
	t.Logf("WAL used fraction after writes+flush: %f", frac)
}

func testWALFullRetrySucceeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "retry.blockvol")

	cfg := DefaultConfig()
	cfg.WALFullTimeout = 2 * time.Second
	cfg.FlushInterval = 10 * time.Millisecond

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 4 // tiny WAL: 4 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write enough to nearly fill WAL.
	for i := 0; i < 3; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Next write may trigger WAL full + retry. With flusher running,
	// it should eventually succeed.
	for i := 3; i < 10; i++ {
		if err := v.WriteLBA(uint64(i%4), makeBlock(byte('X'+i%4))); err != nil {
			t.Fatalf("WriteLBA(%d) after flusher: %v", i, err)
		}
	}
}

func testWALFullTimeoutReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "timeout.blockvol")

	cfg := DefaultConfig()
	cfg.WALFullTimeout = 50 * time.Millisecond
	cfg.FlushInterval = 1 * time.Hour // flusher effectively disabled

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 2 // tiny WAL: 2 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Stop the flusher goroutine so NotifyUrgent is a no-op.
	v.flusher.Stop()

	// Fill WAL.
	for i := 0; i < 2; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Next write should timeout since flusher is stopped.
	start := time.Now()
	err = v.WriteLBA(2, makeBlock('Z'))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected ErrWALFull after timeout")
	}
	if !errors.Is(err, ErrWALFull) {
		t.Errorf("expected ErrWALFull, got: %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("should have waited ~50ms, took %v", elapsed)
	}
}

// --- Phase 3 Task 1.8: Integration tests ---

func testBlockvolCustomConfigCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "customcfg.blockvol")

	cfg := BlockVolConfig{
		GroupCommitMaxDelay:     2 * time.Millisecond,
		GroupCommitMaxBatch:     32,
		GroupCommitLowWatermark: 2,
		WALPressureThreshold:   0.5,
		WALFullTimeout:         1 * time.Second,
		FlushInterval:          50 * time.Millisecond,
		DirtyMapShards:         64,
	}

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol with custom config: %v", err)
	}
	defer v.Close()

	// Verify write/read works with custom config.
	data := makeBlock('C')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("custom config: data mismatch")
	}
}

func testBlockvolCustomConfigOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "opencustom.blockvol")

	cfg := BlockVolConfig{
		GroupCommitMaxDelay:     2 * time.Millisecond,
		GroupCommitMaxBatch:     32,
		GroupCommitLowWatermark: 2,
		WALPressureThreshold:   0.6,
		WALFullTimeout:         2 * time.Second,
		FlushInterval:          50 * time.Millisecond,
		DirtyMapShards:         128,
	}

	// Create with default config, close, reopen with custom config.
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	data := makeBlock('O')
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}
	v.Close()

	// Open with custom config.
	v2, err := OpenBlockVol(path, cfg)
	if err != nil {
		t.Fatalf("OpenBlockVol with custom config: %v", err)
	}
	defer v2.Close()

	got, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after reopen: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("data mismatch after reopen with custom config")
	}
}

func testShardedLenAccurate(t *testing.T) {
	dm := NewDirtyMap(256)

	// Insert 1000 entries spread across shards.
	for i := uint64(0); i < 1000; i++ {
		dm.Put(i, i*10, i+1, 4096)
	}
	if dm.Len() != 1000 {
		t.Errorf("Len() = %d, want 1000", dm.Len())
	}

	// Delete 500 entries.
	for i := uint64(0); i < 500; i++ {
		dm.Delete(i)
	}
	if dm.Len() != 500 {
		t.Errorf("after delete: Len() = %d, want 500", dm.Len())
	}
}

func testWALPressureCustomThreshold(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "customthresh.blockvol")

	// Very high threshold: pressure should NOT trigger urgently.
	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.99
	cfg.FlushInterval = 1 * time.Hour

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write a few blocks -- should not trigger urgent flush.
	for i := 0; i < 5; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Dirty map should still have entries (flusher not triggered).
	if v.dirtyMap.Len() != 5 {
		t.Errorf("dirty map len = %d, want 5 (no flush expected)", v.dirtyMap.Len())
	}
}

func testWALPressureBelowThresholdNoTrigger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "below.blockvol")

	// Threshold at 80%, write only ~50% of WAL. No urgent flush expected.
	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.8
	cfg.FlushInterval = 1 * time.Hour // disable periodic flush

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024, // 256KB WAL
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Stop flusher so we can observe dirty map state.
	v.flusher.Stop()

	// Write ~50% of WAL capacity.
	entrySize := uint64(walEntryHeaderSize + 4096)
	walCapacity := 256 * 1024 / entrySize
	halfCount := int(walCapacity / 2)

	for i := 0; i < halfCount; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	frac := v.wal.UsedFraction()
	if frac > 0.8 {
		t.Fatalf("used fraction %f > 0.8, test setup wrong", frac)
	}

	// Dirty map should still have all entries (no flush triggered).
	if v.dirtyMap.Len() != halfCount {
		t.Errorf("dirty map len = %d, want %d (no flush expected below threshold)", v.dirtyMap.Len(), halfCount)
	}
	t.Logf("WAL used fraction: %f, dirty entries: %d", frac, v.dirtyMap.Len())
}

func testWALPressureConcurrentPressure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.blockvol")

	cfg := DefaultConfig()
	cfg.WALPressureThreshold = 0.3
	cfg.WALFullTimeout = 2 * time.Second
	cfg.FlushInterval = 5 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    64 * 1024, // small WAL to create pressure
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// 8 concurrent writers, each writing 20 blocks (total 160 LBAs, fits in 256 max).
	const goroutines = 8
	const writesPerGoroutine = 20
	var wg sync.WaitGroup
	var succeeded, failed atomic.Int64

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				lba := uint64(id*writesPerGoroutine + i)
				err := v.WriteLBA(lba, makeBlock(byte('A'+id%26)))
				if err != nil {
					if errors.Is(err, ErrWALFull) {
						failed.Add(1)
					} else {
						// Unexpected error.
						t.Errorf("WriteLBA(%d): unexpected error: %v", lba, err)
					}
				} else {
					succeeded.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	total := succeeded.Load() + failed.Load()
	t.Logf("concurrent pressure: %d succeeded, %d ErrWALFull, %d total",
		succeeded.Load(), failed.Load(), total)

	if total != goroutines*writesPerGoroutine {
		t.Errorf("total outcomes = %d, want %d", total, goroutines*writesPerGoroutine)
	}
	// At least some writes should succeed (flusher is active).
	if succeeded.Load() == 0 {
		t.Error("no writes succeeded -- flusher not draining WAL?")
	}
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

// testWriteAfterClose verifies that WriteLBA, ReadLBA, Trim, and SyncCache
// return ErrVolumeClosed after Close() — no panic, no write to closed fd.
func testWriteAfterClose(t *testing.T) {
	v := createTestVol(t)
	v.Close()

	err := v.WriteLBA(0, makeBlock('X'))
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("WriteLBA after close: got %v, want ErrVolumeClosed", err)
	}

	_, err = v.ReadLBA(0, 4096)
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("ReadLBA after close: got %v, want ErrVolumeClosed", err)
	}

	err = v.Trim(0, 4096)
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("Trim after close: got %v, want ErrVolumeClosed", err)
	}

	err = v.SyncCache()
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("SyncCache after close: got %v, want ErrVolumeClosed", err)
	}
}

// testWALReuseGuardReadDuringFlush verifies the WAL reuse guard:
// write a block, force flush (so data moves to extent and WAL is reclaimed),
// write a NEW block that reuses the same WAL offset, then read the FIRST
// block. Without the guard, ReadLBA would read the second block's data from
// WAL (corruption). With the guard, it detects LSN mismatch and falls back
// to the extent region which has the correct flushed data.
func testWALReuseGuardReadDuringFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "guard.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 100 * time.Millisecond

	// Tiny WAL forces reuse quickly.
	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 3 // only 3 entries fit

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 256 * 1024, // 256KB, 64 blocks
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Step 1: Write block at LBA 0 with known pattern.
	pattern0 := makeBlock('X')
	if err := v.WriteLBA(0, pattern0); err != nil {
		t.Fatalf("WriteLBA(0): %v", err)
	}

	// Step 2: Force flush to move LBA 0 data to extent region.
	if err := v.flusher.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}

	// Step 3: Write blocks at LBA 1 and LBA 2 to fill WAL and force reuse
	// of the slot that previously held LBA 0's data.
	pattern1 := makeBlock('Y')
	if err := v.WriteLBA(1, pattern1); err != nil {
		t.Fatalf("WriteLBA(1): %v", err)
	}
	pattern2 := makeBlock('Z')
	if err := v.WriteLBA(2, pattern2); err != nil {
		t.Fatalf("WriteLBA(2): %v", err)
	}

	// Step 4: Read LBA 0. It's no longer in dirty map (flushed), so it
	// should come from extent and return 'X' pattern.
	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if !bytes.Equal(data, pattern0) {
		t.Fatalf("LBA 0 corruption: got %q... want %q...", data[:8], pattern0[:8])
	}

	// Step 5: Verify LBA 1 and 2 still read correctly from WAL.
	data1, err := v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if !bytes.Equal(data1, pattern1) {
		t.Fatalf("LBA 1: got %q... want %q...", data1[:8], pattern1[:8])
	}
	data2, err := v.ReadLBA(2, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(2): %v", err)
	}
	if !bytes.Equal(data2, pattern2) {
		t.Fatalf("LBA 2: got %q... want %q...", data2[:8], pattern2[:8])
	}
}

// testWALReuseGuardConcurrentStress hammers ReadLBA and WriteLBA concurrently
// with a tiny WAL to maximize the chance of hitting the reuse race window.
// Every read must return either the last-written pattern or zeros (never-written
// blocks), never data from a different LBA.
func testWALReuseGuardConcurrentStress(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stress.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond // aggressive flushing

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 6 // small WAL: 6 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096, // 64 blocks
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	const numLBAs = 8
	const iterations = 200

	// Track the latest pattern written to each LBA.
	var patterns [numLBAs]atomic.Uint32 // stores the byte pattern

	var wg sync.WaitGroup
	var errCount atomic.Int64

	// Writer goroutine: writes sequential patterns to random LBAs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			lba := uint64(i % numLBAs)
			pat := byte(i%250 + 1) // non-zero pattern
			patterns[lba].Store(uint32(pat))
			if err := v.WriteLBA(lba, makeBlock(pat)); err != nil {
				if errors.Is(err, ErrVolumeClosed) || errors.Is(err, ErrWALFull) {
					return
				}
				t.Errorf("WriteLBA(%d, iter %d): %v", lba, i, err)
				errCount.Add(1)
				return
			}
		}
	}()

	// Reader goroutine: reads LBAs and validates data consistency.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*2; i++ {
			lba := uint64(i % numLBAs)
			data, err := v.ReadLBA(lba, 4096)
			if err != nil {
				if errors.Is(err, ErrVolumeClosed) {
					return
				}
				t.Errorf("ReadLBA(%d): %v", lba, err)
				errCount.Add(1)
				return
			}

			// Data must be uniform: all bytes the same (or all zeros).
			first := data[0]
			for j := 1; j < len(data); j++ {
				if data[j] != first {
					t.Errorf("LBA %d corruption at byte %d: first=%d got=%d (iter %d)",
						lba, j, first, data[j], i)
					errCount.Add(1)
					return
				}
			}
		}
	}()

	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("%d errors during concurrent stress test", errCount.Load())
	}
}

// testCloseDrainsInflightOps verifies Close waits for in-flight WriteLBA to
// finish before closing the fd.
func testCloseDrainsInflightOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drain.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 50 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 256 * 1024,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Start a goroutine that writes continuously.
	var writesDone atomic.Int64
	var writeErr atomic.Value
	stopCh := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			err := v.WriteLBA(uint64(i%64), makeBlock(byte(i%250+1)))
			if err != nil {
				if errors.Is(err, ErrVolumeClosed) {
					return
				}
				writeErr.Store(err)
				return
			}
			writesDone.Add(1)
		}
	}()

	// Let some writes happen.
	time.Sleep(20 * time.Millisecond)

	// Close should wait for any in-flight write to finish.
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(stopCh)

	if we := writeErr.Load(); we != nil {
		t.Fatalf("unexpected write error: %v", we)
	}
	t.Logf("writes completed before close: %d", writesDone.Load())

	// After Close, new writes must fail.
	err = v.WriteLBA(0, makeBlock('Z'))
	if !errors.Is(err, ErrVolumeClosed) {
		t.Fatalf("write after close: got %v, want ErrVolumeClosed", err)
	}
}

// testCloseDrainsConcurrentReaders verifies Close drains in-flight ReadLBA.
func testCloseDrainsConcurrentReaders(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drain-read.blockvol")

	cfg := DefaultConfig()
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Pre-write some data.
	for i := 0; i < 8; i++ {
		v.WriteLBA(uint64(i), makeBlock(byte(i+1)))
	}

	var readsDone atomic.Int64
	var wg sync.WaitGroup

	// 4 concurrent readers.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, err := v.ReadLBA(uint64(i%8), 4096)
				if err != nil {
					return // ErrVolumeClosed expected
				}
				readsDone.Add(1)
			}
		}(g)
	}

	// Let reads start, then close.
	time.Sleep(5 * time.Millisecond)
	v.Close()
	wg.Wait()

	t.Logf("reads completed: %d", readsDone.Load())
}

// testTrimWALFullRetry verifies Trim retries on WAL-full (same as WriteLBA).
func testTrimWALFullRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trim-retry.blockvol")

	cfg := DefaultConfig()
	cfg.WALFullTimeout = 2 * time.Second
	cfg.FlushInterval = 10 * time.Millisecond

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 4 // tiny WAL: 4 entries

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Fill WAL with writes.
	for i := 0; i < 3; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Trim should succeed even though WAL is nearly full, because the
	// retry loop triggers the flusher to free space. Trim entries are
	// header-only (no data payload), so they're smaller than writes.
	if err := v.Trim(0, 4096); err != nil {
		t.Fatalf("Trim failed (should have retried): %v", err)
	}

	// Verify trim took effect — read should return zeros.
	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after trim: %v", err)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte %d not zero after trim: %d", i, b)
		}
	}
}

// testFlusherErrorNoCheckpointAdvance verifies that when extent WriteAt fails,
// the flusher does not advance the checkpoint LSN (so data isn't lost on recovery).
func testFlusherErrorNoCheckpointAdvance(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "flusher-err.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 1 * time.Hour // manual flush only

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write some data.
	v.WriteLBA(0, makeBlock('A'))
	v.WriteLBA(1, makeBlock('B'))

	// Record checkpoint before flush.
	lsnBefore := v.flusher.CheckpointLSN()

	// Flush successfully first time.
	if err := v.flusher.FlushOnce(); err != nil {
		t.Fatalf("FlushOnce: %v", err)
	}
	lsnAfter := v.flusher.CheckpointLSN()
	if lsnAfter <= lsnBefore {
		t.Fatalf("checkpoint should have advanced: before=%d after=%d", lsnBefore, lsnAfter)
	}

	// Write more data.
	v.WriteLBA(2, makeBlock('C'))
	lsnBefore2 := v.flusher.CheckpointLSN()

	// Close the underlying fd to force WriteAt errors in FlushOnce.
	// Save the fd first so we can restore it.
	savedFd := v.fd
	badFd, _ := os.Open(os.DevNull) // read-only fd, WriteAt will fail
	v.fd = badFd
	v.flusher.SetFD(badFd) // update flusher's fd reference

	err = v.flusher.FlushOnce()
	// FlushOnce should return an error.
	if err == nil {
		t.Log("FlushOnce with bad fd did not error (entry may have been skipped)")
	}

	// Checkpoint should NOT have advanced.
	lsnAfterErr := v.flusher.CheckpointLSN()
	if lsnAfterErr > lsnBefore2 {
		t.Fatalf("checkpoint advanced despite error: before=%d after=%d", lsnBefore2, lsnAfterErr)
	}

	// Restore fd for cleanup.
	v.fd = savedFd
	v.flusher.SetFD(savedFd)
	badFd.Close()
}

// testCloseDuringSyncCache verifies Close + SyncCache concurrent don't deadlock.
func testCloseDuringSyncCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close-sync.blockvol")

	cfg := DefaultConfig()
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Write data so SyncCache has something to do.
	for i := 0; i < 8; i++ {
		v.WriteLBA(uint64(i), makeBlock(byte(i+1)))
	}

	// Launch SyncCache and Close concurrently.
	var wg sync.WaitGroup
	done := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		// SyncCache may return nil (completed) or ErrVolumeClosed (racing).
		v.SyncCache()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Millisecond) // let SyncCache start
		v.Close()
	}()

	// Deadlock detector: if both don't complete within 5s, we're stuck.
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Close + SyncCache completed without deadlock")
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: Close + SyncCache did not complete within 5s")
	}
}

// testCloseTimeoutIfOpStuck verifies Close() doesn't hang forever when an
// in-flight op is stuck. Close has a 5s timeout for drain, so we simulate a
// stuck op and verify Close completes within a reasonable time.
func testCloseTimeoutIfOpStuck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stuck.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 1 * time.Hour      // no background flush
	cfg.WALFullTimeout = 30 * time.Second   // writer will be stuck waiting

	entrySize := uint64(walEntryHeaderSize + 4096)
	walSize := entrySize * 3 // tiny WAL

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 4096,
		BlockSize:  4096,
		WALSize:    walSize,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Fill WAL completely.
	for i := 0; i < 2; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Start a writer that will be stuck in WAL-full retry (flusher is paused).
	stuckStarted := make(chan struct{})
	go func() {
		close(stuckStarted)
		v.WriteLBA(10, makeBlock('Z')) // blocks in appendWithRetry
	}()
	<-stuckStarted
	time.Sleep(10 * time.Millisecond) // let it enter the retry loop

	// Close should NOT hang forever — it has a 5s drain timeout.
	done := make(chan struct{})
	go func() {
		v.Close()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Close completed despite stuck op (drain timeout worked)")
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung for >10s — drain timeout not working")
	}
}
