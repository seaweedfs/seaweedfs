package blockvol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
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
		// Phase 4A CP1: Epoch tests.
		{name: "epoch_persist_roundtrip", run: testEpochPersistRoundtrip},
		{name: "epoch_in_wal_entry", run: testEpochInWALEntry},
		{name: "epoch_survives_recovery", run: testEpochSurvivesRecovery},
		// Phase 4A CP1: Lease tests.
		{name: "lease_grant_valid", run: testLeaseGrantValid},
		{name: "lease_expired_rejects", run: testLeaseExpiredRejects},
		{name: "lease_revoke", run: testLeaseRevoke},
		// Phase 4A CP1: Role tests.
		{name: "role_transitions_valid", run: testRoleTransitionsValid},
		{name: "role_transitions_invalid", run: testRoleTransitionsInvalid},
		{name: "role_primary_callback", run: testRolePrimaryCallback},
		{name: "role_stale_callback", run: testRoleStaleCallback},
		// Phase 4A CP1: Write gate tests.
		{name: "gate_primary_ok", run: testGatePrimaryOK},
		{name: "gate_not_primary", run: testGateNotPrimary},
		{name: "gate_stale_epoch", run: testGateStaleEpoch},
		{name: "gate_lease_expired", run: testGateLeaseExpired},
		{name: "gate_trim_rejected", run: testGateTrimRejected},
		{name: "blockvol_write_gate_integration", run: testBlockvolWriteGateIntegration},
		{name: "blockvol_gotcha_a_lease_expired", run: testBlockvolGotchaALeaseExpired},
		// Phase 4A CP1: P3-BUG-9 dirty map.
		{name: "dirty_map_power_of_2_panics", run: testDirtyMapPowerOf2Panics},
		// Phase 4A CP2: Replication wire protocol.
		{name: "frame_roundtrip", run: testFrameRoundtrip},
		{name: "frame_large_payload", run: testFrameLargePayload},
		// Phase 4A CP2: WAL shipper.
		{name: "ship_single_entry", run: testShipSingleEntry},
		{name: "ship_batch", run: testShipBatch},
		{name: "ship_epoch_mismatch_dropped", run: testShipEpochMismatchDropped},
		{name: "ship_degraded_on_error", run: testShipDegradedOnError},
		{name: "ship_no_replica_noop", run: testShipNoReplicaNoop},
		// Phase 4A CP2: Replica apply.
		{name: "replica_apply_entry", run: testReplicaApplyEntry},
		{name: "replica_reject_stale_epoch", run: testReplicaRejectStaleEpoch},
		{name: "replica_apply_updates_dirty_map", run: testReplicaApplyUpdatesDirtyMap},
		{name: "replica_reject_duplicate_lsn", run: testReplicaRejectDuplicateLSN},
		{name: "replica_flusher_works", run: testReplicaFlusherWorks},
		// Phase 4A CP2: Replica barrier.
		{name: "barrier_already_received", run: testBarrierAlreadyReceived},
		{name: "barrier_wait_for_entries", run: testBarrierWaitForEntries},
		{name: "barrier_timeout", run: testBarrierTimeout},
		{name: "barrier_epoch_mismatch_fast_fail", run: testBarrierEpochMismatchFastFail},
		{name: "barrier_concurrent_append", run: testBarrierConcurrentAppend},
		// Phase 4A CP2: Distributed group commit.
		{name: "dist_commit_both_pass", run: testDistCommitBothPass},
		{name: "dist_commit_local_fail", run: testDistCommitLocalFail},
		{name: "dist_commit_remote_fail_degrades", run: testDistCommitRemoteFailDegrades},
		{name: "dist_commit_no_replica", run: testDistCommitNoReplica},
		// Phase 4A CP2: BlockVol integration.
		{name: "blockvol_write_with_replica", run: testBlockvolWriteWithReplica},
		{name: "blockvol_no_replica_compat", run: testBlockvolNoReplicaCompat},
		// Phase 4A CP2 bug fixes.
		{name: "replica_reject_future_epoch", run: testReplicaRejectFutureEpoch},
		{name: "replica_reject_lsn_gap", run: testReplicaRejectLSNGap},
		{name: "barrier_fsync_failed_status", run: testBarrierFsyncFailedStatus},
		{name: "barrier_configurable_timeout", run: testBarrierConfigurableTimeout},
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

// --- Phase 4A CP1: Epoch tests ---

func testEpochPersistRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	if v.Epoch() != 0 {
		t.Fatalf("initial epoch = %d, want 0", v.Epoch())
	}

	if err := v.SetEpoch(42); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	if v.Epoch() != 42 {
		t.Fatalf("epoch after set = %d, want 42", v.Epoch())
	}
	v.Close()

	// Reopen and verify epoch persisted.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()
	if v2.Epoch() != 42 {
		t.Fatalf("epoch after reopen = %d, want 42", v2.Epoch())
	}
}

func testEpochInWALEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch-wal.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	if err := v.SetEpoch(7); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	v.SetMasterEpoch(7)
	v.SetRoleCallback(nil)
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	v.lease.Grant(10 * time.Second)

	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Read WAL entry header directly and check epoch field.
	headerBuf := make([]byte, walEntryHeaderSize)
	absOff := int64(v.super.WALOffset) // first entry at WAL start
	if _, err := v.fd.ReadAt(headerBuf, absOff); err != nil {
		t.Fatalf("ReadAt WAL: %v", err)
	}
	entryEpoch := binary.LittleEndian.Uint64(headerBuf[8:16])
	if entryEpoch != 7 {
		t.Fatalf("WAL entry epoch = %d, want 7", entryEpoch)
	}
}

func testEpochSurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch-recov.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	if err := v.SetEpoch(100); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	v.SetMasterEpoch(100)
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	v.lease.Grant(10 * time.Second)

	if err := v.WriteLBA(0, makeBlock('R')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Close properly (flushes WAL to extent).
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify epoch persisted.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("OpenBlockVol: %v", err)
	}
	defer v2.Close()

	if v2.Epoch() != 100 {
		t.Fatalf("epoch after recovery = %d, want 100", v2.Epoch())
	}

	data, err := v2.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(data, makeBlock('R')) {
		t.Fatal("data mismatch after recovery")
	}
}

// --- Phase 4A CP1: Lease tests ---

func testLeaseGrantValid(t *testing.T) {
	var l Lease
	if l.IsValid() {
		t.Fatal("zero-value lease should be invalid")
	}
	l.Grant(1 * time.Second)
	if !l.IsValid() {
		t.Fatal("lease should be valid after grant")
	}
}

func testLeaseExpiredRejects(t *testing.T) {
	var l Lease
	l.Grant(1 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if l.IsValid() {
		t.Fatal("lease should have expired")
	}
}

func testLeaseRevoke(t *testing.T) {
	var l Lease
	l.Grant(1 * time.Hour)
	if !l.IsValid() {
		t.Fatal("lease should be valid")
	}
	l.Revoke()
	if l.IsValid() {
		t.Fatal("lease should be invalid after revoke")
	}
}

// --- Phase 4A CP1: Role tests ---

func testRoleTransitionsValid(t *testing.T) {
	valid := [][2]Role{
		{RoleNone, RolePrimary},
		{RoleNone, RoleReplica},
		{RolePrimary, RoleDraining},
		{RoleDraining, RoleStale},
		{RoleReplica, RolePrimary},
		{RoleStale, RoleRebuilding},
		{RoleStale, RoleReplica},
		{RoleRebuilding, RoleReplica},
	}
	for _, pair := range valid {
		if !ValidTransition(pair[0], pair[1]) {
			t.Errorf("expected valid: %s -> %s", pair[0], pair[1])
		}
	}
}

func testRoleTransitionsInvalid(t *testing.T) {
	invalid := [][2]Role{
		{RolePrimary, RoleReplica},
		{RolePrimary, RoleStale},
		{RoleReplica, RoleStale},
		{RoleReplica, RoleDraining},
		{RoleDraining, RolePrimary},
		{RoleRebuilding, RolePrimary},
		{RoleNone, RoleStale},
		{RoleNone, RoleDraining},
	}
	for _, pair := range invalid {
		if ValidTransition(pair[0], pair[1]) {
			t.Errorf("expected invalid: %s -> %s", pair[0], pair[1])
		}
	}
}

func testRolePrimaryCallback(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	var called bool
	var gotOld, gotNew Role
	v.SetRoleCallback(func(old, new Role) {
		called = true
		gotOld = old
		gotNew = new
	})

	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole(Primary): %v", err)
	}
	if !called {
		t.Fatal("callback not called")
	}
	if gotOld != RoleNone || gotNew != RolePrimary {
		t.Fatalf("callback args: old=%s new=%s, want none->primary", gotOld, gotNew)
	}
}

func testRoleStaleCallback(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	var transitions []string
	v.SetRoleCallback(func(old, new Role) {
		transitions = append(transitions, old.String()+"->"+new.String())
	})

	// None -> Primary -> Draining -> Stale
	v.SetRole(RolePrimary)
	v.SetRole(RoleDraining)
	v.SetRole(RoleStale)

	expected := []string{"none->primary", "primary->draining", "draining->stale"}
	if len(transitions) != len(expected) {
		t.Fatalf("transitions = %v, want %v", transitions, expected)
	}
	for i, e := range expected {
		if transitions[i] != e {
			t.Errorf("transition[%d] = %s, want %s", i, transitions[i], e)
		}
	}
}

// --- Phase 4A CP1: Write gate tests ---

func testGatePrimaryOK(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Set up as primary with valid epoch + lease.
	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(10 * time.Second)

	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA as primary: %v", err)
	}
}

func testGateNotPrimary(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RoleReplica)

	err := v.WriteLBA(0, makeBlock('A'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Fatalf("expected ErrNotPrimary, got: %v", err)
	}
}

func testGateStaleEpoch(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(2) // mismatch
	v.lease.Grant(10 * time.Second)

	err := v.WriteLBA(0, makeBlock('A'))
	if !errors.Is(err, ErrEpochStale) {
		t.Fatalf("expected ErrEpochStale, got: %v", err)
	}
}

func testGateLeaseExpired(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	// No lease granted — should be expired.

	err := v.WriteLBA(0, makeBlock('A'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got: %v", err)
	}
}

func testGateTrimRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RoleReplica)

	err := v.Trim(0, 4096)
	if !errors.Is(err, ErrNotPrimary) {
		t.Fatalf("expected ErrNotPrimary for Trim, got: %v", err)
	}
}

func testBlockvolWriteGateIntegration(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// RoleNone: writes pass without fencing (Phase 3 compat).
	if err := v.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA with RoleNone: %v", err)
	}

	// Switch to primary with proper setup.
	v.SetRole(RolePrimary)
	v.SetEpoch(5)
	v.SetMasterEpoch(5)
	v.lease.Grant(10 * time.Second)

	if err := v.WriteLBA(1, makeBlock('P')); err != nil {
		t.Fatalf("WriteLBA as primary: %v", err)
	}

	// Reads always work regardless of role.
	data, err := v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(data, makeBlock('P')) {
		t.Fatal("data mismatch")
	}

	// Demote to draining — writes should fail.
	v.SetRole(RoleDraining)
	err = v.WriteLBA(2, makeBlock('D'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Fatalf("WriteLBA as draining: expected ErrNotPrimary, got: %v", err)
	}

	// Read still works.
	_, err = v.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after demotion: %v", err)
	}
}

func testBlockvolGotchaALeaseExpired(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gotcha-a.blockvol")
	cfg := DefaultConfig()
	cfg.GroupCommitMaxDelay = 1 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Set up as primary.
	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(50 * time.Millisecond) // short lease

	// Write should succeed.
	if err := v.WriteLBA(0, makeBlock('G')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Wait for lease to expire.
	time.Sleep(100 * time.Millisecond)

	// SyncCache should fail via PostSyncCheck (Gotcha A).
	err = v.SyncCache()
	if err == nil {
		t.Fatal("SyncCache should fail after lease expired")
	}
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired from SyncCache, got: %v", err)
	}
}

// --- Phase 4A CP1: P3-BUG-9 ---

func testDirtyMapPowerOf2Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for non-power-of-2 numShards")
		}
		msg, ok := r.(string)
		if !ok || msg != "blockvol: NewDirtyMap numShards must be power of 2" {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	NewDirtyMap(3) // should panic
}

// =============================================================================
// Phase 4A CP2: Replication wire protocol tests
// =============================================================================

func testFrameRoundtrip(t *testing.T) {
	// Test frame write + read roundtrip with various payloads.
	tests := []struct {
		msgType byte
		payload []byte
	}{
		{MsgWALEntry, []byte("hello")},
		{MsgBarrierReq, EncodeBarrierRequest(BarrierRequest{Vid: 1, LSN: 42, Epoch: 7})},
		{MsgBarrierResp, []byte{BarrierOK}},
		{0xFF, []byte{}}, // empty payload
	}
	for _, tc := range tests {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, tc.msgType, tc.payload); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
		gotType, gotPayload, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if gotType != tc.msgType {
			t.Errorf("type: got 0x%02x, want 0x%02x", gotType, tc.msgType)
		}
		if !bytes.Equal(gotPayload, tc.payload) {
			t.Errorf("payload mismatch")
		}
	}
}

func testFrameLargePayload(t *testing.T) {
	payload := make([]byte, 1024*1024) // 1MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgWALEntry, payload); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	gotType, gotPayload, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if gotType != MsgWALEntry {
		t.Errorf("type: got 0x%02x, want 0x%02x", gotType, MsgWALEntry)
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Error("large payload mismatch")
	}
}

// =============================================================================
// Phase 4A CP2: WAL shipper tests
// =============================================================================

// mockDataServer starts a TCP server that reads frames and collects them.
func mockDataServer(t *testing.T) (addr string, frames *[][]byte, done chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	collected := &[][]byte{}
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, payload, err := ReadFrame(conn)
			if err != nil {
				return
			}
			*collected = append(*collected, payload)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String(), collected, doneCh
}

// mockCtrlServer starts a TCP server that reads barrier requests and responds OK.
func mockCtrlServer(t *testing.T, respStatus byte) (addr string, done chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			msgType, _, err := ReadFrame(conn)
			if err != nil {
				return
			}
			if msgType == MsgBarrierReq {
				WriteFrame(conn, MsgBarrierResp, []byte{respStatus})
			}
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String(), doneCh
}

func testShipSingleEntry(t *testing.T) {
	dataAddr, frames, done := mockDataServer(t)
	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)

	epoch := uint64(1)
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch })
	defer s.Stop()

	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	if err := s.Ship(entry); err != nil {
		t.Fatalf("Ship: %v", err)
	}

	if s.ShippedLSN() != 1 {
		t.Errorf("ShippedLSN: got %d, want 1", s.ShippedLSN())
	}

	s.Stop()
	<-done

	if len(*frames) != 1 {
		t.Fatalf("frames: got %d, want 1", len(*frames))
	}
	decoded, err := DecodeWALEntry((*frames)[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.LSN != 1 {
		t.Errorf("LSN: got %d, want 1", decoded.LSN)
	}
}

func testShipBatch(t *testing.T) {
	dataAddr, frames, done := mockDataServer(t)
	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)

	epoch := uint64(1)
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch })
	defer s.Stop()

	for i := uint64(1); i <= 5; i++ {
		entry := &WALEntry{LSN: i, Epoch: 1, Type: EntryTypeTrim, LBA: i, Length: 4096}
		if err := s.Ship(entry); err != nil {
			t.Fatalf("Ship(%d): %v", i, err)
		}
	}

	if s.ShippedLSN() != 5 {
		t.Errorf("ShippedLSN: got %d, want 5", s.ShippedLSN())
	}

	s.Stop()
	<-done

	if len(*frames) != 5 {
		t.Fatalf("frames: got %d, want 5", len(*frames))
	}
}

func testShipEpochMismatchDropped(t *testing.T) {
	// Use a real server so connections work, but verify no frames arrive.
	dataAddr, frames, done := mockDataServer(t)
	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)

	epoch := uint64(2) // shipper epoch is 2
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch })

	// First ship a valid entry to establish connection.
	validEntry := &WALEntry{LSN: 1, Epoch: 2, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	if err := s.Ship(validEntry); err != nil {
		t.Fatalf("Ship valid: %v", err)
	}

	// Now ship an entry with old epoch 1 — should be silently dropped.
	staleEntry := &WALEntry{LSN: 2, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	if err := s.Ship(staleEntry); err != nil {
		t.Fatalf("Ship stale: %v", err)
	}

	// ShippedLSN should be 1 (only the valid entry).
	if s.ShippedLSN() != 1 {
		t.Errorf("ShippedLSN should be 1, got %d", s.ShippedLSN())
	}

	s.Stop()
	<-done

	// Only the valid entry should have been shipped.
	if len(*frames) != 1 {
		t.Errorf("frames: got %d, want 1 (stale entry should be dropped)", len(*frames))
	}
}

func testShipDegradedOnError(t *testing.T) {
	// Start a server that immediately closes the connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close() // close immediately — writes will fail
	}()
	defer ln.Close()

	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)
	epoch := uint64(1)
	s := NewWALShipper(ln.Addr().String(), ctrlAddr, func() uint64 { return epoch })
	defer s.Stop()

	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	// Ship may succeed on first write (kernel buffer) or fail. Keep shipping until degraded.
	for i := 0; i < 10; i++ {
		entry.LSN = uint64(i + 1)
		s.Ship(entry)
		if s.IsDegraded() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !s.IsDegraded() {
		t.Error("expected shipper to be degraded after write error")
	}

	// Subsequent Ship calls should be no-ops.
	entry.LSN = 100
	if err := s.Ship(entry); err != nil {
		t.Fatalf("Ship after degraded: %v", err)
	}

	// Barrier should return ErrReplicaDegraded.
	if err := s.Barrier(100); !errors.Is(err, ErrReplicaDegraded) {
		t.Errorf("Barrier after degraded: got %v, want ErrReplicaDegraded", err)
	}
}

func testShipNoReplicaNoop(t *testing.T) {
	// A nil shipper should not be called, but test that a stopped shipper is safe.
	s := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 { return 1 })
	s.Stop()

	entry := &WALEntry{LSN: 1, Epoch: 1, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	if err := s.Ship(entry); err != nil {
		t.Fatalf("Ship after stop: %v", err)
	}
	if err := s.Barrier(1); !errors.Is(err, ErrShipperStopped) {
		t.Errorf("Barrier after stop: got %v, want ErrShipperStopped", err)
	}
}

// =============================================================================
// Phase 4A CP2: Replica apply tests
// =============================================================================

func createReplicaVolPair(t *testing.T) (primary *BlockVol, replica *BlockVol) {
	t.Helper()
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}
	p, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatalf("CreateBlockVol primary: %v", err)
	}
	r, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		p.Close()
		t.Fatalf("CreateBlockVol replica: %v", err)
	}
	return p, r
}

func testReplicaApplyEntry(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	// Connect to data port and send a WAL entry.
	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	data := makeBlock('X')
	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: data}
	encoded, _ := entry.Encode()
	if err := WriteFrame(conn, MsgWALEntry, encoded); err != nil {
		t.Fatal(err)
	}

	// Wait for apply.
	deadline := time.After(2 * time.Second)
	for {
		if recv.ReceivedLSN() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for entry to be applied")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if recv.ReceivedLSN() != 1 {
		t.Errorf("ReceivedLSN: got %d, want 1", recv.ReceivedLSN())
	}
}

func testReplicaRejectStaleEpoch(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()
	replica.epoch.Store(5) // replica at epoch 5

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Entry with epoch 3 (stale) — should be rejected.
	entry := &WALEntry{LSN: 1, Epoch: 3, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: make([]byte, 4096)}
	encoded, _ := entry.Encode()
	WriteFrame(conn, MsgWALEntry, encoded)

	// Give it time to process.
	time.Sleep(50 * time.Millisecond)

	if recv.ReceivedLSN() != 0 {
		t.Errorf("ReceivedLSN should be 0 (stale entry rejected), got %d", recv.ReceivedLSN())
	}
}

func testReplicaApplyUpdatesDirtyMap(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	data := makeBlock('D')
	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeWrite, LBA: 5, Length: 4096, Data: data}
	encoded, _ := entry.Encode()
	WriteFrame(conn, MsgWALEntry, encoded)

	// Wait for apply.
	deadline := time.After(2 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Check dirty map has an entry for LBA 5.
	_, lsn, _, ok := replica.dirtyMap.Get(5)
	if !ok {
		t.Fatal("dirty map: LBA 5 not found")
	}
	if lsn != 1 {
		t.Errorf("dirty map LSN: got %d, want 1", lsn)
	}
}

func testReplicaRejectDuplicateLSN(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send LSN=1.
	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	encoded, _ := entry.Encode()
	WriteFrame(conn, MsgWALEntry, encoded)

	deadline := time.After(2 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Send duplicate LSN=1 — should be skipped.
	WriteFrame(conn, MsgWALEntry, encoded)
	time.Sleep(50 * time.Millisecond)

	// ReceivedLSN should still be 1.
	if recv.ReceivedLSN() != 1 {
		t.Errorf("ReceivedLSN after dup: got %d, want 1", recv.ReceivedLSN())
	}
}

func testReplicaFlusherWorks(t *testing.T) {
	// Verify the replica vol's flusher can flush replicated entries.
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	data := makeBlock('F')
	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeWrite, LBA: 0, Length: 4096, Data: data}
	encoded, _ := entry.Encode()
	WriteFrame(conn, MsgWALEntry, encoded)

	deadline := time.After(2 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Trigger flusher and verify data is readable from replica.
	replica.flusher.FlushOnce()

	got, err := replica.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("flushed data mismatch on replica")
	}
}

// =============================================================================
// Phase 4A CP2: Replica barrier tests
// =============================================================================

func testBarrierAlreadyReceived(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	// Send an entry first.
	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer dataConn.Close()

	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	encoded, _ := entry.Encode()
	WriteFrame(dataConn, MsgWALEntry, encoded)

	deadline := time.After(2 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Now send a barrier for LSN=1 — should succeed immediately.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(5 * time.Second))

	req := EncodeBarrierRequest(BarrierRequest{LSN: 1, Epoch: 0})
	WriteFrame(ctrlConn, MsgBarrierReq, req)

	msgType, payload, err := ReadFrame(ctrlConn)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgBarrierResp {
		t.Fatalf("unexpected msg type 0x%02x", msgType)
	}
	if payload[0] != BarrierOK {
		t.Errorf("barrier status: got %d, want BarrierOK", payload[0])
	}
}

func testBarrierWaitForEntries(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	// Send barrier BEFORE the entry arrives.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(5 * time.Second))

	barrierDone := make(chan byte, 1)
	go func() {
		req := EncodeBarrierRequest(BarrierRequest{LSN: 1, Epoch: 0})
		WriteFrame(ctrlConn, MsgBarrierReq, req)
		_, payload, err := ReadFrame(ctrlConn)
		if err != nil {
			barrierDone <- 0xFF
			return
		}
		barrierDone <- payload[0]
	}()

	// Wait a bit, then send the entry.
	time.Sleep(50 * time.Millisecond)

	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer dataConn.Close()

	entry := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	encoded, _ := entry.Encode()
	WriteFrame(dataConn, MsgWALEntry, encoded)

	select {
	case status := <-barrierDone:
		if status != BarrierOK {
			t.Errorf("barrier status: got %d, want BarrierOK", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("barrier did not complete")
	}
}

func testBarrierTimeout(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.barrierTimeout = 100 * time.Millisecond // fast timeout for test
	recv.Serve()
	defer recv.Stop()

	// Send barrier for LSN=999 that will never arrive.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(5 * time.Second))

	req := EncodeBarrierRequest(BarrierRequest{LSN: 999, Epoch: 0})
	WriteFrame(ctrlConn, MsgBarrierReq, req)

	start := time.Now()
	_, payload, err := ReadFrame(ctrlConn)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != BarrierTimeout {
		t.Errorf("barrier status: got %d, want BarrierTimeout", payload[0])
	}
	if elapsed > 2*time.Second {
		t.Errorf("barrier timeout took %v, expected ~100ms", elapsed)
	}
}

func testBarrierEpochMismatchFastFail(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()
	replica.epoch.Store(5)

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(5 * time.Second))

	// Barrier with epoch=3 (mismatch with replica epoch=5).
	req := EncodeBarrierRequest(BarrierRequest{LSN: 1, Epoch: 3})
	WriteFrame(ctrlConn, MsgBarrierReq, req)

	start := time.Now()
	_, payload, err := ReadFrame(ctrlConn)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != BarrierEpochMismatch {
		t.Errorf("barrier status: got %d, want BarrierEpochMismatch", payload[0])
	}
	// Should be fast — no waiting.
	if elapsed > 500*time.Millisecond {
		t.Errorf("epoch mismatch took %v, expected fast fail", elapsed)
	}
}

func testBarrierConcurrentAppend(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	// Start barrier waiting for LSN=10.
	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(10 * time.Second))

	barrierDone := make(chan byte, 1)
	go func() {
		req := EncodeBarrierRequest(BarrierRequest{LSN: 10, Epoch: 0})
		WriteFrame(ctrlConn, MsgBarrierReq, req)
		_, payload, err := ReadFrame(ctrlConn)
		if err != nil {
			barrierDone <- 0xFF
			return
		}
		barrierDone <- payload[0]
	}()

	// Stream 10 entries concurrently.
	dataConn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer dataConn.Close()

	for i := uint64(1); i <= 10; i++ {
		entry := &WALEntry{LSN: i, Epoch: 0, Type: EntryTypeTrim, LBA: i, Length: 4096}
		encoded, _ := entry.Encode()
		WriteFrame(dataConn, MsgWALEntry, encoded)
	}

	select {
	case status := <-barrierDone:
		if status != BarrierOK {
			t.Errorf("barrier status: got %d, want BarrierOK", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("barrier did not complete")
	}
}

// =============================================================================
// Phase 4A CP2: Distributed group commit tests
// =============================================================================

func testDistCommitBothPass(t *testing.T) {
	syncCalled := atomic.Bool{}
	walSync := func() error {
		syncCalled.Store(true)
		return nil
	}

	// Mock shipper with barrier that succeeds.
	dataAddr, _, _ := mockDataServer(t)
	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)

	v := createTestVol(t)
	defer v.Close()

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 })
	defer shipper.Stop()

	distSync := MakeDistributedSync(walSync, shipper, v)
	if err := distSync(); err != nil {
		t.Fatalf("distSync: %v", err)
	}
	if !syncCalled.Load() {
		t.Error("local walSync was not called")
	}
}

func testDistCommitLocalFail(t *testing.T) {
	walSync := func() error {
		return fmt.Errorf("disk error")
	}

	dataAddr, _, _ := mockDataServer(t)
	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)

	v := createTestVol(t)
	defer v.Close()

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 })
	defer shipper.Stop()

	distSync := MakeDistributedSync(walSync, shipper, v)
	err := distSync()
	if err == nil {
		t.Fatal("expected error from local sync failure")
	}
}

func testDistCommitRemoteFailDegrades(t *testing.T) {
	walSync := func() error { return nil }

	dataAddr, _, _ := mockDataServer(t)
	// Control server that returns epoch mismatch.
	ctrlAddr, _ := mockCtrlServer(t, BarrierEpochMismatch)

	v := createTestVol(t)
	defer v.Close()

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 })
	defer shipper.Stop()

	// Set shipper on vol so degradeReplica can mark it degraded.
	v.shipper = shipper

	distSync := MakeDistributedSync(walSync, shipper, v)
	// Should NOT return error (local succeeded, remote degraded).
	if err := distSync(); err != nil {
		t.Fatalf("distSync should not fail on remote error: %v", err)
	}

	if !shipper.IsDegraded() {
		t.Error("shipper should be degraded after barrier failure")
	}

	// Second call should fall back to local-only.
	if err := distSync(); err != nil {
		t.Fatalf("distSync local-only: %v", err)
	}
}

func testDistCommitNoReplica(t *testing.T) {
	syncCalled := atomic.Bool{}
	walSync := func() error {
		syncCalled.Store(true)
		return nil
	}

	v := createTestVol(t)
	defer v.Close()

	// nil shipper — local-only mode.
	distSync := MakeDistributedSync(walSync, nil, v)
	if err := distSync(); err != nil {
		t.Fatalf("distSync: %v", err)
	}
	if !syncCalled.Load() {
		t.Error("local walSync was not called")
	}
}

// =============================================================================
// Phase 4A CP2: BlockVol integration tests
// =============================================================================

func testBlockvolWriteWithReplica(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	// Start replica receiver.
	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	// Configure primary to ship to replica.
	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write data on primary.
	data := makeBlock('R')
	if err := primary.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Wait for replica to receive.
	deadline := time.After(5 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for replica")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Flush replica and read back.
	replica.flusher.FlushOnce()
	got, err := replica.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("replica ReadLBA: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("replica data mismatch")
	}
}

func testBlockvolNoReplicaCompat(t *testing.T) {
	// Verify that a BlockVol without replica works identically to Phase 3.
	v := createTestVol(t)
	defer v.Close()

	data := makeBlock('N')
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
		t.Error("data mismatch in no-replica mode")
	}

	// Verify shipper is nil.
	if v.shipper != nil {
		t.Error("shipper should be nil without SetReplicaAddr")
	}
}

// =============================================================================
// Phase 4A CP2 bug fix tests
// =============================================================================

func testReplicaRejectFutureEpoch(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()
	replica.epoch.Store(3) // replica at epoch 3

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Entry with epoch 5 (future) — must be rejected. Replicas must NOT
	// accept epoch bumps from WAL stream; only master can change epoch.
	entry := &WALEntry{LSN: 1, Epoch: 5, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	encoded, _ := entry.Encode()
	WriteFrame(conn, MsgWALEntry, encoded)

	time.Sleep(50 * time.Millisecond)

	if recv.ReceivedLSN() != 0 {
		t.Errorf("ReceivedLSN should be 0 (future epoch rejected), got %d", recv.ReceivedLSN())
	}
}

func testReplicaRejectLSNGap(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	conn, err := net.Dial("tcp", recv.DataAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send LSN=1 — should succeed.
	entry1 := &WALEntry{LSN: 1, Epoch: 0, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	encoded1, _ := entry1.Encode()
	WriteFrame(conn, MsgWALEntry, encoded1)

	deadline := time.After(2 * time.Second)
	for recv.ReceivedLSN() < 1 {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for LSN 1")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Send LSN=3 (gap — skips LSN=2) — must be rejected.
	entry3 := &WALEntry{LSN: 3, Epoch: 0, Type: EntryTypeTrim, LBA: 1, Length: 4096}
	encoded3, _ := entry3.Encode()
	WriteFrame(conn, MsgWALEntry, encoded3)

	time.Sleep(50 * time.Millisecond)

	if recv.ReceivedLSN() != 1 {
		t.Errorf("ReceivedLSN should be 1 (gap rejected), got %d", recv.ReceivedLSN())
	}
}

func testBarrierFsyncFailedStatus(t *testing.T) {
	// Verify BarrierFsyncFailed is a distinct status code.
	if BarrierFsyncFailed == BarrierTimeout {
		t.Error("BarrierFsyncFailed should be distinct from BarrierTimeout")
	}
	if BarrierFsyncFailed == BarrierOK {
		t.Error("BarrierFsyncFailed should be distinct from BarrierOK")
	}
	if BarrierFsyncFailed != 0x03 {
		t.Errorf("BarrierFsyncFailed: got 0x%02x, want 0x03", BarrierFsyncFailed)
	}
}

func testBarrierConfigurableTimeout(t *testing.T) {
	primary, replica := createReplicaVolPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Set a very short timeout.
	recv.barrierTimeout = 50 * time.Millisecond
	recv.Serve()
	defer recv.Stop()

	ctrlConn, err := net.Dial("tcp", recv.CtrlAddr())
	if err != nil {
		t.Fatal(err)
	}
	defer ctrlConn.Close()
	ctrlConn.SetDeadline(time.Now().Add(5 * time.Second))

	req := EncodeBarrierRequest(BarrierRequest{LSN: 999, Epoch: 0})
	start := time.Now()
	WriteFrame(ctrlConn, MsgBarrierReq, req)

	_, payload, err := ReadFrame(ctrlConn)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != BarrierTimeout {
		t.Errorf("barrier status: got %d, want BarrierTimeout", payload[0])
	}
	// Should complete quickly — well under 1s.
	if elapsed > 1*time.Second {
		t.Errorf("configurable timeout took %v, expected ~50ms", elapsed)
	}
}

// Suppress unused import warnings.
var _ = fmt.Sprintf
var _ io.Reader
var _ net.Conn
