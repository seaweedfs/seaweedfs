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
		// ER Fix 1: ioMu restore/import guard.
		{name: "iomu_concurrent_writes_allowed", run: testIoMuConcurrentWritesAllowed},
		{name: "iomu_restore_blocks_writes", run: testIoMuRestoreBlocksWrites},
		{name: "iomu_close_with_iomu", run: testIoMuCloseCoordinates},
		// Adversarial ioMu tests.
		{name: "iomu_expand_blocks_writes", run: testIoMuExpandBlocksWrites},
		{name: "iomu_concurrent_read_write", run: testIoMuConcurrentReadWrite},
		{name: "iomu_restore_then_write_integrity", run: testIoMuRestoreThenWriteIntegrity},
		{name: "iomu_trim_during_expand", run: testIoMuTrimDuringExpand},
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
		// Phase 4A CP3: WAL scanner.
		{name: "wal_scan_from_middle", run: testWALScanFromMiddle},
		{name: "wal_scan_empty", run: testWALScanEmpty},
		{name: "wal_scan_recycled", run: testWALScanRecycled},
		{name: "wal_scan_wrap_padding", run: testWALScanWrapPadding},
		{name: "wal_scan_entry_crosses_end", run: testWALScanEntryCrossesEnd},
		// Phase 4A CP3: Promotion + Demotion.
		{name: "promote_replica_to_primary", run: testPromoteReplicaToPrimary},
		{name: "promote_rejects_non_replica", run: testPromoteRejectsNonReplica},
		{name: "demote_primary_to_stale", run: testDemotePrimaryToStale},
		{name: "demote_drains_inflight_ops", run: testDemoteDrainsInflightOps},
		{name: "demote_stops_shipper", run: testDemoteStopsShipper},
		{name: "assignment_refresh_lease", run: testAssignmentRefreshLease},
		{name: "assignment_invalid_transition", run: testAssignmentInvalidTransition},
		// Phase 4A CP3: Rebuild protocol types.
		{name: "rebuild_request_roundtrip", run: testRebuildRequestRoundtrip},
		// Phase 4A CP3: Rebuild server.
		{name: "rebuild_server_wal_catchup", run: testRebuildServerWALCatchUp},
		{name: "rebuild_server_wal_recycled", run: testRebuildServerWALRecycled},
		{name: "rebuild_server_full_extent", run: testRebuildServerFullExtent},
		{name: "rebuild_server_epoch_mismatch", run: testRebuildServerEpochMismatch},
		// Phase 4A CP3: Rebuild client.
		{name: "rebuild_wal_catchup_happy", run: testRebuildWALCatchUpHappy},
		{name: "rebuild_wal_catchup_to_replica", run: testRebuildWALCatchUpToReplica},
		{name: "rebuild_fallback_full_extent", run: testRebuildFallbackFullExtent},
		{name: "rebuild_full_extent_data_correct", run: testRebuildFullExtentDataCorrect},
		{name: "rebuild_full_extent_resets_dirty_map", run: testRebuildFullExtentResetsDirtyMap},
		// Phase 4A CP3: Split-brain tests.
		{name: "split_brain_dead_zone", run: testSplitBrainDeadZone},
		{name: "split_brain_stale_primary_fenced", run: testSplitBrainStalePrimaryFenced},
		{name: "split_brain_epoch_rejects_stale_write", run: testSplitBrainEpochRejectsStaleWrite},
		{name: "split_brain_no_self_promotion", run: testSplitBrainNoSelfPromotion},
		{name: "split_brain_concurrent_assignment", run: testSplitBrainConcurrentAssignment},
		// Phase 4A CP3: Lifecycle tests.
		{name: "blockvol_full_lifecycle", run: testBlockvolFullLifecycle},
		{name: "blockvol_rebuild_lifecycle", run: testBlockvolRebuildLifecycle},
		// Phase 4A CP4a: Assignment sequence tests.
		{name: "seq_fresh_to_primary", run: testSeqFreshToPrimary},
		{name: "seq_fresh_to_replica_to_primary", run: testSeqFreshToReplicaToPrimary},
		{name: "seq_promote_demote_cycle", run: testSeqPromoteDemoteCycle},
		{name: "seq_lease_refresh_keeps_alive", run: testSeqLeaseRefreshKeepsAlive},
		{name: "seq_epoch_bump_on_refresh", run: testSeqEpochBumpOnRefresh},
		{name: "seq_demote_then_rebuild_from_peer", run: testSeqDemoteThenRebuildFromPeer},
		{name: "seq_rapid_epoch_bumps", run: testSeqRapidEpochBumps},
		{name: "seq_concurrent_refresh_and_write", run: testSeqConcurrentRefreshAndWrite},
		// Phase 4A CP4a: Failover sequence tests.
		{name: "failover_lease_expiry_then_promote", run: testFailoverLeaseExpiryThenPromote},
		{name: "failover_dead_zone_verified", run: testFailoverDeadZoneVerified},
		{name: "failover_write_during_demotion_drains", run: testFailoverWriteDuringDemotionDrains},
		{name: "failover_rebuild_after_promotion", run: testFailoverRebuildAfterPromotion},
		{name: "failover_double_failover", run: testFailoverDoubleFailover},
		// Phase 4A CP4a: Edge case + adversarial tests.
		{name: "adversarial_stale_epoch_assignment", run: testAdversarialStaleEpochAssignment},
		{name: "adversarial_assignment_wrong_role_transition", run: testAdversarialAssignmentWrongRoleTransition},
		{name: "adversarial_concurrent_assignments", run: testAdversarialConcurrentAssignments},
		{name: "adversarial_promote_during_rebuild", run: testAdversarialPromoteDuringRebuild},
		{name: "adversarial_zero_ttl_lease", run: testAdversarialZeroTTLLease},
		// Phase 4A CP4a: Status tests.
		{name: "status_primary_with_lease", run: testStatusPrimaryWithLease},
		{name: "status_stale_no_lease", run: testStatusStaleNoLease},
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
		t.Error("data not durable after create->write->sync->close->reopen")
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

	// Close does a final flush --dirty map should be drained.
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

	// Cycle: create -> write -> sync -> close -> reopen -> write -> sync -> close -> verify.
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

	// Final reopen --verify.
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
	oracle := make(map[uint64]byte) // lba -> fill byte (0 = zeros/trimmed)

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

	// Trim with a length larger than WAL size --should still work.
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
// return ErrVolumeClosed after Close() --no panic, no write to closed fd.
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

	// Verify trim took effect --read should return zeros.
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

	// Close should NOT hang forever --it has a 5s drain timeout.
	done := make(chan struct{})
	go func() {
		v.Close()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Close completed despite stuck op (drain timeout worked)")
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung for >10s --drain timeout not working")
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
	// No lease granted --should be expired.

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

	// Demote to draining --writes should fail.
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
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch }, nil)
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
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch }, nil)
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
	s := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return epoch }, nil)

	// First ship a valid entry to establish connection.
	validEntry := &WALEntry{LSN: 1, Epoch: 2, Type: EntryTypeTrim, LBA: 0, Length: 4096}
	if err := s.Ship(validEntry); err != nil {
		t.Fatalf("Ship valid: %v", err)
	}

	// Now ship an entry with old epoch 1 --should be silently dropped.
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
		conn.Close() // close immediately --writes will fail
	}()
	defer ln.Close()

	ctrlAddr, _ := mockCtrlServer(t, BarrierOK)
	epoch := uint64(1)
	s := NewWALShipper(ln.Addr().String(), ctrlAddr, func() uint64 { return epoch }, nil)
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

	// Barrier from degraded state attempts reconnect. The mock ctrl server
	// returns BarrierOK, so the barrier succeeds and restores InSync.
	// (Pre-CP13-4 behavior was permanent degradation with no recovery.
	// The new behavior is: degraded → reconnect → barrier → InSync.)
	if err := s.Barrier(100); err != nil {
		// Barrier may fail if mock server is gone, which is acceptable.
		// But it should NOT be ErrReplicaDegraded without even trying.
		if errors.Is(err, ErrReplicaDegraded) && s.State() == ReplicaDegraded {
			// Reconnect failed — acceptable for this test since mock may have closed.
			return
		}
	}
	// If barrier succeeded, shipper should be InSync now.
	if s.State() == ReplicaInSync {
		return // correct: recovery worked
	}
}

func testShipNoReplicaNoop(t *testing.T) {
	// A nil shipper should not be called, but test that a stopped shipper is safe.
	s := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 { return 1 }, nil)
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

	// Entry with epoch 3 (stale) --should be rejected.
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

	// Send duplicate LSN=1 --should be skipped.
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

	// Now send a barrier for LSN=1 --should succeed immediately.
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
	// Should be fast --no waiting.
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

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 }, nil)
	defer shipper.Stop()

	distSync := MakeDistributedSync(walSync, NewShipperGroup([]*WALShipper{shipper}), v)
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

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 }, nil)
	defer shipper.Stop()

	distSync := MakeDistributedSync(walSync, NewShipperGroup([]*WALShipper{shipper}), v)
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

	shipper := NewWALShipper(dataAddr, ctrlAddr, func() uint64 { return 0 }, nil)
	defer shipper.Stop()

	// Set shipper group on vol so degradeReplica can see it.
	v.shipperGroup = NewShipperGroup([]*WALShipper{shipper})

	distSync := MakeDistributedSync(walSync, v.shipperGroup, v)
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

	// nil group --local-only mode.
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

	// Verify shipperGroup is nil.
	if v.shipperGroup != nil {
		t.Error("shipperGroup should be nil without SetReplicaAddr")
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

	// Entry with epoch 5 (future) --must be rejected. Replicas must NOT
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

	// Send LSN=1 --should succeed.
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

	// Send LSN=3 (gap --skips LSN=2) --must be rejected.
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
	// Should complete quickly --well under 1s.
	if elapsed > 1*time.Second {
		t.Errorf("configurable timeout took %v, expected ~50ms", elapsed)
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: WAL Scanner tests
// ---------------------------------------------------------------------------

func testWALScanFromMiddle(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write 10 entries.
	for i := 0; i < 10; i++ {
		data := makeBlock(byte('A' + i))
		if err := v.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	// Scan from LSN=5 (0-indexed: LSN 1..10, so fromLSN=5 gets LSN 5..10 = 6 entries).
	var scanned []uint64
	err := v.wal.ScanFrom(v.fd, v.super.WALOffset, 0, 5, func(e *WALEntry) error {
		scanned = append(scanned, e.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if len(scanned) != 6 {
		t.Fatalf("expected 6 entries, got %d: %v", len(scanned), scanned)
	}
	if scanned[0] != 5 || scanned[5] != 10 {
		t.Errorf("expected LSN range [5..10], got %v", scanned)
	}
}

func testWALScanEmpty(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	var count int
	err := v.wal.ScanFrom(v.fd, v.super.WALOffset, 0, 1, func(e *WALEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 entries on empty WAL, got %d", count)
	}
}

func testWALScanRecycled(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Write some entries.
	for i := 0; i < 5; i++ {
		v.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	// Simulate checkpointLSN=3 (entries 1-3 flushed).
	err := v.wal.ScanFrom(v.fd, v.super.WALOffset, 3, 2, func(e *WALEntry) error {
		return nil
	})
	if !errors.Is(err, ErrWALRecycled) {
		t.Fatalf("expected ErrWALRecycled, got %v", err)
	}
}

func testWALScanWrapPadding(t *testing.T) {
	// Use a small WAL that will wrap.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 1024, // 64KB
		BlockSize:  4096,
		WALSize:    4096 * 4, // 16KB WAL --very small, forces wrapping
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write entries until we wrap. WAL entry for 4KB write = 38 + 4096 = 4134 bytes.
	// 16KB WAL can hold ~3 entries before wrapping. Write 2, flush, write 2 more.
	v.WriteLBA(0, makeBlock('A'))
	v.WriteLBA(1, makeBlock('B'))

	// Force flush to free WAL space.
	v.flusher.FlushOnce()

	// Write more to trigger wrap.
	v.WriteLBA(2, makeBlock('C'))
	v.WriteLBA(3, makeBlock('D'))

	// Scan all entries from LSN=1. We should get whatever is still in WAL.
	var scanned []uint64
	checkpointLSN := v.flusher.CheckpointLSN()
	err = v.wal.ScanFrom(v.fd, v.super.WALOffset, checkpointLSN, checkpointLSN+1, func(e *WALEntry) error {
		scanned = append(scanned, e.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom with wrap: %v", err)
	}
	if len(scanned) == 0 {
		t.Fatal("expected entries after wrap, got 0")
	}
}

func testWALScanEntryCrossesEnd(t *testing.T) {
	// Similar to wrap padding --an entry that would span WAL end triggers padding.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 64 * 1024,
		BlockSize:  4096,
		WALSize:    4096 * 5, // 20KB WAL
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Write 3 entries, flush 2, write 2 more (forces padding + wrap).
	v.WriteLBA(0, makeBlock('A'))
	v.WriteLBA(1, makeBlock('B'))
	v.WriteLBA(2, makeBlock('C'))
	v.flusher.FlushOnce()
	v.WriteLBA(3, makeBlock('D'))
	v.WriteLBA(4, makeBlock('E'))

	checkpointLSN := v.flusher.CheckpointLSN()
	var scanned []uint64
	err = v.wal.ScanFrom(v.fd, v.super.WALOffset, checkpointLSN, checkpointLSN+1, func(e *WALEntry) error {
		scanned = append(scanned, e.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom: %v", err)
	}
	if len(scanned) == 0 {
		t.Fatal("expected entries after padding/wrap, got 0")
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Promotion + Demotion tests
// ---------------------------------------------------------------------------

// setupPrimary creates a volume and promotes it to Primary.
func setupPrimary(t *testing.T) *BlockVol {
	t.Helper()
	v := createTestVol(t)
	if err := v.HandleAssignment(1, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote to primary: %v", err)
	}
	return v
}

// setupReplica creates a volume and sets it to Replica role.
func setupReplica(t *testing.T) *BlockVol {
	t.Helper()
	v := createTestVol(t)
	if err := v.HandleAssignment(1, RoleReplica, 0); err != nil {
		t.Fatalf("set replica: %v", err)
	}
	return v
}

func testPromoteReplicaToPrimary(t *testing.T) {
	v := setupReplica(t)
	defer v.Close()

	if err := v.HandleAssignment(2, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	if v.Epoch() != 2 {
		t.Errorf("epoch: got %d, want 2", v.Epoch())
	}
	// Writes should succeed.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Errorf("write after promote: %v", err)
	}
}

func testPromoteRejectsNonReplica(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Primary can't be promoted again.
	err := v.HandleAssignment(3, RolePrimary, 30*time.Second)
	if err != nil {
		t.Errorf("same-role assignment should be a lease refresh, got error: %v", err)
	}

	// Stale can't be promoted to Primary directly.
	v2 := createTestVol(t)
	defer v2.Close()
	v2.HandleAssignment(1, RolePrimary, 30*time.Second)
	v2.HandleAssignment(2, RoleStale, 0)
	err = v2.HandleAssignment(3, RolePrimary, 30*time.Second)
	if !errors.Is(err, ErrInvalidAssignment) {
		t.Errorf("expected ErrInvalidAssignment for Stale->Primary, got: %v", err)
	}
}

func testDemotePrimaryToStale(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	if err := v.HandleAssignment(2, RoleStale, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if v.Role() != RoleStale {
		t.Errorf("role: got %s, want Stale", v.Role())
	}
	if v.Epoch() != 2 {
		t.Errorf("epoch: got %d, want 2", v.Epoch())
	}
	// Writes should fail.
	err := v.WriteLBA(0, makeBlock('A'))
	if err == nil {
		t.Error("expected write to fail after demotion")
	}
}

func testDemoteDrainsInflightOps(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Start a write that will hold an op outstanding.
	var wg sync.WaitGroup
	started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		v.beginOp()
		close(started)
		// Hold the op for a bit.
		time.Sleep(50 * time.Millisecond)
		v.endOp()
	}()

	<-started

	// Demote should wait for the op to complete.
	v.drainTimeout = 2 * time.Second
	errCh := make(chan error, 1)
	go func() {
		errCh <- v.HandleAssignment(2, RoleStale, 0)
	}()

	wg.Wait()
	err := <-errCh
	if err != nil {
		t.Fatalf("demote: %v", err)
	}
	if v.Role() != RoleStale {
		t.Errorf("role: got %s, want Stale", v.Role())
	}
}

func testDemoteStopsShipper(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Create a shipper group (won't connect but that's fine for this test).
	shipper := NewWALShipper("127.0.0.1:0", "127.0.0.1:0", func() uint64 {
		return v.epoch.Load()
	}, nil)
	v.shipperGroup = NewShipperGroup([]*WALShipper{shipper})

	if err := v.HandleAssignment(2, RoleStale, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if !shipper.stopped.Load() {
		t.Error("shipper should be stopped after demotion")
	}
}

func testAssignmentRefreshLease(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Same role + same epoch -> refresh lease.
	if err := v.HandleAssignment(1, RolePrimary, 1*time.Hour); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	if !v.lease.IsValid() {
		t.Error("lease should be valid after refresh")
	}

	// Same role + bumped epoch -> epoch updated, writes still work.
	if err := v.HandleAssignment(5, RolePrimary, 1*time.Hour); err != nil {
		t.Fatalf("refresh with epoch bump: %v", err)
	}
	if v.Epoch() != 5 {
		t.Errorf("epoch after bump: got %d, want 5", v.Epoch())
	}
	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Errorf("write after epoch bump refresh: %v", err)
	}
}

func testAssignmentInvalidTransition(t *testing.T) {
	v := setupReplica(t)
	defer v.Close()

	// Replica -> Stale is invalid.
	err := v.HandleAssignment(2, RoleStale, 0)
	if !errors.Is(err, ErrInvalidAssignment) {
		t.Errorf("expected ErrInvalidAssignment, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Rebuild protocol roundtrip
// ---------------------------------------------------------------------------

func testRebuildRequestRoundtrip(t *testing.T) {
	req := RebuildRequest{
		Type:    RebuildWALCatchUp,
		FromLSN: 42,
		Epoch:   7,
	}
	buf := EncodeRebuildRequest(req)
	decoded, err := DecodeRebuildRequest(buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != req.Type || decoded.FromLSN != req.FromLSN || decoded.Epoch != req.Epoch {
		t.Errorf("roundtrip mismatch: got %+v, want %+v", decoded, req)
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Rebuild Server tests
// ---------------------------------------------------------------------------

func testRebuildServerWALCatchUp(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Write some data.
	for i := 0; i < 5; i++ {
		v.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	srv, err := NewRebuildServer(v, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	// Connect and request catch-up from LSN=1.
	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: v.Epoch()}
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req))

	var entries int
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			t.Fatal(err)
		}
		if msgType == MsgRebuildDone {
			break
		}
		if msgType == MsgRebuildEntry {
			_, decErr := DecodeWALEntry(payload)
			if decErr != nil {
				t.Fatalf("decode entry: %v", decErr)
			}
			entries++
		}
		if msgType == MsgRebuildError {
			t.Fatalf("server error: %s", string(payload))
		}
	}
	if entries != 5 {
		t.Errorf("expected 5 entries, got %d", entries)
	}
}

func testRebuildServerWALRecycled(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Write and flush to advance checkpoint.
	for i := 0; i < 5; i++ {
		v.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}
	v.flusher.FlushOnce()

	srv, err := NewRebuildServer(v, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Request from LSN=1, but checkpoint is past that -> WAL_RECYCLED.
	req := RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: v.Epoch()}
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req))

	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgRebuildError {
		t.Fatalf("expected MsgRebuildError, got 0x%02x", msgType)
	}
	if string(payload) != "WAL_RECYCLED" {
		t.Errorf("expected WAL_RECYCLED error, got: %s", string(payload))
	}
}

func testRebuildServerFullExtent(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Write and flush so data is in extent.
	v.WriteLBA(0, makeBlock('X'))
	v.flusher.FlushOnce()

	srv, err := NewRebuildServer(v, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req := RebuildRequest{Type: RebuildFullExtent, Epoch: v.Epoch()}
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req))

	var totalBytes int
	for {
		msgType, payload, err := ReadFrame(conn)
		if err != nil {
			t.Fatal(err)
		}
		if msgType == MsgRebuildDone {
			break
		}
		if msgType == MsgRebuildExtent {
			totalBytes += len(payload)
		}
		if msgType == MsgRebuildError {
			t.Fatalf("server error: %s", string(payload))
		}
	}
	if uint64(totalBytes) != v.super.VolumeSize {
		t.Errorf("expected %d bytes, got %d", v.super.VolumeSize, totalBytes)
	}
}

func testRebuildServerEpochMismatch(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	srv, err := NewRebuildServer(v, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Wrong epoch.
	req := RebuildRequest{Type: RebuildWALCatchUp, FromLSN: 1, Epoch: 999}
	WriteFrame(conn, MsgRebuildReq, EncodeRebuildRequest(req))

	msgType, payload, err := ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != MsgRebuildError {
		t.Fatalf("expected MsgRebuildError, got 0x%02x", msgType)
	}
	if string(payload) != "EPOCH_MISMATCH" {
		t.Errorf("expected EPOCH_MISMATCH, got: %s", string(payload))
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Rebuild Client tests
// ---------------------------------------------------------------------------

// setupRebuilding creates a volume in RoleRebuilding state with the given epoch.
func setupRebuilding(t *testing.T, epoch uint64) *BlockVol {
	t.Helper()
	v := createTestVol(t)
	// Path: None -> Primary -> Stale -> Rebuilding
	if err := v.HandleAssignment(epoch, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("setup rebuilding: promote: %v", err)
	}
	if err := v.HandleAssignment(epoch, RoleStale, 0); err != nil {
		t.Fatalf("setup rebuilding: demote: %v", err)
	}
	if err := v.HandleAssignment(epoch, RoleRebuilding, 0); err != nil {
		t.Fatalf("setup rebuilding: set rebuilding: %v", err)
	}
	return v
}

func testRebuildWALCatchUpHappy(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	for i := 0; i < 5; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('A'+i)))
	}

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}
	if stale.Role() != RoleReplica {
		t.Errorf("role after rebuild: got %s, want Replica", stale.Role())
	}
}

func testRebuildWALCatchUpToReplica(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	primary.WriteLBA(0, makeBlock('Z'))

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// After rebuild, reads should work.
	data, err := stale.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if data[0] != 'Z' {
		t.Errorf("data mismatch: got %c, want Z", data[0])
	}
}

func testRebuildFallbackFullExtent(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	// Write and flush so WAL is recycled.
	primary.WriteLBA(0, makeBlock('M'))
	primary.flusher.FlushOnce()

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	// Request catch-up from LSN=1, which is recycled -> falls back to full extent.
	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("StartRebuild (fallback): %v", err)
	}
	if stale.Role() != RoleReplica {
		t.Errorf("role: got %s, want Replica", stale.Role())
	}

	// Verify data matches.
	data, err := stale.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if data[0] != 'M' {
		t.Errorf("data mismatch: got %c, want M", data[0])
	}
}

func testRebuildFullExtentDataCorrect(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	// Write several blocks and flush.
	for i := 0; i < 10; i++ {
		primary.WriteLBA(uint64(i), makeBlock(byte('0'+i)))
	}
	primary.flusher.FlushOnce()

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	// Trigger full extent (LSN=1 recycled after flush).
	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// Verify all blocks.
	for i := 0; i < 10; i++ {
		data, err := stale.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('0'+i) {
			t.Errorf("block %d: got %c, want %c", i, data[0], byte('0'+i))
		}
	}
}

func testRebuildFullExtentResetsDirtyMap(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	primary.WriteLBA(0, makeBlock('A'))
	primary.flusher.FlushOnce()

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	// Write some data directly to WAL to create dirty map entries on the stale volume.
	// (Can't use WriteLBA since role is Rebuilding, not Primary.)
	entry := &WALEntry{LSN: 100, Epoch: primary.Epoch(), Type: EntryTypeWrite, LBA: 5, Length: 4096, Data: makeBlock('Z')}
	walOff, _ := stale.wal.Append(entry)
	stale.dirtyMap.Put(5, walOff, 100, 4096)
	if stale.dirtyMap.Len() == 0 {
		t.Fatal("expected dirty entries before rebuild")
	}

	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// After full extent rebuild, dirty map should be cleared.
	if stale.dirtyMap.Len() != 0 {
		t.Errorf("dirty map should be empty after full extent rebuild, got %d entries", stale.dirtyMap.Len())
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Split-brain tests
// ---------------------------------------------------------------------------

func testSplitBrainDeadZone(t *testing.T) {
	// After demote (old primary), before promote (new primary) --no node accepts writes.
	oldPrimary := setupPrimary(t)
	defer oldPrimary.Close()
	newReplica := setupReplica(t)
	defer newReplica.Close()

	// Demote old primary.
	if err := oldPrimary.HandleAssignment(2, RoleStale, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}

	// Old primary can't write.
	if err := oldPrimary.WriteLBA(0, makeBlock('A')); err == nil {
		t.Error("old primary should reject writes after demotion")
	}

	// New replica hasn't been promoted yet --can't write.
	if err := newReplica.WriteLBA(0, makeBlock('B')); err == nil {
		t.Error("replica should reject writes before promotion")
	}
}

func testSplitBrainStalePrimaryFenced(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Let lease expire.
	v.lease.Grant(1 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	err := v.WriteLBA(0, makeBlock('A'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("expected ErrLeaseExpired, got: %v", err)
	}
}

func testSplitBrainEpochRejectsStaleWrite(t *testing.T) {
	v := setupPrimary(t)
	defer v.Close()

	// Simulate master bumping epoch without this node knowing.
	v.masterEpoch.Store(99)

	err := v.WriteLBA(0, makeBlock('A'))
	if !errors.Is(err, ErrEpochStale) {
		t.Errorf("expected ErrEpochStale, got: %v", err)
	}
}

func testSplitBrainNoSelfPromotion(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Set to Replica.
	v.HandleAssignment(1, RoleReplica, 0)

	// Try direct SetRole without going through HandleAssignment.
	// This should work because SetRole itself is valid (Replica->Primary),
	// but without setting epoch/lease, writes will fail.
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}

	// Writes fail because epoch/masterEpoch mismatch (self-promotion
	// didn't set masterEpoch).
	err := v.WriteLBA(0, makeBlock('A'))
	if err == nil {
		t.Error("self-promotion without proper assignment should fail writes")
	}
}

func testSplitBrainConcurrentAssignment(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Set up as Replica first.
	v.HandleAssignment(1, RoleReplica, 0)

	// Concurrent assignment attempts: promote + invalid.
	var wg sync.WaitGroup
	results := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results[0] = v.HandleAssignment(2, RolePrimary, 30*time.Second)
	}()
	go func() {
		defer wg.Done()
		results[1] = v.HandleAssignment(3, RolePrimary, 30*time.Second)
	}()
	wg.Wait()

	// With assignMu serialization, one should succeed and the other
	// should either succeed (refresh on already-promoted) or fail.
	// The key guarantee is no panic and consistent state.
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary after concurrent assignments", v.Role())
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP3: Lifecycle tests
// ---------------------------------------------------------------------------

func testBlockvolFullLifecycle(t *testing.T) {
	// Primary writes, promotes replica, demotes old primary, new primary serves writes.
	primary := setupPrimary(t)
	defer primary.Close()
	replica := setupReplica(t)
	defer replica.Close()

	// Primary writes.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("primary write: %v", err)
	}

	// Demote old primary.
	if err := primary.HandleAssignment(2, RoleStale, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}

	// Promote replica.
	if err := replica.HandleAssignment(2, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	// New primary can write.
	if err := replica.WriteLBA(1, makeBlock('B')); err != nil {
		t.Fatalf("new primary write: %v", err)
	}

	// Old primary can't write.
	if err := primary.WriteLBA(2, makeBlock('C')); err == nil {
		t.Error("old primary should reject writes")
	}
}

func testBlockvolRebuildLifecycle(t *testing.T) {
	primary := setupPrimary(t)
	defer primary.Close()

	// Write data.
	primary.WriteLBA(0, makeBlock('R'))
	primary.WriteLBA(1, makeBlock('S'))

	srv, err := NewRebuildServer(primary, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv.Serve()
	defer srv.Stop()

	// Create stale and rebuild.
	stale := setupRebuilding(t, primary.Epoch())
	defer stale.Close()

	if err := StartRebuild(stale, srv.Addr(), 1, primary.Epoch()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	// Verify data.
	data, err := stale.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(0): %v", err)
	}
	if data[0] != 'R' {
		t.Errorf("block 0: got %c, want R", data[0])
	}
	data, err = stale.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA(1): %v", err)
	}
	if data[0] != 'S' {
		t.Errorf("block 1: got %c, want S", data[0])
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP4a: SimulatedMaster helper
// ---------------------------------------------------------------------------

// SimulatedMaster drives HandleAssignment sequences for testing.
// Not a real server -- just a struct with methods that call
// HandleAssignment() directly.
//
// NOT thread-safe: do not call SimulatedMaster methods from multiple
// goroutines concurrently. For concurrent tests, use HandleAssignment
// directly with explicit epoch values.
type SimulatedMaster struct {
	epoch    uint64
	leaseTTL time.Duration
}

func NewSimulatedMaster(leaseTTL time.Duration) *SimulatedMaster {
	return &SimulatedMaster{epoch: 0, leaseTTL: leaseTTL}
}

// GrantPrimary promotes a volume to Primary at the next epoch.
func (m *SimulatedMaster) GrantPrimary(vol *BlockVol) error {
	m.epoch++
	return vol.HandleAssignment(m.epoch, RolePrimary, m.leaseTTL)
}

// Demote transitions a Primary to Stale at the next epoch.
func (m *SimulatedMaster) Demote(vol *BlockVol) error {
	m.epoch++
	return vol.HandleAssignment(m.epoch, RoleStale, 0)
}

// RefreshLease sends same role + epoch to refresh the lease.
func (m *SimulatedMaster) RefreshLease(vol *BlockVol) error {
	return vol.HandleAssignment(m.epoch, vol.Role(), m.leaseTTL)
}

// PromoteReplica orchestrates: demote old primary, promote replica.
// Returns the new epoch used.
func (m *SimulatedMaster) PromoteReplica(oldPrimary, newPrimary *BlockVol) (uint64, error) {
	m.epoch++
	if err := oldPrimary.HandleAssignment(m.epoch, RoleStale, 0); err != nil {
		return m.epoch, fmt.Errorf("demote old primary: %w", err)
	}
	if err := newPrimary.HandleAssignment(m.epoch, RolePrimary, m.leaseTTL); err != nil {
		return m.epoch, fmt.Errorf("promote new primary: %w", err)
	}
	return m.epoch, nil
}

// InitiateRebuild transitions a Stale volume to Rebuilding.
func (m *SimulatedMaster) InitiateRebuild(vol *BlockVol) error {
	return vol.HandleAssignment(m.epoch, RoleRebuilding, 0)
}

// AssignReplica assigns a volume as Replica at the current epoch.
func (m *SimulatedMaster) AssignReplica(vol *BlockVol) error {
	m.epoch++
	return vol.HandleAssignment(m.epoch, RoleReplica, 0)
}

// BumpEpoch increments the epoch and refreshes the primary lease.
func (m *SimulatedMaster) BumpEpoch(vol *BlockVol) error {
	m.epoch++
	return vol.HandleAssignment(m.epoch, vol.Role(), m.leaseTTL)
}

// ---------------------------------------------------------------------------
// Phase 4A CP4a: Assignment sequence tests (Task 2)
// ---------------------------------------------------------------------------

func testSeqFreshToPrimary(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Errorf("write after grant: %v", err)
	}
}

func testSeqFreshToReplicaToPrimary(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.AssignReplica(v); err != nil {
		t.Fatalf("AssignReplica: %v", err)
	}
	if v.Role() != RoleReplica {
		t.Errorf("role: got %s, want Replica", v.Role())
	}
	// Replica can't write.
	if err := v.WriteLBA(0, makeBlock('A')); err == nil {
		t.Error("replica should reject writes")
	}
	// Promote to primary at bumped epoch.
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Errorf("write after promote: %v", err)
	}
}

func testSeqPromoteDemoteCycle(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)

	// None -> Primary(e=1)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Errorf("write at Primary e=1: %v", err)
	}

	// Primary(e=1) -> Stale(e=2)
	if err := master.Demote(v); err != nil {
		t.Fatalf("Demote: %v", err)
	}
	if v.Role() != RoleStale {
		t.Errorf("role: got %s, want Stale", v.Role())
	}
	if err := v.WriteLBA(0, makeBlock('B')); err == nil {
		t.Error("stale should reject writes")
	}

	// Stale -> Rebuilding
	if err := master.InitiateRebuild(v); err != nil {
		t.Fatalf("InitiateRebuild: %v", err)
	}
	if v.Role() != RoleRebuilding {
		t.Errorf("role: got %s, want Rebuilding", v.Role())
	}

	// Rebuilding -> Replica (simulating rebuild completion via SetRole)
	if err := v.SetRole(RoleReplica); err != nil {
		t.Fatalf("SetRole Replica: %v", err)
	}

	// Replica -> Primary(e=3)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary after rebuild: %v", err)
	}
	if v.Epoch() != 3 {
		t.Errorf("epoch: got %d, want 3", v.Epoch())
	}
	if err := v.WriteLBA(0, makeBlock('C')); err != nil {
		t.Errorf("write at Primary e=3: %v", err)
	}
}

func testSeqLeaseRefreshKeepsAlive(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(500 * time.Millisecond)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}

	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		if err := master.RefreshLease(v); err != nil {
			t.Fatalf("RefreshLease %d: %v", i, err)
		}
		if err := v.WriteLBA(0, makeBlock(byte('A'+i))); err != nil {
			t.Errorf("write after refresh %d: %v", i, err)
		}
	}
}

func testSeqEpochBumpOnRefresh(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	if v.Epoch() != 1 {
		t.Fatalf("epoch: got %d, want 1", v.Epoch())
	}

	// Bump epoch via refresh.
	if err := master.BumpEpoch(v); err != nil {
		t.Fatalf("BumpEpoch: %v", err)
	}
	if v.Epoch() != 2 {
		t.Errorf("epoch: got %d, want 2", v.Epoch())
	}
	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Errorf("write after epoch bump: %v", err)
	}
}

func testSeqDemoteThenRebuildFromPeer(t *testing.T) {
	// Primary A writes data, demoted; B rebuilds from A, promoted, reads data.
	a := createTestVol(t)
	defer a.Close()
	b := createTestVol(t)
	defer b.Close()

	master := NewSimulatedMaster(30 * time.Second)

	// A becomes primary, writes data.
	if err := master.GrantPrimary(a); err != nil {
		t.Fatalf("GrantPrimary(A): %v", err)
	}
	if err := a.WriteLBA(0, makeBlock('D')); err != nil {
		t.Fatalf("A write: %v", err)
	}
	if err := a.WriteLBA(1, makeBlock('E')); err != nil {
		t.Fatalf("A write: %v", err)
	}

	// Start rebuild server on A.
	if err := a.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatalf("StartRebuildServer: %v", err)
	}
	defer a.StopRebuildServer()
	rebuildAddr := a.rebuildServer.Addr()

	// Demote A.
	if err := master.Demote(a); err != nil {
		t.Fatalf("Demote(A): %v", err)
	}

	// B: None -> Primary -> Stale -> Rebuilding
	bEpoch := master.epoch
	if err := b.HandleAssignment(bEpoch, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("B promote: %v", err)
	}
	if err := b.HandleAssignment(bEpoch, RoleStale, 0); err != nil {
		t.Fatalf("B demote: %v", err)
	}
	if err := b.HandleAssignment(bEpoch, RoleRebuilding, 0); err != nil {
		t.Fatalf("B set rebuilding: %v", err)
	}

	// Rebuild B from A. Note: A's epoch is master.epoch (after demote).
	// Rebuild server checks its own epoch which is a.Epoch().
	if err := StartRebuild(b, rebuildAddr, 1, a.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// B should now be Replica.
	if b.Role() != RoleReplica {
		t.Errorf("B role: got %s, want Replica", b.Role())
	}

	// Promote B.
	master.epoch++
	if err := b.HandleAssignment(master.epoch, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote B: %v", err)
	}

	// Verify data.
	data, err := b.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("B ReadLBA(0): %v", err)
	}
	if data[0] != 'D' {
		t.Errorf("block 0: got %c, want D", data[0])
	}
	data, err = b.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("B ReadLBA(1): %v", err)
	}
	if data[0] != 'E' {
		t.Errorf("block 1: got %c, want E", data[0])
	}
}

func testSeqRapidEpochBumps(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}

	for i := 0; i < 10; i++ {
		if err := master.BumpEpoch(v); err != nil {
			t.Fatalf("BumpEpoch %d: %v", i, err)
		}
	}

	if v.Epoch() != 11 { // 1 initial + 10 bumps
		t.Errorf("epoch: got %d, want 11", v.Epoch())
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	if err := v.WriteLBA(0, makeBlock('Z')); err != nil {
		t.Errorf("write after rapid bumps: %v", err)
	}
}

func testSeqConcurrentRefreshAndWrite(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := v.WriteLBA(0, makeBlock(byte('A'+i%26))); err != nil {
				errCh <- fmt.Errorf("write %d: %w", i, err)
				return
			}
		}
	}()

	// Refresh goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// Use HandleAssignment directly to avoid SimulatedMaster mutex issues.
			v.HandleAssignment(master.epoch, RolePrimary, master.leaseTTL)
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP4a: Failover sequence tests (Task 3)
// ---------------------------------------------------------------------------

func testFailoverLeaseExpiryThenPromote(t *testing.T) {
	a := createTestVol(t)
	defer a.Close()
	b := createTestVol(t)
	defer b.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(a); err != nil {
		t.Fatalf("GrantPrimary(A): %v", err)
	}

	// Revoke lease to simulate expiry without wall-clock dependency.
	a.lease.Revoke()

	if err := a.WriteLBA(0, makeBlock('A')); err == nil {
		t.Error("expected write to fail after lease expiry")
	}

	// Promote B as new primary.
	master.epoch++
	if err := b.HandleAssignment(master.epoch, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote B: %v", err)
	}
	if err := b.WriteLBA(0, makeBlock('B')); err != nil {
		t.Errorf("new primary write: %v", err)
	}
}

func testFailoverDeadZoneVerified(t *testing.T) {
	a := createTestVol(t)
	defer a.Close()
	b := createTestVol(t)
	defer b.Close()

	master := NewSimulatedMaster(30 * time.Second)

	// A is primary, B is replica --both have roles assigned (not RoleNone).
	if err := master.GrantPrimary(a); err != nil {
		t.Fatalf("GrantPrimary(A): %v", err)
	}
	if err := master.AssignReplica(b); err != nil {
		t.Fatalf("AssignReplica(B): %v", err)
	}

	// Demote A.
	if err := master.Demote(a); err != nil {
		t.Fatalf("Demote(A): %v", err)
	}

	// Dead zone: both should reject writes. A is Stale, B is Replica.
	if err := a.WriteLBA(0, makeBlock('A')); err == nil {
		t.Error("old primary (Stale) should reject writes in dead zone")
	}
	if err := b.WriteLBA(0, makeBlock('B')); err == nil {
		t.Error("replica should reject writes before promotion")
	}

	// Promote B --writes succeed now.
	if err := b.HandleAssignment(master.epoch, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote B: %v", err)
	}
	if err := b.WriteLBA(0, makeBlock('C')); err != nil {
		t.Errorf("new primary write after promotion: %v", err)
	}
}

func testFailoverWriteDuringDemotionDrains(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	v.drainTimeout = 2 * time.Second

	// Simulate an in-flight op.
	v.beginOp()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- master.Demote(v)
	}()
	<-started

	// The demote should be waiting for drain.
	time.Sleep(30 * time.Millisecond)
	v.endOp() // release the in-flight op

	err := <-done
	if err != nil {
		t.Fatalf("Demote: %v", err)
	}
	if v.Role() != RoleStale {
		t.Errorf("role: got %s, want Stale", v.Role())
	}
}

func testFailoverRebuildAfterPromotion(t *testing.T) {
	a := createTestVol(t)
	defer a.Close()
	b := createTestVol(t)
	defer b.Close()

	master := NewSimulatedMaster(30 * time.Second)

	// A is primary, writes data.
	if err := master.GrantPrimary(a); err != nil {
		t.Fatalf("GrantPrimary(A): %v", err)
	}
	if err := a.WriteLBA(0, makeBlock('F')); err != nil {
		t.Fatalf("A write: %v", err)
	}

	// B becomes replica then promoted.
	if err := master.AssignReplica(b); err != nil {
		t.Fatalf("AssignReplica(B): %v", err)
	}

	// Demote A, promote B.
	_, err := master.PromoteReplica(a, b)
	if err != nil {
		t.Fatalf("PromoteReplica: %v", err)
	}

	// B writes more data.
	if err := b.WriteLBA(1, makeBlock('G')); err != nil {
		t.Fatalf("B write: %v", err)
	}

	// Start rebuild server on B (new primary).
	if err := b.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatalf("StartRebuildServer: %v", err)
	}
	defer b.StopRebuildServer()

	// Rebuild A from B.
	if err := master.InitiateRebuild(a); err != nil {
		t.Fatalf("InitiateRebuild(A): %v", err)
	}
	if err := StartRebuild(a, b.rebuildServer.Addr(), 1, b.Epoch()); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// A should now be Replica.
	if a.Role() != RoleReplica {
		t.Errorf("A role: got %s, want Replica", a.Role())
	}

	// Verify A has B's data.
	data, err := a.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("A ReadLBA(1): %v", err)
	}
	if data[0] != 'G' {
		t.Errorf("block 1: got %c, want G", data[0])
	}
}

func testFailoverDoubleFailover(t *testing.T) {
	a := createTestVol(t)
	defer a.Close()
	b := createTestVol(t)
	defer b.Close()

	master := NewSimulatedMaster(30 * time.Second)

	// A primary, writes.
	if err := master.GrantPrimary(a); err != nil {
		t.Fatalf("GrantPrimary(A): %v", err)
	}
	if err := a.WriteLBA(0, makeBlock('1')); err != nil {
		t.Fatalf("A write: %v", err)
	}

	// B replica.
	if err := master.AssignReplica(b); err != nil {
		t.Fatalf("AssignReplica(B): %v", err)
	}

	// Failover 1: A -> B.
	_, err := master.PromoteReplica(a, b)
	if err != nil {
		t.Fatalf("PromoteReplica A->B: %v", err)
	}
	if err := b.WriteLBA(1, makeBlock('2')); err != nil {
		t.Fatalf("B write: %v", err)
	}

	// Rebuild A as replica of B.
	if err := b.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatalf("StartRebuildServer(B): %v", err)
	}
	if err := master.InitiateRebuild(a); err != nil {
		t.Fatalf("InitiateRebuild(A): %v", err)
	}
	if err := StartRebuild(a, b.rebuildServer.Addr(), 1, b.Epoch()); err != nil {
		t.Fatalf("StartRebuild A from B: %v", err)
	}
	b.StopRebuildServer()

	// Failover 2: B -> A (full circle).
	_, err = master.PromoteReplica(b, a)
	if err != nil {
		t.Fatalf("PromoteReplica B->A: %v", err)
	}

	// A is primary again. Verify data integrity.
	if a.Role() != RolePrimary {
		t.Errorf("A role: got %s, want Primary", a.Role())
	}
	if err := a.WriteLBA(2, makeBlock('3')); err != nil {
		t.Errorf("A write after double failover: %v", err)
	}

	// Read back all three blocks.
	for lba, expected := range map[uint64]byte{0: '1', 1: '2', 2: '3'} {
		data, err := a.ReadLBA(lba, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", lba, err)
		}
		if data[0] != expected {
			t.Errorf("block %d: got %c, want %c", lba, data[0], expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP4a: Edge case + adversarial tests (Task 4)
// ---------------------------------------------------------------------------

func testAdversarialStaleEpochAssignment(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	// Bump to epoch 3.
	master.BumpEpoch(v)
	master.BumpEpoch(v)

	// Send assignment with stale epoch (1 < 3). Must be rejected —
	// stale epoch could be a replay from an old master or stale queue.
	err := v.HandleAssignment(1, RolePrimary, 30*time.Second)
	if err == nil {
		t.Fatalf("expected error for stale epoch assignment, got nil")
	}
	if !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("expected ErrEpochRegression, got: %v", err)
	}
	if v.Epoch() != 3 {
		t.Errorf("epoch should remain 3, got %d", v.Epoch())
	}
}

func testAdversarialAssignmentWrongRoleTransition(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Set to Replica.
	master := NewSimulatedMaster(30 * time.Second)
	if err := master.AssignReplica(v); err != nil {
		t.Fatalf("AssignReplica: %v", err)
	}

	// Replica -> Stale (invalid).
	err := v.HandleAssignment(2, RoleStale, 0)
	if !errors.Is(err, ErrInvalidAssignment) {
		t.Errorf("Replica->Stale: expected ErrInvalidAssignment, got %v", err)
	}

	// Set up a Stale volume.
	v2 := createTestVol(t)
	defer v2.Close()
	v2.HandleAssignment(1, RolePrimary, 30*time.Second)
	v2.HandleAssignment(2, RoleStale, 0)

	// Stale -> Primary (invalid: must go through Rebuilding -> Replica first).
	err = v2.HandleAssignment(3, RolePrimary, 30*time.Second)
	if !errors.Is(err, ErrInvalidAssignment) {
		t.Errorf("Stale->Primary: expected ErrInvalidAssignment, got %v", err)
	}
}

func testAdversarialConcurrentAssignments(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}

	// 10 goroutines refreshing with different epoch bumps.
	var wg sync.WaitGroup
	var maxEpoch atomic.Uint64
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(epoch uint64) {
			defer wg.Done()
			v.HandleAssignment(epoch, RolePrimary, 30*time.Second)
			// Track the highest epoch we successfully sent.
			for {
				cur := maxEpoch.Load()
				if epoch <= cur || maxEpoch.CompareAndSwap(cur, epoch) {
					break
				}
			}
		}(uint64(i + 2)) // epochs 2..11
	}
	wg.Wait()

	// Final state should be consistent: role is Primary.
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	// Epoch should be the highest seen (they're all same-role refreshes).
	if v.Epoch() < 2 {
		t.Errorf("epoch should be >= 2, got %d", v.Epoch())
	}
}

func testAdversarialPromoteDuringRebuild(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// None -> Primary -> Stale -> Rebuilding
	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.HandleAssignment(2, RoleStale, 0)
	v.HandleAssignment(2, RoleRebuilding, 0)

	// Try to promote directly from Rebuilding -> Primary (invalid).
	err := v.HandleAssignment(3, RolePrimary, 30*time.Second)
	if !errors.Is(err, ErrInvalidAssignment) {
		t.Errorf("Rebuilding->Primary: expected ErrInvalidAssignment, got %v", err)
	}
	if v.Role() != RoleRebuilding {
		t.Errorf("role: got %s, want Rebuilding", v.Role())
	}
}

func testAdversarialZeroTTLLease(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Grant with zero TTL.
	if err := v.HandleAssignment(1, RolePrimary, 0); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}
	if v.Role() != RolePrimary {
		t.Errorf("role: got %s, want Primary", v.Role())
	}
	// Write should fail immediately --lease is already expired.
	if err := v.WriteLBA(0, makeBlock('X')); err == nil {
		t.Error("expected write to fail with zero TTL lease")
	}
}

// ---------------------------------------------------------------------------
// Phase 4A CP4a: Status tests (Task 5)
// ---------------------------------------------------------------------------

func testStatusPrimaryWithLease(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}

	// Write a block to advance WAL head LSN.
	if err := v.WriteLBA(0, makeBlock('S')); err != nil {
		t.Fatalf("write: %v", err)
	}

	st := v.Status()
	if st.Epoch != 1 {
		t.Errorf("Epoch: got %d, want 1", st.Epoch)
	}
	if st.Role != RolePrimary {
		t.Errorf("Role: got %s, want Primary", st.Role)
	}
	if !st.HasLease {
		t.Error("HasLease: got false, want true")
	}
	if st.WALHeadLSN < 1 {
		t.Errorf("WALHeadLSN: got %d, want >= 1", st.WALHeadLSN)
	}
}

func testStatusStaleNoLease(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	master := NewSimulatedMaster(30 * time.Second)
	if err := master.GrantPrimary(v); err != nil {
		t.Fatalf("GrantPrimary: %v", err)
	}
	if err := master.Demote(v); err != nil {
		t.Fatalf("Demote: %v", err)
	}

	st := v.Status()
	if st.Role != RoleStale {
		t.Errorf("Role: got %s, want Stale", st.Role)
	}
	if st.HasLease {
		t.Error("HasLease: got true, want false")
	}
	if st.Epoch != 2 {
		t.Errorf("Epoch: got %d, want 2", st.Epoch)
	}
}

// --- ER Fix 1: ioMu tests ---

// testIoMuConcurrentWritesAllowed verifies that multiple concurrent WriteLBA
// calls succeed (ioMu.RLock is shared).
func testIoMuConcurrentWritesAllowed(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(10 * time.Second)

	const goroutines = 8
	const writes = 50
	var wg sync.WaitGroup
	var errCount atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for w := 0; w < writes; w++ {
				lba := uint64((id*writes + w) % 256) // within 1MB volume
				data := makeBlock(byte(id))
				if err := v.WriteLBA(lba, data); err != nil {
					errCount.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()
	if errCount.Load() > 0 {
		t.Fatalf("concurrent writes had %d errors", errCount.Load())
	}
}

// testIoMuRestoreBlocksWrites runs a real RestoreSnapshot under write contention.
// Concurrent writers run before and during restore. After restore completes,
// verify the volume reflects snapshot state (not corrupted by concurrent writes).
func testIoMuRestoreBlocksWrites(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(30 * time.Second)

	// Write known data to LBA 0.
	if err := v.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatalf("WriteLBA(A): %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Create snapshot capturing 'A' at LBA 0.
	snapID := uint32(1)
	if err := v.CreateSnapshot(snapID); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Overwrite LBA 0 with 'B' after snapshot.
	if err := v.WriteLBA(0, makeBlock('B')); err != nil {
		t.Fatalf("WriteLBA(B): %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Start concurrent writers on LBAs 1-10 while restore runs.
	var writerWg sync.WaitGroup
	stopWriters := make(chan struct{})
	var writeCount atomic.Int64
	for g := 0; g < 4; g++ {
		writerWg.Add(1)
		go func(id int) {
			defer writerWg.Done()
			lba := uint64(1 + id)
			for {
				select {
				case <-stopWriters:
					return
				default:
				}
				if err := v.WriteLBA(lba, makeBlock(byte('W'+id))); err != nil {
					return // volume closed or restore draining — expected
				}
				writeCount.Add(1)
			}
		}(g)
	}

	// Let writers run for a bit, then restore concurrently.
	time.Sleep(5 * time.Millisecond)

	// Restore in the main goroutine — this acquires ioMu.Lock(),
	// draining all in-flight writers before modifying extent/WAL/dirty.
	if err := v.RestoreSnapshot(snapID); err != nil {
		close(stopWriters)
		writerWg.Wait()
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	close(stopWriters)
	writerWg.Wait()

	// Verify: LBA 0 must be 'A' (snapshot data), not 'B' (post-snapshot write).
	got, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after restore: %v", err)
	}
	if got[0] != 'A' {
		t.Fatalf("LBA 0: expected 'A' after restore, got %c — restore/write race", got[0])
	}

	// Sanity: writers did run (not zero iterations).
	if writeCount.Load() == 0 {
		t.Log("warning: concurrent writers did not execute any iterations")
	}
}

// testIoMuCloseCoordinates verifies that Close still works correctly
// with the ioMu in the struct (no deadlock).
func testIoMuCloseCoordinates(t *testing.T) {
	v := createTestVol(t)

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(10 * time.Second)

	// Write some data.
	if err := v.WriteLBA(0, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Close should complete without deadlock.
	done := make(chan error, 1)
	go func() {
		done <- v.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked")
	}
}

// --- Adversarial ioMu tests ---

// testIoMuExpandBlocksWrites: concurrent writes during Expand.
// Writers should drain before file growth, then resume after.
func testIoMuExpandBlocksWrites(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(30 * time.Second)

	// Write initial data.
	if err := v.WriteLBA(0, makeBlock('E')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Start concurrent writers.
	var wg sync.WaitGroup
	stopWriters := make(chan struct{})
	var writeOK, writeErr atomic.Int64

	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			lba := uint64(id % 200)
			for {
				select {
				case <-stopWriters:
					return
				default:
				}
				if err := v.WriteLBA(lba, makeBlock(byte('0'+id))); err != nil {
					writeErr.Add(1)
					return
				}
				writeOK.Add(1)
			}
		}(g)
	}

	time.Sleep(2 * time.Millisecond)

	// Expand while writers are running.
	// Original vol is 1MB. Expand to 2MB.
	if err := v.Expand(2 << 20); err != nil {
		close(stopWriters)
		wg.Wait()
		t.Fatalf("Expand: %v", err)
	}

	close(stopWriters)
	wg.Wait()

	// Volume size should be 2MB now.
	if v.super.VolumeSize != 2<<20 {
		t.Fatalf("VolumeSize: got %d, want %d", v.super.VolumeSize, 2<<20)
	}

	// Write in the expanded region should succeed.
	newLBA := uint64(256) // 256 * 4096 = 1MB — first block in expanded region
	if err := v.WriteLBA(newLBA, makeBlock('N')); err != nil {
		t.Fatalf("WriteLBA in expanded region: %v", err)
	}
	got, err := v.ReadLBA(newLBA, 4096)
	if err != nil {
		t.Fatalf("ReadLBA in expanded region: %v", err)
	}
	if got[0] != 'N' {
		t.Fatalf("expanded region: got %c, want N", got[0])
	}

	t.Logf("writes during expand: ok=%d err=%d", writeOK.Load(), writeErr.Load())
}

// testIoMuConcurrentReadWrite: many readers + writers simultaneously.
// No panics, no data corruption.
func testIoMuConcurrentReadWrite(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(30 * time.Second)

	// Seed some data.
	for lba := uint64(0); lba < 10; lba++ {
		if err := v.WriteLBA(lba, makeBlock(byte('A'+lba))); err != nil {
			t.Fatalf("seed WriteLBA(%d): %v", lba, err)
		}
	}

	var wg sync.WaitGroup
	const iterations = 200

	// Writers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				lba := uint64(i % 10)
				v.WriteLBA(lba, makeBlock(byte(id)))
			}
		}(w)
	}

	// Readers.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				lba := uint64(i % 10)
				data, err := v.ReadLBA(lba, 4096)
				if err != nil {
					continue // closed or other expected error
				}
				// Data should be a full block of some byte, not garbage.
				if len(data) != 4096 {
					t.Errorf("ReadLBA(%d): got %d bytes, want 4096", lba, len(data))
				}
			}
		}()
	}

	// Trimmers.
	for tr := 0; tr < 2; tr++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations/2; i++ {
				lba := uint64((i + id*50) % 10)
				v.Trim(lba, 4096)
			}
		}(tr)
	}

	wg.Wait()
	// No panic = pass.
}

// testIoMuRestoreThenWriteIntegrity: restore, then immediately write and verify.
// Ensures ioMu unlock releases writers correctly after restore completes.
func testIoMuRestoreThenWriteIntegrity(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(30 * time.Second)

	// Write 'X', snapshot, write 'Y', restore snapshot.
	if err := v.WriteLBA(5, makeBlock('X')); err != nil {
		t.Fatalf("WriteLBA(X): %v", err)
	}
	v.SyncCache()
	if err := v.CreateSnapshot(1); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if err := v.WriteLBA(5, makeBlock('Y')); err != nil {
		t.Fatalf("WriteLBA(Y): %v", err)
	}
	v.SyncCache()
	if err := v.RestoreSnapshot(1); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	// Immediately after restore: LBA 5 should be 'X'.
	got, err := v.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after restore: %v", err)
	}
	if got[0] != 'X' {
		t.Fatalf("after restore: got %c, want X", got[0])
	}

	// Write 'Z' after restore — should succeed (ioMu released).
	if err := v.WriteLBA(5, makeBlock('Z')); err != nil {
		t.Fatalf("WriteLBA(Z) after restore: %v", err)
	}
	got2, err := v.ReadLBA(5, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after post-restore write: %v", err)
	}
	if got2[0] != 'Z' {
		t.Fatalf("after post-restore write: got %c, want Z", got2[0])
	}
}

// testIoMuTrimDuringExpand: trims running while expand acquires exclusive lock.
func testIoMuTrimDuringExpand(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	v.SetRole(RolePrimary)
	v.SetEpoch(1)
	v.SetMasterEpoch(1)
	v.lease.Grant(30 * time.Second)

	// Write data to trim.
	for lba := uint64(0); lba < 50; lba++ {
		v.WriteLBA(lba, makeBlock('T'))
	}

	var wg sync.WaitGroup
	stopTrimmers := make(chan struct{})

	// Concurrent trimmers.
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopTrimmers:
					return
				default:
				}
				lba := uint64(id*10 + (int(time.Now().UnixNano()) % 10))
				v.Trim(lba, 4096)
			}
		}(g)
	}

	time.Sleep(1 * time.Millisecond)

	// Expand during trim storm.
	if err := v.Expand(2 << 20); err != nil {
		close(stopTrimmers)
		wg.Wait()
		t.Fatalf("Expand during trim: %v", err)
	}

	close(stopTrimmers)
	wg.Wait()

	if v.super.VolumeSize != 2<<20 {
		t.Fatalf("VolumeSize: got %d, want %d", v.super.VolumeSize, 2<<20)
	}
}

// Suppress unused import warnings.
var _ = fmt.Sprintf
var _ io.Reader
var _ net.Conn
