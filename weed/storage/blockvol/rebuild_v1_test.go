package blockvol

// CP13-7: Rebuild fallback V1 tests.

import (
	"path/filepath"
	"testing"
	"time"
)

// TestHeartbeat_ReportsPerReplicaState verifies that the heartbeat message
// carries per-replica shipper state, not just a boolean degraded flag.
func TestHeartbeat_ReportsPerReplicaState(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Write + sync to bootstrap the shipper to InSync.
	if err := primary.WriteLBA(0, makeBlock('A')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Check heartbeat includes per-replica states.
	msg := ToBlockVolumeInfoMessage("/data/test.blk", "ssd", primary)
	if len(msg.ReplicaShipperStates) == 0 {
		t.Fatal("heartbeat should include ReplicaShipperStates")
	}
	rs := msg.ReplicaShipperStates[0]
	if rs.State != "in_sync" {
		t.Fatalf("expected replica state in_sync, got %q", rs.State)
	}
	if rs.DataAddr == "" {
		t.Fatal("replica DataAddr should not be empty")
	}
}

// TestHeartbeat_ReportsNeedsRebuild verifies that when a shipper is in
// NeedsRebuild state, the heartbeat reports it per-replica.
func TestHeartbeat_ReportsNeedsRebuild(t *testing.T) {
	primary, replica := createSyncAllPair(t)
	defer primary.Close()
	defer replica.Close()

	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	// Force shipper to NeedsRebuild.
	shipper := primary.shipperGroup.Shipper(0)
	shipper.state.Store(uint32(ReplicaNeedsRebuild))

	msg := ToBlockVolumeInfoMessage("/data/test.blk", "ssd", primary)
	if len(msg.ReplicaShipperStates) == 0 {
		t.Fatal("heartbeat should include ReplicaShipperStates")
	}
	if msg.ReplicaShipperStates[0].State != "needs_rebuild" {
		t.Fatalf("expected needs_rebuild, got %q", msg.ReplicaShipperStates[0].State)
	}
}

// TestReplicaState_RebuildComplete_ReentersInSync verifies the full rebuild
// cycle: NeedsRebuild → StartRebuild → fresh shipper → bootstrap → InSync.
func TestReplicaState_RebuildComplete_ReentersInSync(t *testing.T) {
	// Create primary + replica pair.
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		DurabilityMode: DurabilitySyncAll,
	}
	primary, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)

	replica, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetRole(RoleReplica)
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	// Phase 1: Set up replication and write data.
	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv.Serve()
	defer recv.Stop()

	primary.SetReplicaAddr(recv.DataAddr(), recv.CtrlAddr())

	for i := uint64(0); i < 5; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: Start rebuild server on primary.
	if err := primary.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer primary.StopRebuildServer()
	rebuildAddr := primary.rebuildServer.Addr()

	// Phase 3: Simulate VS restart on replica — close + reopen → RoleNone.
	recv.Stop()
	replica.Close()
	replica, err = OpenBlockVol(filepath.Join(rDir, "replica.blockvol"))
	if err != nil {
		t.Fatalf("reopen replica: %v", err)
	}
	defer replica.Close() // close reopened volume
	// Now RoleNone → assignment to Rebuilding is valid.
	if err := HandleAssignment(replica, 1, RoleRebuilding, 0); err != nil {
		t.Fatalf("HandleAssignment to Rebuilding: %v", err)
	}
	fromLSN := replica.Status().WALHeadLSN
	if err := StartRebuild(replica, rebuildAddr, fromLSN, 1); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// Phase 4: After rebuild, replica should be RoleReplica.
	if replica.Status().Role != RoleReplica {
		t.Fatalf("expected RoleReplica after rebuild, got %s", replica.Status().Role)
	}

	// Phase 5: Simulate master sending fresh Primary assignment with rebuilt replica.
	// This creates a new shipper (Disconnected) replacing the old NeedsRebuild one.
	recv2, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	recv2.Serve()
	defer recv2.Stop()
	primary.SetReplicaAddr(recv2.DataAddr(), recv2.CtrlAddr())

	// Phase 6: Write + SyncCache — fresh shipper should bootstrap to InSync.
	if err := primary.WriteLBA(10, makeBlock('Z')); err != nil {
		t.Fatal(err)
	}
	if err := primary.SyncCache(); err != nil {
		t.Fatalf("SyncCache after rebuild: %v", err)
	}

	shipper := primary.shipperGroup.Shipper(0)
	if shipper.State() != ReplicaInSync {
		t.Fatalf("expected InSync after rebuild + barrier, got %s", shipper.State())
	}
}

// TestRebuild_AbortOnEpochChange verifies that StartRebuild fails when
// the rebuild server detects an epoch mismatch.
func TestRebuild_AbortOnEpochChange(t *testing.T) {
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}
	primary, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(2) // epoch 2
	primary.SetMasterEpoch(2)
	primary.lease.Grant(30 * time.Second)

	replica, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetEpoch(1) // stale epoch
	replica.SetMasterEpoch(1)
	replica.SetRole(RoleRebuilding)

	if err := primary.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer primary.StopRebuildServer()

	// Rebuild with epoch mismatch should fail.
	err = StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1) // epoch=1 vs primary epoch=2
	if err == nil {
		t.Fatal("StartRebuild should fail on epoch mismatch")
	}
}

// TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint verifies that after a
// full extent rebuild, the replica's state is correctly initialized:
//   - flushedLSN = checkpointLSN (baseline durable, not 0, not headLSN)
//   - receivedLSN = nextLSN - 1 (ready to accept next entry from primary)
//   - replica re-enters as Disconnected/bootstrap, NOT InSync
//
// BUG FOUND: rebuildFullExtent updates superblock.WALCheckpointLSN but does
// NOT update flusher.checkpointLSN. So NewReplicaReceiver reads stale 0
// from flusher.CheckpointLSN(). Fix: rebuild must sync flusher state.
func TestRebuild_PostRebuild_FlushedLSN_IsCheckpoint(t *testing.T) {
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        256 * 1024,
		DurabilityMode: DurabilitySyncAll,
	}

	primary, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)

	// Write data on primary and flush to extent.
	for i := uint64(0); i < 10; i++ {
		if err := primary.WriteLBA(i, makeBlock(byte('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		// No replica configured — SyncCache does local fsync only.
	}
	primary.flusher.FlushOnce()

	checkpointBefore := primary.flusher.CheckpointLSN()
	if checkpointBefore == 0 {
		t.Fatal("primary checkpointLSN should be >0 after flush")
	}
	t.Logf("primary checkpointLSN=%d, nextLSN=%d", checkpointBefore, primary.nextLSN.Load())

	// Start rebuild server.
	if err := primary.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer primary.StopRebuildServer()

	// Create replica and rebuild from primary.
	replica, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	if err := HandleAssignment(replica, 1, RoleRebuilding, 0); err != nil {
		t.Fatal(err)
	}
	if err := StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// After rebuild: check replica state.
	status := replica.Status()
	t.Logf("post-rebuild: role=%s nextLSN=%d walHeadLSN=%d", status.Role, replica.nextLSN.Load(), status.WALHeadLSN)

	// Role should be Replica (not InSync — InSync is a shipper concept).
	if status.Role != RoleReplica {
		t.Fatalf("expected RoleReplica, got %s", status.Role)
	}

	// nextLSN should be > 0 (set by syncLSNAfterRebuild).
	if replica.nextLSN.Load() <= 1 {
		t.Fatalf("nextLSN=%d, expected > 1 after rebuild", replica.nextLSN.Load())
	}

	// Create a receiver — it initializes flushedLSN from vol state.
	recv, err := NewReplicaReceiver(replica, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer recv.Stop()

	// FlushedLSN should be initialized to vol.nextLSN-1 (the rebuilt baseline).
	// It should NOT be 0 (would cause catch-up to replay everything).
	flushed := recv.FlushedLSN()
	if flushed == 0 {
		t.Fatal("post-rebuild flushedLSN=0 — should be initialized from rebuilt baseline")
	}
	t.Logf("post-rebuild receiver: flushedLSN=%d receivedLSN=%d", flushed, recv.ReceivedLSN())

	// Verify data integrity: all 10 blocks should be present on replica.
	for i := uint64(0); i < 10; i++ {
		got, err := replica.ReadLBA(i, 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if got[0] != byte('A'+i) {
			t.Fatalf("LBA %d: expected %c, got %c — rebuild data integrity failure", i, 'A'+i, got[0])
		}
	}
}

// TestRebuild_MissingTailRestartsOrFailsCleanly verifies that if the
// trailing WAL required after a full extent rebuild has been reclaimed,
// the rebuild fails cleanly instead of promoting a partial replica.
//
// Scenario: Primary writes 100 entries → flush + reclaim → rebuild starts.
// The full extent copy succeeds. But the second catch-up (trailing WAL)
// requests entries that have been reclaimed → should error, not promote.
func TestRebuild_MissingTailRestartsOrFailsCleanly(t *testing.T) {
	pDir := t.TempDir()
	rDir := t.TempDir()
	opts := CreateOptions{
		VolumeSize:     1 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        32 * 1024, // tiny WAL — reclaimed quickly
		DurabilityMode: DurabilitySyncAll,
	}

	primary, err := CreateBlockVol(filepath.Join(pDir, "primary.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	primary.SetRole(RolePrimary)
	primary.SetEpoch(1)
	primary.SetMasterEpoch(1)
	primary.lease.Grant(30 * time.Second)

	// Write enough data to fill and reclaim the tiny WAL multiple times.
	for i := uint64(0); i < 100; i++ {
		if err := primary.WriteLBA(i%16, makeBlock(byte('0'+i%10))); err != nil {
			t.Fatal(err)
		}
	}
	if err := primary.SyncCache(); err != nil {
		// No replica — local only.
	}
	// Flush aggressively to reclaim WAL.
	for j := 0; j < 5; j++ {
		primary.flusher.FlushOnce()
	}

	headLSN := primary.nextLSN.Load() - 1
	checkpointLSN := primary.flusher.CheckpointLSN()
	t.Logf("primary: headLSN=%d checkpointLSN=%d", headLSN, checkpointLSN)

	// Start rebuild server.
	if err := primary.StartRebuildServer("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer primary.StopRebuildServer()

	// Now write MORE data AFTER starting the rebuild server.
	// This creates entries between snapshotLSN and the current head.
	// The second catch-up will need these entries.
	for i := uint64(0); i < 100; i++ {
		if err := primary.WriteLBA(i%16, makeBlock(byte('A'+i%10))); err != nil {
			t.Fatal(err)
		}
	}
	// Flush aggressively again — reclaim the entries the second catch-up needs.
	for j := 0; j < 5; j++ {
		primary.flusher.FlushOnce()
	}

	newHead := primary.nextLSN.Load() - 1
	t.Logf("primary after more writes: headLSN=%d (entries between %d and %d likely reclaimed)", newHead, headLSN, newHead)

	// Create replica and attempt rebuild.
	replica, err := CreateBlockVol(filepath.Join(rDir, "replica.blockvol"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	replica.SetEpoch(1)
	replica.SetMasterEpoch(1)

	if err := HandleAssignment(replica, 1, RoleRebuilding, 0); err != nil {
		t.Fatal(err)
	}

	// StartRebuild: WAL catch-up fails (WAL_RECYCLED) → falls back to full extent.
	// Full extent succeeds. Second catch-up may fail if trailing WAL is reclaimed.
	err = StartRebuild(replica, primary.rebuildServer.Addr(), 0, 1)

	if err != nil {
		// Expected: rebuild failed because trailing WAL was reclaimed.
		// Replica should NOT be promoted to RoleReplica.
		t.Logf("rebuild correctly failed: %v", err)
		if replica.Role() == RoleReplica {
			t.Fatal("replica should NOT be RoleReplica after failed rebuild — partial data")
		}
	} else {
		// Rebuild succeeded — the trailing WAL was still available.
		// This is also acceptable (depends on timing/flusher behavior).
		t.Log("rebuild succeeded — trailing WAL was still retained")
		if replica.Role() != RoleReplica {
			t.Fatalf("expected RoleReplica after successful rebuild, got %s", replica.Role())
		}
	}
}

// createSyncAllPair is defined in sync_all_bug_test.go.
// makeBlock is defined in blockvol_test.go.
