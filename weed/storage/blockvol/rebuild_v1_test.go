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

// createSyncAllPair is defined in sync_all_bug_test.go.
// makeBlock is defined in blockvol_test.go.
