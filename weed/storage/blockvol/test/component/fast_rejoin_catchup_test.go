package component

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/protocol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestFastRejoinCatchUp_FailoverRejoinUsesRetainedWAL proves the complement of
// the rebuild-rejoin scenario: when the old primary returns quickly enough that
// the new primary still retains the missing WAL, the protocol must choose
// catch-up and the real shipper path must converge without starting rebuild.
func TestFastRejoinCatchUp_FailoverRejoinUsesRetainedWAL(t *testing.T) {
	nodeAPath := filepath.Join(t.TempDir(), "nodeA.blk")
	nodeBPath := filepath.Join(t.TempDir(), "nodeB.blk")

	blockSize := uint32(4096)
	opts := blockvol.CreateOptions{
		VolumeSize: 8 * 1024 * 1024,
		BlockSize:  blockSize,
		// Keep enough WAL so the quick rejoin stays within the retained window.
		WALSize: 2 * 1024 * 1024,
	}

	nodeA, err := blockvol.CreateBlockVol(nodeAPath, opts)
	if err != nil {
		t.Fatal(err)
	}
	nodeA.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	nodeB, err := blockvol.CreateBlockVol(nodeBPath, opts)
	if err != nil {
		nodeA.Close()
		t.Fatal(err)
	}
	defer nodeB.Close()
	// Preload nodeB's local data before assigning replica role so we start
	// Phase 1 from an already-synced pair.
	nodeB.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)

	initialBlocks := 64
	for i := 0; i < initialBlocks; i++ {
		data := deterministicBlockR10(uint64(i), 1, blockSize)
		if err := nodeA.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("nodeA initial write LBA %d: %v", i, err)
		}
	}
	if err := nodeA.SyncCache(); err != nil {
		t.Fatalf("nodeA initial SyncCache: %v", err)
	}
	if err := nodeA.ForceFlush(); err != nil {
		t.Fatalf("nodeA initial ForceFlush: %v", err)
	}

	for i := 0; i < initialBlocks; i++ {
		data, err := nodeA.ReadLBA(uint64(i), blockSize)
		if err != nil {
			t.Fatalf("nodeA read for initial sync LBA %d: %v", i, err)
		}
		if err := nodeB.WriteLBA(uint64(i), data); err != nil {
			t.Fatalf("nodeB initial sync write LBA %d: %v", i, err)
		}
	}
	if err := nodeB.SyncCache(); err != nil {
		t.Fatalf("nodeB initial SyncCache: %v", err)
	}
	if err := nodeB.ForceFlush(); err != nil {
		t.Fatalf("nodeB initial ForceFlush: %v", err)
	}
	nodeB.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	nodeAInitialLSN := nodeA.Status().WALHeadLSN
	nodeBTailBefore := nodeB.Status().CheckpointLSN
	t.Logf("Phase 1: synced at LSN=%d checkpoint=%d", nodeAInitialLSN, nodeBTailBefore)

	if err := nodeA.Close(); err != nil {
		t.Fatalf("close nodeA: %v", err)
	}
	t.Log("Phase 2: nodeA (old primary) stopped")

	nodeB.HandleAssignment(2, blockvol.RolePrimary, 30*time.Second)
	t.Log("Phase 3: nodeB promoted to primary (epoch=2)")

	postFailoverWrites := 12
	for i := 0; i < postFailoverWrites; i++ {
		lba := uint64(i % 8)
		data := deterministicBlockR10(lba, uint64(i+2), blockSize)
		if err := nodeB.WriteLBA(lba, data); err != nil {
			t.Fatalf("nodeB post-failover write %d: %v", i, err)
		}
	}
	// Sync WAL durability but do not flush/recycle it; the gap should remain
	// recoverable from retained WAL.
	if err := nodeB.SyncCache(); err != nil {
		t.Fatalf("nodeB post-failover SyncCache: %v", err)
	}

	nodeBStatus := nodeB.Status()
	nodeBWALTail := nodeBStatus.CheckpointLSN
	nodeBWALHead := nodeBStatus.WALHeadLSN
	if nodeBWALTail > nodeAInitialLSN {
		t.Fatalf("expected retained WAL for catch-up: tail=%d initial=%d", nodeBWALTail, nodeAInitialLSN)
	}
	if nodeBWALHead <= nodeAInitialLSN {
		t.Fatalf("expected head to advance after failover writes: head=%d initial=%d", nodeBWALHead, nodeAInitialLSN)
	}
	t.Logf("Phase 3: post-failover head=%d tail=%d", nodeBWALHead, nodeBWALTail)

	nodeA, err = blockvol.OpenBlockVol(nodeAPath)
	if err != nil {
		t.Fatalf("reopen nodeA: %v", err)
	}
	defer nodeA.Close()
	nodeA.HandleAssignment(2, blockvol.RoleReplica, 30*time.Second)
	nodeARestartLSN := nodeA.Status().WALHeadLSN
	t.Logf("Phase 4: nodeA restarted as replica, WALHeadLSN=%d", nodeARestartLSN)

	eng := protocol.NewEngine()
	eng.ApplyEvent(protocol.AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 2, Role: protocol.RolePrimary,
		Replicas: []protocol.ReplicaAssignment{
			{ReplicaID: "nodeA", Endpoint: protocol.Endpoint{DataAddr: "a", CtrlAddr: "b"}},
		},
	})
	boolTrue := true
	eng.ApplyEvent(protocol.ReadinessObserved{
		VolumeID: "vol-1", RoleApplied: &boolTrue,
		ShipperConfigured: &boolTrue, ShipperConnected: &boolTrue,
	})

	result := eng.ApplyEvent(protocol.SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "nodeA",
		Ack:            protocol.SyncAck{AppliedLSN: nodeARestartLSN, DurableLSN: nodeARestartLSN},
		PrimaryWALTail: nodeBWALTail,
		PrimaryWALHead: nodeBWALHead,
	})

	var catchUpCmd *protocol.IssueCatchUpCommand
	var rebuildCmd *protocol.IssueRebuildCommand
	for _, cmd := range result.Commands {
		if c, ok := cmd.(protocol.IssueCatchUpCommand); ok {
			catchUpCmd = &c
		}
		if c, ok := cmd.(protocol.IssueRebuildCommand); ok {
			rebuildCmd = &c
		}
	}
	if catchUpCmd == nil {
		if rebuildCmd != nil {
			t.Fatalf("expected catch-up, got rebuild target=%d (replica=%d tail=%d head=%d)",
				rebuildCmd.TargetLSN, nodeARestartLSN, nodeBWALTail, nodeBWALHead)
		}
		t.Fatalf("expected catch-up command for retained WAL (replica=%d tail=%d head=%d)",
			nodeARestartLSN, nodeBWALTail, nodeBWALHead)
	}
	t.Logf("Phase 5: engine decided CATCH-UP start=%d target=%d", catchUpCmd.StartLSN, catchUpCmd.TargetLSN)

	if err := nodeA.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatalf("start replica receiver: %v", err)
	}
	recvAddr := nodeA.ReplicaReceiverAddr()
	nodeB.SetReplicaAddrs([]blockvol.ReplicaAddr{{
		ServerID: "nodeA",
		DataAddr: recvAddr.DataAddr,
		CtrlAddr: recvAddr.CtrlAddr,
	}})

	achieved, err := nodeB.CatchUpReplicaTo("nodeA", catchUpCmd.TargetLSN)
	if err != nil {
		t.Fatalf("catch-up failed: %v", err)
	}
	if achieved < catchUpCmd.TargetLSN {
		t.Fatalf("achieved_lsn=%d, want >= %d", achieved, catchUpCmd.TargetLSN)
	}

	states := nodeB.ReplicaShipperStates()
	if len(states) != 1 {
		t.Fatalf("shipper states=%+v, want 1 shipper", states)
	}
	if states[0].State != "in_sync" {
		t.Fatalf("shipper state=%s, want in_sync", states[0].State)
	}

	if err := nodeB.ForceFlush(); err != nil {
		t.Fatalf("nodeB final ForceFlush: %v", err)
	}
	if err := nodeA.ForceFlush(); err != nil {
		t.Fatalf("nodeA final ForceFlush: %v", err)
	}

	for lba := uint64(0); lba < uint64(initialBlocks); lba++ {
		bData, err := nodeB.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("nodeB read LBA %d: %v", lba, err)
		}
		aData, err := nodeA.ReadLBA(lba, blockSize)
		if err != nil {
			t.Fatalf("nodeA read LBA %d: %v", lba, err)
		}
		if !bytes.Equal(bData, aData) {
			t.Fatalf("LBA %d mismatch after catch-up", lba)
		}
	}

	if _, _, active := nodeA.ActiveRebuildSession(); active {
		t.Fatal("unexpected rebuild session on fast rejoin catch-up path")
	}

	t.Logf("FAST REJOIN PASSED: retained WAL allowed catch-up to %d without rebuild", achieved)
}
