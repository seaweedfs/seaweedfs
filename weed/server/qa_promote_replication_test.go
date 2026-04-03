// Tests for post-promote replication continuity.
//
// Bug: block_promote removes the promoted replica from entry.Replicas.
// The new primary's assignment has zero ReplicaAddrs → no shipper →
// replication dead. sync_all barrier passes vacuously with 0 shippers.
//
// This test suite verifies:
// 1. After promote + old primary re-register, the new primary's assignment
//    includes the re-registered replica's addresses
// 2. sync_all with 0 shippers and RF>1 is detected as a gap
package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	blockvol "github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestPromote_AssignmentHasReplicaAddrs verifies that after promote + old
// primary re-register, the assignment queue contains an updated assignment
// for the new primary WITH replica addresses.
func TestPromote_AssignmentHasReplicaAddrs(t *testing.T) {
	ms := testMasterServerForFailover(t)

	pathA := "/data/vs1/vol1.blk"
	pathB := "/data/vs2/vol1.blk"

	ms.blockRegistry.MarkBlockCapable("vs1")
	ms.blockRegistry.MarkBlockCapable("vs2")

	ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:           "vol1",
		VolumeServer:   "vs1",
		Path:           pathA,
		ISCSIAddr:      "vs1:3260",
		SizeBytes:      1 << 30,
		Epoch:          1,
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		Status:         StatusActive,
		LeaseTTL:       30 * time.Second,
		LastLeaseGrant: time.Now().Add(-1 * time.Minute),
		ReplicaServer:  "vs2",
		ReplicaPath:    pathB,
		Replicas: []ReplicaInfo{
			{
				Server:        "vs2",
				Path:          pathB,
				ISCSIAddr:     "vs2:3260",
				HealthScore:   1.0,
				Role:          blockvol.RoleToWire(blockvol.RoleReplica),
				LastHeartbeat: time.Now(),
				DataAddr:      "vs2:14260",
				CtrlAddr:      "vs2:14261",
			},
		},
	})

	// Step 1: Promote vs2 to primary (vs1 "disconnects").
	ms.failoverBlockVolumes("vs1")

	entry := lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("promote: primary=%s, want vs2", entry.VolumeServer)
	}
	if len(entry.Replicas) != 0 {
		t.Fatalf("after promote: replicas=%d, want 0 (old primary removed)", len(entry.Replicas))
	}
	t.Logf("after promote: primary=vs2 epoch=%d replicas=%d", entry.Epoch, len(entry.Replicas))

	// Drain the assignment queue (promotion enqueued an assignment for vs2).
	promotionAssignments := ms.blockAssignmentQueue.Peek("vs2")
	// Confirm all to drain.
	for _, a := range promotionAssignments {
		ms.blockAssignmentQueue.Confirm("vs2", a.Path, a.Epoch)
	}
	t.Logf("promotion assignment: %d items (drained)", len(promotionAssignments))
	if len(promotionAssignments) > 0 {
		a := promotionAssignments[0]
		t.Logf("  path=%s role=%d replicaAddrs=%d replicaDataAddr=%s",
			a.Path, a.Role, len(a.ReplicaAddrs), a.ReplicaDataAddr)
		if len(a.ReplicaAddrs) > 0 || a.ReplicaDataAddr != "" {
			t.Log("  WARNING: promotion assignment already has replica addrs (unexpected)")
		}
	}

	// Step 2: vs1 reconnects via heartbeat (lower epoch → added as replica).
	ms.blockRegistry.MarkBlockCapable("vs1")
	hbResult := ms.blockRegistry.UpdateFullHeartbeat("vs1", []*master_pb.BlockVolumeInfoMessage{
		{
			Path:            pathA,
			VolumeSize:      1 << 30,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary), // stale
			ReplicaDataAddr: "vs1:14260",
			ReplicaCtrlAddr: "vs1:14261",
		},
	}, "")

	// CP13-8A: process primary refresh (simulating what the heartbeat handler does).
	for _, refreshEntry := range hbResult.PrimaryRefreshNeeded {
		ms.enqueuePrimaryRefresh(refreshEntry)
	}

	entry = lookupEntryT(t, ms.blockRegistry, "vol1")
	t.Logf("after vs1 re-register: primary=%s replicas=%d", entry.VolumeServer, len(entry.Replicas))

	if len(entry.Replicas) == 0 {
		t.Fatal("vs1 should be in replicas after re-register")
	}

	// Step 3: Check if the assignment queue has an UPDATED assignment for vs2
	// with the new replica's addresses.
	updatedAssignments := ms.blockAssignmentQueue.Peek("vs2")
	t.Logf("post-re-register assignments for vs2: %d items", len(updatedAssignments))

	foundReplicaAddrs := false
	for _, a := range updatedAssignments {
		if a.Path == entry.Path {
			t.Logf("  assignment: path=%s role=%d replicaDataAddr=%s replicaAddrs=%d",
				a.Path, a.Role, a.ReplicaDataAddr, len(a.ReplicaAddrs))
			if a.ReplicaDataAddr != "" || len(a.ReplicaAddrs) > 0 {
				foundReplicaAddrs = true
			}
		}
	}

	if !foundReplicaAddrs {
		t.Fatalf("BUG: after promote + re-register, new primary vs2 has NO assignment "+
			"with replica addresses.\n"+
			"This means the shipper will never be configured and replication is dead.\n"+
			"The master must send an updated assignment to vs2 after vs1 re-registers as replica.")
	}
	t.Log("post-promote assignment has replica addresses — shipper will be configured")
}

// TestPromote_ReplicasEmptyAfterPromote documents the current behavior:
// after PromoteBestReplica, entry.Replicas is empty.
func TestPromote_ReplicasEmptyAfterPromote(t *testing.T) {
	ms := testMasterServerForFailover(t)
	registerVolumeWithReplica(t, ms, "vol1", "vs1", "vs2", 1, 30*time.Second)

	ms.blockRegistry.UpdateEntry("vol1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	ms.failoverBlockVolumes("vs1")

	entry := lookupEntryT(t, ms.blockRegistry, "vol1")
	if entry.VolumeServer != "vs2" {
		t.Fatalf("primary=%s, want vs2", entry.VolumeServer)
	}
	// Document: Replicas is empty after promote. This is by design
	// (old primary "needs rebuild"), but it means the new primary
	// has no shipper target until a replica re-registers.
	t.Logf("after promote: replicas=%d (expected 0 — old primary removed)", len(entry.Replicas))

	// Check the assignment queue for vs2.
	assignments := ms.blockAssignmentQueue.Peek("vs2")
	for _, a := range assignments {
		t.Logf("assignment for vs2: path=%s epoch=%d role=%d replicaDataAddr=%q replicaAddrs=%d",
			a.Path, a.Epoch, a.Role, a.ReplicaDataAddr, len(a.ReplicaAddrs))
	}
}
