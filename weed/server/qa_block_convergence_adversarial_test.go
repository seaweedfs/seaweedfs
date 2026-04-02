package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 10 P2: Adversarial convergence tests
// ============================================================

// --- Adversarial 1: Rapid triple reassignment (vs2 → vs3 → vs4) ---

func TestAdversarial_P10P2_RapidTripleReassignment(t *testing.T) {
	bs, volPath := createP2BlockService(t)

	makeAssignment := func(epoch uint64, serverID, dataAddr string) []blockvol.BlockVolumeAssignment {
		return []blockvol.BlockVolumeAssignment{
			{
				Path:            volPath,
				Epoch:           epoch,
				Role:            uint32(blockvol.RolePrimary),
				LeaseTtlMs:      30000,
				ReplicaServerID: serverID,
				ReplicaDataAddr: dataAddr,
				ReplicaCtrlAddr: dataAddr,
			},
		}
	}

	// Three rapid reassignments.
	bs.ProcessAssignments(makeAssignment(1, "vs2-node:18080", "10.0.0.2:14260"))
	time.Sleep(50 * time.Millisecond)

	bs.ProcessAssignments(makeAssignment(2, "vs3-node:18080", "10.0.0.3:14260"))
	bs.ProcessAssignments(makeAssignment(3, "vs4-node:18080", "10.0.0.4:14260"))

	time.Sleep(300 * time.Millisecond)

	// Only vs4 should exist in engine.
	vs2ID := volPath + "/vs2-node:18080"
	vs3ID := volPath + "/vs3-node:18080"
	vs4ID := volPath + "/vs4-node:18080"

	if bs.v2Orchestrator.Registry.Sender(vs2ID) != nil {
		t.Fatal("vs2 sender should be removed")
	}
	if bs.v2Orchestrator.Registry.Sender(vs3ID) != nil {
		t.Fatal("vs3 sender should be removed")
	}
	if bs.v2Orchestrator.Registry.Sender(vs4ID) == nil {
		t.Fatal("vs4 sender should exist")
	}

	// Runtime: no stale tasks for vs2 or vs3.
	bs.v2Recovery.mu.Lock()
	_, hasVs2 := bs.v2Recovery.tasks[vs2ID]
	_, hasVs3 := bs.v2Recovery.tasks[vs3ID]
	bs.v2Recovery.mu.Unlock()

	if hasVs2 || hasVs3 {
		t.Fatalf("stale runtime tasks: vs2=%v vs3=%v", hasVs2, hasVs3)
	}

	// Heartbeat: must report vs4 address.
	msgs := bs.CollectBlockVolumeHeartbeat()
	hb := findHeartbeatMsg(msgs, volPath)
	if hb == nil {
		t.Fatal("volume not in heartbeat")
	}
	if hb.ReplicaDataAddr != "10.0.0.4:14260" {
		t.Fatalf("heartbeat addr=%s, want 10.0.0.4:14260", hb.ReplicaDataAddr)
	}

	t.Log("adversarial 1: rapid triple reassignment → only vs4 in all truth surfaces")
}

// --- Adversarial 2: Same replica, address change (epoch bump) ---

func TestAdversarial_P10P2_SameReplica_AddressChange(t *testing.T) {
	bs, volPath := createP2BlockService(t)

	replicaID := volPath + "/vs2-node:18080"

	// Epoch 1: vs2 at address 10.0.0.2.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.2:14260",
			ReplicaCtrlAddr: "10.0.0.2:14261",
		},
	})

	time.Sleep(100 * time.Millisecond)

	senderBefore := bs.v2Orchestrator.Registry.Sender(replicaID)
	if senderBefore == nil {
		t.Fatal("sender should exist at epoch 1")
	}

	// Epoch 2: SAME ServerID, DIFFERENT address (replica restarted on new IP).
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            volPath,
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs2-node:18080",
			ReplicaDataAddr: "10.0.0.99:14260", // different IP
			ReplicaCtrlAddr: "10.0.0.99:14261",
		},
	})

	time.Sleep(200 * time.Millisecond)

	// Same sender identity (pointer).
	senderAfter := bs.v2Orchestrator.Registry.Sender(replicaID)
	if senderAfter == nil {
		t.Fatal("sender should still exist")
	}
	if senderAfter != senderBefore {
		t.Fatal("sender identity must be preserved (same pointer) across address change")
	}

	// Endpoint updated.
	if senderAfter.Endpoint().DataAddr != "10.0.0.99:14260" {
		t.Fatalf("endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}

	// Heartbeat: new address.
	msgs := bs.CollectBlockVolumeHeartbeat()
	hb := findHeartbeatMsg(msgs, volPath)
	if hb == nil {
		t.Fatal("volume not in heartbeat")
	}
	if hb.ReplicaDataAddr != "10.0.0.99:14260" {
		t.Fatalf("heartbeat addr=%s, want 10.0.0.99:14260", hb.ReplicaDataAddr)
	}

	// No duplicate sender.
	count := 0
	for _, s := range bs.v2Orchestrator.Registry.All() {
		if s.ReplicaID() == replicaID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate senders: %d for %s", count, replicaID)
	}

	t.Log("adversarial 2: same replica, address change → identity preserved, endpoint updated, no duplicate")
}
