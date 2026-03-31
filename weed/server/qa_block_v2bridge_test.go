package weed_server

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

func newTestControlBridge() *v2bridge.ControlBridge {
	return v2bridge.NewControlBridge()
}

func newTestOrchestrator() *engine.RecoveryOrchestrator {
	return engine.NewRecoveryOrchestrator()
}

// ============================================================
// Phase 08 P1: ProcessAssignments → V2 engine state tests
// Proves real assignment delivery changes engine state.
// ============================================================

func TestV2Bridge_ProcessAssignment_CreatesEngineSender(t *testing.T) {
	bs := &BlockService{
		v2Bridge:       newTestControlBridge(),
		v2Orchestrator: newTestOrchestrator(),
		localServerID:  "vs1:9333",
	}

	// Real assignment from master.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-test-1",
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	// Engine should have a sender for this replica.
	orch := bs.V2Orchestrator()
	sender := orch.Registry.Sender("pvc-test-1/vs2")
	if sender == nil {
		t.Fatal("engine should have sender for pvc-test-1/vs2 after ProcessAssignment")
	}
	if !sender.HasActiveSession() {
		t.Fatal("sender should have active session (catch-up recovery)")
	}
}

func TestV2Bridge_ProcessAssignment_EpochBump(t *testing.T) {
	bs := &BlockService{
		v2Bridge:       newTestControlBridge(),
		v2Orchestrator: newTestOrchestrator(),
		localServerID:  "vs1:9333",
	}

	// Epoch 1 assignment.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-test-1",
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	orch := bs.V2Orchestrator()

	// Epoch bump.
	orch.InvalidateEpoch(2)
	orch.UpdateSenderEpoch("pvc-test-1/vs2", 2)

	// Epoch 2 assignment.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-test-1",
			Epoch:           2,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	sender := orch.Registry.Sender("pvc-test-1/vs2")
	if sender == nil {
		t.Fatal("sender should exist at epoch 2")
	}
	if !sender.HasActiveSession() {
		t.Fatal("should have new session at epoch 2")
	}

	// Log shows invalidation.
	hasInvalidation := false
	for _, e := range orch.Log.EventsFor("pvc-test-1/vs2") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("log must show session invalidation on epoch bump")
	}
}

func TestV2Bridge_ProcessAssignment_AddressChange(t *testing.T) {
	bs := &BlockService{
		v2Bridge:       newTestControlBridge(),
		v2Orchestrator: newTestOrchestrator(),
		localServerID:  "vs1:9333",
	}

	// First assignment.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-test-1",
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	orch := bs.V2Orchestrator()
	sender := orch.Registry.Sender("pvc-test-1/vs2")
	if sender == nil {
		t.Fatal("sender should exist")
	}

	// Same ServerID, different address (address change).
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-test-1",
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "vs2",
			ReplicaDataAddr: "10.0.0.5:9333", // different IP
			ReplicaCtrlAddr: "10.0.0.5:9334",
		},
	})

	// Sender identity preserved (same pointer).
	senderAfter := orch.Registry.Sender("pvc-test-1/vs2")
	if senderAfter != sender {
		t.Fatal("sender identity must be preserved across address change")
	}

	// Endpoint updated.
	if senderAfter.Endpoint().DataAddr != "10.0.0.5:9333" {
		t.Fatalf("endpoint not updated: %s", senderAfter.Endpoint().DataAddr)
	}
}
