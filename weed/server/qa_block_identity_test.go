package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 10 P1: Stable identity on the real control wire
//
// Proofs:
//   1. Real ingress: proto wire → decode → ProcessAssignments → ControlBridge → ReplicaID
//   2. Canonical local identity: non-default serverID flows through block/control path
//   3. Fail-closed: missing ServerID on proto wire → replica skipped
// ============================================================

// --- Real ingress proof: proto wire → engine ReplicaID ---

func TestP10P1_RealIngress_ProtoToReplicaID(t *testing.T) {
	// Simulate the real control ingress chain:
	// 1. Master builds proto assignment with stable ServerID
	// 2. Proto is encoded (as it would be over gRPC wire)
	// 3. Volume server decodes proto → Go assignment
	// 4. ProcessAssignments → ControlBridge → engine
	// 5. Engine has sender with correct ReplicaID = <path>/<ServerID>

	// Step 1: Master builds proto (simulates master_grpc_server_block.go).
	protoAssignment := &master_pb.BlockVolumeAssignment{
		Path:            "pvc-vol-1",
		Epoch:           5,
		Role:            uint32(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerId: "vs2-node.cluster:18080", // stable ID from registry
		ReplicaDataAddr: "10.0.0.2:14260",
		ReplicaCtrlAddr: "10.0.0.2:14261",
		ReplicaAddrs: []*master_pb.ReplicaAddrMessage{
			{
				DataAddr: "10.0.0.2:14260",
				CtrlAddr: "10.0.0.2:14261",
				ServerId: "vs2-node.cluster:18080",
			},
		},
	}

	// Step 2: Proto round-trip (simulates gRPC wire encode/decode).
	// In real gRPC, this is automatic. Here we verify the Go struct
	// carries the fields after proto-to-Go conversion.
	goAssignment := blockvol.AssignmentFromProto(protoAssignment)

	// Verify: Go wire type preserves the stable ID.
	if goAssignment.ReplicaServerID != "vs2-node.cluster:18080" {
		t.Fatalf("scalar ReplicaServerID=%q, want vs2-node.cluster:18080", goAssignment.ReplicaServerID)
	}
	if len(goAssignment.ReplicaAddrs) != 1 || goAssignment.ReplicaAddrs[0].ServerID != "vs2-node.cluster:18080" {
		t.Fatalf("multi-replica ServerID not preserved")
	}

	// Step 3-4: ProcessAssignments → ControlBridge → engine.
	bs := &BlockService{
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		localServerID:  "vs1-node.cluster:18080",
	}

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{goAssignment})

	// Step 5: Verify engine sender has the correct stable ReplicaID.
	expectedReplicaID := "pvc-vol-1/vs2-node.cluster:18080"
	sender := bs.v2Orchestrator.Registry.Sender(expectedReplicaID)
	if sender == nil {
		t.Fatalf("engine sender not found for %s", expectedReplicaID)
	}
	if !sender.HasActiveSession() {
		t.Fatal("sender should have active session")
	}

	t.Logf("P10P1 real ingress: proto(ServerId=%s) → decode → ProcessAssignments → sender(%s) with session",
		"vs2-node.cluster:18080", expectedReplicaID)
}

// --- Canonical local identity: non-default serverID ---

func TestP10P1_CanonicalLocalIdentity(t *testing.T) {
	// Prove that when the volume server has a canonical ID different from
	// plain ip:port, the block/control path uses that canonical ID.

	canonicalID := "my-custom-volume-server-id"

	bs := &BlockService{
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		localServerID:  canonicalID, // explicitly NOT ip:port shaped
	}

	// Process a replica assignment that targets THIS volume server.
	// The ControlBridge uses localServerID for replica/rebuild assignments.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-vol-2",
			Epoch:           3,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaServerID: "other-server-id",
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
		},
	})

	// The sender should use the ServerID from the assignment, not localServerID.
	sender := bs.v2Orchestrator.Registry.Sender("pvc-vol-2/other-server-id")
	if sender == nil {
		t.Fatal("sender should exist with ServerID from assignment, not from localServerID")
	}

	// Now process a replica assignment where THIS server is the replica.
	// The ControlBridge uses localServerID for the local ReplicaID.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-vol-3",
			Epoch:           1,
			Role:            uint32(blockvol.RoleReplica),
			ReplicaDataAddr: "10.0.0.1:14260",
			ReplicaCtrlAddr: "10.0.0.1:14261",
		},
	})

	// For a replica assignment, ControlBridge builds ReplicaID as <path>/<localServerID>.
	expectedLocalReplicaID := "pvc-vol-3/" + canonicalID
	localSender := bs.v2Orchestrator.Registry.Sender(expectedLocalReplicaID)
	if localSender == nil {
		t.Fatalf("local replica sender not found for %s — canonical ID not used", expectedLocalReplicaID)
	}

	t.Logf("P10P1 canonical identity: localServerID=%q → local ReplicaID=%s", canonicalID, expectedLocalReplicaID)
}

// --- Fail-closed: missing ServerID on proto wire ---

func TestP10P1_FailClosed_MissingServerID(t *testing.T) {
	// When the proto assignment has addresses but no ServerID,
	// the ControlBridge must skip that replica (fail closed).

	bs := &BlockService{
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		localServerID:  "vs1:18080",
	}

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            "pvc-vol-4",
			Epoch:           1,
			Role:            uint32(blockvol.RolePrimary),
			ReplicaDataAddr: "10.0.0.2:9333",
			ReplicaCtrlAddr: "10.0.0.2:9334",
			// ReplicaServerID intentionally empty — should be skipped.
		},
	})

	// No sender should exist — the replica was skipped due to missing ServerID.
	// The ControlBridge logs "scalar replica assignment without ServerID" and skips.
	// Check that no sender exists with an address-derived ID.
	addressDerivedID := "pvc-vol-4/10.0.0.2:9333"
	if bs.v2Orchestrator.Registry.Sender(addressDerivedID) != nil {
		t.Fatal("address-derived sender should NOT exist — missing ServerID must fail closed")
	}

	t.Log("P10P1 fail-closed: missing ServerID → replica skipped, no address-derived fallback")
}
