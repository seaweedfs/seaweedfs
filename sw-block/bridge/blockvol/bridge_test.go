package blockvol

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 07 P0: Bridge adapter tests
// Validates E1-E3 expectations against concrete adapter code.
// ============================================================

// --- E1: Real assignment → engine intent ---

func TestControlAdapter_StableIdentity(t *testing.T) {
	ca := NewControlAdapter()

	primary := MasterAssignment{
		VolumeName:      "pvc-data-1",
		Epoch:           3,
		Role:            "primary",
		PrimaryServerID: "vs1",
	}
	replicas := []MasterAssignment{
		{
			VolumeName:      "pvc-data-1",
			Epoch:           3,
			Role:            "replica",
			ReplicaServerID: "vs2",
			DataAddr:        "10.0.0.2:9333",
			CtrlAddr:        "10.0.0.2:9334",
			AddrVersion:     1,
		},
	}

	intent := ca.ToAssignmentIntent(primary, replicas)

	if intent.Epoch != 3 {
		t.Fatalf("epoch=%d", intent.Epoch)
	}
	if len(intent.Replicas) != 1 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}

	// ReplicaID is stable: volume-name/server-id (NOT address).
	r := intent.Replicas[0]
	if r.ReplicaID != "pvc-data-1/vs2" {
		t.Fatalf("ReplicaID=%s (must be volume/server, not address)", r.ReplicaID)
	}

	// Endpoint is the address (mutable).
	if r.Endpoint.DataAddr != "10.0.0.2:9333" {
		t.Fatalf("DataAddr=%s", r.Endpoint.DataAddr)
	}

	// Recovery target mapped.
	if intent.RecoveryTargets["pvc-data-1/vs2"] != engine.SessionCatchUp {
		t.Fatalf("recovery=%s", intent.RecoveryTargets["pvc-data-1/vs2"])
	}
}

func TestControlAdapter_AddressChangePreservesIdentity(t *testing.T) {
	ca := NewControlAdapter()

	// First assignment.
	intent1 := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica", DataAddr: "10.0.0.2:9333", AddrVersion: 1},
		},
	)

	// Address changes — new assignment.
	intent2 := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica", DataAddr: "10.0.0.3:9333", AddrVersion: 2},
		},
	)

	// Same ReplicaID despite different address.
	if intent1.Replicas[0].ReplicaID != intent2.Replicas[0].ReplicaID {
		t.Fatalf("identity changed: %s → %s",
			intent1.Replicas[0].ReplicaID, intent2.Replicas[0].ReplicaID)
	}

	// Endpoint updated.
	if intent2.Replicas[0].Endpoint.DataAddr != "10.0.0.3:9333" {
		t.Fatalf("endpoint not updated: %s", intent2.Replicas[0].Endpoint.DataAddr)
	}
}

func TestControlAdapter_RebuildRoleMapping(t *testing.T) {
	ca := NewControlAdapter()

	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "rebuilding", DataAddr: "10.0.0.2:9333"},
		},
	)

	if intent.RecoveryTargets["vol1/vs2"] != engine.SessionRebuild {
		t.Fatalf("rebuilding role should map to SessionRebuild, got %s",
			intent.RecoveryTargets["vol1/vs2"])
	}
}

func TestControlAdapter_PrimaryNoRecovery(t *testing.T) {
	ca := NewControlAdapter()

	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{}, // no replicas
	)

	if len(intent.RecoveryTargets) != 0 {
		t.Fatal("primary should not have recovery targets")
	}
}

// --- E2: Real storage truth → RetainedHistory ---

func TestStorageAdapter_RetainedHistoryFromRealState(t *testing.T) {
	sa := NewStorageAdapter()
	sa.UpdateState(BlockVolState{
		WALHeadLSN:        100,
		WALTailLSN:        30,
		CommittedLSN:      90,
		CheckpointLSN:     50,
		CheckpointTrusted: true,
	})

	rh := sa.GetRetainedHistory()

	if rh.HeadLSN != 100 {
		t.Fatalf("HeadLSN=%d", rh.HeadLSN)
	}
	if rh.TailLSN != 30 {
		t.Fatalf("TailLSN=%d", rh.TailLSN)
	}
	if rh.CommittedLSN != 90 {
		t.Fatalf("CommittedLSN=%d", rh.CommittedLSN)
	}
	if rh.CheckpointLSN != 50 {
		t.Fatalf("CheckpointLSN=%d", rh.CheckpointLSN)
	}
	if !rh.CheckpointTrusted {
		t.Fatal("CheckpointTrusted should be true")
	}
}

func TestStorageAdapter_WALPinRejectsRecycled(t *testing.T) {
	sa := NewStorageAdapter()
	sa.UpdateState(BlockVolState{WALTailLSN: 50})

	_, err := sa.PinWALRetention(30) // 30 < tail 50
	if err == nil {
		t.Fatal("WAL pin should be rejected when range is recycled")
	}
}

func TestStorageAdapter_SnapshotPinRejectsInvalid(t *testing.T) {
	sa := NewStorageAdapter()
	sa.UpdateState(BlockVolState{CheckpointLSN: 50, CheckpointTrusted: false})

	_, err := sa.PinSnapshot(50)
	if err == nil {
		t.Fatal("snapshot pin should be rejected when checkpoint is untrusted")
	}
}

// --- E3: Engine integration through bridge ---

func TestBridge_E2E_AssignmentToRecovery(t *testing.T) {
	// Full bridge flow: master assignment → adapter → engine.
	ca := NewControlAdapter()
	sa := NewStorageAdapter()
	sa.UpdateState(BlockVolState{
		WALHeadLSN:        100,
		WALTailLSN:        30,
		CommittedLSN:      100,
		CheckpointLSN:     50,
		CheckpointTrusted: true,
	})

	// Step 1: master assignment → engine intent.
	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", AddrVersion: 1},
		},
	)

	// Step 2: engine processes intent.
	drv := engine.NewRecoveryDriver(sa)
	drv.Orchestrator.ProcessAssignment(intent)

	// Step 3: plan recovery from real storage state.
	plan, err := drv.PlanRecovery("vol1/vs2", 70)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != engine.OutcomeCatchUp {
		t.Fatalf("outcome=%s", plan.Outcome)
	}
	if !plan.Proof.Recoverable {
		t.Fatalf("proof: %s", plan.Proof.Reason)
	}

	// Step 4: execute through engine executor.
	exec := engine.NewCatchUpExecutor(drv, plan)
	if err := exec.Execute([]uint64{80, 90, 100}, 0); err != nil {
		t.Fatal(err)
	}

	if drv.Orchestrator.Registry.Sender("vol1/vs2").State() != engine.StateInSync {
		t.Fatalf("state=%s", drv.Orchestrator.Registry.Sender("vol1/vs2").State())
	}
}
