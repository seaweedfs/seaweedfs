package blockvol

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
)

// ============================================================
// Phase 07 P0/P1: Bridge adapter tests
// ============================================================

// --- E1: Stable identity ---

func TestControlAdapter_StableIdentity(t *testing.T) {
	ca := NewControlAdapter()

	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "pvc-data-1", Epoch: 3, Role: "primary", PrimaryServerID: "vs1"},
		[]MasterAssignment{
			{VolumeName: "pvc-data-1", Epoch: 3, Role: "replica", ReplicaServerID: "vs2",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", AddrVersion: 1},
		},
	)

	r := intent.Replicas[0]
	if r.ReplicaID != "pvc-data-1/vs2" {
		t.Fatalf("ReplicaID=%s (must be volume/server)", r.ReplicaID)
	}
	if intent.RecoveryTargets["pvc-data-1/vs2"] != engine.SessionCatchUp {
		t.Fatalf("recovery=%s", intent.RecoveryTargets["pvc-data-1/vs2"])
	}
}

func TestControlAdapter_AddressChangePreservesIdentity(t *testing.T) {
	ca := NewControlAdapter()

	intent1 := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica", DataAddr: "10.0.0.2:9333", AddrVersion: 1}},
	)
	intent2 := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica", DataAddr: "10.0.0.3:9333", AddrVersion: 2}},
	)

	if intent1.Replicas[0].ReplicaID != intent2.Replicas[0].ReplicaID {
		t.Fatal("identity changed")
	}
}

func TestControlAdapter_RebuildRoleMapping(t *testing.T) {
	ca := NewControlAdapter()
	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "rebuilding", DataAddr: "10.0.0.2:9333"}},
	)
	if intent.RecoveryTargets["vol1/vs2"] != engine.SessionRebuild {
		t.Fatalf("got %s", intent.RecoveryTargets["vol1/vs2"])
	}
}

func TestControlAdapter_PrimaryNoRecovery(t *testing.T) {
	ca := NewControlAdapter()
	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]MasterAssignment{},
	)
	if len(intent.RecoveryTargets) != 0 {
		t.Fatal("primary should not have recovery targets")
	}
}

// --- E2: Storage adapter via contract interfaces ---

func TestStorageAdapter_RetainedHistoryFromReader(t *testing.T) {
	psa := NewPushStorageAdapter()
	psa.UpdateState(BlockVolState{
		WALHeadLSN: 100, WALTailLSN: 30, CommittedLSN: 90,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})

	rh := psa.GetRetainedHistory()
	if rh.HeadLSN != 100 || rh.TailLSN != 30 || rh.CommittedLSN != 90 {
		t.Fatalf("head=%d tail=%d committed=%d", rh.HeadLSN, rh.TailLSN, rh.CommittedLSN)
	}
	if rh.CheckpointLSN != 50 || !rh.CheckpointTrusted {
		t.Fatalf("checkpoint=%d trusted=%v", rh.CheckpointLSN, rh.CheckpointTrusted)
	}
}

func TestStorageAdapter_WALPinRejectsRecycled(t *testing.T) {
	psa := NewPushStorageAdapter()
	psa.UpdateState(BlockVolState{WALTailLSN: 50})

	_, err := psa.PinWALRetention(30)
	if err == nil {
		t.Fatal("should reject recycled range")
	}
}

func TestStorageAdapter_SnapshotPinRejectsUntrusted(t *testing.T) {
	psa := NewPushStorageAdapter()
	psa.UpdateState(BlockVolState{CheckpointLSN: 50, CheckpointTrusted: false})

	_, err := psa.PinSnapshot(50)
	if err == nil {
		t.Fatal("should reject untrusted checkpoint")
	}
}

func TestStorageAdapter_PinReleaseSymmetry(t *testing.T) {
	psa := NewPushStorageAdapter()
	psa.UpdateState(BlockVolState{WALTailLSN: 0, CheckpointLSN: 50, CheckpointTrusted: true})

	walPin, _ := psa.PinWALRetention(10)
	snapPin, _ := psa.PinSnapshot(50)
	basePin, _ := psa.PinFullBase(100)

	// Pins tracked.
	if len(psa.releaseFuncs) != 3 {
		t.Fatalf("pins=%d", len(psa.releaseFuncs))
	}

	// Release all.
	psa.ReleaseWALRetention(walPin)
	psa.ReleaseSnapshot(snapPin)
	psa.ReleaseFullBase(basePin)

	if len(psa.releaseFuncs) != 0 {
		t.Fatalf("leaked pins=%d", len(psa.releaseFuncs))
	}
}

// --- E3: End-to-end bridge flow ---

func TestBridge_E2E_AssignmentToRecovery(t *testing.T) {
	ca := NewControlAdapter()
	psa := NewPushStorageAdapter()
	psa.UpdateState(BlockVolState{
		WALHeadLSN: 100, WALTailLSN: 30, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	})

	intent := ca.ToAssignmentIntent(
		MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary", PrimaryServerID: "vs1"},
		[]MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", AddrVersion: 1},
		},
	)

	drv := engine.NewRecoveryDriver(psa)
	drv.Orchestrator.ProcessAssignment(intent)

	plan, err := drv.PlanRecovery("vol1/vs2", 70)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Outcome != engine.OutcomeCatchUp {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	exec := engine.NewCatchUpExecutor(drv, plan)
	if err := exec.Execute([]uint64{80, 90, 100}, 0); err != nil {
		t.Fatal(err)
	}

	if drv.Orchestrator.Registry.Sender("vol1/vs2").State() != engine.StateInSync {
		t.Fatalf("state=%s", drv.Orchestrator.Registry.Sender("vol1/vs2").State())
	}
}

// --- E5: Contract interface boundary ---

func TestContract_BlockVolReaderInterface(t *testing.T) {
	// Verify the contract interface is implementable.
	var _ BlockVolReader = &pushReader{psa: NewPushStorageAdapter()}
	var _ BlockVolPinner = &pushPinner{psa: NewPushStorageAdapter()}
}
