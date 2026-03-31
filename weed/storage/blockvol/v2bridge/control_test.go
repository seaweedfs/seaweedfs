package v2bridge

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 08 P1: Real control delivery tests
// Identity: ReplicaID = <path>/<ServerID> — NOT address-derived.
// ============================================================

func TestControl_PrimaryAssignment_StableServerID(t *testing.T) {
	cb := NewControlBridge()

	a := blockvol.BlockVolumeAssignment{
		Path:            "pvc-data-1",
		Epoch:           3,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaServerID: "vs2",
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
	}

	intent := cb.ConvertAssignment(a, "vs1")

	if len(intent.Replicas) != 1 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}

	r := intent.Replicas[0]
	// ReplicaID uses ServerID, not address.
	if r.ReplicaID != "pvc-data-1/vs2" {
		t.Fatalf("ReplicaID=%s, want pvc-data-1/vs2", r.ReplicaID)
	}
	if r.Endpoint.DataAddr != "10.0.0.2:9333" {
		t.Fatalf("DataAddr=%s", r.Endpoint.DataAddr)
	}
	if intent.RecoveryTargets["pvc-data-1/vs2"] != engine.SessionCatchUp {
		t.Fatalf("recovery=%s", intent.RecoveryTargets["pvc-data-1/vs2"])
	}
}

func TestControl_AddressChange_IdentityPreserved(t *testing.T) {
	cb := NewControlBridge()

	// Same ServerID, different address.
	a1 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RolePrimary),
		ReplicaServerID: "vs2",
		ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	a2 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RolePrimary),
		ReplicaServerID: "vs2",
		ReplicaDataAddr: "10.0.0.5:9333", ReplicaCtrlAddr: "10.0.0.5:9334",
	}

	intent1 := cb.ConvertAssignment(a1, "vs1")
	intent2 := cb.ConvertAssignment(a2, "vs1")

	if intent1.Replicas[0].ReplicaID != intent2.Replicas[0].ReplicaID {
		t.Fatalf("identity changed: %s → %s",
			intent1.Replicas[0].ReplicaID, intent2.Replicas[0].ReplicaID)
	}
	if intent2.Replicas[0].Endpoint.DataAddr != "10.0.0.5:9333" {
		t.Fatal("endpoint should be updated")
	}
}

func TestControl_MultiReplica_StableServerIDs(t *testing.T) {
	cb := NewControlBridge()

	a := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 2, Role: uint32(blockvol.RolePrimary),
		ReplicaAddrs: []blockvol.ReplicaAddr{
			{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", ServerID: "vs2"},
			{DataAddr: "10.0.0.3:9333", CtrlAddr: "10.0.0.3:9334", ServerID: "vs3"},
		},
	}

	intent := cb.ConvertAssignment(a, "vs1")
	if len(intent.Replicas) != 2 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}

	ids := map[string]bool{}
	for _, r := range intent.Replicas {
		ids[r.ReplicaID] = true
	}
	if !ids["vol1/vs2"] || !ids["vol1/vs3"] {
		t.Fatalf("IDs: %v (should use ServerID, not address)", ids)
	}
}

func TestControl_MissingServerID_FailsClosed(t *testing.T) {
	cb := NewControlBridge()

	// Scalar: no ServerID → no replica created.
	a1 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
		// ReplicaServerID intentionally empty.
	}
	intent1 := cb.ConvertAssignment(a1, "vs1")
	if len(intent1.Replicas) != 0 {
		t.Fatalf("scalar without ServerID should produce 0 replicas, got %d", len(intent1.Replicas))
	}

	// Multi: one with ServerID, one without → only one replica.
	a2 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RolePrimary),
		ReplicaAddrs: []blockvol.ReplicaAddr{
			{DataAddr: "10.0.0.2:9333", ServerID: "vs2"},
			{DataAddr: "10.0.0.3:9333", ServerID: ""}, // empty → skipped
		},
	}
	intent2 := cb.ConvertAssignment(a2, "vs1")
	if len(intent2.Replicas) != 1 {
		t.Fatalf("multi with 1 missing ServerID: replicas=%d, want 1", len(intent2.Replicas))
	}
}

func TestControl_EpochFencing_IntegratedPath(t *testing.T) {
	cb := NewControlBridge()
	driver := engine.NewRecoveryDriver(nil)

	a1 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RolePrimary),
		ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	driver.Orchestrator.ProcessAssignment(cb.ConvertAssignment(a1, "vs1"))

	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s == nil || !s.HasActiveSession() {
		t.Fatal("should have session at epoch 1")
	}

	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("vol1/vs2", 2)

	if s.HasActiveSession() {
		t.Fatal("old session should be invalidated")
	}

	a2 := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 2, Role: uint32(blockvol.RolePrimary),
		ReplicaServerID: "vs2", ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	driver.Orchestrator.ProcessAssignment(cb.ConvertAssignment(a2, "vs1"))

	if !s.HasActiveSession() {
		t.Fatal("should have new session at epoch 2")
	}

	hasInvalidation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/vs2") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("log must show invalidation")
	}
}

func TestControl_RebuildAssignment(t *testing.T) {
	cb := NewControlBridge()
	a := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 3, Role: uint32(blockvol.RoleRebuilding),
		ReplicaDataAddr: "10.0.0.2:9333", ReplicaCtrlAddr: "10.0.0.2:9334",
		RebuildAddr: "10.0.0.1:15000",
	}
	intent := cb.ConvertAssignment(a, "vs2")
	if intent.RecoveryTargets["vol1/vs2"] != engine.SessionRebuild {
		t.Fatalf("recovery=%s", intent.RecoveryTargets["vol1/vs2"])
	}
}

func TestControl_ReplicaAssignment(t *testing.T) {
	cb := NewControlBridge()
	a := blockvol.BlockVolumeAssignment{
		Path: "vol1", Epoch: 1, Role: uint32(blockvol.RoleReplica),
		ReplicaDataAddr: "10.0.0.1:14260", ReplicaCtrlAddr: "10.0.0.1:14261",
	}
	intent := cb.ConvertAssignment(a, "vs2")
	if intent.Replicas[0].ReplicaID != "vol1/vs2" {
		t.Fatalf("ReplicaID=%s", intent.Replicas[0].ReplicaID)
	}
}
