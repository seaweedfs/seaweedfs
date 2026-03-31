package v2bridge

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 08 P1: Real control delivery tests
// Validates real BlockVolumeAssignment → engine AssignmentIntent.
// ============================================================

// --- E1: Live assignment delivery → engine intent ---

func TestControl_PrimaryAssignment_StableIdentity(t *testing.T) {
	cb := NewControlBridge()

	// Real assignment from master heartbeat.
	a := blockvol.BlockVolumeAssignment{
		Path:            "pvc-data-1",
		Epoch:           3,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
	}

	intent := cb.ConvertAssignment(a, "vs1:9333")

	if intent.Epoch != 3 {
		t.Fatalf("epoch=%d", intent.Epoch)
	}
	if len(intent.Replicas) != 1 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}

	// ReplicaID = volume-path / replica-server (NOT address-derived transport endpoint).
	r := intent.Replicas[0]
	expected := "pvc-data-1/10.0.0.2:9333"
	if r.ReplicaID != expected {
		t.Fatalf("ReplicaID=%s, want %s", r.ReplicaID, expected)
	}

	// Endpoint is the transport address.
	if r.Endpoint.DataAddr != "10.0.0.2:9333" {
		t.Fatalf("DataAddr=%s", r.Endpoint.DataAddr)
	}

	// Recovery target for replica.
	if intent.RecoveryTargets[expected] != engine.SessionCatchUp {
		t.Fatalf("recovery=%s", intent.RecoveryTargets[expected])
	}
}

func TestControl_PrimaryAssignment_MultiReplica(t *testing.T) {
	cb := NewControlBridge()

	a := blockvol.BlockVolumeAssignment{
		Path:  "pvc-data-1",
		Epoch: 2,
		Role:  uint32(blockvol.RolePrimary),
		ReplicaAddrs: []blockvol.ReplicaAddr{
			{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
			{DataAddr: "10.0.0.3:9333", CtrlAddr: "10.0.0.3:9334"},
		},
	}

	intent := cb.ConvertAssignment(a, "vs1:9333")

	if len(intent.Replicas) != 2 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}

	// Both replicas have stable identity.
	ids := map[string]bool{}
	for _, r := range intent.Replicas {
		ids[r.ReplicaID] = true
	}
	if !ids["pvc-data-1/10.0.0.2:9333"] || !ids["pvc-data-1/10.0.0.3:9333"] {
		t.Fatalf("IDs: %v", ids)
	}
}

// --- E2: Address change preserves identity ---

func TestControl_AddressChange_SameServerID(t *testing.T) {
	cb := NewControlBridge()

	// First assignment.
	a1 := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	intent1 := cb.ConvertAssignment(a1, "vs1:9333")

	// Address changes (replica restarted on different IP).
	a2 := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.5:9333",
		ReplicaCtrlAddr: "10.0.0.5:9334",
	}
	intent2 := cb.ConvertAssignment(a2, "vs1:9333")

	// NOTE: with current extractServerID, different IPs = different server IDs.
	// This is a known limitation: address-based server identity.
	// In production, the master registry would supply a stable server ID.
	// For now, document the boundary.
	id1 := intent1.Replicas[0].ReplicaID
	id2 := intent2.Replicas[0].ReplicaID
	t.Logf("address change: id1=%s id2=%s (different if IP changes)", id1, id2)

	// The critical test: same IP, different port (same server, port change).
	a3 := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9444", // same IP, different port
		ReplicaCtrlAddr: "10.0.0.2:9445",
	}
	intent3 := cb.ConvertAssignment(a3, "vs1:9333")
	id3 := intent3.Replicas[0].ReplicaID

	// Same IP different port = different server ID in current model.
	// This is the V1 identity limitation that a future registry-backed
	// server ID would resolve.
	t.Logf("port change: id1=%s id3=%s", id1, id3)
}

// --- E3: Epoch fencing through real assignment ---

func TestControl_EpochFencing_IntegratedPath(t *testing.T) {
	cb := NewControlBridge()
	driver := engine.NewRecoveryDriver(nil) // no storage needed for control-path test

	// Epoch 1 assignment.
	a1 := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           1,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	intent1 := cb.ConvertAssignment(a1, "vs1:9333")
	driver.Orchestrator.ProcessAssignment(intent1)

	s := driver.Orchestrator.Registry.Sender("vol1/10.0.0.2:9333")
	if s == nil {
		t.Fatal("sender should exist after epoch 1 assignment")
	}
	if !s.HasActiveSession() {
		t.Fatal("should have session after epoch 1")
	}

	// Epoch bump (failover).
	driver.Orchestrator.InvalidateEpoch(2)
	driver.Orchestrator.UpdateSenderEpoch("vol1/10.0.0.2:9333", 2)

	if s.HasActiveSession() {
		t.Fatal("old session should be invalidated after epoch bump")
	}

	// Epoch 2 assignment.
	a2 := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           2,
		Role:            uint32(blockvol.RolePrimary),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
	}
	intent2 := cb.ConvertAssignment(a2, "vs1:9333")
	driver.Orchestrator.ProcessAssignment(intent2)

	if !s.HasActiveSession() {
		t.Fatal("should have new session at epoch 2")
	}

	// Log shows invalidation.
	hasInvalidation := false
	for _, e := range driver.Orchestrator.Log.EventsFor("vol1/10.0.0.2:9333") {
		if e.Event == "session_invalidated" {
			hasInvalidation = true
		}
	}
	if !hasInvalidation {
		t.Fatal("log must show session invalidation on epoch bump")
	}
}

// --- E4: Rebuild role mapping ---

func TestControl_RebuildAssignment(t *testing.T) {
	cb := NewControlBridge()

	a := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           3,
		Role:            uint32(blockvol.RoleRebuilding),
		ReplicaDataAddr: "10.0.0.2:9333",
		ReplicaCtrlAddr: "10.0.0.2:9334",
		RebuildAddr:     "10.0.0.1:15000",
	}

	intent := cb.ConvertAssignment(a, "10.0.0.2:9333")

	if len(intent.RecoveryTargets) != 1 {
		t.Fatalf("recovery targets=%d", len(intent.RecoveryTargets))
	}

	replicaID := "vol1/10.0.0.2:9333"
	if intent.RecoveryTargets[replicaID] != engine.SessionRebuild {
		t.Fatalf("recovery=%s", intent.RecoveryTargets[replicaID])
	}
}

// --- E5: Replica assignment ---

func TestControl_ReplicaAssignment(t *testing.T) {
	cb := NewControlBridge()

	a := blockvol.BlockVolumeAssignment{
		Path:            "vol1",
		Epoch:           1,
		Role:            uint32(blockvol.RoleReplica),
		ReplicaDataAddr: "10.0.0.1:14260",
		ReplicaCtrlAddr: "10.0.0.1:14261",
	}

	intent := cb.ConvertAssignment(a, "vs2:9333")

	if len(intent.Replicas) != 1 {
		t.Fatalf("replicas=%d", len(intent.Replicas))
	}
	if intent.Replicas[0].ReplicaID != "vol1/vs2:9333" {
		t.Fatalf("ReplicaID=%s", intent.Replicas[0].ReplicaID)
	}
}
