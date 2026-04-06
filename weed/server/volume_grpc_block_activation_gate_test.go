package weed_server

import (
	"log"
	"os"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

func TestT4_DegradedProjection_GatesActivation(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-degraded")

	// Inject degraded projection.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeDegraded, Reason: "barrier_timeout"},
	}
	bs.coreProjMu.Unlock()

	bs.evaluateActivationGate(path)

	gated, reason := bs.IsActivationGated(path)
	if !gated {
		t.Fatal("expected activation gated for degraded projection")
	}
	if reason == "" {
		t.Fatal("expected non-empty gate reason")
	}
}

func TestT4_NeedsRebuildProjection_GatesActivation(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-rebuild")

	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeNeedsRebuild, Reason: "gap_too_large"},
	}
	bs.coreProjMu.Unlock()

	bs.evaluateActivationGate(path)

	gated, reason := bs.IsActivationGated(path)
	if !gated {
		t.Fatal("expected activation gated for needs_rebuild projection")
	}
	if reason == "" {
		t.Fatal("expected non-empty gate reason")
	}
}

func TestT4_HealthyProjection_ClearsGate(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-healthy")

	// Start gated.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeDegraded, Reason: "barrier_timeout"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, _ := bs.IsActivationGated(path); !gated {
		t.Fatal("expected gated initially")
	}

	// Transition to healthy.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModePublishHealthy},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)

	if gated, _ := bs.IsActivationGated(path); gated {
		t.Fatal("expected gate cleared for publish_healthy projection")
	}
}

func TestT4_GateEnforcedBeforeHeartbeat(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-pre-hb")

	// Inject degraded projection and evaluate gate.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeDegraded, Reason: "test"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)

	// Gate is set BEFORE any heartbeat. Verify the heartbeat carries
	// the gated state.
	msgs := bs.CollectBlockVolumeHeartbeat()
	var found *blockvol.BlockVolumeInfoMessage
	for i := range msgs {
		if msgs[i].Path == path {
			found = &msgs[i]
			break
		}
	}
	if found == nil {
		t.Fatal("heartbeat message not found for gated volume")
	}
	if !found.ActivationGated {
		t.Fatal("heartbeat should report ActivationGated=true")
	}
	if found.ActivationGateReason == "" {
		t.Fatal("heartbeat should report non-empty ActivationGateReason")
	}
}

func TestT4_RecoveryFromGated_ReenablesServing(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-recovery")

	// Start gated (needs_rebuild).
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeNeedsRebuild, Reason: "gap"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, _ := bs.IsActivationGated(path); !gated {
		t.Fatal("expected gated")
	}

	// Simulate recovery: projection transitions to replica_ready.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeReplicaReady},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, _ := bs.IsActivationGated(path); gated {
		t.Fatal("expected gate cleared after recovery to replica_ready")
	}

	// And then to publish_healthy.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModePublishHealthy},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, _ := bs.IsActivationGated(path); gated {
		t.Fatal("expected gate cleared after recovery to publish_healthy")
	}
}

func TestT4_ApplyCoreAssignment_GatesDegradedPrimary(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-assignment")

	// Process primary assignment through the core path. The V2 core will
	// compute the projection (allocated_only for a no-replica assignment).
	bs.applyCoreAssignmentEvent(blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      5,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	})

	// Fresh assignment with no replicas → allocated_only. This must NOT
	// be gated (no stale data risk on fresh bootstrap).
	if gated, reason := bs.IsActivationGated(path); gated {
		t.Fatalf("fresh primary assignment must not be gated, got reason=%q", reason)
	}

	// Now inject degraded projection (simulating barrier failure) and
	// re-evaluate. THIS must gate.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		VolumeID: path,
		Mode:     engine.ModeView{Name: engine.ModeDegraded, Reason: "incomplete_reconstruction"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)

	gated, reason := bs.IsActivationGated(path)
	if !gated {
		t.Fatal("expected activation gated after projection transitions to degraded")
	}
	if reason == "" {
		t.Fatal("expected non-empty gate reason")
	}
}

// bootstrap_pending must NOT be gated — gating it creates the Stage 0B
// chicken-and-egg deadlock (iSCSI removed → no writes → shipper never
// connects → mode never advances).
func TestT4_BootstrapPending_NotGated(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-bootstrap")

	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeBootstrapPending, Reason: "awaiting_shipper_connected"},
	}
	bs.coreProjMu.Unlock()

	bs.evaluateActivationGate(path)

	if gated, reason := bs.IsActivationGated(path); gated {
		t.Fatalf("bootstrap_pending must NOT be gated, got reason=%q", reason)
	}
}

// allocated_only must NOT be gated — fresh volume with no replicas yet.
func TestT4_AllocatedOnly_NotGated(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-allocated")

	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeAllocatedOnly},
	}
	bs.coreProjMu.Unlock()

	bs.evaluateActivationGate(path)

	if gated, reason := bs.IsActivationGated(path); gated {
		t.Fatalf("allocated_only must NOT be gated, got reason=%q", reason)
	}
}

// Transition from gated (degraded) to bootstrap_pending must clear the gate.
func TestT4_DegradedToBootstrapPending_ClearsGate(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-degrade-bootstrap")

	// Start gated.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeDegraded, Reason: "barrier_timeout"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, _ := bs.IsActivationGated(path); !gated {
		t.Fatal("expected gated for degraded")
	}

	// Transition to bootstrap_pending (recovery started).
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeBootstrapPending, Reason: "recovery_in_progress"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)
	if gated, reason := bs.IsActivationGated(path); gated {
		t.Fatalf("bootstrap_pending should clear gate, got reason=%q", reason)
	}
}

// P20-T4-C3: Missing projection with active V2 core fails closed.
func TestT4_MissingProjection_FailsClosed(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-missing-proj")

	// v2Core is non-nil (production config), but no projection cached.
	// This must fail closed, not silently leave serving enabled.
	if bs.v2Core == nil {
		t.Fatal("test requires v2Core != nil")
	}

	bs.evaluateActivationGate(path)

	gated, reason := bs.IsActivationGated(path)
	if !gated {
		t.Fatal("expected activation gated when V2 core is active but projection missing")
	}
	if reason != "missing_engine_projection" {
		t.Fatalf("reason=%q, want %q", reason, "missing_engine_projection")
	}
}

// P20-T4-C6: Gate actually removes iSCSI target (enforcement, not bookkeeping).
func TestT4_GateRemovesISCSITarget(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "gate-iscsi-remove")

	// Create a real TargetServer (no listen, just registry).
	logger := log.New(os.Stderr, "iscsi-test: ", log.LstdFlags)
	ts := iscsi.NewTargetServer("127.0.0.1:0", iscsi.DefaultTargetConfig(), logger)
	bs.targetServer = ts

	// Register volume with target.
	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	name := volumeNameFromPath(path)
	iqn := bs.iqnPrefix + blockvol.SanitizeIQN(name)
	adapter := blockvol.NewBlockVolAdapter(vol)
	ts.AddVolume(iqn, adapter)

	if !ts.HasTarget(iqn) {
		t.Fatal("target should exist before gate")
	}

	// Inject degraded projection and gate.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModeDegraded, Reason: "test"},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)

	if ts.HasTarget(iqn) {
		t.Fatal("target should be removed after gate (enforcement, not just bookkeeping)")
	}

	// Inject healthy projection and ungate.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		Mode: engine.ModeView{Name: engine.ModePublishHealthy},
	}
	bs.coreProjMu.Unlock()
	bs.evaluateActivationGate(path)

	if !ts.HasTarget(iqn) {
		t.Fatal("target should be restored after ungate")
	}
}
