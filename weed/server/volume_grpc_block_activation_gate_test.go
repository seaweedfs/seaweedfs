package weed_server

import (
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
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

	// Pre-inject degraded projection so that after assignment processing
	// the gate is evaluated.
	bs.coreProjMu.Lock()
	bs.coreProj[path] = engine.PublicationProjection{
		VolumeID: path,
		Mode:     engine.ModeView{Name: engine.ModeDegraded, Reason: "incomplete_reconstruction"},
	}
	bs.coreProjMu.Unlock()

	// Process primary assignment through the core path.
	bs.applyCoreAssignmentEvent(blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      5,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	})

	// The gate should have been evaluated after assignment.
	gated, reason := bs.IsActivationGated(path)
	if !gated {
		t.Fatal("expected activation gated after primary assignment with degraded projection")
	}
	if reason == "" {
		t.Fatal("expected non-empty gate reason")
	}
}
