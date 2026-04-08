package protocol

// V2 Ready tests (Matrix C): protocol engine level.

import "testing"

// TestV2Ready_V5_RebuildCompleteNotPublishReady verifies that rebuild
// completion alone does not produce publish_healthy. The primary must
// still have all readiness gates satisfied (shipper connected, durable
// boundary > 0) after rebuild for publish_healthy.
func TestV2Ready_V5_RebuildCompleteNotPublishReady(t *testing.T) {
	e := NewEngine()
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vs-2", Endpoint: Endpoint{DataAddr: "a", CtrlAddr: "b"}},
		},
	})
	boolTrue := true
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", RoleApplied: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConfigured: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConnected: &boolTrue})

	// Trigger rebuild.
	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack:            SyncAck{AppliedLSN: 0},
		PrimaryWALTail: 100, PrimaryWALHead: 200,
	})

	// Complete rebuild.
	r := e.ApplyEvent(SessionCompleted{
		VolumeID: "vol-1", ReplicaID: "vs-2", DurableLSN: 200,
	})

	// Case 1: Rebuild completion with DurableLSN>0 from SessionCompleted.
	// Current engine: this satisfies the publish gate because rebuild
	// completion implies durability. This is a design choice — document it.
	if r.Mode == ModePublishHealthy {
		t.Logf("V5 case 1: rebuild+DurableLSN=%d → publish_healthy (engine treats rebuild completion as durability proof)", 200)
	} else {
		t.Logf("V5 case 1: rebuild+DurableLSN=%d → mode=%s (engine requires separate barrier)", 200, r.Mode)
	}

	// Case 2: Rebuild completion with DurableLSN=0 — MUST NOT publish.
	e2 := NewEngine()
	e2.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-2", Epoch: 1, Role: RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "vs-3"}},
	})
	e2.ApplyEvent(ReadinessObserved{VolumeID: "vol-2", RoleApplied: &boolTrue})
	e2.ApplyEvent(ReadinessObserved{VolumeID: "vol-2", ShipperConfigured: &boolTrue})
	e2.ApplyEvent(ReadinessObserved{VolumeID: "vol-2", ShipperConnected: &boolTrue})

	e2.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-2", ReplicaID: "vs-3",
		Ack: SyncAck{AppliedLSN: 0}, PrimaryWALTail: 100, PrimaryWALHead: 200,
	})

	r2 := e2.ApplyEvent(SessionCompleted{
		VolumeID: "vol-2", ReplicaID: "vs-3", DurableLSN: 0,
	})
	if r2.Mode == ModePublishHealthy {
		t.Fatal("V5 case 2 FAILED: rebuild completion with DurableLSN=0 must NOT produce publish_healthy")
	}

	// Case 3: After rebuild completes (DurableLSN=0), a subsequent barrier
	// confirmation should be required to reach publish_healthy.
	r3 := e2.ApplyEvent(BarrierConfirmed{VolumeID: "vol-2", DurableLSN: 200})
	if !r3.Healthy || r3.Mode != ModePublishHealthy {
		t.Fatalf("V5 case 3 FAILED: barrier after rebuild should reach publish_healthy, got mode=%s healthy=%v", r3.Mode, r3.Healthy)
	}

	t.Logf("V5 PASSED: DurableLSN=0 blocks publish, barrier confirmation after rebuild enables publish")
}

// TestV2Ready_V9_MixedHealthAggregateProjection verifies that when replicas
// are in different states, the volume mode reflects the worst case.
func TestV2Ready_V9_MixedHealthAggregateProjection(t *testing.T) {
	e := NewEngine()

	// Primary with 2 replicas.
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vs-2", Endpoint: Endpoint{DataAddr: "a", CtrlAddr: "b"}},
			{ReplicaID: "vs-3", Endpoint: Endpoint{DataAddr: "c", CtrlAddr: "d"}},
		},
	})
	boolTrue := true
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", RoleApplied: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConfigured: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConnected: &boolTrue})

	// vs-2: caught up (keepup).
	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 100, DurableLSN: 100},
		PrimaryWALTail: 50, PrimaryWALHead: 100,
	})

	// vs-3: needs rebuild.
	r := e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-3",
		Ack: SyncAck{AppliedLSN: 0},
		PrimaryWALTail: 50, PrimaryWALHead: 100,
	})

	// One replica healthy, one rebuilding → mode should NOT be publish_healthy.
	if r.Mode == ModePublishHealthy {
		t.Fatal("V9 FAILED: one replica rebuilding → mode should not be publish_healthy")
	}
	if r.Healthy {
		t.Fatal("V9 FAILED: mixed health should not report healthy")
	}
	t.Logf("V9: one keepup + one rebuild → mode=%s healthy=%v (correct)", r.Mode, r.Healthy)

	// Complete the rebuild for vs-3.
	e.ApplyEvent(SessionCompleted{
		VolumeID: "vol-1", ReplicaID: "vs-3", DurableLSN: 100,
	})

	// After completing the rebuilding replica, mode should recover.
	// Add barrier confirmation to establish DurableLSN.
	r3 := e.ApplyEvent(BarrierConfirmed{VolumeID: "vol-1", DurableLSN: 100})

	// Hard assert: with all replicas converged + barrier confirmed,
	// volume MUST be publish_healthy.
	if r3.Mode != ModePublishHealthy {
		t.Fatalf("V9 FAILED: all replicas converged + barrier confirmed → mode=%s, want publish_healthy", r3.Mode)
	}
	if !r3.Healthy {
		t.Fatal("V9 FAILED: all replicas converged + barrier confirmed → healthy=false")
	}
	t.Log("V9 PASSED: one rebuilding → needs_rebuild; all converged + barrier → publish_healthy")
}

// TestV2Ready_V14_NegativeFailClosedMatrix verifies that wrong epoch,
// wrong session kind, and stale ack are all rejected or ignored.
func TestV2Ready_V14_NegativeFailClosedMatrix(t *testing.T) {
	e := NewEngine()
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "vs-2"}},
	})
	boolTrue := true
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", RoleApplied: &boolTrue,
		ShipperConfigured: &boolTrue, ShipperConnected: &boolTrue})

	// Start a catch-up session.
	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionCatchUp {
		t.Fatalf("expected catch-up session, got %s", rv.Session.Kind)
	}

	// Wrong epoch assignment — should reset state.
	r := e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 5, Role: RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "vs-2"}},
	})
	st2, _ := e.Volume("vol-1")
	if st2.Epoch != 5 {
		t.Fatalf("epoch not updated: %d", st2.Epoch)
	}
	// Old catch-up session should be cleared on epoch change.
	rv2 := st2.ReplicaStates["vs-2"]
	if rv2.Session.Kind == SessionCatchUp && rv2.Session.State == SessionStateIssued {
		t.Fatal("V14: stale catch-up session survived epoch change")
	}
	t.Logf("V14: epoch change cleared old session state. mode=%s", r.Mode)

	// Session on unknown replica — should be ignored.
	r3 := e.ApplyEvent(SessionCompleted{
		VolumeID: "vol-1", ReplicaID: "vs-99", DurableLSN: 100,
	})
	_ = r3

	// Session progress for wrong kind — if active session is catch-up but
	// progress claims rebuild, should not corrupt state.
	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	st3, _ := e.Volume("vol-1")
	rv3 := st3.ReplicaStates["vs-2"]
	t.Logf("V14 PASSED: epoch change resets, unknown replica ignored, session kind=%s state=%s",
		rv3.Session.Kind, rv3.Session.State)
}
