package protocol

import "testing"

// --- Assignment ---

func TestAssignment_SetsIdentity(t *testing.T) {
	e := NewEngine()
	r := e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1",
		Epoch:    1,
		Role:     RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vs-2", Endpoint: Endpoint{DataAddr: "10.0.0.2:4260", CtrlAddr: "10.0.0.2:4261"}},
		},
	})

	st, ok := e.Volume("vol-1")
	if !ok {
		t.Fatal("volume not found")
	}
	if st.Epoch != 1 || st.Role != RolePrimary {
		t.Fatalf("epoch=%d role=%s", st.Epoch, st.Role)
	}
	if len(r.Commands) < 2 {
		t.Fatalf("commands=%d, want at least ApplyRole + ConfigureShipper", len(r.Commands))
	}
	if r.Mode != ModeBootstrapPending {
		t.Fatalf("mode=%s", r.Mode)
	}
}

// --- Readiness chain ---

func TestReadiness_BootstrapChain(t *testing.T) {
	e := NewEngine()
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RolePrimary,
		Replicas: []ReplicaAssignment{{ReplicaID: "vs-2", Endpoint: Endpoint{DataAddr: "a", CtrlAddr: "b"}}},
	})

	boolTrue := true

	r := e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", RoleApplied: &boolTrue})
	if r.ModeReason != "awaiting_shipper_configured" {
		t.Fatalf("reason=%q", r.ModeReason)
	}

	r = e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConfigured: &boolTrue})
	if r.ModeReason != "awaiting_shipper_connected" {
		t.Fatalf("reason=%q", r.ModeReason)
	}

	r = e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ShipperConnected: &boolTrue})
	if r.ModeReason != "awaiting_barrier_durability" {
		t.Fatalf("reason=%q", r.ModeReason)
	}

	r = e.ApplyEvent(BarrierConfirmed{VolumeID: "vol-1", DurableLSN: 1})
	if r.Mode != ModePublishHealthy {
		t.Fatalf("mode=%s", r.Mode)
	}
	if !r.Healthy {
		t.Fatal("expected healthy")
	}
}

// --- SyncAck: primary decision ---

func TestSyncAck_ReplicaCaughtUp_KeepUp(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	r := e.ApplyEvent(SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "vs-2",
		Ack:            SyncAck{DurableLSN: 100, AppliedLSN: 100},
		PrimaryWALTail: 50,
		PrimaryWALHead: 100,
	})

	// Replica is caught up → no catch-up/rebuild command.
	for _, cmd := range r.Commands {
		switch cmd.(type) {
		case IssueCatchUpCommand, IssueRebuildCommand:
			t.Fatalf("unexpected command: %T", cmd)
		}
	}

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionKeepUp {
		t.Fatalf("session=%s, want keepup", rv.Session.Kind)
	}
}

func TestSyncAck_ReplicaBehindWithinWAL_CatchUp(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	r := e.ApplyEvent(SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "vs-2",
		Ack:            SyncAck{DurableLSN: 30, AppliedLSN: 50},
		PrimaryWALTail: 20, // replica at 50, tail at 20 → within WAL
		PrimaryWALHead: 100,
	})

	var catchUp *IssueCatchUpCommand
	for _, cmd := range r.Commands {
		if c, ok := cmd.(IssueCatchUpCommand); ok {
			catchUp = &c
		}
	}
	if catchUp == nil {
		t.Fatal("expected IssueCatchUpCommand")
	}
	if catchUp.StartLSN != 50 || catchUp.TargetLSN != 100 {
		t.Fatalf("catchup start=%d target=%d", catchUp.StartLSN, catchUp.TargetLSN)
	}

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionCatchUp {
		t.Fatalf("session=%s, want catchup", rv.Session.Kind)
	}
}

func TestSyncAck_ReplicaBeyondWAL_Rebuild(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	r := e.ApplyEvent(SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "vs-2",
		Ack:            SyncAck{DurableLSN: 5, AppliedLSN: 10},
		PrimaryWALTail: 500, // replica at 10, tail at 500 → gap beyond WAL
		PrimaryWALHead: 1000,
	})

	var rebuild *IssueRebuildCommand
	for _, cmd := range r.Commands {
		if c, ok := cmd.(IssueRebuildCommand); ok {
			rebuild = &c
		}
	}
	if rebuild == nil {
		t.Fatal("expected IssueRebuildCommand")
	}

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionRebuild {
		t.Fatalf("session=%s, want rebuild", rv.Session.Kind)
	}
	if r.Mode != ModeNeedsRebuild {
		t.Fatalf("mode=%s, want needs_rebuild", r.Mode)
	}
}

func TestSyncAck_FreshReplica_WALRetained_CatchUp(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	r := e.ApplyEvent(SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "vs-2",
		Ack:            SyncAck{DurableLSN: 0, AppliedLSN: 0},
		PrimaryWALTail: 1, // WAL retained from beginning
		PrimaryWALHead: 50,
	})

	var catchUp *IssueCatchUpCommand
	for _, cmd := range r.Commands {
		if c, ok := cmd.(IssueCatchUpCommand); ok {
			catchUp = &c
		}
	}
	if catchUp == nil {
		t.Fatal("fresh replica with WAL retained should get catch-up, not rebuild")
	}
}

func TestSyncAck_FreshReplica_WALNotRetained_Rebuild(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	r := e.ApplyEvent(SyncAckReceived{
		VolumeID:       "vol-1",
		ReplicaID:      "vs-2",
		Ack:            SyncAck{DurableLSN: 0, AppliedLSN: 0},
		PrimaryWALTail: 500, // WAL starts at 500, replica at 0
		PrimaryWALHead: 1000,
	})

	var rebuild *IssueRebuildCommand
	for _, cmd := range r.Commands {
		if c, ok := cmd.(IssueRebuildCommand); ok {
			rebuild = &c
		}
	}
	if rebuild == nil {
		t.Fatal("fresh replica with WAL not retained should get rebuild")
	}
}

// --- Session lifecycle ---

func TestSession_ProgressDoesNotReDecide(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	// Issue catch-up.
	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	// Progress during catch-up.
	e.ApplyEvent(SessionProgress{VolumeID: "vol-1", ReplicaID: "vs-2", Progress: 75})

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionCatchUp {
		t.Fatalf("session=%s, should stay catchup during progress", rv.Session.Kind)
	}
	if rv.Session.State != SessionStateRunning {
		t.Fatalf("state=%s, want running", rv.Session.State)
	}
	if rv.Session.Progress != 75 {
		t.Fatalf("progress=%d", rv.Session.Progress)
	}
}

func TestSession_CompletedReturnsToKeepUp(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	e.ApplyEvent(SessionCompleted{VolumeID: "vol-1", ReplicaID: "vs-2", DurableLSN: 100})

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionKeepUp {
		t.Fatalf("session=%s, want keepup after completion", rv.Session.Kind)
	}
}

func TestSession_FailedDoesNotAutoEscalate(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	r := e.ApplyEvent(SessionFailed{VolumeID: "vol-1", ReplicaID: "vs-2", Reason: "transport_lost"})

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	if rv.Session.Kind != SessionCatchUp {
		t.Fatalf("session kind=%s, should preserve kind on failure", rv.Session.Kind)
	}
	if rv.Session.State != SessionStateFailed {
		t.Fatalf("session state=%s, want failed", rv.Session.State)
	}
	// Key: failure doesn't auto-become NeedsRebuild.
	if r.Mode == ModeNeedsRebuild {
		t.Fatal("failed session must NOT auto-escalate to needs_rebuild")
	}
	if r.Mode != ModeDegraded {
		t.Fatalf("mode=%s, want degraded after session failure", r.Mode)
	}
}

func TestSession_FailedThenSyncAck_ReDecides(t *testing.T) {
	e := NewEngine()
	setupPrimary(e, "vol-1", 1)

	e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 50}, PrimaryWALTail: 20, PrimaryWALHead: 100,
	})

	e.ApplyEvent(SessionFailed{VolumeID: "vol-1", ReplicaID: "vs-2", Reason: "transport_lost"})

	// Next sync ack triggers re-decision. Replica made progress to 80.
	r := e.ApplyEvent(SyncAckReceived{
		VolumeID: "vol-1", ReplicaID: "vs-2",
		Ack: SyncAck{AppliedLSN: 80}, PrimaryWALTail: 20, PrimaryWALHead: 120,
	})

	st, _ := e.Volume("vol-1")
	rv := st.ReplicaStates["vs-2"]
	// Should re-decide based on new facts, not stay in old failed state.
	if rv.Session.State == SessionStateFailed {
		t.Fatal("sync ack after failure should re-decide, not stay failed")
	}
	_ = r
}

// --- Mode derivation ---

func TestMode_NoReplicas_AllocatedOnly(t *testing.T) {
	e := NewEngine()
	r := e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RolePrimary,
	})
	if r.Mode != ModeAllocatedOnly {
		t.Fatalf("mode=%s", r.Mode)
	}
}

func TestMode_ReplicaReady(t *testing.T) {
	e := NewEngine()
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: "vol-1", Epoch: 1, Role: RoleReplica,
	})
	boolTrue := true
	e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", RoleApplied: &boolTrue})
	r := e.ApplyEvent(ReadinessObserved{VolumeID: "vol-1", ReceiverReady: &boolTrue})
	if r.Mode != ModeReplicaReady {
		t.Fatalf("mode=%s", r.Mode)
	}
}

// --- Helpers ---

func setupPrimary(e *Engine, volumeID string, epoch uint64) {
	e.ApplyEvent(AssignmentDelivered{
		VolumeID: volumeID, Epoch: epoch, Role: RolePrimary,
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vs-2", Endpoint: Endpoint{DataAddr: "10.0.0.2:4260", CtrlAddr: "10.0.0.2:4261"}},
		},
	})
	boolTrue := true
	e.ApplyEvent(ReadinessObserved{VolumeID: volumeID, RoleApplied: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: volumeID, ShipperConfigured: &boolTrue})
	e.ApplyEvent(ReadinessObserved{VolumeID: volumeID, ShipperConnected: &boolTrue})
}
