package replication

import (
	"fmt"
	"testing"
)

// ============================================================
// Phase 05 Slice 4: Integration closure
//
// Tests validate V2-boundary cases through the real engine entry
// path (assignment intent → recovery → completion/escalation),
// with observability verification.
// ============================================================

// --- V2 Boundary 1: Changed-address recovery through assignment ---

func TestIntegration_ChangedAddress_FullFlow(t *testing.T) {
	log := NewRecoveryLog()
	r := NewRegistry()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 30, CommittedLSN: 100,
	}

	// Initial assignment.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-replica1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", CtrlAddr: "10.0.0.1:9334", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-replica1": SessionCatchUp},
	})

	s := r.Sender("vol1-replica1")
	id := s.SessionID()
	s.BeginConnect(id)
	log.Record("vol1-replica1", id, "connect", "initial")

	outcome, proof, _ := s.RecordHandshakeFromHistory(id, 80, &primary)
	log.Record("vol1-replica1", id, "handshake", fmt.Sprintf("outcome=%s proof=%s", outcome, proof.Reason))

	s.BeginCatchUp(id)
	s.RecordCatchUpProgress(id, 100)
	s.CompleteSessionByID(id)
	log.Record("vol1-replica1", id, "completed", "in_sync")

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}

	// Replica restarts on new address — assignment with updated endpoint.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "vol1-replica1", Endpoint: Endpoint{DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334", Version: 2}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"vol1-replica1": SessionCatchUp},
	})

	// Sender identity preserved.
	if r.Sender("vol1-replica1") != s {
		t.Fatal("sender identity must survive address change")
	}

	// New session on new endpoint.
	id2 := s.SessionID()
	if id2 == id {
		t.Fatal("should have new session ID")
	}
	if s.Endpoint().DataAddr != "10.0.0.2:9333" {
		t.Fatalf("endpoint not updated: %s", s.Endpoint().DataAddr)
	}

	s.BeginConnect(id2)
	log.Record("vol1-replica1", id2, "connect", "after address change")

	o2, _, _ := s.RecordHandshakeFromHistory(id2, 100, &primary)
	if o2 != OutcomeZeroGap {
		t.Fatalf("o2=%s", o2)
	}
	s.CompleteSessionByID(id2)
	log.Record("vol1-replica1", id2, "completed", "zero_gap after address change")

	// Observability.
	events := log.EventsFor("vol1-replica1")
	if len(events) < 4 {
		t.Fatalf("expected at least 4 events, got %d", len(events))
	}
	t.Logf("changed-address: %d recovery events logged", len(events))
}

// --- V2 Boundary 2: NeedsRebuild → rebuild through assignment ---

func TestIntegration_NeedsRebuild_ThenRebuildAssignment(t *testing.T) {
	r := NewRegistry()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 60, CommittedLSN: 100,
		CheckpointLSN: 40, CheckpointTrusted: true,
	}

	// Initial catch-up attempt fails — gap beyond retention.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	s := r.Sender("r1")
	id := s.SessionID()
	s.BeginConnect(id)
	o, _, _ := s.RecordHandshakeFromHistory(id, 30, &primary)
	if o != OutcomeNeedsRebuild {
		t.Fatalf("should need rebuild: %s", o)
	}

	// Registry status shows NeedsRebuild.
	status := r.Status()
	if status.Rebuilding != 1 {
		t.Fatalf("rebuilding=%d", status.Rebuilding)
	}

	// Rebuild assignment from coordinator.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "10.0.0.1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionRebuild},
	})

	id2 := s.SessionID()
	s.BeginConnect(id2)
	s.RecordHandshake(id2, 0, 100)

	// History-driven rebuild source: checkpoint at 40 but tail at 60 →
	// CheckpointLSN (40) < TailLSN (60) → unreplayable → full base.
	s.SelectRebuildFromHistory(id2, &primary)
	s.BeginRebuildTransfer(id2)
	s.RecordRebuildTransferProgress(id2, 100)
	s.CompleteRebuild(id2)

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}

	status = r.Status()
	if status.InSync != 1 {
		t.Fatalf("in_sync=%d", status.InSync)
	}
}

// --- V2 Boundary 3: Epoch bump during recovery → new assignment ---

func TestIntegration_EpochBump_DuringRecovery(t *testing.T) {
	r := NewRegistry()
	primary := RetainedHistory{HeadLSN: 100, TailLSN: 0, CommittedLSN: 100}

	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           1,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	s := r.Sender("r1")
	id := s.SessionID()
	s.BeginConnect(id)

	// Epoch bumps (failover) mid-recovery.
	r.InvalidateEpoch(2)
	s.UpdateEpoch(2)

	// Old session dead.
	if err := s.RecordHandshake(id, 0, 100); err == nil {
		t.Fatal("old session should be rejected after epoch bump")
	}

	// New assignment at epoch 2.
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
		},
		Epoch:           2,
		RecoveryTargets: map[string]SessionKind{"r1": SessionCatchUp},
	})

	id2 := s.SessionID()
	s.BeginConnect(id2)
	o, _, _ := s.RecordHandshakeFromHistory(id2, 100, &primary)
	if o != OutcomeZeroGap {
		t.Fatalf("epoch 2: %s", o)
	}
	s.CompleteSessionByID(id2)

	if s.State() != StateInSync {
		t.Fatalf("state=%s", s.State())
	}
}

// --- V2 Boundary 4: Multi-replica mixed outcomes ---

func TestIntegration_MultiReplica_MixedOutcomes(t *testing.T) {
	r := NewRegistry()
	primary := RetainedHistory{
		HeadLSN: 100, TailLSN: 40, CommittedLSN: 100,
		CheckpointLSN: 50, CheckpointTrusted: true,
	}

	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "r2:9333", Version: 1}},
			{ReplicaID: "r3", Endpoint: Endpoint{DataAddr: "r3:9333", Version: 1}},
		},
		Epoch: 1,
		RecoveryTargets: map[string]SessionKind{
			"r1": SessionCatchUp,
			"r2": SessionCatchUp,
			"r3": SessionCatchUp,
		},
	})

	// r1: zero-gap.
	r1 := r.Sender("r1")
	id1 := r1.SessionID()
	r1.BeginConnect(id1)
	o1, _, _ := r1.RecordHandshakeFromHistory(id1, 100, &primary)
	if o1 != OutcomeZeroGap {
		t.Fatalf("r1: %s", o1)
	}
	r1.CompleteSessionByID(id1)

	// r2: catch-up.
	r2 := r.Sender("r2")
	id2 := r2.SessionID()
	r2.BeginConnect(id2)
	o2, p2, _ := r2.RecordHandshakeFromHistory(id2, 60, &primary)
	if o2 != OutcomeCatchUp || !p2.Recoverable {
		t.Fatalf("r2: outcome=%s proof=%v", o2, p2)
	}
	r2.BeginCatchUp(id2)
	r2.RecordCatchUpProgress(id2, 100)
	r2.CompleteSessionByID(id2)

	// r3: needs rebuild.
	r3 := r.Sender("r3")
	id3 := r3.SessionID()
	r3.BeginConnect(id3)
	o3, p3, _ := r3.RecordHandshakeFromHistory(id3, 20, &primary)
	if o3 != OutcomeNeedsRebuild || p3.Recoverable {
		t.Fatalf("r3: outcome=%s proof=%v", o3, p3)
	}

	// Registry status.
	status := r.Status()
	if status.InSync != 2 {
		t.Fatalf("in_sync=%d", status.InSync)
	}
	if status.Rebuilding != 1 {
		t.Fatalf("rebuilding=%d", status.Rebuilding)
	}
	if status.TotalCount != 3 {
		t.Fatalf("total=%d", status.TotalCount)
	}
}

// --- Observability ---

func TestIntegration_RegistryStatus_Snapshot(t *testing.T) {
	r := NewRegistry()
	r.ApplyAssignment(AssignmentIntent{
		Replicas: []ReplicaAssignment{
			{ReplicaID: "r1", Endpoint: Endpoint{DataAddr: "r1:9333", Version: 1}},
			{ReplicaID: "r2", Endpoint: Endpoint{DataAddr: "r2:9333", Version: 1}},
		},
		Epoch: 1,
	})

	status := r.Status()
	if status.TotalCount != 2 {
		t.Fatalf("total=%d", status.TotalCount)
	}
	if len(status.Senders) != 2 {
		t.Fatalf("senders=%d", len(status.Senders))
	}

	// Both disconnected (no recovery started).
	for _, ss := range status.Senders {
		if ss.State != StateDisconnected {
			t.Fatalf("%s: state=%s", ss.ReplicaID, ss.State)
		}
		if ss.Session != nil {
			t.Fatalf("%s: should have no session", ss.ReplicaID)
		}
	}
}

func TestIntegration_RecoveryLog(t *testing.T) {
	log := NewRecoveryLog()

	log.Record("r1", 1, "connect", "initial")
	log.Record("r1", 1, "handshake", "catch-up")
	log.Record("r2", 2, "connect", "rebuild")
	log.Record("r1", 1, "completed", "in_sync")

	all := log.Events()
	if len(all) != 4 {
		t.Fatalf("events=%d", len(all))
	}

	r1Events := log.EventsFor("r1")
	if len(r1Events) != 3 {
		t.Fatalf("r1 events=%d", len(r1Events))
	}

	r2Events := log.EventsFor("r2")
	if len(r2Events) != 1 {
		t.Fatalf("r2 events=%d", len(r2Events))
	}
}
